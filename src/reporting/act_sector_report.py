import multiprocessing as mp
import os
from collections import defaultdict
from typing import Optional

import pandas as pd
from more_itertools import chunked
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from core.infra.logging_config import setup_logging
from core.infra.logging_config import timelog, get_logger
from reporting.harness_infra import AGGREGATOR_DOCUMENT_XML_SECTOR_KEYWORD_5IN3, \
    AGGREGATOR_DOCUMENT_XML_SUBINDUSTRY_KEYWORD_5IN3, \
    DISCOVERED_ALERTS_GOVERNMENT_PUBLICATIONS_TITLE_SUBINDUSTRY_KEYWORD_SCALED, \
    DISCOVERED_ALERTS_GOVERNMENT_PUBLICATIONS_WATSON_EXTRACTIONS, LoadingsQueryStructure

DB_CONNECTION_STRING = "DB_CONNECTION_STRING"
MYSQL_ENGINE_CONNECTION = os.getenv(DB_CONNECTION_STRING, "admin:password@localhost/katalyst?port=3357")
MYSQL_ENGINE = f'mysql+mysqlconnector://{MYSQL_ENGINE_CONNECTION}'

# Configure logger
logger = get_logger(__name__)

class ContentSearchFilter:
    def __init__(self,
                 search_terms_query: callable,
                 content_search_target_queries: list[callable], ):
        self.search_terms_query = search_terms_query
        self.content_search_target_queries = content_search_target_queries

    def get_search_terms(self, session) -> dict[str, list[int]]:
        search_terms_query_sql = self.search_terms_query()
        result = session.execute(text(search_terms_query_sql)).fetchall()

        active_content_mappings = defaultdict(list)

        for row in result:
            content_id = row[0]
            content = row[1]
            if content and content.strip():  # Check if content is not null or empty
                content = content.lower().strip()
                active_content_mappings[content].append(content_id)

        return active_content_mappings

    def get_content_search_targets(self, session: Session, query_ids: list[int]) -> list[str]:
        content_search_targets = []

        for query_sql in self.content_search_target_queries:
            content_search_target_query = query_sql(query_ids)
            result = session.execute(text(content_search_target_query)).fetchall()

            for row in result:
                content_search_target_string = row[0]
                if content_search_target_string:
                    content_search_targets.append(content_search_target_string.lower())

        return content_search_targets

    def get_matching_content_ids(self, session: Session, query_ids: list[int]) -> list[int]:
        active_content_mappings = self.get_search_terms(session)
        content_search_targets = self.get_content_search_targets(session, query_ids)

        matching_content_ids: set[int] = set()

        for content, content_ids in active_content_mappings.items():
            for target in content_search_targets:
                if target and content in target:
                    matching_content_ids.update(content_ids)

        return list(matching_content_ids)


MAPPING_QUERIES = {
    "agg_xml_sector_kw_5in3": AGGREGATOR_DOCUMENT_XML_SECTOR_KEYWORD_5IN3,
    "agg_xml_subindustry_kw_5in3": AGGREGATOR_DOCUMENT_XML_SUBINDUSTRY_KEYWORD_5IN3,
    # "da_sector_kw_title_scaled": DISCOVERED_ALERTS_GOVERNMENT_PUBLICATIONS_TITLE_SECTOR_KEYWORD_SCALED,
    "da_subindustry_kw_title_scaled": DISCOVERED_ALERTS_GOVERNMENT_PUBLICATIONS_TITLE_SUBINDUSTRY_KEYWORD_SCALED,
    "da_extractions": DISCOVERED_ALERTS_GOVERNMENT_PUBLICATIONS_WATSON_EXTRACTIONS,
    # "da_medium_xml_sector_kw_scaled": "", # LegislativeBillStatusMediumRussell3000QueryBySectorKeywordBillDocumentsWrapper - this depends on sectors identified in aggregator_billdocument_sector_keyword_5in3 so is covered
}


@timelog(logger)
def _run_query(query_sql: str, engine):
    return pd.read_sql(query_sql, engine)


def enrich_query_results(template_query: LoadingsQueryStructure, query_results: pd.DataFrame) -> pd.DataFrame:
    # test if all of bill_type, bill_number, title is in query_results
    if 'bill_type' in query_results.columns and 'bill_number' in query_results.columns and 'title' in query_results.columns:
        query_results['bill'] = query_results['bill_type'] + ' ' + query_results['bill_number'] + ' - ' + query_results[
            'title']

    query_results['impact'] = template_query.impact.value

    return query_results


# Define run_query as a top-level function
def run_loadings_query(template_query: LoadingsQueryStructure, gp_ids: list[int]) -> tuple[
    LoadingsQueryStructure, Optional[pd.DataFrame]]:

    # tuple SQL in clause hack
    if len(gp_ids) == 1:
        gp_ids = gp_ids * 2

    result = pd.DataFrame()
    try:
        engine = create_engine(MYSQL_ENGINE)
        Session = sessionmaker(bind=engine)
        session = Session()

        logger.info(f"Running query for template '{template_query.name}'")
        if template_query.search_terms_query is not None:
            content_search_filter = ContentSearchFilter(template_query.search_terms_query,
                                                        template_query.content_search_target_queries)
            logger.info(f"Getting matching content ids for template '{template_query.name}'")
            matching_content_ids = content_search_filter.get_matching_content_ids(session, gp_ids)
            logger.info(f"Matching content ids for template '{template_query.name}': {len(matching_content_ids)}")
            if matching_content_ids:
                params = {"gp_ids": gp_ids, "matching_content_ids": matching_content_ids}
                query_sql = template_query.loadings_query(**params)
                result = _run_query(query_sql, engine)
        else:
            params = {"gp_ids": gp_ids}
            query_sql = template_query.loadings_query(**params)
            result = _run_query(query_sql, engine)

        result = enrich_query_results(template_query, result)
        logger.info(f"Query for template '{template_query.name}' has completed - size {result.shape}.")
        session.close()
        return template_query, result
    except Exception as e:
        logger.error(f"Error running query for template '{template_query.name}': {e}")
        return template_query, result


def run_harness_pool(gp_ids: list[int], num_processes: int = None) -> dict[LoadingsQueryStructure, pd.DataFrame]:
    max_gpid_parallel = 2
    gp_id_chunks = list(chunked(gp_ids, max_gpid_parallel))

    with mp.Pool(processes=num_processes, initializer=setup_logging) as pool:
        template_query_results = pool.starmap(
            run_loadings_query,
            [(loading_query_structure, gp_id_chunk)
             for _, loading_query_structure in MAPPING_QUERIES.items()
             for gp_id_chunk in gp_id_chunks]
        )

    result_dict = _combine_query_structures(template_query_results)

    return result_dict


def _combine_query_structures(template_query_results):
    result_dict = {}
    for query_structure, df in template_query_results:
        if query_structure in result_dict:
            if not df.empty:
                result_dict[query_structure] = pd.concat([result_dict[query_structure], df])
        else:
            result_dict[query_structure] = df
    return result_dict


def run_harness_m(gp_ids: list[int]) -> dict[LoadingsQueryStructure, pd.DataFrame]:
    if not isinstance(MAPPING_QUERIES, dict):
        logger.error(f"MAPPING_QUERIES is not a dictionary: {MAPPING_QUERIES}")
        return

    template_query_results = [
        run_loadings_query(loading_query_structure, gp_ids)
        for _, loading_query_structure in MAPPING_QUERIES.items()
    ]

    result_dict = _combine_query_structures(template_query_results)

    return result_dict


def create_summary_df(result_dict: dict[LoadingsQueryStructure, pd.DataFrame], value_col: str = None, value_col_count: bool = True) -> pd.DataFrame:
    # Step 1: Extract all distinct sectors across all DataFrames
    all_sectors = {
        sector
        for result in result_dict.values()
        if 'Sector' in result.columns
        for sector in result['Sector'].unique()
    }

    # Convert the sectors to a sorted list
    sorted_sectors = sorted(all_sectors)

    # each row will be a bill / sheet_name combination
    # get unique list of bill and sheet names

    # Step 1: Build the lookup DataFrame for all bills, their types, numbers, and titles
    lookup_data = []
    for loadings_query_structure, result in result_dict.items():
        if {'bill', 'bill_type', 'bill_number', 'title'}.issubset(result.columns):
            for _, row in result[['bill', 'bill_type', 'bill_number', 'title']].drop_duplicates().iterrows():
                lookup_data.append({
                    'bill': row['bill'],
                    'bill_type': row['bill_type'],
                    'bill_number': row['bill_number'],
                    'title': row['title']
                })

    # Create the lookup DataFrame with unique bill entries
    lookup_df = pd.DataFrame(lookup_data).drop_duplicates(subset=['bill'])

    # Step 2: Create the summary DataFrame with bill and sheet names as rows and sectors as columns
    index_tuples_set = {
        (row['bill'], tqr.name)
        for tqr, result in result_dict.items()
        if 'bill' in result.columns
        for _, row in result.iterrows()
    }

    # Convert the set back to a list
    index_tuples = list(index_tuples_set)

    summary_df = pd.DataFrame(index=pd.MultiIndex.from_tuples(index_tuples, names=['bill', 'sheet_name']),
                              columns=sorted_sectors)

    # Step 3: Populate the DataFrame with values where sectors are present
    for loadings_query_structure, result in result_dict.items():
        if 'Sector' in result.columns and 'bill' in result.columns:
            # in subindustry - could have multipel rows for the one sector - so have to maintain state across row iterations
            for _, row in result.iterrows():
                present_sectors = row['Sector']
                if value_col:
                    value_cols = [c for c in result.columns if value_col in c]  # look for substring in column name
                    values = []
                    for c in value_cols:
                        values.append(row[c])

                    # Update existing value or add new one
                    current_value = summary_df.loc[(row['bill'], loadings_query_structure.name), present_sectors]

                    if pd.notna(current_value):  # Check if there is an existing value
                        existing_values = set([v.strip() for v in current_value.split(",")])
                        new_values = set(values)
                        combined_values = existing_values.union(new_values)
                        summary_df.loc[(row['bill'], loadings_query_structure.name), present_sectors] = ", ".join(sorted(combined_values))
                    else:
                        summary_df.loc[(row['bill'], loadings_query_structure.name), present_sectors] = ", ".join(sorted(set(values)))
                else:
                    summary_df.loc[(row['bill'], loadings_query_structure.name), present_sectors] = 1

    # iterate through summary_df - iv value_col_count is True, then count the number of values in each cell - handle nas
    if value_col_count:
        for index, row in summary_df.iterrows():
            for sector in sorted_sectors:
                if pd.notna(row[sector]):
                    summary_df.loc[index, sector] = len(row[sector].split(","))
                else:
                    summary_df.loc[index, sector] = ""


    # Step 4: Fill NaN values with empty strings
    summary_df = summary_df.fillna("")

    # Step 5: Merge with the lookup DataFrame to get bill_type, bill_number, and title
    summary_df = summary_df.reset_index().merge(lookup_df, on='bill', how='left')

    # Step 6: Sort by bill_type and bill_number before removing columns
    summary_df = summary_df.sort_values(by=['bill_type', 'bill_number', 'sheet_name'])

    # Step 7: Drop 'bill_type', 'bill_number', and 'title'
    summary_df = summary_df.drop(columns=['bill_type', 'bill_number', 'title'])

    return summary_df


def add_to_first_element(dictionary, new_key, new_value):
    items = list(dictionary.items())

    items.insert(0, (new_key, new_value))

    return dict(items)

def generate_act_sectors_reports(gp_ids:list[int]) -> dict[str, pd.DataFrame]:
    template_query_results = run_harness_pool(gp_ids, 6)
    sector_summary_df = create_summary_df(template_query_results, value_col='ContentList', value_col_count=True)

    # Dynamically find the sector columns (excluding 'bill' and 'sheet_name')
    sector_columns = [col for col in sector_summary_df.columns if col not in ['bill', 'sheet_name']]

    # Grouping by 'bill' and dynamically aggregating across the sector columns by taking the maximum value
    aggregated_df = sector_summary_df.groupby('bill').agg(
        {col: lambda x: 1 if x.notna().any() and (x != '').any() else '' for col in sector_columns}
    ).reset_index()

    content_list_summary = create_summary_df(template_query_results, value_col='ContentList', value_col_count=False)

    act_sector_reports = {
        "sectors": aggregated_df,
        "sectors_detail": sector_summary_df,
        "content_list": content_list_summary
    }

    for template, df in template_query_results.items():
        act_sector_reports[template.name] = df

    return act_sector_reports



