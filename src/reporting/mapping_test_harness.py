import concurrent.futures
import pandas as pd
import logging
import mysql.connector


import multiprocessing as mp
import logging
import pandas as pd

from datetime import datetime

from projects.engine.regression.harness.mapping_query_library import aggregator_billdocument_sector_keyword_5in3, \
    aggregator_billdocument_subindustry_keyword_5in3, discovered_alerts_sector_keyword_title_scaled, \
    discovered_alerts_subindustry_keyword_title_scaled, discovered_alerts_extractions

# Configure logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a stream handler to print to stdout
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

# Create a formatter with a timestamp
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(stream_handler)

MAPPING_QUERIES = {
    "agg_xml_sector_kw_5in3": aggregator_billdocument_sector_keyword_5in3,
    "agg_xml_subindustry_kw_5in3": aggregator_billdocument_subindustry_keyword_5in3,
    "da_sector_kw_title_scaled": discovered_alerts_sector_keyword_title_scaled,
    "da_subindustry_kw_title_scaled": discovered_alerts_subindustry_keyword_title_scaled,
    "da_extractions": discovered_alerts_extractions,
    # "da_medium_xml_sector_kw_scaled": "", # LegislativeBillStatusMediumRussell3000QueryBySectorKeywordBillDocumentsWrapper - this depends on sectors identified in aggregator_billdocument_sector_keyword_5in3 so is covered
}


def _get_conn():
    mysql_config = {
        "user": "admin",
        "password": "password",
        "host": "127.0.0.1",
        "database": "katalyst",
        "port": 3357,
    }

    connection = mysql.connector.connect(**mysql_config)

    return connection


# Define run_query as a top-level function
def run_query(template_query, gp_ids):
    template, query_fn = template_query
    try:
        conn = _get_conn()  # Get a new connection for each process
        query_sql = query_fn(gp_ids)
        logger.info(f"Running query: {template}; query: \n {query_sql}")
        result = pd.read_sql(query_sql, conn)
        conn.close()  # Close the connection after query execution
        return template, result
    except Exception as e:
        logger.error(f"Error running query for template '{template}': {e}")
        return template, None


def run_harness_m(gp_ids: list[str], num_processes: int = None):
    query_results = {}

    if not isinstance(MAPPING_QUERIES, dict):
        logger.error(f"MAPPING_QUERIES is not a dictionary: {MAPPING_QUERIES}")
        return

    with mp.Pool(processes=num_processes) as pool:
        # Pass gp_ids as an argument to each process along with the query
        results = pool.starmap(run_query, [(item, gp_ids) for item in MAPPING_QUERIES.items()])

    # Collect the results into the dictionary
    for template, result in results:
        if result is not None:
            query_results[template] = result
            logger.info(f"Query for template '{template}' has completed.")
        else:
            logger.error(f"Query for template '{template}' returned no results.")

    return query_results

def run_harness(gp_ids: list[str]):
    query_results = {}
    conn = _get_conn()

    for template, query_fn in MAPPING_QUERIES.items():
        query_sql = query_fn(gp_ids)
        logger.info(f"Running query: {template}; query: \n {query_sql}")
        query_results[template] = pd.read_sql(query_sql, conn)
        logger.info(f"Query for template '{template}' has completed.")

    return query_results


def generate_sector_summary(query_results):
    # Step 1: Extract all distinct Sectors across all DataFrames
    all_sectors = set()
    for df in query_results.values():
        if 'Sector' in df.columns:
            all_sectors.update(df['Sector'].unique())

    # Convert the sectors to a sorted list
    sorted_sectors = sorted(all_sectors)

    # Step 2: Create the summary DataFrame with keys as rows and sectors as columns
    summary_df = pd.DataFrame(index=query_results.keys(), columns=sorted_sectors)

    # Step 3: Populate the DataFrame with 1s or blanks
    for key, df in query_results.items():
        if 'Sector' in df.columns:
            present_sectors = df['Sector'].unique()
            for sector in present_sectors:
                summary_df.loc[key, sector] = 1

    # Fill any remaining NaN values with empty string
    summary_df = summary_df.fillna("").reset_index()

    return summary_df


def add_to_first_element(dictionary, new_key, new_value):
    items = list(dictionary.items())

    items.insert(0, (new_key, new_value))

    return dict(items)


if __name__ == "__main__":
    gp_ids = [44420, 44420]
    query_results = run_harness_m(gp_ids, 6)

    sector_summary = generate_sector_summary(query_results)

    # Get the current date and time
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

    query_results_summary = add_to_first_element(query_results, "Sector Summary", sector_summary)
    # Define the filename with the current datetime
    filename = f"query_diags_{current_datetime}.xlsx"

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        for template, df in query_results_summary.items():
            # Write data to Excel without the index
            df.to_excel(writer, sheet_name=template, index=False)

            # Access the workbook and worksheet objects
            workbook = writer.book
            worksheet = writer.sheets[template]

            # Auto-size columns, but limit the size to a maximum of 100
            for i, col in enumerate(df.columns):
                # Handle NaN or missing values by replacing them with empty strings
                col_data = df[col].astype(str).fillna('')

                # Calculate the maximum length between the header and the data, avoiding ambiguous Series
                max_len = max(col_data.apply(lambda x: len(str(x))).max(), len(col)) + 2  # Add extra padding
                max_len = min(max_len, 100)  # Limit column width to 100

                # Set the column width
                worksheet.set_column(i, i, max_len)

            # Define the range of the table
            (max_row, max_col) = df.shape

            # Add the table with auto-filter enabled
            worksheet.add_table(0, 0, max_row, max_col - 1, {
                'columns': [{'header': column} for column in df.columns],
                'autofilter': True
            })