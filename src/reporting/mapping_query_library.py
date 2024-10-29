
def gics_sector_keyword_mappings_query():
    query = (
        "SELECT gskm1.id, gskm1.content "
        "FROM gics_sector_keyword_mappings gskm1 "
        "WHERE gskm1.active_flag = true"
    )
    return query


def gics_subindustry_keyword_mappings_query():
    query = (
        "SELECT gskm1.id, gskm1.content "
        "FROM gics_subindustry_keyword_mappings gskm1 "
        "WHERE gskm1.active_flag = true"
    )
    return query


def theme_type_entries_label_query():
    query = (
        "SELECT tte1.id, tte1.label "
        "FROM theme_type_entries tte1 "
        "WHERE tte1.active_flag = true"
    )
    return query


def policy_area_entries_label_query():
    query = (
        "SELECT pae1.id, pae1.label "
        "FROM policy_area_entries pae1 "
        "WHERE pae1.active_flag = true"
    )
    return query


def government_publications_title_query(gp_ids):
    query = (
        f"SELECT gp1.title "
        f"FROM government_publications gp1 "
        f"WHERE gp1.id IN {tuple(gp_ids)}"
    )
    return query


def government_publications_summary_text_query(gp_ids):
    query = (
        f"SELECT gp1.summary_text "
        f"FROM government_publications gp1 "
        f"WHERE gp1.id IN {tuple(gp_ids)}"
    )
    return query


def gov_publication_documents_content_query(gp_ids):
    query = (
        f"SELECT gpd1.content "
        f"FROM gov_publication_documents gpd1 "
        f"WHERE gpd1.government_publications_id IN {tuple(gp_ids)}"
    )
    return query


def gov_publication_titles_title_content_query(gp_ids):
    query = (
        f"SELECT gpt1.title_content "
        f"FROM gov_publication_titles gpt1 "
        f"WHERE gpt1.government_publications_id IN {tuple(gp_ids)}"
    )
    return query


def committee_repository_documents_title_query(gp_ids):
    query = (
        f"SELECT crd1.title "
        f"FROM committee_repository_documents crd1 "
        f"WHERE crd1.id IN {tuple(gp_ids)}"
    )
    return query


def committee_repository_documents_description_query(gp_ids):
    query = (
        f"SELECT IF(INSTR(crd1.description, 'Meeting Date') > 0, "
        f"SUBSTRING(crd1.description, 1, INSTR(crd1.description, 'Meeting Date') - 1), "
        f"crd1.description) "
        f"FROM committee_repository_documents crd1 "
        f"WHERE crd1.id IN {tuple(gp_ids)}"
    )
    return query


def federal_register_documents_title_query(gp_ids):
    query = (
        f"SELECT frd1.title "
        f"FROM federal_register_documents frd1 "
        f"WHERE frd1.id IN {tuple(gp_ids)}"
    )
    return query


def federal_register_documents_abstract_query(gp_ids):
    query = (
        f"SELECT frd1.document_abstract "
        f"FROM federal_register_documents frd1 "
        f"WHERE frd1.id IN {tuple(gp_ids)}"
    )
    return query


def senate_committee_hearings_matter_description_query(gp_ids):
    query = (
        f"SELECT sch1.matter_description "
        f"FROM senate_committee_hearings sch1 "
        f"WHERE sch1.id IN {tuple(gp_ids)}"
    )
    return query


def aggregator_billdocument_sector_keyword_5in3(gp_ids: list[int], matching_content_ids: list[int]):
    query = f"""
        SELECT
        gp1.id,
        gp1.bill_type,
        gp1.bill_number,
        gp1.title,
        gs1.id SectorId,
        gs1.label Sector,
       GROUP_CONCAT(DISTINCT gskm1.content ORDER BY gskm1.content SEPARATOR ', ') AS ContentList,
       COUNT(DISTINCT gskm1.content) ContentCount,
       SUM(
           CASE WHEN INSTR(gskm1.content, ' ') = 0 THEN
               (LENGTH(LOWER(gpd1.content)) - LENGTH(REPLACE(LOWER(gpd1.content), CONCAT(' ', LOWER(gskm1.content), ' '), ''))) / LENGTH(CONCAT(' ', LOWER(gskm1.content), ' '))
           ELSE
               (LENGTH(LOWER(gpd1.content)) - LENGTH(REPLACE(LOWER(gpd1.content), LOWER(gskm1.content), ''))) / LENGTH(gskm1.content)
           END
       ) TotalCount
    FROM gics_sectors gs1
    JOIN government_publications gp1
       ON gp1.id in {tuple(gp_ids)}
    JOIN gov_publication_documents gpd1
       JOIN gics_sector_keyword_mappings gskm1
    WHERE
       gskm1.id in {tuple(matching_content_ids)}
       AND gpd1.content REGEXP concat('[[:<:]]', gskm1.content, '[[:>:]]')
       AND gskm1.active_flag = true
       AND gskm1.gics_sectors_id = gs1.id
       AND gpd1.government_publications_id = gp1.id
        GROUP BY gp1.id, 
        gp1.bill_type,
        gp1.bill_number,
        gp1.title, 
        gs1.id, 
        gs1.label 
       HAVING ContentCount > 3
       AND TotalCount > 5
    order by gp1.id, gs1.id
    """
    return query


def aggregator_billdocument_subindustry_keyword_5in3(gp_ids: list[str], matching_content_ids: list[int]):
    query = f"""
        
        SELECT
            gp1.id,
            gp1.bill_type,
            gp1.bill_number,
            gp1.title,
            gs1.id SectorId,
            gs1.label Sector,
            GROUP_CONCAT(DISTINCT gsi1.label ORDER BY gsi1.label SEPARATOR ', ') AS SubIndustryList,
            GROUP_CONCAT(DISTINCT gskm1.content ORDER BY gskm1.content SEPARATOR ', ') AS ContentList,
            COUNT(DISTINCT gskm1.content) ContentCount,
           SUM(
               CASE WHEN INSTR(gskm1.content, ' ') = 0 THEN
                   (LENGTH(LOWER(gpd1.content)) - LENGTH(REPLACE(LOWER(gpd1.content), CONCAT(' ', LOWER(gskm1.content), ' '), ''))) / LENGTH(CONCAT(' ', LOWER(gskm1.content), ' '))
               ELSE
                   (LENGTH(LOWER(gpd1.content)) - LENGTH(REPLACE(LOWER(gpd1.content), LOWER(gskm1.content), ''))) / LENGTH(gskm1.content)
               END
           ) TotalCount
        FROM government_publications gp1
        JOIN gov_publication_documents gpd1
            on gpd1.government_publications_id = gp1.id
        JOIN gics_sectors gs1
        JOIN gics_industy_groups gig1 on gs1.id = gig1.gics_sectors_id
        JOIN gics_industries gi1
            ON gi1.gics_industy_groups_id = gig1.id
        JOIN gics_subindustries gsi1
            ON gsi1.gics_industries_id = gi1.id
        JOIN gics_subindustry_keyword_mappings gskm1
            ON gskm1.gics_subindustries_id = gsi1.id
            AND gskm1.id in {tuple(matching_content_ids)}
            AND gskm1.active_flag = true
            AND gpd1.content REGEXP concat('[[:<:]]', gskm1.content, '[[:>:]]')
        WHERE gp1.id in {tuple(gp_ids)}
        GROUP BY gp1.id, gp1.bill_type,
            gp1.bill_number,
            gp1.title, gs1.id, gs1.label
        HAVING ContentCount > 3
           AND TotalCount > 5
        order by gp1.id, gs1.id;
        
        """
    return query


def discovered_alerts_sector_keyword_title_scaled(gp_ids: list[str], matching_content_ids: list[int]):
    query = f"""
    SELECT GovernmentPublicationId,
        BillType bill_type, BillNumber bill_number, Title title,
       Sector,
       Source,
       GROUP_CONCAT(DISTINCT Content ORDER BY Content SEPARATOR ', ') AS ContentList,
       COUNT(DISTINCT Content) ContentCount,
       AVG(Relevance) ContentRelevanceAverage
    FROM (
       SELECT
           gp1.id GovernmentPublicationId,
           gp1.bill_type BillType,
           gp1.bill_number BillNumber,
           gp1.title Title,
           gs1.label Sector,
           'government_publications' Source,
           gskm1.content Content,
           gskm1.relevance Relevance
       FROM  gics_sector_keyword_mappings gskm1
        join gics_sectors gs1 on gs1.id = gskm1.gics_sectors_id
       JOIN government_publications gp1
           ON (
               LOWER(gp1.title) REGEXP concat('[[:<:]]', LOWER(gskm1.content), '[[:>:]]')
               OR LOWER(gp1.summary_text) REGEXP concat('[[:<:]]', LOWER(gskm1.content), '[[:>:]]')
           )
        AND gp1.id in {tuple(gp_ids)}
       WHERE gskm1.active_flag = true
       AND gskm1.id in {tuple(matching_content_ids)}
       UNION
       SELECT
           gpt1.government_publications_id GovernmentPublicationId,
           gp1.bill_type BillType,
           gp1.bill_number BillNumber,
           gp1.title Title,
           gs1.label Sector,
           'gov_publication_titles' Source,
           gskm1.content Content,
           gskm1.relevance Relevance
       FROM  gics_sector_keyword_mappings gskm1
        join gics_sectors gs1 on gs1.id = gskm1.gics_sectors_id
        JOIN gov_publication_titles gpt1
           ON LOWER(gpt1.title_content) REGEXP concat('[[:<:]]', LOWER(gskm1.content), '[[:>:]]')
           AND gpt1.government_publications_id in {tuple(gp_ids)}
        join government_publications gp1 on gp1.id = gpt1.government_publications_id
       WHERE gskm1.active_flag = true
        
    ) AS InnerQuery
    GROUP BY GovernmentPublicationId, BillType, BillNumber, Title,  Sector, Source
    HAVING ContentCount >= 1
       AND ContentRelevanceAverage >= (1 - (ContentCount * 0.1))
    ORDER BY GovernmentPublicationId, Sector;
    """
    return query


def discovered_alerts_subindustry_keyword_title_scaled(gp_ids: list[str], matching_content_ids: list[int]):
    query = f"""
    SELECT GovernmentPublicationId,
        BillType bill_type, BillNumber bill_number, Title title,
       Sector,
       SubIndustry,
       Source,
       GROUP_CONCAT(DISTINCT Content ORDER BY Content SEPARATOR ', ') AS ContentList,
       COUNT(DISTINCT Content) ContentCount,
       AVG(Relevance) ContentRelevanceAverage
    FROM (
       SELECT
           gp1.id GovernmentPublicationId,
           gp1.bill_type BillType,
           gp1.bill_number BillNumber,
           gp1.title Title,
           gs1.label Sector,
           gsi1.label SubIndustry,
           'government_publications' Source,
           gskm1.content Content,
           gskm1.relevance Relevance
       FROM  gics_subindustry_keyword_mappings gskm1
        join gics_subindustries gsi1 on gsi1.id = gskm1.gics_subindustries_id
        join gics_industries gi1 on gi1.id = gsi1.gics_industries_id
        join gics_industy_groups gig1 on gig1.id = gi1.gics_industy_groups_id
        join gics_sectors gs1 on gs1.id = gig1.gics_sectors_id
       JOIN government_publications gp1
           ON (
               LOWER(gp1.title) REGEXP concat('[[:<:]]', LOWER(gskm1.content), '[[:>:]]')
               OR LOWER(gp1.summary_text) REGEXP concat('[[:<:]]', LOWER(gskm1.content), '[[:>:]]')
           )
        AND gp1.id in {tuple(gp_ids)}
       WHERE gskm1.active_flag = true
       AND gskm1.id in {tuple(matching_content_ids)}
       UNION
       SELECT
           gpt1.government_publications_id GovernmentPublicationId,
           gp1.bill_type BillType,
           gp1.bill_number BillNumber,
           gp1.title Title,
           gs1.label Sector,
           gsi1.label SubIndustry,
           'gov_publication_titles' Source,
           gskm1.content Content,
           gskm1.relevance Relevance
       FROM gics_subindustry_keyword_mappings gskm1
              JOIN gov_publication_titles gpt1
           ON LOWER(gpt1.title_content) REGEXP concat('[[:<:]]', LOWER(gskm1.content), '[[:>:]]')
           AND gpt1.government_publications_id in {tuple(gp_ids)}
        join government_publications gp1 on gp1.id = gpt1.government_publications_id
           join gics_subindustries gsi1 on gsi1.id = gskm1.gics_subindustries_id
        join gics_industries gi1 on gi1.id = gsi1.gics_industries_id
        join gics_industy_groups gig1 on gig1.id = gi1.gics_industy_groups_id
        join gics_sectors gs1 on gs1.id = gig1.gics_sectors_id
       WHERE gskm1.active_flag = true
       AND gskm1.id in {tuple(matching_content_ids)}
    ) AS InnerQuery
    GROUP BY GovernmentPublicationId, BillType, BillNumber, Title,  Sector, SubIndustry, Source
    HAVING ContentCount >= 1
       AND ContentRelevanceAverage >= (1 - (ContentCount * 0.1))
    ORDER BY GovernmentPublicationId, Sector, SubIndustry;
    
    """
    return query


# watson extractions
def discovered_alerts_extractions(gp_ids: list[str]):
    query = f"""
    SELECT
        gp1.id GovernmentPublicationId2,
        gp1.bill_type bill_type,
        gp1.bill_number bill_number,
        gp1.title title,
        gs1.label Sector,
        gsi1.label SubIndustry,
        r3s1.id StockId2,
        Source,
        ActContentList,
        StockContentList,
       r3s1.company_name,
       r3s1.ticker,
       r3s1.shared_russell_3000_stock_id,
       SUM(ActContentCount) ActContentCount2,
        AVG(ActRelevanceAverage) ActRelevanceAverage2,
        SUM(StockContentCount) StockContentCount2,
        AVG(StockRelevanceAverage) StockRelevanceAverage2
        FROM russell_3000_stocks r3s1
        JOIN gics_subindustries gsi1 ON gsi1.id = r3s1.gics_subindustries_id
        JOIN gics_industries gi1 ON gi1.id = gsi1.gics_industries_id
        JOIN gics_industy_groups gig1 ON gig1.id = gi1.gics_industy_groups_id
        JOIN gics_sectors gs1 ON gs1.id = gig1.gics_sectors_id
        JOIN government_publications gp1
        JOIN (
           SELECT
               StockId,
               GovernmentPublicationId,
               Source,
               GROUP_CONCAT(DISTINCT ActContent ORDER BY ActContent SEPARATOR ', ') AS ActContentList,
               GROUP_CONCAT(DISTINCT StockContent ORDER BY StockContent SEPARATOR ', ') AS StockContentList,
               COUNT(DISTINCT ActContent) ActContentCount,
               AVG(ActRelevance) ActRelevanceAverage,
               COUNT(DISTINCT StockContent) StockContentCount,
               AVG(StockRelevance) StockRelevanceAverage
           FROM (
               SELECT
                   gpe1.government_publications_id GovernmentPublicationId,
                   r3s2.id StockId,
                   'Entities' As Source,
                   gpe1.content ActContent,
                   gpe1.relevance ActRelevance,
                   r3se1.content StockContent,
                   r3se1.relevance StockRelevance
               FROM russell_3000_stocks r3s2
               JOIN russell_3000_stock_entities r3se1
                   ON r3se1.russell_3000_stocks_id = r3s2.id
               JOIN gov_publication_entities gpe1
                   ON (
                       LOWER(gpe1.content) REGEXP concat('[[:<:]]', LOWER(r3se1.content), '[[:>:]]')
                       OR LOWER(r3se1.content) REGEXP concat('[[:<:]]', LOWER(gpe1.content), '[[:>:]]')
                   )
                   AND r3se1.relevance >= 0.7
                   AND gpe1.relevance >= 0.7
                   AND NOT EXISTS (
                       SELECT 1
                       FROM nlu_analysis_exclusions nae1
                       WHERE nae1.label = r3se1.content
                   )
                   AND NOT EXISTS (
                       SELECT 1
                       FROM nlu_analysis_exclusions nae1
                       WHERE nae1.label = gpe1.content
                   )
               WHERE gpe1.government_publications_id in {tuple(gp_ids)}
               AND r3s2.active_flag = true
               GROUP BY gpe1.government_publications_id, r3s2.id, Source
               UNION
               SELECT
                   gpk1.government_publications_id GovernmentPublicationId,
                   r3s3.Id StockId,
                   'Keywords' As Source,
                   gpk1.content ActContent,
                   gpk1.relevance ActRelevance,
                   r3sk1.content StockContent,
                   r3sk1.relevance StockRelevance
               FROM russell_3000_stocks r3s3
               JOIN russell_3000_stock_keywords r3sk1
                   ON r3sk1.russell_3000_stocks_id = r3s3.id
               JOIN gov_publication_keywords gpk1
                   ON (
                       LOWER(gpk1.content) REGEXP concat('[[:<:]]', LOWER(r3sk1.content), '[[:>:]]')
                       OR LOWER(r3sk1.content) REGEXP concat('[[:<:]]', LOWER(gpk1.content), '[[:>:]]')
                   )
                   AND r3sk1.relevance >= 0.7
                   AND gpk1.relevance >= 0.7
                   AND NOT EXISTS (
                       SELECT 1
                       FROM nlu_analysis_exclusions nae1
                       WHERE nae1.label = r3sk1.content
                   )
                   AND NOT EXISTS (
                       SELECT 1
                       FROM nlu_analysis_exclusions nae1
                       WHERE nae1.label = gpk1.content
                   )
               WHERE gpk1.government_publications_id in {tuple(gp_ids)}
               AND r3s3.active_flag = true
               GROUP BY gpk1.government_publications_id, r3s3.Id, Source
               UNION
               SELECT
                   gpc1.government_publications_id GovernmentPublicationId,
                   r3s4.id StockId,
                    'Concepts' As Source,
                   gpc1.content ActContent,
                   gpc1.relevance ActRelevance,
                   r3sc1.content StockContent,
                   r3sc1.relevance StockRelevance
               FROM russell_3000_stocks r3s4
               JOIN russell_3000_stock_concepts r3sc1
                   ON r3sc1.russell_3000_stocks_id = r3s4.id
               JOIN gov_publication_concepts gpc1
                   ON (
                       LOWER(gpc1.content) REGEXP concat('[[:<:]]', LOWER(r3sc1.content), '[[:>:]]')
                       OR LOWER(r3sc1.content) REGEXP concat('[[:<:]]', LOWER(gpc1.content), '[[:>:]]')
                   )
                   AND r3sc1.relevance >= 0.7
                   AND gpc1.relevance >= 0.7
                   AND NOT EXISTS (
                       SELECT 1
                       FROM nlu_analysis_exclusions nae1
                       WHERE nae1.label = r3sc1.content
                   )
                   AND NOT EXISTS (
                       SELECT 1
                       FROM nlu_analysis_exclusions nae1
                       WHERE nae1.label = gpc1.content
                   )
               WHERE gpc1.government_publications_id in {tuple(gp_ids)}
               GROUP BY gpc1.government_publications_id, r3s4.id, Source
          ) As InnerQuery
           GROUP BY GovernmentPublicationId, StockId
        ) AS OuterQuery
           ON OuterQuery.StockId = r3s1.id
           AND OuterQuery.GovernmentPublicationId = gp1.id
        GROUP BY gp1.id, Sector, SubIndustry, r3s1.id, Source
        HAVING SUM(ActContentCount) >= 1
           AND AVG(ActRelevanceAverage) >= (1 - (SUM(ActContentCount) * 0.1))
           AND SUM(StockContentCount) >= 1
           AND AVG(StockRelevanceAverage) >= (1 - (SUM(StockContentCount) * 0.1))
"""
    return query