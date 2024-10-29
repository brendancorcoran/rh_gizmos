import pandas as pd
import mysql.connector

import warnings

warnings.filterwarnings('ignore')

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


def _run_query(query: str) -> pd.DataFrame:
    try:
        conn = _get_conn()
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as error:
        print("Error while connecting to MySQL", error)
        return pd.DataFrame()


def get_government_publications(conn, congressional_cycle: str) -> pd.DataFrame:
    query = f"""
        SELECT
          gp1.id,
          gp1.bill_type,
          gp1.bill_number,
          gp1.bill_chamber,
          gp1.title,
          gp1.summary_text
        FROM
          katalyst.government_publications gp1
          JOIN katalyst.congressional_cycles cc1 ON gp1.congressional_cycles_id = cc1.id
        WHERE
          cc1.label = '{congressional_cycle}'
      AND gp1.active_flag = 1
        """
    return pd.read_sql_query(query, conn)


def get_government_publication_themes(
    conn, government_publication_id: int, theme_types_id: int
) -> pd.DataFrame:
    query = f"""
    SELECT
  gp1.id,
  gp1.bill_chamber,
  gp1.bill_number,
  gp1.bill_type,
  gp1.title,
  tt1.id AS ThemeTypeId,
  tt1.shared_theme_type_id
FROM
  theme_types tt1 
  JOIN government_publications gp1 ON gp1.id = '{government_publication_id}'
WHERE
  EXISTS (
    SELECT
      gpd1.id,
      COUNT(DISTINCT tte1.label) AS ContentCount
    FROM
      gov_publication_documents gpd1 
      JOIN theme_type_entries tte1 ON tte1.theme_types_id = tt1.id
    WHERE
      LOWER(gpd1.content) LIKE CONCAT('%',LOWER(tte1.label),'%')
      AND tte1.active_flag = TRUE
      AND tte1.theme_types_id = {theme_types_id}
      AND gpd1.government_publications_id = {government_publication_id}
    GROUP BY
      gpd1.id
    HAVING
      ContentCount > 3
  )
  AND EXISTS (
    SELECT
      gpd1.id,
      SUM(
        CASE 
          WHEN INSTR(tte1.label,' ') = 0 THEN (LENGTH(LOWER(gpd1.content)) - LENGTH(REPLACE(LOWER(gpd1.content),CONCAT(' ',LOWER(tte1.label),' '),''))) / LENGTH(CONCAT(' ',LOWER(tte1.label),' '))
          ELSE (LENGTH(LOWER(gpd1.content)) - LENGTH(REPLACE(LOWER(gpd1.content),LOWER(tte1.label),''))) / LENGTH(tte1.label)
        END
      ) AS TotalCount
    FROM
      gov_publication_documents gpd1 
      JOIN theme_type_entries tte1 ON tte1.theme_types_id = tt1.id
    WHERE
      LOWER(gpd1.content) LIKE CONCAT('%',LOWER(tte1.label),'%')
      AND tte1.active_flag = TRUE
      AND tte1.theme_types_id = {theme_types_id}
      AND gpd1.government_publications_id = {government_publication_id}
    GROUP BY
      gpd1.id
    HAVING
      TotalCount > 5
  )
        

        """
    return pd.read_sql_query(query, conn)

def governement_publication_themes_runner(theme_types_id: int):
    conn = _get_conn()
    government_publications = get_government_publications(conn, "118")
    themes_dfs = []
    for index, row in government_publications.iterrows():
        # for every 1000 rows print a statement
        if index % 1000 == 0:
            print(f"Processing row {index}")
        government_publication_id = row["id"]
        themes_df = get_government_publication_themes(conn, government_publication_id, theme_types_id)
        if not themes_df.empty:
            print(f'adding {themes_df}')
            themes_dfs.append(themes_df)

    print(len(themes_dfs))

    # concatentate
    final_df = pd.concat(themes_dfs)

    # write out
    final_df.to_csv("government_publication_themes.csv", index=False)

    conn.close()


def twitter_sentiment(twitter_timeline_entry_ids:list[str]) -> pd.DataFrame:
    query = f"""
    select tts1.sentiment_origin, tts1.extract, tts1.score, st1.label
        from twitter_timeline_sentiments tts1
        join sentiment_types st1 on tts1.sentiment_types_id = st1.id
        join twitter_user_timeline_entries tute1 on tute1.id = tts1.twitter_user_timeline_entries_id
        where tute1.twitter_timeline_entry_id in {tuple(twitter_timeline_entry_ids)};
    """
    return _run_query(query)

def get_government_publications():
    query = f"""

    select id, bill_type, bill_number, title
    from government_publications
    where congressional_cycles_id = 12
    and bill_type in ('HR', 'S')

    """
    return _run_query(query)

if __name__ == "__main__":
    theme_types_id = 10
    governement_publication_themes_runner(theme_types_id)