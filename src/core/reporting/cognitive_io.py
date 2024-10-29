import psycopg2
import pandas as pd
from elasticsearch import Elasticsearch

from tweet.stage.index_domain import ESIndex


def _get_conn():
    conn = psycopg2.connect(
        dbname="public", user="postgres", password="password", host="localhost"
    )
    return conn


def _run_query(query: str) -> pd.DataFrame:
    try:
        conn = _get_conn()
        df = pd.read_sql_query(query, conn)
        df = enrich_data(df)
        conn.close()
        return df
    except Exception as error:
        print("Error while connecting to PostgreSQL", error)
        return pd.DataFrame()


def _dt_convert(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def enrich_data(df: pd.DataFrame) -> pd.DataFrame:

    # id fields
    if "id" not in df.columns:
        if "twitter_timeline_entry_id" in df.columns:
            df["id"] = df["twitter_timeline_entry_id"]
        else:
            df["id"] = df.index

    df = _dt_convert(df, "published_at")
    df = _dt_convert(df, "actioned_at")

    # Check if 'party_affiliation' column exists before updating
    if "party_affiliation" in df.columns:
        df["party_affiliation"] = df["party_affiliation"].map(
            {"R": "Republican", "D": "Democrat", "I": "Independent"}
        )

    # Check if 'associated_house' column exists before updating
    if "associated_house" in df.columns:
        df["associated_house"] = df["associated_house"].map(
            {"House": "Congressmen", "S": "Senators"}
        )

    # Check if both 'party_affiliation' and 'associated_house' columns exist before adding 'party_house' column
    if "party_affiliation" in df.columns and "associated_house" in df.columns:
        df["party_house"] = df["party_affiliation"] + " " + df["associated_house"]

    # Check if 'label' column exists before adding 'phase' column
    if "label" in df.columns:
        df["phase"] = df["label"].map(
            {
                "Introduced in House": "Introduced phase",
                "Introduced in Senate": "Introduced phase",
                "Reported to House": "Reported phase",
                "Reported to Senate": "Reported phase",
                "Passed House": "Vote stage",
                "Passed Senate": "Vote stage",
            }
        )

    if "specific_freeform_type" in df.columns and "specific_freeform_id" in df.columns:
        df["bill"] = df["specific_freeform_type"] + df["specific_freeform_id"]

    if "label" in df.columns:
        df["event_type"] = df["label"].map(
            {
                "Introduced in House": "Introduced",
                "Introduced in Senate": "Introduced",
                "Reported to House": "Reported",
                "Reported to Senate": "Reported",
                "Passed House": "Passed",
                "Passed Senate": "Passed",
                "Public Law": "Signed",
            }
        )

        df["chamber"] = df["label"].map(
            {
                "Introduced in House": "House",
                "Introduced in Senate": "Senate",
                "Reported to House": "House",
                "Reported to Senate": "Senate",
                "Passed House": "House",
                "Passed Senate": "Senate",
                "Public Law": "Public Law",
            }
        )
    if "description" in df.columns:
        df["state"] = df["description"]

    return df


def get_katalyst_event_ids_for_theme(theme: str):

    query = f"""
    select ke1.id
        from "Katalyst_Events" ke1
        join public."Katalyst_Event_Theme_Type_Relevances" KETTR on ke1.id = KETTR.katalyst_events_id
        join public."Theme_types" Tt on KETTR.theme_types_id = Tt.id
        where Tt.descrription = '{theme}'
        and ke1.specific_freeform_id is not null;
    """
    return _run_query(query)


def get_events(congressional_cycle: str, theme: str) -> pd.DataFrame:
    query = f"""
    SELECT
      ke1.id,
      ke1.specific_freeform_type,
      ke1.specific_freeform_id,
      ke1.title,
      ke1.starting_overall_at,
      ke1.finishing_overall_at,
      ke1.updated_at,
      ed1.id AS event_details_id,
      ed1.label,
      ed1.actioned_at,
      st1.description
    FROM
      "Katalyst_Events" ke1 
      JOIN "Event_Details" ed1 ON ke1.id = ed1.katalyst_events_id
      JOIN "Schedule_Types_Event_Tags" stet1 ON ed1.id = stet1.event_details_id
      JOIN "Schedule_Types" st1 ON stet1.schedule_types_id = st1.id
    WHERE
      1 = 1
      AND st1.description IN ('Completed')
      AND ed1.label != 'Scheduled'
      AND EXISTS (
        SELECT
          1
        FROM
          "Katalyst_Event_Congressional_Cycles" kecc1 
          JOIN "Congressional_Cycles" cc1 ON kecc1.congressional_cycles_id = cc1.id
        WHERE
          ke1.id = kecc1.katalyst_events_id
          AND cc1.label = '{congressional_cycle}'
      )
      AND EXISTS (
        SELECT
          1
        FROM
          "Theme_Types_Event_Tags" tte1 
          JOIN "Theme_types" tt1 ON tte1.theme_types_id = tt1.id
        WHERE
          tte1.katalyst_events_id = ke1.id
          AND tt1.descrription = '{theme}'
      )
    """
    return _run_query(query)


def get_events_katalyst_events(katalyst_events:list[int]) -> pd.DataFrame:

    query = f"""
    SELECT
      ke1.id,
      ke1.specific_freeform_type,
      ke1.specific_freeform_id,
      ke1.title,
      ke1.starting_overall_at,
      ke1.finishing_overall_at,
      ke1.updated_at,
      ed1.id AS event_details_id,
      ed1.label,
      ed1.actioned_at,
      st1.description
    FROM
      "Katalyst_Events" ke1 
      JOIN "Event_Details" ed1 ON ke1.id = ed1.katalyst_events_id
      JOIN "Schedule_Types_Event_Tags" stet1 ON ed1.id = stet1.event_details_id
      JOIN "Schedule_Types" st1 ON stet1.schedule_types_id = st1.id
    WHERE
      1 = 1
      AND st1.description IN ('Completed')
      AND ed1.label != 'Scheduled'
      AND ke1.specific_freeform_type IN ('S', 'HR')
      AND ke1.id in {tuple(katalyst_events)}
"""
    return _run_query(query)


def get_events_latest_stage(congressional_cycle: str, theme: str) -> pd.DataFrame:
    query = f"""
        WITH RankedEventDetails AS (
        SELECT
            ke1.id,
            ke1.specific_freeform_type,
            ke1.specific_freeform_id,
            ke1.title,
            ed1.id AS event_details_id,
            ed1.label,
            ed1.actioned_at,
            st1.description,
            ROW_NUMBER() OVER (PARTITION BY ke1.id ORDER BY ed1.actioned_at DESC) AS rn
        FROM
            "Katalyst_Events" ke1
        JOIN "Event_Details" ed1 ON ke1.id = ed1.katalyst_events_id
        JOIN "Schedule_Types_Event_Tags" stet1 ON ed1.id = stet1.event_details_id
        JOIN "Schedule_Types" st1 ON stet1.schedule_types_id = st1.id
        WHERE
            1 = 1
            AND st1.description IN ('Completed')
            AND ed1.label != 'Scheduled'
            AND EXISTS (
                SELECT 1
                FROM "Katalyst_Event_Congressional_Cycles" kecc1
                JOIN "Congressional_Cycles" cc1 ON kecc1.congressional_cycles_id = cc1.id
                WHERE
                    ke1.id = kecc1.katalyst_events_id
                    AND cc1.label = '{congressional_cycle}'
            )
            AND EXISTS (
                SELECT 1
                FROM "Theme_Types_Event_Tags" tte1
                JOIN "Theme_types" tt1 ON tte1.theme_types_id = tt1.id
                WHERE
                    tte1.katalyst_events_id = ke1.id
                    AND tt1.descrription = '{theme}'
            )
    )
    SELECT
        id,
        specific_freeform_type,
        specific_freeform_id,
        title,
        event_details_id,
        label,
        actioned_at,
        description
    FROM
        RankedEventDetails
    WHERE
        rn = 1
    ORDER BY
        id, actioned_at;
    """
    return _run_query(query)


def get_event_tweets(congressional_cycle: str, theme: str) -> pd.DataFrame:
    query = f"""
    SELECT
          ke1.id,
          ke1.title,
          ke1.specific_freeform_type,
          ke1.specific_freeform_id,
          et1.id AS event_tweet_id,
          et1.published_at,
          tu1.twitter_handle,
          tu1.party_affiliation,
          tu1.associated_house
        FROM
          "Katalyst_Events" ke1
          JOIN "Event_Tweets" et1 ON ke1.id = et1.katalyst_events_id
          JOIN "Twitter_Users" tu1 ON et1.twitter_users_id = tu1.id
        WHERE
          1 = 1
          AND EXISTS (
            SELECT
              1
            FROM
              "Katalyst_Event_Congressional_Cycles" kecc1
              JOIN "Congressional_Cycles" cc1 ON kecc1.congressional_cycles_id = cc1.id
            WHERE
              ke1.id = kecc1.katalyst_events_id
              AND cc1.label = '{congressional_cycle}'
          )
          AND EXISTS (
            SELECT
              1
            FROM
              "Theme_Types_Event_Tags" tte1
              JOIN "Theme_types" tt1 ON tte1.theme_types_id = tt1.id
            WHERE
              tte1.katalyst_events_id = et1.katalyst_events_id
              AND tt1.descrription = '{theme}'
          )
        order by et1.published_at desc;
    """
    return _run_query(query)


def get_event_tweets_katalyst_events(katalyst_events:list[int]) -> pd.DataFrame:
    query = f"""
    SELECT
          ke1.id,
          ke1.title,
          ke1.specific_freeform_type,
          ke1.specific_freeform_id,
          et1.id AS event_tweet_id,
          et1.published_at,
          tu1.twitter_handle,
          tu1.party_affiliation,
          tu1.associated_house
        FROM
          "Katalyst_Events" ke1
          JOIN "Event_Tweets" et1 ON ke1.id = et1.katalyst_events_id
          JOIN "Twitter_Users" tu1 ON et1.twitter_users_id = tu1.id
        WHERE
          1 = 1
          AND ke1.id in {tuple(katalyst_events)}
        order by et1.published_at desc;
    """
    return _run_query(query)


def get_event_news_theme(congressional_cycle: str, theme: str) -> pd.DataFrame:
    query = f"""
        
    SELECT
      ke1.id, ke1.specific_freeform_type, ke1.specific_freeform_id, ke1.title, ke1.starting_overall_at, ke1.finishing_overall_at,
      ke1.updated_at,
      na1.id as news_article_id,
      na1.published_at,
      na1.article_author
    FROM
      "Katalyst_Events" ke1
    join "News_Articles_Katalyst_Events" nake1 on ke1.id = nake1.katalyst_events_id
    join "News_Articles" na1 on nake1.news_articles_id = na1.id
    WHERE
      1=1
      AND EXISTS (
        SELECT
          1
        FROM
          "Katalyst_Event_Congressional_Cycles" kecc1
          JOIN "Congressional_Cycles" cc1 ON kecc1.congressional_cycles_id = cc1.id
        WHERE
          ke1.id = kecc1.katalyst_events_id
          AND cc1.label = '{congressional_cycle}'
      )
      AND EXISTS (
        SELECT
          1
        FROM
          "Theme_Types_Event_Tags" tte1
          JOIN "Theme_types" tt1 ON tte1.theme_types_id = tt1.id
        WHERE
          tte1.katalyst_events_id = ke1.id
          AND tt1.descrription = '{theme}'
      )
    order by ke1.id, na1.published_at desc"""
    return _run_query(query)


def get_event_news_katalyst_events(katalyst_events:list[int]) -> pd.DataFrame:
    query = f"""

    SELECT
      ke1.id, ke1.specific_freeform_type, ke1.specific_freeform_id, ke1.title, ke1.starting_overall_at, ke1.finishing_overall_at,
      ke1.updated_at,
      na1.id as news_article_id,
      na1.published_at,
      na1.article_author
    FROM
      "Katalyst_Events" ke1
    join "News_Articles_Katalyst_Events" nake1 on ke1.id = nake1.katalyst_events_id
    join "News_Articles" na1 on nake1.news_articles_id = na1.id
    WHERE
      1=1
      AND ke1.id in {tuple(katalyst_events)}
    order by ke1.id, na1.published_at desc"""
    return _run_query(query)

def get_event_tweets_with_stage(published_at: str, theme: str) -> pd.DataFrame:
    """
    Fetch data from the database based on the published_at date and description.

    :param published_at: The date to filter the published_at field.
    :param theme: The description to filter the tt1.description field.
    :return: DataFrame containing the query results.
    """
    query = f"""
            SELECT
              et1.id as event_tweet_id,
              et1.published_at,
              tu1.twitter_handle,
              tu1.party_affiliation,
              tu1.associated_house,
              ke1.title,
              ke1.specific_freeform_type,
              ke1.specific_freeform_id,
              ed1.id as event_details_id,
              ed1.label,
              ed1.katalyst_events_id,
              ed1.actioned_at
            FROM
              "Katalyst_Events" ke1
              JOIN "Event_Tweets" et1 on ke1.id = et1.katalyst_events_id
              JOIN "Twitter_Users" tu1 ON et1.twitter_users_id = tu1.id
              JOIN "Event_Details" ed1 ON et1.katalyst_events_id = ed1.katalyst_events_id
                AND et1.published_at >= ed1.actioned_at
              LEFT JOIN "Event_Details" ed2 ON ed2.id = (
                  SELECT
                    MIN(ed3.id)
                  FROM
                    "Event_Details" ed3
                  WHERE
                    ed3.katalyst_events_id = et1.katalyst_events_id
                    AND ed3.id > ed1.id
                )
                AND et1.published_at < COALESCE(ed2.actioned_at,NOW())
              JOIN "Schedule_Types_Event_Tags" stet1 ON stet1.event_details_id = ed1.id
                AND stet1.schedule_types_id = 4
              LEFT JOIN "Schedule_Types_Event_Tags" stet2 ON stet2.event_details_id = ed2.id
                AND stet2.schedule_types_id = 4
              JOIN "Katalyst_Event_Congressional_Cycles" kecc1 ON kecc1.katalyst_events_id = ke1.id
                AND kecc1.congressional_cycles_id = 12
            WHERE
              et1.published_at >= '{published_at}'
              AND EXISTS (
                SELECT
                  1
                FROM
                  "Theme_Types_Event_Tags" tte1
                  JOIN "Theme_types" tt1 ON tte1.theme_types_id = tt1.id
                WHERE
                  tte1.katalyst_events_id = et1.katalyst_events_id
                  AND tt1.descrription = '{theme}'
              );
            """
    return _run_query(query)


def get_event_details(published_at: str, theme: str) -> pd.DataFrame:
    """
    Fetch data from the database based on the published_at date and description.

    :param published_at: The date to filter the published_at field.
    :param theme: The description to filter the tt1.description field.
    :return: DataFrame containing the query results.
    """
    query = f"""
                SELECT
                  ke1.id,
                  ke1.title,
                  ke1.specific_freeform_type,
                  ke1.specific_freeform_id,
                  ed1.id,
                  ed1.label,
                  ed1.katalyst_events_id,
                  ed1.actioned_at,
                  COUNT(DISTINCT et1.id)
                FROM
                  "Katalyst_Events" ke1
                  JOIN "Event_Tweets" et1 ON ke1.id = et1.katalyst_events_id
                  JOIN "Twitter_Users" tu1 ON et1.twitter_users_id = tu1.id
                  JOIN "Event_Details" ed1 ON et1.katalyst_events_id = ed1.katalyst_events_id
                    AND et1.published_at >= ed1.actioned_at
                  LEFT JOIN "Event_Details" ed2 ON ed2.id = (
                      SELECT
                        MIN(ed3.id)
                      FROM
                        "Event_Details" ed3
                      WHERE
                        ed3.katalyst_events_id = et1.katalyst_events_id
                        AND ed3.id > ed1.id
                    )
                    AND et1.published_at < COALESCE(ed2.actioned_at,NOW())
                  JOIN "Schedule_Types_Event_Tags" stet1 ON stet1.event_details_id = ed1.id
                    AND stet1.schedule_types_id = 4
                  LEFT JOIN "Schedule_Types_Event_Tags" stet2 ON stet2.event_details_id = ed2.id
                    AND stet2.schedule_types_id = 4
                  JOIN "Katalyst_Event_Congressional_Cycles" kecc1 ON kecc1.katalyst_events_id = ke1.id
                    AND kecc1.congressional_cycles_id = 12
                WHERE
                  et1.published_at >= '{published_at}'
                  AND EXISTS (
                    SELECT
                      1
                    FROM
                      "Theme_Types_Event_Tags" tte1
                      JOIN "Theme_types" tt1 ON tte1.theme_types_id = tt1.id
                    WHERE
                      tte1.katalyst_events_id = et1.katalyst_events_id
                      AND tt1.descrription = '{theme}'
                  )
                GROUP BY
                  ke1.id,
                  ke1.title,
                  ke1.specific_freeform_type,
                  ke1.specific_freeform_id,
                  ed1.id,
                  ed1.label,
                  ed1.katalyst_events_id,
                  ed1.actioned_at
                ORDER BY
                  ed1.actioned_at DESC
            """
    return _run_query(query)


def search_documents_by_date(es, date, size=1000):
    query = {
        "size": size,
        "query": {
            "range": {
                "published_at": {"gt": date}  # Greater than the provided datetime
            }
        },
    }

    # Initialize the scroll
    response = es.search(index="twitter_entries", body=query, scroll="2m")
    scroll_id = response["_scroll_id"]
    total_hits = response["hits"]["total"]["value"]

    documents = [hit["_source"] for hit in response["hits"]["hits"]]

    # Continue scrolling until all documents are retrieved
    while len(documents) < total_hits:
        response = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = response["_scroll_id"]
        documents.extend([hit["_source"] for hit in response["hits"]["hits"]])

    # Clear the scroll context
    es.clear_scroll(scroll_id=scroll_id)

    print(f"Total documents retrieved: {len(documents)}")
    return documents



def es_search_terms(
    es_client: Elasticsearch, es_index: ESIndex, search_terms: list, published_at_gt: str = "2023-01-01"
) -> pd.DataFrame:
    terms_query = {
        "size": 1000,
        "query": {
            "bool": {
                "should": [
                    {
                        "match_phrase": {
                            "twitter_timeline_entry_text": {"query": term, "slop": 0}
                        }
                    }
                    for term in search_terms
                ],
                "minimum_should_match": 1,
                "filter": [{"range": {"published_at": {"gt": published_at_gt}}}],
            }
        }
    }

    # Initialize the scroll
    response = es_client.search(index=es_index.index, body=terms_query, scroll="2m")
    scroll_id = response["_scroll_id"]
    total_hits = response["hits"]["total"]["value"]

    documents = [hit["_source"] for hit in response["hits"]["hits"]]

    # Continue scrolling until all documents are retrieved
    while len(documents) < total_hits:
        response = es_client.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = response["_scroll_id"]
        documents.extend([hit["_source"] for hit in response["hits"]["hits"]])

    # Clear the scroll context
    es_client.clear_scroll(scroll_id=scroll_id)

    print(f"Total documents retrieved: {len(documents)}")
    df = pd.DataFrame(documents)
    df = enrich_data(df)

    return df