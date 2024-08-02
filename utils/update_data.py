import psycopg2
import os
import requests
import argparse
import datetime
import time
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)

load_dotenv()

database_url = os.environ.get("DATABASE_URL")
watchmode_key = os.environ.get("WATCHMODE_KEY")
tmdb_key = os.environ.get("TMDB_KEY")
tmdb_token = os.environ.get("TMDB_TOKEN")

# Table definitions
create_analytics_table = """
CREATE TABLE IF NOT EXISTS analytics (
    id                 SERIAL PRIMARY KEY,
    date               DATE NOT NULL,
    platform           VARCHAR(80) NOT NULL,
    media_type         VARCHAR(5) NOT NULL,
    tmdb_id            INTEGER NOT NULL,
    title_id           INTEGER NOT NULL,
    release_year       INTEGER,
    vote_count         INTEGER,
    vote_average       FLOAT,
    popularity         FLOAT,
    genre              VARCHAR(20),
    country            VARCHAR(50),
    language           VARCHAR(20)
)
"""

create_countries_table = """
CREATE TABLE IF NOT EXISTS countries (
    country_code       VARCHAR(2) PRIMARY KEY,
    english_name       VARCHAR(50),
    native_name        VARCHAR(50)
)
"""

create_languages_table = """
CREATE TABLE IF NOT EXISTS languages (
    language_code      VARCHAR(2) PRIMARY KEY,
    english_name       VARCHAR(25),
    name               VARCHAR(25)
)
"""

create_genres_table = """
CREATE TABLE IF NOT EXISTS genres (
    id                 SERIAL PRIMARY KEY,
    tmdb_genre_id      INTEGER,
    tmdb_type          VARCHAR(5) NOT NULL,
    tmdb_name          VARCHAR(20) NOT NULL,
    unified_name       VARCHAR(20) NOT NULL
)
"""

create_platforms_table = """
CREATE TABLE IF NOT EXISTS platforms (
    platform_id        INTEGER PRIMARY KEY,
    name               VARCHAR(80),
    type               VARCHAR(10) NOT NULL,
    region             VARCHAR(2)  NOT NULL
)
"""

create_catalogs_table = """
CREATE TABLE IF NOT EXISTS catalogs (
    id                 SERIAL PRIMARY KEY,
    title_id           INTEGER,
    title              VARCHAR(150),
    year               INTEGER,
    imdb_id            VARCHAR(10),
    tmdb_id            INTEGER NOT NULL,
    tmdb_type          VARCHAR(5) NOT NULL,
    platform_type      VARCHAR(10) NOT NULL,
    platform_region    VARCHAR(2) NOT NULL,
    platform_id        INTEGER NOT NULL
)
"""

create_details_table = """
CREATE TABLE IF NOT EXISTS title_details ( 
    id                 SERIAL PRIMARY KEY,
    title_id           INTEGER NOT NULL,
    tmdb_id            INTEGER NOT NULL,
    tmdb_type          VARCHAR(5) NOT NULL,
    title              VARCHAR(200),
    original_language  VARCHAR(2),
    release_date       DATE,
    status             VARCHAR(25),
    runtime            FLOAT,
    vote_average       FLOAT,
    vote_count         INTEGER,
    popularity         FLOAT
)
"""

create_title_genres_table = """
CREATE TABLE IF NOT EXISTS title_genres ( 
    id                SERIAL PRIMARY KEY,
    title_id          INTEGER NOT NULL,
    tmdb_type         VARCHAR(5) NOT NULL,
    genre             VARCHAR(20) NOT NULL
)
"""

create_title_countries_table = """
CREATE TABLE IF NOT EXISTS title_countries ( 
    id                 SERIAL PRIMARY KEY,
    title_id           INTEGER NOT NULL,
    country_code       VARCHAR(2) NOT NULL
)
"""


# Pipeline functions
def repeat_get_request(url, headers=None, params=None, max_retries=5, wait=5):
    """
    Wrapper function for repeating api calls if error is returned (Occasionally 
    hitting rate limiting errors)

    Args
        url (str) : Url to send get request
        headers (dict) : Headers for get request. Default None
        params (dict) : Params for get request. Default None
        max_retries (int) : Max times to repeat failed request. Default 5
        wait (int) : Time (seconds) to wait before retrying. Default 5

    returns
        (dict) response.json() of get request
    """
    status_code = 429
    attempts = 0

    while status_code != 200 and attempts < max_retries:
        response = requests.get(url, headers=headers, params=params)
        status_code = response.status_code

        if status_code != 200:
            time.sleep(wait)

        attempts += 1

    if status_code == 200:
        return response.json()

    return

def table_count(table_name, engine):
    """
    Helper function for returning the row count of a specified table

    Args
        table_name (str)
        engine (sqlalchemy.engine.base.Engine)

    Returns
        (int) Row count of specified table
    """
    return pd.read_sql(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        """,
        engine,
    ).iloc[0, 0]


def check_refresh(engine, gap=15):
    """
    Checks whether appropriate to run a data refresh. Pulls latest date data
    was inserted into the analytics table - if today - last refresh < gap then
    returns False, else returns True. Heroku Scheduler's minimum frequency
    for running jobs is daily, but api constraints limit the amount of calls
    that can be made per month (without paying)
    
    Args
        engine (sqlalchemy.engine.base.Engine)
        gap (int) : Amount of time (days) that need to have passed before running
                    refresh

    Returns
        (bool) True if refresh should be run (enough time has passed) otherwise False
    """
    last_update = pd.read_sql(
        """
        SELECT
            MAX(date)
        FROM analytics
        """,
        engine,
    ).iloc[0, 0]

    if last_update is not None and (datetime.date.today() - last_update).days < 15:
        return False
    return True


def create_tables(cursor, conn):
    """
    Creates database tables if they do not already exist. Create table functions
    are defined at the top of this file. Some of the tables are 'temporary'
    tables that are dropped at the end of the job run - not using real
    temporary tables in case the process fails midway and needs to be picked up.

    Args
        cursor (psycopg2.extensions.cursor)
        conn (psycopg2.extensions.connection)

    Returns
        None
    """
    logging.info("Creating tables")
    for statement in [
        create_analytics_table,
        create_genres_table,
        create_countries_table,
        create_languages_table,
        create_catalogs_table,
        create_details_table,
        create_title_genres_table,
        create_title_countries_table,
        create_platforms_table,
    ]:
        cursor.execute(statement)
    conn.commit()
    logging.info("Tables created successfully")
    return


def pull_genres_table(engine):
    """
    Pulls genre information from tmdb endpoint and saves to genres table.
    Note that genres is a static list, so this process only runs once
    when first running the process

    Args
        engine (sqlalchemy.engine.base.Engine)
    
    Returns
        None
    """
    genre_count = table_count("genres", engine)

    if genre_count == 0:
        logging.info("Pulling genres")
        movie_genres = repeat_get_request(
            "https://api.themoviedb.org/3/genre/movie/list",
            headers={
                "Authorization": f"Bearer {tmdb_token}",
                "Content-Type": "application/json;charset=utf-8",
            },
            params={"api_key": tmdb_key},
        )
        movie_genres = pd.DataFrame(movie_genres["genres"])
        movie_genres["tmdb_type"] = "movie"

        tv_genres = repeat_get_request(
            "https://api.themoviedb.org/3/genre/tv/list",
            headers={
                "Authorization": f"Bearer {tmdb_token}",
                "Content-Type": "application/json;charset=utf-8",
            },
            params={"api_key": tmdb_key},
        )
        tv_genres = pd.DataFrame(tv_genres["genres"])
        tv_genres["tmdb_type"] = "tv"

        genres = pd.concat([tv_genres, movie_genres])

        # Append genres (found in the data, but not listed on site)
        extra = pd.DataFrame({"id": [-1], "name": ["Musical"], "tmdb_type": ["tv"]})
        genres = pd.concat([genres, extra], ignore_index=True)

        # Create unified label for closely aligned movie/tv genres - also flag
        # fields that only apply to one genre or another
        genre_key = {
            "Action": "Action & Adventure",
            "Adventure": "Action & Adventure",
            "War": "War & Politics",
            "Fantasy": "Sci-Fi & Fantasy",
            "Science Fiction": "Sci-Fi & Fantasy",
            "Music": "Musical",
            "Kids": "Kids (tv)",
            "News": "News (tv)",
            "Reality": "Reality (tv)",
            "Soap": "Soap (tv)",
            "Talk": "Talk (tv)",
            "History": "History (movie)",
            "Romance": "Romance (movie)",
            "Thriller": "Thriller (movie)",
        }
        genres["unified_name"] = genres.name.apply(
            lambda x: genre_key.get(x) if x in genre_key else x
        )
        genres.rename(
            columns={"id": "tmdb_genre_id", "name": "tmdb_name"}, inplace=True
        )
        genres.to_sql("genres", engine, if_exists="append", index=False)
        logging.info("Pulled genres successfully")
    else:
        logging.info("Genres table already populated")
    return


def pull_countries_table(engine):
    """
    Pulls country information from tmdb endpoint and saves to countries table.
    Note that countries is a static list, so this process only runs once
    when first running the process

    Args
        engine (sqlalchemy.engine.base.Engine)

    Returns
        None
    """
    country_count = table_count("countries", engine)

    if country_count == 0:
        logging.info("Pulling countries")
        countries = repeat_get_request(
            "https://api.themoviedb.org/3/configuration/countries",
            headers={
                "Authorization": f"Bearer {tmdb_token}",
                "Content-Type": "application/json;charset=utf-8",
            },
            params={"api_key": tmdb_key},
        )
        countries = pd.DataFrame(countries)
        countries.rename(columns={"iso_3166_1": "country_code"}, inplace=True)
        countries.to_sql("countries", engine, if_exists="append", index=False)
        logging.info("Countries pulled successfully")
    else:
        logging.info("Countries table already populated")
    return


def pull_languages_table(engine):
    """
    Pulls language information from tmdb endpoint and saves to languages table.
    Note that languages is a static list, so this process only runs once
    when first running the process

    Args
        engine (sqlalchemy.engine.base.Engine)
    
    Returns
        None
    """
    language_count = table_count("languages", engine)

    if language_count == 0:
        logging.info("Pulling languages")
        languages = repeat_get_request(
            "https://api.themoviedb.org/3/configuration/languages",
            headers={
                "Authorization": f"Bearer {tmdb_token}",
                "Content-Type": "application/json;charset=utf-8",
            },
            params={"api_key": tmdb_key},
        )
        languages = pd.DataFrame(languages)
        languages.rename(columns={"iso_639_1": "language_code"}, inplace=True)
        languages.to_sql("languages", engine, if_exists="append", index=False)
        logging.info("Languages pulled successfully")
    else:
        logging.info("Language table already populated")
    return


def pull_watchmode_sources(engine, source_region, source_type):
    """
    Pulls all available sources (platforms) from the watchmode catalog that
    match search criteria (region and type). Writes results to 'platforms' table

    Args
        engine (sqlalchemy.engine.base.Engine)
        source_region (str) : Watchmode region (US, CA, etc)
        source_type (str) : Watchmode type (sub, free, purchase, etc)

    Returns
        None
    """
    logging.info("Pulling platforms")
    platforms = repeat_get_request(
        f"https://api.watchmode.com/v1/sources/?apiKey={watchmode_key}",
        params={"types": "sub", "regions": "US"},
    )
    platforms = pd.DataFrame(
        [
            {
                "platform_id": platform.get("id"),
                "name": platform.get("name"),
                "region": source_region,
                "type": source_type,
            }
            for platform in platforms
        ]
    ).drop_duplicates()

    # Insert platform information into database
    platforms.to_sql("platforms", engine, if_exists="append", index=False)
    logging.info("Pulled platforms successfully")
    return


def pull_watchmode_catalogs(engine):
    """
    Pulls content catalogs for each platform in the platforms table. The
    watchmode api returns a max of 250 results per api call, so this repeats
    calls to the api until reaching the final page of results. Writes results
    to the 'catalogs' table

    Args
        engine (sqlalchemy.engine.base.Engine)

    Returns 
        None
    """
    platforms = pd.read_sql(
        """
        SELECT *
        FROM platforms
        """,
        engine,
    )
    logging.info(f"Pulling catalogs for {len(platforms)} platforms")

    for row in platforms.itertuples():
        logging.info(f"Pulling catalog: {row.name}")
        results = []
        page = 1
        total_pages = 2

        while page <= total_pages:
            page_results = repeat_get_request(
                f"https://api.watchmode.com/v1/list-titles/?apiKey={watchmode_key}",
                params={
                    "regions": row.region,
                    "source_types": row.type,
                    "source_ids": row.platform_id,
                    "sort_by": "release_date_desc",
                    "page": page,
                    "limit": 250,
                },
            )

            # append titles to results
            if page_results.get("titles"):
                results.extend(page_results.get("titles"))

            # increment
            page = page_results["page"]
            total_pages = page_results["total_pages"]
            page += 1

        # Save results (if anything was returned for the platform)
        if len(results) > 0:
            platform_catalog = pd.DataFrame(results)
            platform_catalog.drop("type", axis=1, inplace=True)
            platform_catalog["platform_type"] = row.type
            platform_catalog["platform_region"] = row.region
            platform_catalog["platform_id"] = row.platform_id
            platform_catalog.rename(columns={"id": "title_id"}, inplace=True)

            platform_catalog.to_sql("catalogs", engine, if_exists="append", index=False)

    logging.info("Pulled platforms successfully")
    return


def pull_tmdb_details(engine, chunk_size=500):
    """
    Iterates through distinct titles in the 'catalogs' table and pulls full
    details from the tmdb api. Writes results to 3 tables - title details,
    title_genres, and title_countries. A single title can be associated with
    multiple genres/countries, so each relationship is stored as a single row
    in one of these (join) tables.

    Args
        engine (sqlalchemy.engine.base.Engine)
        chunk_size (int) : Size of chunk for pulling details/appending to db.
                           Defaults to 500

    Returns
        None
    """
    titles = pd.read_sql(
        """
        SELECT DISTINCT
            title_id,
            tmdb_id,
            tmdb_type
        FROM catalogs
        ORDER BY tmdb_id, tmdb_type
        """,
        engine,
    )
    logging.info(f"Pulling details for {len(titles)} titles from TMDB")

    # Pull/write titles by chunk
    i = 0
    missing = 0
    while i * chunk_size < len(titles):
        logging.info(f"Chunk: {i * chunk_size} - {(i + 1) * chunk_size}")
        chunk = titles.iloc[i * chunk_size : (i + 1) * chunk_size].copy()

        chunk_results = []
        for row in chunk.itertuples():
            response = repeat_get_request(
                f"https://api.themoviedb.org/3/{row.tmdb_type}/{row.tmdb_id}",
                headers={
                    "Authorization": f"Bearer {tmdb_token}",
                    "Content-Type": "application/json;charset=utf-8",
                },
                params={"api_key": tmdb_key},
            )

            if response is not None:
                # Limit to relevant fields
                response = {
                    key: response.get(key)
                    for key in [
                        # shared fields
                        "id",
                        "vote_average",
                        "vote_count",
                        "popularity",
                        "original_language",
                        "origin_country",
                        "genres",
                        # movie specific fields
                        "title",
                        "release_date",
                        "runtime",
                        "status",
                        # tv specifc fields
                        "name",
                        "first_air_date",
                        "episode_run_time",
                        "number_of_episodes",
                    ]
                }
                response["title_id"] = row.title_id

                # Combine movie/tv fields
                if response.get("title") is None:
                    response["title"] = response.get("name")

                if response.get("release_date") is None:
                    response["release_date"] = response.get("first_air_date")

                # Estimate runtime for tv shows
                if response.get("runtime") is None:
                    if (
                        response.get("episode_run_time") is not None
                        and len(response.get("episode_run_time")) > 0
                        and response.get("number_of_episodes") is not None
                    ):
                        response["runtime"] = np.mean(
                            response.get("episode_run_time")
                        ) * response.get("number_of_episodes")

                # Add tmdb info
                response["tmdb_id"] = row.tmdb_id
                response["tmdb_type"] = row.tmdb_type
                response["title_id"] = row.title_id

                chunk_results.append(response)
            else:
                missing += 1
                logging.info(f"Unable to pull title details for {row.tmdb_id} ({row.tmdb_type})")

        chunk_results = pd.DataFrame(chunk_results)

        # Parse results into 3 dataframes - details, title_genres, title_countries
        # These will be stored, then ultimately joined together on the descriptive
        # tables to get English genre/country names
        # Details
        details = chunk_results[
            [
                "title_id",
                "tmdb_id",
                "tmdb_type",
                "title",
                "original_language",
                "release_date",
                "status",
                "runtime",
                "vote_average",
                "vote_count",
                "popularity",
            ]
        ].copy()
        details.release_date = pd.to_datetime(details.release_date).dt.date

        details.to_sql("title_details", engine, if_exists="append", index=False)

        # Genres
        title_genres = chunk_results[["title_id", "tmdb_type", "genres"]].copy()
        title_genres.genres = title_genres.genres.apply(
            lambda x: [i.get("name") for i in x if i is not None]
        )
        title_genres = title_genres.explode("genres")
        title_genres.rename(columns={"genres": "genre"}, inplace=True)
        title_genres.dropna(subset="genre", inplace=True)

        title_genres.to_sql("title_genres", engine, if_exists="append", index=False)

        # Countries
        title_countries = chunk_results[["title_id", "origin_country"]].copy()
        title_countries = title_countries.explode("origin_country")
        title_countries.rename(columns={"origin_country": "country_code"}, inplace=True)
        title_countries.dropna(subset="country_code", inplace=True)

        title_countries.to_sql(
            "title_countries", engine, if_exists="append", index=False
        )

        i += 1

    logging.info(f"Title details pulled successfully ({len(titles)-missing} out of {len(titles)} titles)")
    return


def update_analytics_table(cursor, conn):
    """
    Combines newly scraped data into simplified view and appends to
    analytics table with the current date.

    Args
        cursor (psycopg2.extensions.cursor)
        conn (psycopg2.extensions.connection)

    Returns
        None
    """
    logging.info("Updating analytics table")
    cursor.execute(
        """
        INSERT INTO analytics (date, platform, media_type, tmdb_id, title_id, release_year, vote_count, vote_average, popularity, genre, country, language)
        SELECT 
            CURRENT_DATE AS date,
            p.name AS platform,
            ca.tmdb_type AS media_type,
            ca.tmdb_id,
            ca.title_id,
            CAST(EXTRACT(YEAR FROM d.release_date) AS INTEGER) AS release_year,
            d.vote_count,
            d.vote_average,
            d.popularity,
            g.unified_name AS genre,
            co.english_name AS country,
            l.english_name AS language
        FROM catalogs ca
        LEFT JOIN platforms p
            ON ca.platform_id = p.platform_id
        LEFT JOIN title_details d
            ON ca.title_id = d.title_id
        LEFT JOIN title_genres tg
            ON ca.title_id = tg.title_id
        LEFT JOIN genres g
            ON tg.genre = g.tmdb_name
            AND tg.tmdb_type = g.tmdb_type
        LEFT JOIN title_countries tc
            ON d.title_id = tc.title_id
        LEFT JOIN countries co
            ON tc.country_code = co.country_code
        LEFT JOIN languages l
            ON d.original_language = l.language_code;
        """
    )
    conn.commit()
    logging.info("Analytics table updated successfully")
    return


def drop_temporary_tables(cursor, conn):
    """
    Drops temporary database tables that will be recreated/repopulated
    on the next run.

    Args
        cursor (psycopg2.extensions.cursor)
        conn (psycopg2.extensions.connection)

    Returns
        None
    """
    logging.info("Dropping temporary tables")
    for table in [
        "platforms",
        "catalogs",
        "title_details",
        "title_countries",
        "title_genres",
    ]:
        cursor.execute(
            f"""
            DROP TABLE {table}
            """
        )
    conn.commit()
    logging.info("Temporary tables dropped successfully")
    return


def main():

    # Initialize arguments
    parser = argparse.ArgumentParser(description="Updates streaming catalog data")
    parser.add_argument(
        "-t",
        "--type",
        type=str,
        required=True,
        help="Streaming source type (sub, free, purchase, tve)",
    )
    parser.add_argument(
        "-r", "--region", type=str, required=True, help="Streaming source region"
    )

    # Parse arguments
    args = parser.parse_args()
    source_type = args.type
    source_region = args.region

    # Database connections - psycopg2 for creating/dropping tables, pandas for reading/inserting data
    engine = create_engine(database_url.replace("postgres://", "postgresql://"))
    conn = psycopg2.connect(database_url, sslmode="require")
    cursor = conn.cursor()

    # Initialize tables and run pipeline
    create_tables(cursor, conn)
    if check_refresh(engine):
        logging.info("Starting data refresh")
        pull_genres_table(engine)
        pull_countries_table(engine)
        pull_languages_table(engine)
        pull_watchmode_sources(engine, source_region, source_type)
        pull_watchmode_catalogs(engine)
        pull_tmdb_details(engine)
        update_analytics_table(cursor, conn)
        drop_temporary_tables(cursor, conn)
        logging.info("Data refresh completed successfully")
    else:
        logging.info("Not enough time since last refresh. Ending process.")

    conn.close()
    cursor.close()
    return


if __name__ == "__main__":
    main()
