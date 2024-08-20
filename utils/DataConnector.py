import pandas as pd
import numpy as np
from sqlalchemy import create_engine


class DataConnector(object):
    """
    Data Connector object for reading app data. If demo=True, data is sourced
    from a local parquet file, otherwise a connection is made to the database
    (specified by database_url). DataConnector has methods for querying data
    for all the app figures/filters and returning in the appropriate format. Each
    method will either filter the local parquet file, or construct a filtered
    query and read from the database.
    """

    def __init__(self, demo=False, database_url=None):
        self.demo = demo
        self.demo_data = self._load_demo_data(demo)
        if not self.demo:
            self.engine = create_engine(database_url)
        self.platform_order = self._define_platform_order(demo)

    def last_refreshed(self):
        # Returns the last date the app data was refreshed (if reading from DB)
        if self.demo:
            return
        else:
            return pd.read_sql(
                "SELECT MAX(date) FROM analytics", 
                self.engine
            ).iloc[0, 0]

    def get_available_platforms(self):
        # Returns distinct platforms (dataframe) for filter
        if self.demo:
            return self.demo_data[["platform"]].drop_duplicates().dropna()
        else:
            return pd.read_sql(
                "SELECT DISTINCT platform FROM analytics WHERE platform IS NOT NULL",
                self.engine,
            )

    def get_available_languages(self):
        # Returns distinct languages (dataframe) for filter
        if self.demo:
            return self.demo_data[["language"]].drop_duplicates().dropna()
        else:
            return pd.read_sql(
                "SELECT DISTINCT language FROM analytics WHERE language IS NOT NULL",
                self.engine,
            )

    def get_available_genres(self):
        # Returns distinct genres (dataframe) for filter
        if self.demo:
            return self.demo_data[["genre"]].drop_duplicates().dropna()
        else:
            return pd.read_sql(
                "SELECT DISTINCT genre FROM analytics WHERE genre IS NOT NULL",
                self.engine,
            )

    def get_available_countries(self):
        # Returns distinct countries (dataframe) for filter
        if self.demo:
            return self.demo_data[["country"]].drop_duplicates().dropna()
        else:
            return pd.read_sql(
                "SELECT DISTINCT country FROM analytics WHERE country IS NOT NULL",
                self.engine,
            )

    def get_platform_count(self, filters):
        # Returns total count of currently selected platforms (for metric card)
        if self.demo:
            filtered = self._filter_demo_data(filters)
            return filtered.platform.nunique()
        else:
            subquery = self._construct_filtered_subquery(filters)
            return pd.read_sql(
                f"""
                SELECT 
                    COUNT(DISTINCT platform)
                FROM (
                    {subquery}
                )
                """,
                self.engine,
            ).iloc[0, 0]

    def get_movie_count(self, filters):
        # Returns number of distinct movies for selected filters (for metric card)
        if self.demo:
            filtered = self._filter_demo_data(filters)
            return filtered[filtered.media_type == "movie"].title_id.nunique()
        else:
            subquery = self._construct_filtered_subquery(filters)
            return pd.read_sql(
                f"""
                SELECT 
                    COUNT(DISTINCT title_id)
                FROM (
                    {subquery}
                )
                WHERE media_type = 'movie'
                """,
                self.engine,
            ).iloc[0, 0]

    def get_tv_count(self, filters):
        # Returns number of distinct tv shows for selected filters (for metric card)
        if self.demo:
            filtered = self._filter_demo_data(filters)
            return filtered[filtered.media_type == "tv"].title_id.nunique()
        else:
            subquery = self._construct_filtered_subquery(filters)
            return pd.read_sql(
                f"""
                SELECT 
                    COUNT(DISTINCT title_id)
                FROM (
                    {subquery}
                )
                WHERE media_type = 'tv'
                """,
                self.engine,
            ).iloc[0, 0]

    def get_overview_data(self, filters):
        # Returns data for summary figure (rating, popularity, count)
        if self.demo:
            filtered = self._filter_demo_data(filters)
            titles = filtered[
                ["platform", "title_id", "vote_average", "popularity"]
            ].drop_duplicates()
            agg = (
                titles.groupby("platform")
                .agg(
                    {"vote_average": "mean", "popularity": "mean", "title_id": "count"}
                )
                .reset_index()
            )
            return agg.rename(
                columns={
                    "vote_average": "average_rating",
                    "popularity": "average_popularity",
                    "title_id": "title_count",
                }
            )
        else:
            subquery = self._construct_filtered_subquery(filters)
            query = f"""
            SELECT
                platform,
                AVG(vote_average) AS average_rating,
                AVG(popularity) AS average_popularity,
                COUNT(title_id) AS title_count
            FROM (
                SELECT DISTINCT
                    platform,
                    title_id,
                    vote_average,
                    popularity
                FROM (
                    {subquery}
                )
            )
            GROUP BY 1
            """
            return pd.read_sql(query, self.engine)

    def get_title_count_data(self, filters):
        # Returns data for title count figure (tv count, movie count, total for each platform)
        if self.demo:
            filtered = self._filter_demo_data(filters)
            titles = filtered[["platform", "title_id", "media_type"]].drop_duplicates()
            agg = (
                titles.groupby(["platform", "media_type"])
                .agg({"title_id": "count"})
                .reset_index()
            )

            agg = agg.pivot(
                columns="media_type", index="platform", values="title_id"
            ).reset_index()

            for col in ["movie", "tv"]:
                if col not in agg.columns:
                    agg[col] = None

            return agg.rename(columns={"movie": "movies", "title_id": "total"})
        else:
            subquery = self._construct_filtered_subquery(filters)
            query = f"""
            SELECT
                platform,
                COUNT(DISTINCT CASE WHEN media_type = 'movie' THEN title_id ELSE NULL END) AS movies,
                COUNT(DISTINCT CASE WHEN media_type = 'tv' THEN title_id ELSE NULL END) AS tv,
                COUNT(DISTINCT title_id) AS total
            FROM (
                {subquery}
            )
            GROUP BY 1
            ORDER BY 4 DESC
            """
            return pd.read_sql(query, self.engine)

    def get_quality_data(self, filters):
        # Returns data for quality boxplot - right now this is just returning everything -
        # In the future probably want to update to compute quartiles so I can return smaller data
        if self.demo:
            filtered = self._filter_demo_data(filters)
            return filtered.drop_duplicates()
        else:
            subquery = self._construct_filtered_subquery(filters)
            query = f"""
            SELECT DISTINCT
                platform,
                title_id,
                vote_average
            FROM (
                {subquery}
            )
            """
            return pd.read_sql(query, self.engine)

    def get_diversity_data(self, filters):
        if self.demo:
            filtered = self._filter_demo_data(filters)
            genre_counts = (
                filtered.groupby(["platform", "genre"]).tmdb_id.nunique().reset_index()
            )
            platform_counts = (
                filtered.groupby("platform").tmdb_id.nunique().reset_index()
            )
            summary = genre_counts.merge(
                platform_counts,
                how="left",
                on="platform",
                suffixes=["_genre", "_platform"],
            )

            summary["ratio"] = summary.tmdb_id_genre / summary.tmdb_id_platform
            agg = (
                summary.groupby("platform")
                .agg(
                    {
                        "genre": "count",
                        "ratio": [
                            lambda x: round(x.max(), 3),
                            lambda x: round((x * np.log(x)).sum() * -1, 3),
                        ],
                    }
                )
                .reset_index()
            )
            agg.columns = ["platform", "richness", "dominance", "shannon"]
            return agg
        else:
            subquery = self._construct_filtered_subquery(filters)
            query = f"""
                    SELECT
                        platform,
                        ROUND(MAX(ratio),3) AS dominance,
                        COUNT(DISTINCT genre) AS richness,
                        ROUND(SUM(ratio * LN(ratio)) * -1,3) AS shannon
                    FROM (
                        SELECT 
                            g.platform,
                            genre,
                            CAST(genre_count AS DECIMAL) / platform_count AS ratio
                        FROM (
                            SELECT 
                                platform,
                                genre,
                                COUNT(DISTINCT title_id) AS genre_count
                            FROM (
                                {subquery}
                            )
                            GROUP BY 1,2
                            ORDER BY 1,2
                        ) g
                        LEFT JOIN (
                            SELECT
                                platform,
                                COUNT(DISTINCT title_id) AS platform_count
                            FROM (
                                {subquery}
                            )
                            GROUP BY 1
                        ) p
                            ON g.platform = p.platform
                    ) sub
                    GROUP BY 1
            """
            return pd.read_sql(query, self.engine)

    def get_top_genre_data(self, filters, n=3):
        # Returns data for top genre figure (counts) - Included n parameter so its
        # easier to update if necessary
        if self.demo:
            filtered = self._filter_demo_data(filters)
            agg = (
                filtered.groupby(["platform", "genre"]).title_id.nunique().reset_index()
            )
            agg.sort_values("title_id", ascending=False, inplace=True)
            agg = agg.groupby("platform").head(3)
            return agg.rename(columns={"title_id": "title_count"})
        else:
            subquery = self._construct_filtered_subquery(filters)
            query = f"""
            SELECT 
                platform,
                genre,
                title_count
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY platform ORDER BY title_count DESC) AS rnk
                FROM (
                    SELECT
                        platform,
                        genre,
                        COUNT(DISTINCT title_id) AS title_count
                    FROM (
                        {subquery}
                    )
                    GROUP BY 1,2
                    ORDER BY 3 DESC
                )
            )
            WHERE rnk <= {n}
            """
            return pd.read_sql(query, self.engine)

    def get_top_country_data(self, filters, n=3):
        # Returns data for top country figure
        if self.demo:
            filtered = self._filter_demo_data(filters)
            agg = (
                filtered.groupby(["platform", "media_type", "country"])
                .title_id.nunique()
                .reset_index()
            )
            agg.sort_values("title_id", ascending=False, inplace=True)
            agg = agg.groupby(["platform", "media_type"]).head(3).reset_index()
            return agg.rename(columns={"title_id": "title_count"})
        else:
            subquery = self._construct_filtered_subquery(filters)
            query = f"""
            SELECT 
                platform,
                media_type,
                country,
                title_count
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY platform,media_type ORDER BY title_count DESC) AS rnk
                FROM (
                    SELECT
                        platform,
                        media_type,
                        country,
                        COUNT(DISTINCT title_id) AS title_count
                    FROM (
                        {subquery}
                    )
                    WHERE country IS NOT NULL
                    GROUP BY 1,2,3
                    ORDER BY 3 DESC
                )
            )
            WHERE rnk <= {n}
            """
            return pd.read_sql(query, self.engine)

    def get_recent_content_data(self, filters, min_year=2014):
        # Returns count of titles by platform by year, for all years of data after min_year
        if self.demo:
            filtered = self._filter_demo_data(filters)
            agg = (
                filtered.groupby(["platform", "release_year"])
                .title_id.nunique()
                .reset_index()
            )
            agg = agg[agg.release_year >= min_year]
            return agg.rename(columns={"title_id": "title_count"})
        else:
            subquery = self._construct_filtered_subquery(filters)
            query = f"""
            SELECT
                platform,
                release_year,
                COUNT(DISTINCT title_id) AS title_count
            FROM (
                {subquery}
            )
            WHERE release_year >= {min_year}
            GROUP BY 1,2
            ORDER BY 2,3 DESC
            """
            return pd.read_sql(query, self.engine)

    def get_change_data(self, filters):
        if self.demo:
            # Need to update the demo data to include multiple weeks 
            return
        else:
            subquery = self._construct_filtered_subquery(filters, two_dates=True)
            query = f"""
                    WITH last_two_dates AS (
                        {subquery}
                    ),
                    current AS (
                        SELECT DISTINCT
                            platform,
                            date,
                            media_type,
                            tmdb_id
                        FROM last_two_dates
                        WHERE date = (
                            SELECT MAX(date)
                            FROM last_two_dates
                        )
                    ), previous AS (
                        SELECT DISTINCT
                            platform,
                            date,
                            media_type,
                            tmdb_id
                        FROM last_two_dates
                        WHERE date = (
                            SELECT DISTINCT date
                            FROM last_two_dates
                            ORDER BY date DESC
                            LIMIT 1
                            OFFSET 1
                        )
                    ), comparison AS (
                        SELECT 
                            COALESCE(current.platform, previous.platform) AS platform,
                            COALESCE(current.media_type, previous.media_type) AS media_type,
                            CASE 
                                WHEN current.tmdb_id IS NULL THEN 0
                                ELSE 1
                            END AS in_current,
                            CASE 
                                WHEN previous.tmdb_id IS NULL THEN 0
                                ELSE 1
                            END AS in_previous
                        FROM current
                        FULL OUTER JOIN previous
                            ON current.platform = previous.platform
                            AND current.tmdb_id = previous.tmdb_id
                            AND current.media_type = previous.media_type
                    ), gain_loss AS (
                        SELECT 
                            platform,
                            SUM(CASE 
                                WHEN media_type = 'movie' AND in_current = 1 AND in_previous = 0 THEN 1
                                ELSE 0
                            END) AS movie_gained,
                            -1 * SUM(CASE 
                                WHEN media_type = 'movie' AND in_current = 0 AND in_previous = 1 THEN 1
                                ELSE 0
                            END) AS movie_lost,
                            SUM(CASE 
                                WHEN media_type = 'tv' AND in_current = 1 AND in_previous = 0 THEN 1
                                ELSE 0
                            END) AS tv_gained,
                            -1 * SUM(CASE 
                                WHEN media_type = 'tv' AND in_current = 0 AND in_previous = 1 THEN 1
                                ELSE 0
                            END) AS tv_lost
                        FROM comparison
                        GROUP BY platform
                    )
                    SELECT
                        *,
                        movie_gained + movie_lost + tv_gained + tv_lost AS net_change
                    FROM gain_loss
            """
            return pd.read_sql(query, self.engine)

    def _construct_filtered_subquery(self, filters, two_dates=False):
        # Uses the dcc.Store filters data to construct a query string for a filtered view of the analytics data
        # The other query methods will all use this subquery as a starting point for pulling/aggregating data
        # two_dates indicates whether the query should read from the most recent date of data, or the two 
        # most recent dates. This is used for the comparing changes over the last two weeks
        if two_dates:
            query = """
            SELECT *
            FROM analytics
            WHERE date in (
                SELECT DISTINCT date
                FROM analytics
                ORDER BY date desc
                LIMIT 2
            )
            """
        else:
            query = """
            SELECT *
            FROM analytics
            WHERE date = (
                SELECT MAX(date)
                FROM analytics
            )
            """

        if len(filters["media-type"]) == 1:
            query += f"AND media_type = '{filters['media-type'][0].lower()}'"

        if len(filters["platform"]) != 0:
            if len(filters["platform"]) == 1:
                query += f"AND platform = '{filters['platform'][0]}'"
            else:
                query += f"AND platform IN {tuple(filters['platform'])}"

        if (
            filters["rating"] is not None
            and filters["rating"] != [0, 10]
            and len(filters["rating"]) > 0
        ):
            query += f"""
            AND vote_average >= {filters['rating'][0]} 
            AND vote_average <= {filters['rating'][1]}
            """

        if (
            filters["release-year"] is not None
            and filters["release-year"] != [1902, 2024]
            and len(filters["release-year"]) > 0
        ):
            query += f"""
            AND release_year >= {filters['release-year'][0]} 
            AND release_year <= {filters['release-year'][1]}
            """

        if len(filters["genre"]) > 0:
            if len(filters["genre"]) == 1:
                query += f"""
                AND title_id IN (
                    SELECT
                        title_id
                    FROM analytics 
                    WHERE genre = '{filters["genre"][0]}'
                )
                """
            else:
                query += f"""
                AND title_id IN (
                    SELECT
                        title_id
                    FROM analytics
                    WHERE genre IN {tuple(filters["genre"])}
                )
                """

        if len(filters["country"]) > 0:
            if len(filters["country"]) == 1:
                query += f"""
                AND title_id IN (
                    SELECT
                        title_id
                    FROM analytics
                    WHERE country = '{filters["country"][0]}'
                )
                """
            else:
                query += f"""
                AND title_id IN (
                    SELECT
                        title_id
                    FROM analytics
                    WHERE country IN {tuple(filters["country"])}
                )
                """

        if len(filters["language"]) != 0:
            if len(filters["language"]) == 1:
                query += f"AND language = '{filters['language'][0]}'"
            else:
                query += f"AND language IN {tuple(filters['language'])}"

        return query

    def _filter_demo_data(self, filters):
        if len(filters["media-type"]) == 1:
            media_filter = self.demo_data.media_type == filters["media-type"][0].lower()
        else:
            media_filter = pd.Series(True, index=self.demo_data.index)

        if len(filters["platform"]) != 0:
            platform_filter = self.demo_data.platform.isin(filters["platform"])
        else:
            platform_filter = pd.Series(True, index=self.demo_data.index)

        if filters["rating"] is not None and len(filters["rating"]) > 0:
            rating_filter = (self.demo_data.vote_average >= filters["rating"][0]) & (
                self.demo_data.vote_average <= filters["rating"][1]
            )
        else:
            rating_filter = pd.Series(True, index=self.demo_data.index)

        if (
            filters["release-year"] is not None
            and len(filters["release-year"]) > 0
            and filters["release-year"] != [1902, 2024]
        ):
            year_filter = (
                self.demo_data.release_year >= filters["release-year"][0]
            ) & (self.demo_data.release_year <= filters["release-year"][1])
        else:
            year_filter = pd.Series(True, index=self.demo_data.index)

        if len(filters["genre"]) != 0:
            genre_filter = self.demo_data.genre.isin(filters["genre"])
        else:
            genre_filter = pd.Series(True, index=self.demo_data.index)

        if len(filters["country"]) != 0:
            country_filter = self.demo_data.country.isin(filters["country"])
        else:
            country_filter = pd.Series(True, index=self.demo_data.index)

        if len(filters["language"]) != 0:
            language_filter = self.demo_data.language.isin(filters["language"])
        else:
            language_filter = pd.Series(True, index=self.demo_data.index)

        return self.demo_data[
            (media_filter)
            & (platform_filter)
            & (rating_filter)
            & (year_filter)
            & (genre_filter)
            & (country_filter)
            & (language_filter)
        ]

    def _load_demo_data(self, demo):
        # Load data from parquet file (if running locally in demo mode)
        if demo:
            return pd.read_parquet("demo_data.parquet")
        return

    def _define_platform_order(self, demo):
        # Helper function to sort all platforms in descending order by overall title count
        # Useful for having the same order across multiple figures
        if demo:
            sorted = (
                self.demo_data.groupby("platform")
                .title_id.nunique()
                .sort_values(ascending=False)
            )
            return sorted.index.tolist()
        else:
            return pd.read_sql(
                """
                SELECT platform
                FROM analytics
                GROUP BY 1
                ORDER BY COUNT(DISTINCT title_id) DESC
                """,
                self.engine,
            ).platform.tolist()
