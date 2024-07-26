import pandas as pd
import numpy as np


def filter_data(data, filters):
    """
    Filters app data based on dictionary of selections. 

    Args
        data (pd.DataFrame)
        filters (dict) : 'data' attribute of dcc.Store
    
    Returns
        Filtered dataframe
    """
    filtered = data.copy()

    # construct filters for each of the inputs
    if len(filters["media-type"]) == 1:
        media_filter = filtered.tmdb_type.isin(
            [i.lower() for i in filters["media-type"]]
        )
    else:
        media_filter = pd.Series(True, index=filtered.index)

    if len(filters["platform"]) != 0:
        platform_filter = filtered.streaming_source_name.isin(filters["platform"])
    else:
        platform_filter = pd.Series(True, index=filtered.index)

    if filters["rating"] is not None and len(filters["rating"]) > 0:
        rating_filter = (filtered.vote_average >= filters["rating"][0]) & (
            filtered.vote_average <= filters["rating"][1]
        )
    else:
        rating_filter = pd.Series(True, index=filtered.index)

    if (
        filters["release-year"] is not None
        and len(filters["release-year"]) > 0
        and filters["release-year"] != [1902, 2024]
    ):
        year_filter = (filtered.year >= filters["release-year"][0]) & (
            filtered.year <= filters["release-year"][1]
        )
    else:
        year_filter = pd.Series(True, index=filtered.index)

    if len(filters["genre"]) != 0:
        genre_filter = pd.Series(
            np.zeros(shape=len(filtered)), index=filtered.index
        ).astype(bool)
        for genre in filters["genre"]:
            genre_filter = genre_filter | filtered[f"genre_{genre}"]
    else:
        genre_filter = pd.Series(True, index=filtered.index)

    if len(filters["country"]) != 0:
        country_filter = pd.Series(
            np.zeros(shape=len(filtered)), index=filtered.index
        ).astype(bool)
        for country in filters["country"]:
            country_filter = country_filter | filtered[f"country_{country}"]
    else:
        country_filter = pd.Series(True, index=filtered.index)

    if len(filters["language"]) != 0:
        language_filter = filtered.original_language.isin(filters["language"])
    else:
        language_filter = pd.Series(True, index=filtered.index)

    return filtered[
        (media_filter)
        & (platform_filter)
        & (rating_filter)
        & (year_filter)
        & (genre_filter)
        & (country_filter)
        & (language_filter)
    ]
