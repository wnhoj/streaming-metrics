import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import os
import json
import dash
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Input, Output, State, MATCH, ALL
from sqlalchemy import create_engine

from layout.navbar import navbar
from layout.filters import filters
from layout.dashboard import dashboard
from layout.about import about

from utils import *

if os.environ.get("ENVIRONMENT") == "heroku":
    database_url = os.environ.get("DATABASE_URL")
    engine = create_engine(database_url.replace("postgres://", "postgresql://"))
    data = pd.read_sql(
        """
        SELECT *
        FROM demo
        """, 
        engine
    )
else:
    data = pd.read_parquet("demo_data.parquet")

with open("assets/language_codes.json", "r") as f:
    language_codes = json.load(f)

with open("assets/country_codes.json", "r") as f:
    country_codes = json.load(f)

app = Dash(
    __name__,
    title="Streaming Metrics",
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200",  # Icons
        "https://fonts.googleapis.com/css2?family=Roboto:ital,wght@0,100;0,300;0,400;0,500;0,700;0,900;1,100;1,300;1,400;1,500;1,700;1,900&display=swap",  # Font
    ],
)
server = app.server

app.layout = html.Div(
    [
        dcc.Store(
            id="filters-store",
            data={
                "media-type": [],
                "platform": [],
                "rating": [],
                "release-year": [],
                "genre": [],
                "language": [],
                "country": [],
            },
        ),
        navbar,
        dbc.Container(
            dbc.Stack(
                [
                    dcc.Markdown(
                        """
                        *Data for this app is pulled from the [TMDB API](https://developer.themoviedb.org/docs/getting-started) and the [Watchmode API](https://api.watchmode.com/)*
                        """,
                        link_target="_blank",
                        id="attribution",
                    ),
                    filters,
                    dashboard,
                    about,
                ],
                gap=3,
            ),
            id="content",
            className="p-3",
        ),
    ],
    id="page",
)


# Layout callbacks (collapse, modals, etc)
@app.callback(
    Output("filter-collapse", "is_open"),
    Input("filter-header-btn", "n_clicks"),
    State("filter-collapse", "is_open"),
)
def open_close_filter_collapse(n, current_state):
    if n == 0:
        raise dash.exceptions.PreventUpdate()
    return not current_state


@app.callback(
    Output("filter-header-icon", "children"), Input("filter-collapse", "is_open")
)
def switch_filter_header_icon(is_open):
    if is_open:
        return "keyboard_arrow_up"
    else:
        return "keyboard_arrow_down"


@app.callback(
    Output({"type": "graph-modal", "index": MATCH}, "is_open"),
    Input({"type": "graph-info-btn", "index": MATCH}, "n_clicks"),
)
def show_graph_info_modals(n):
    if n == 0:
        raise dash.exceptions.PreventUpdate()
    return True


@app.callback(Output("about-modal", "is_open"), Input("page-info-btn", "n_clicks"))
def show_about_modal(n):
    if n == 0:
        raise dash.exceptions.PreventUpdate()
    return True


# Filter callbacks (initialization, storing, clearing)
@app.callback(Output("platform", "options"), Input("platform", "id"))
def populate_platform_options(_):
    platforms = data.streaming_source_name.unique()
    return [{"label": i, "value": i} for i in sorted(platforms)]


@app.callback(Output("language", "options"), Input("language", "id"))
def populate_language_options(_):
    languages = data.original_language.dropna().unique()
    return [
        (
            {"label": language_codes.get(i), "value": i}
            if i in language_codes
            else {"label": i, "value": i}
        )
        for i in sorted(languages)
    ]


@app.callback(Output("genre", "options"), Input("genre", "id"))
def populate_genre_options(_):
    genres = [col.replace("genre_", "") for col in data.columns if "genre_" in col]
    return [{"label": i, "value": i} for i in sorted(genres)]


@app.callback(Output("country", "options"), Input("country", "id"))
def populate_country_options(_):
    countries = [
        col.replace("country_", "") for col in data.columns if "country_" in col
    ]
    countries = sorted(
        countries, key=lambda x: country_codes.get(x) if x in country_codes else "Zzzz"
    )
    return [
        (
            {"label": country_codes.get(i), "value": i}
            if i in country_codes
            else {"label": i, "value": i}
        )
        for i in countries
    ]


@app.callback(
    Output("filters-store", "data"),
    Input("media-type", "value"),
    Input("platform", "value"),
    Input("rating", "value"),
    Input("release-year", "value"),
    Input("genre", "value"),
    Input("language", "value"),
    Input("country", "value"),
    State("filters-store", "data"),
)
def update_filters_store(
    media_type, platform, rating, release_year, genre, language, country, data
):
    data["media-type"] = media_type
    data["platform"] = platform
    data["rating"] = rating
    data["release-year"] = release_year
    data["genre"] = genre
    data["language"] = language
    data["country"] = country
    return data


@app.callback(
    Output("media-type", "value"),
    Output("platform", "value"),
    Output("rating", "value"),
    Output("release-year", "value"),
    Output("genre", "value"),
    Output("language", "value"),
    Output("country", "value"),
    Input("clear-filters-btn", "n_clicks"),
)
def clear_all_filters(n):
    if n == 0:
        raise dash.exceptions.PreventUpdate()
    return [["Movie", "TV"], [], [0, 10], [1902, 2024], [], [], []]


@app.callback(Output("filter-tooltip", "children"), Input("filter-collapse", "is_open"))
def change_tooltip_message(is_open):
    if is_open:
        return "Click to hide filters"
    return "Click to show filters"


# Metric card callbacks
@app.callback(
    Output({"type": "metric-value", "index": "platform-count"}, "children"),
    Input("filters-store", "data"),
)
def display_platform_count(filters):
    filtered = filter_data(data, filters)
    return filtered.streaming_source_name.nunique()


@app.callback(
    Output({"type": "metric-value", "index": "movie-count"}, "children"),
    Input("filters-store", "data"),
)
def display_movie_count(filters):
    filtered = filter_data(data, filters)
    return f"{filtered[filtered.tmdb_type == 'movie'].tmdb_id.nunique():,}"


@app.callback(
    Output({"type": "metric-value", "index": "tv-count"}, "children"),
    Input("filters-store", "data"),
)
def display_tv_count(filters):
    filtered = filter_data(data, filters)
    return f"{filtered[filtered.tmdb_type == 'tv'].tmdb_id.nunique():,}"


# Figure callbacks
@app.callback(
    Output({"type": "graph", "index": "summary"}, "figure"),
    Input("filters-store", "data"),
)
def summary_figure(filters):
    filtered = filter_data(data, filters)
    filtered = (
        filtered.groupby("streaming_source_name")
        .agg(
            {
                "tmdb_id": "count",
                "popularity": lambda x: round(np.mean(x), 2),
                "vote_average": lambda x: round(np.mean(x), 2),
            }
        )
        .reset_index()
    )
    filtered.columns = [
        "Platform",
        "Title Count",
        "Average Popularity",
        "Average Rating",
    ]

    figure = px.scatter(
        filtered,
        x="Title Count",
        y="Average Rating",
        size="Average Popularity",
        color="Average Popularity",
        hover_name="Platform",
        template="plotly_white",
        color_continuous_scale="dense",
    )

    return figure


@app.callback(
    Output({"type": "graph", "index": "title-counts"}, "figure"),
    Input("filters-store", "data"),
)
def title_count_figure(filters):
    filtered = filter_data(data, filters)
    platform_order = (
        filtered.streaming_source_name.value_counts().index.tolist()
    )  # consistent order across figures

    filtered = (
        filtered.groupby(["streaming_source_name"])
        .agg(
            {
                "tmdb_type": [
                    lambda x: (x == "movie").sum(),
                    lambda x: (x == "tv").sum(),
                ],
                "tmdb_id": "count",
            }
        )
        .reset_index()
    )
    filtered.columns = ["Platform", "Movies", "TV Shows", "Total"]
    filtered.sort_values("Total", ascending=True, inplace=True)

    figure = go.Figure(
        data=[
            go.Bar(
                y=filtered.Movies,
                x=filtered.Platform,
                name="Movies",
                marker={"opacity": 0.9},
            ),
            go.Bar(
                y=filtered["TV Shows"],
                x=filtered.Platform,
                name="TV Shows",
                marker={"opacity": 0.9},
            ),
        ],
        layout=go.Layout(
            barmode="stack",
            template="plotly_white",
            margin={"t": 0, "b": 0},
            yaxis={"title": "Title Count"},
            xaxis={
                "categoryorder": "array",
                "categoryarray": platform_order,
                "tickangle": 45,
            },
            colorway=np.array(px.colors.sequential.dense)[[3, -3]].tolist(),
        ),
    )

    return figure


@app.callback(
    Output({"type": "graph", "index": "quality"}, "figure"),
    Input("filters-store", "data"),
)
def quality_figure(filters):
    filtered = filter_data(data, filters)
    platform_order = (
        filtered.streaming_source_name.value_counts().index.tolist()
    )  # consistent order across figures

    colors = px.colors.sequential.dense
    colors = colors * np.ceil(
        filtered.streaming_source_name.nunique() / len(colors)
    ).astype(int)
    figure = go.Figure(
        data=[
            go.Box(
                y=filtered[filtered.streaming_source_name == source].vote_average,
                name=source,
                jitter=0.3,
                boxpoints="outliers",
                marker_color=colors[i],
            )
            for i, source in enumerate(platform_order)
        ],
        layout=go.Layout(
            template="plotly_white",
            margin={"t": 0, "b": 0},
            yaxis={"title": "Rating"},
            xaxis={"tickangle": 45},
        ),
    )

    return figure


@app.callback(
    Output({"type": "graph", "index": "top-genre"}, "figure"),
    Input("filters-store", "data"),
)
def top_genre_figure(filters):
    filtered = filter_data(data, filters)
    platform_order = (
        filtered.streaming_source_name.value_counts().index.tolist()
    )  # consistent order across figures

    # counts for each genre for each platform
    filtered = filtered[
        ["streaming_source_name"] + [i for i in filtered.columns if "genre_" in i]
    ]
    filtered = filtered.groupby("streaming_source_name").sum().reset_index()
    filtered = filtered.melt(
        id_vars=["streaming_source_name"],
        value_vars=[i for i in filtered.columns if "genre_" in i],
        var_name="genre",
    )
    # sort/grab top 3 per platform
    filtered.sort_values("value", ascending=False, inplace=True)
    filtered = filtered.groupby("streaming_source_name").head(3)
    filtered.genre = filtered.genre.apply(lambda x: x.replace("genre_", ""))

    figure = px.bar(
        filtered,
        x="streaming_source_name",
        y="value",
        color="genre",
        barmode="stack",
        template="plotly_white",
        labels={"value": "Title Count", "streaming_source_name": "Platform"},
        color_discrete_sequence=px.colors.sequential.dense,
        category_orders={"streaming_source_name": platform_order},
    )
    figure.update_layout(xaxis_title=None, xaxis_tickangle=45)

    return figure


@app.callback(
    Output({"type": "graph", "index": "top-country"}, "figure"),
    Input("filters-store", "data"),
)
def top_country_figure(filters):
    filtered = filter_data(data, filters)

    # counts for each country for each platform
    filtered = filtered[
        ["streaming_source_name", "tmdb_type"]
        + [i for i in filtered.columns if "country_" in i]
    ]
    filtered = (
        filtered.groupby(["streaming_source_name", "tmdb_type"]).sum().reset_index()
    )
    filtered = filtered.melt(
        id_vars=["streaming_source_name", "tmdb_type"],
        value_vars=[i for i in filtered.columns if "country_" in i],
        var_name="country",
    )

    filtered.sort_values("value", ascending=False, inplace=True)
    filtered = filtered.groupby(["streaming_source_name", "tmdb_type"]).head(3)
    filtered.country = filtered.country.apply(
        lambda x: country_codes.get(x.replace("country_", ""))
    )

    figure = px.treemap(
        filtered,
        path=[px.Constant("All"), "streaming_source_name", "tmdb_type", "country"],
        values="value",
        color_discrete_sequence=px.colors.sequential.dense_r,
    )

    return figure


@app.callback(
    Output({"type": "graph", "index": "recent-content"}, "figure"),
    Input("filters-store", "data"),
)
def top_country_figure(filters):
    filtered = filter_data(data, filters)
    platform_order = (
        filtered.streaming_source_name.value_counts().index.tolist()
    )  # consistent order across figures

    # titles released over the past 15 years
    filtered = (
        filtered[filtered.year >= 2010]
        .groupby(["streaming_source_name", "year"])
        .tmdb_id.count()
        .reset_index()
    )

    figure = px.bar(
        filtered,
        x="year",
        y="tmdb_id",
        color="streaming_source_name",
        barmode="group",
        template="plotly_white",
        color_discrete_sequence=px.colors.sequential.dense,
        category_orders={"streaming_source_name": platform_order},
        labels={
            "streaming_source_name": "Platform",
            "tmdb_id": "Title Count",
            "year": "Relase Year",
        },
    )
    return figure


if __name__ == "__main__":
    if os.environ.get("environment") == "heroku":
        app.run(debug=False)
    else:
        app.run(debug=True)
