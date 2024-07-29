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

from utils.DataConnector import DataConnector


if os.environ.get("ENVIRONMENT") == "heroku":
    database_url = os.environ.get("DATABASE_URL")
    data_connector = DataConnector(
        demo=False, database_url=database_url.replace("postgres://", "postgresql://")
    )
    initial_platforms = [
        "Netflix",
        "Prime Video",
        "Hulu",
        "Peacock Premium",
        "AppleTV+",
        "Disney+",
        "Max",
        "Crunchyroll Premium",
    ]
else:
    data_connector = DataConnector(demo=True, database_url=None)
    initial_platforms = ["Peacock Premium", "Hulu", "Disney+", "Max"]


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
    platforms = data_connector.get_available_platforms()
    return [{"label": i, "value": i} for i in sorted(platforms.platform)]


@app.callback(Output("language", "options"), Input("language", "id"))
def populate_language_options(_):
    languages = data_connector.get_available_languages()
    return [{"label": i, "value": i} for i in sorted(languages.language)]


@app.callback(Output("genre", "options"), Input("genre", "id"))
def populate_genre_options(_):
    genres = data_connector.get_available_genres()
    return [{"label": i, "value": i} for i in sorted(genres.genre)]


@app.callback(Output("country", "options"), Input("country", "id"))
def populate_country_options(_):
    countries = data_connector.get_available_countries()
    return [{"label": i, "value": i} for i in sorted(countries.country)]


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
        return [["Movie", "TV"], initial_platforms, [0, 10], [1902, 2024], [], [], []]
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
    return data_connector.get_platform_count(filters)


@app.callback(
    Output({"type": "metric-value", "index": "movie-count"}, "children"),
    Input("filters-store", "data"),
)
def display_movie_count(filters):
    return f"{data_connector.get_movie_count(filters):,}"


@app.callback(
    Output({"type": "metric-value", "index": "tv-count"}, "children"),
    Input("filters-store", "data"),
)
def display_tv_count(filters):
    return f"{data_connector.get_tv_count(filters):,}"


# Figure callbacks
@app.callback(
    Output({"type": "graph", "index": "summary"}, "figure"),
    Input("filters-store", "data"),
)
def summary_figure(filters):
    data = data_connector.get_overview_data(filters)

    figure = px.scatter(
        data,
        x="title_count",
        y="average_rating",
        size="average_popularity",
        color="average_popularity",
        hover_name="platform",
        template="plotly_white",
        color_continuous_scale="dense",
        labels={
            "platform": "Platform",
            "title_count": "Title Count",
            "average_rating": "Average Rating",
            "average_popularity": "Average Popularity",
        },
    )

    return figure


@app.callback(
    Output({"type": "graph", "index": "title-counts"}, "figure"),
    Input("filters-store", "data"),
)
def title_count_figure(filters):
    data = data_connector.get_title_count_data(filters)
    platform_order = [
        i for i in data_connector.platform_order if i in set(data.platform)
    ]

    figure = go.Figure(
        data=[
            go.Bar(
                y=data.movies,
                x=data.platform,
                name="Movies",
                marker={"opacity": 0.9},
            ),
            go.Bar(
                y=data.tv,
                x=data.platform,
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
                "tickangle": 45,
                "categoryorder": "array",
                "categoryarray": platform_order,
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
    data = data_connector.get_quality_data(filters)
    platform_order = [
        i for i in data_connector.platform_order if i in set(data.platform)
    ]

    colors = px.colors.sequential.dense
    colors = colors * np.ceil(data.platform.nunique() / len(colors)).astype(int)

    figure = go.Figure(
        data=[
            go.Box(
                y=data[data.platform == source].vote_average,
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
            xaxis={
                "tickangle": 45,
                "categoryorder": "array",
                "categoryarray": platform_order,
            },
        ),
    )

    return figure


@app.callback(
    Output({"type": "graph", "index": "top-genre"}, "figure"),
    Input("filters-store", "data"),
)
def top_genre_figure(filters):
    data = data_connector.get_top_genre_data(filters)
    platform_order = [
        i for i in data_connector.platform_order if i in set(data.platform)
    ]

    figure = px.bar(
        data,
        x="platform",
        y="title_count",
        color="genre",
        barmode="stack",
        template="plotly_white",
        labels={"title_count": "Title Count", "platform": "Platform"},
        color_discrete_sequence=px.colors.sequential.dense,
        category_orders={"platform": platform_order},
    )
    figure.update_layout(xaxis_title=None, xaxis_tickangle=45)

    return figure


@app.callback(
    Output({"type": "graph", "index": "top-country"}, "figure"),
    Input("filters-store", "data"),
)
def top_country_figure(filters):
    data = data_connector.get_top_country_data(filters)

    figure = px.treemap(
        data,
        path=[px.Constant("All"), "platform", "media_type", "country"],
        values="title_count",
        color_discrete_sequence=px.colors.sequential.dense_r,
    )

    return figure


@app.callback(
    Output({"type": "graph", "index": "recent-content"}, "figure"),
    Input("filters-store", "data"),
)
def recent_content_figure(filters):
    data = data_connector.get_recent_content_data(filters)
    platform_order = [
        i for i in data_connector.platform_order if i in set(data.platform)
    ]

    figure = px.bar(
        data.dropna(),
        x="release_year",
        y="title_count",
        color="platform",
        barmode="group",
        template="plotly_white",
        color_discrete_sequence=px.colors.sequential.dense,
        category_orders={"platform": platform_order},
        labels={
            "platform": "Platform",
            "title_count": "Title Count",
            "release_year": "Relase Year",
        },
    )
    return figure


if __name__ == "__main__":
    if os.environ.get("environment") == "heroku":
        app.run(debug=False)
    else:
        app.run(debug=True)
