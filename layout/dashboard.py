import json
import dash_bootstrap_components as dbc
from dash import html, dcc

from .components.MetricCard import MetricCard
from .components.FigureCard import FigureCard

with open("assets/figure_descriptions.json", "r") as f:
    figure_descriptions = json.load(f)

dashboard = dbc.Row(
    dbc.Col(
        [
            dbc.Row(
                [
                    dbc.Col(MetricCard("Platforms", id="platform-count"), width=4),
                    dbc.Col(MetricCard("Movies", id="movie-count"), width=4),
                    dbc.Col(MetricCard("TV Shows", id="tv-count"), width=4),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        FigureCard(
                            "Overview",
                            id="summary",
                            description=figure_descriptions.get("summary"),
                        ),
                        sm=12,
                        md=7,
                    ),
                    dbc.Col(
                        FigureCard(
                            "Catalog Size",
                            id="title-counts",
                            description=figure_descriptions.get("title-counts"),
                        ),
                        sm=12,
                        md=5,
                    ),
                ],
                className="dashboard-row",
            ),
            dbc.Row(
                dbc.Col(
                    FigureCard(
                        "Content Quality",
                        id="quality",
                        description=figure_descriptions.get("quality"),
                    ),
                    width=12,
                ),
                className="dashboard-row",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        FigureCard(
                            "Top Genres",
                            id="top-genre",
                            description=figure_descriptions.get("top-genre"),
                        ),
                        sm=12,
                        md=6,
                    ),
                    dbc.Col(
                        FigureCard(
                            "Top Countries",
                            id="top-country",
                            description=figure_descriptions.get("top-country"),
                        ),
                        sm=12,
                        md=6,
                    ),
                ],
                className="dashboard-row",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        FigureCard(
                            "Recent Content",
                            id="recent-content",
                            description=figure_descriptions.get("recent-content"),
                        ),
                        sm=12,
                        md=12,
                    ),
                ],
                className="dashboard-row",
            ),
        ],
    ),
    id="dashboard",
)
