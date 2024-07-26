import dash_bootstrap_components as dbc
from dash import html, dcc

filters = dbc.Row(
    dbc.Col(
        dbc.Card(
            [
                dbc.CardHeader(
                    [
                        dbc.Tooltip(
                            "Click to show filters",
                            id="filter-tooltip",
                            placement="left",
                            target="filter-header-btn",
                        ),
                        dbc.Button(
                            [
                                html.P("Filters", className="m-0"),
                                html.Span(
                                    "keyboard_arrow_down",
                                    id="filter-header-icon",
                                    className="material-symbols-outlined",
                                ),
                            ],
                            id="filter-header-btn",
                            className="w-100 p-3 d-flex justify-content-between",
                            color="light",
                            n_clicks=0,
                        ),
                    ],
                    className="p-0 m-0",
                ),
                dbc.Collapse(
                    dbc.CardBody(
                        dbc.Stack(
                            [
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            [
                                                dbc.Label(
                                                    "Media Type",
                                                    html_for="media-type",
                                                ),
                                                dcc.Checklist(
                                                    ["Movie", "TV"],
                                                    ["Movie", "TV"],
                                                    id="media-type",
                                                    className="d-flex justify-content-evenly",
                                                    inline=True,
                                                ),
                                            ],
                                            md=3,
                                            sm=12,
                                        ),
                                        dbc.Col(
                                            [
                                                dbc.Label(
                                                    "Platform",
                                                    html_for="platform",
                                                ),
                                                dcc.Dropdown(
                                                    id="platform", multi=True, value=[]
                                                ),
                                            ],
                                            md=9,
                                            sm=12,
                                        ),
                                    ]
                                ),
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            [
                                                dbc.Label(
                                                    "Rating",
                                                    html_for="rating",
                                                ),
                                                dcc.RangeSlider(
                                                    0,
                                                    10,
                                                    0.5,
                                                    marks=None,
                                                    tooltip={
                                                        "placement": "bottom",
                                                        "always_visible": True,
                                                    },
                                                    id="rating",
                                                ),
                                            ],
                                            md=4,
                                            sm=12,
                                        ),
                                        dbc.Col(
                                            [
                                                dbc.Label(
                                                    "Release Year",
                                                    html_for="release-year",
                                                ),
                                                dcc.RangeSlider(
                                                    1902,
                                                    2024,
                                                    1,
                                                    marks=None,
                                                    tooltip={
                                                        "placement": "bottom",
                                                        "always_visible": True,
                                                    },
                                                    id="release-year",
                                                ),
                                            ],
                                            md=8,
                                            sm=12,
                                        ),
                                    ]
                                ),
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            [
                                                dbc.Label(
                                                    "Genre",
                                                    html_for="genre",
                                                ),
                                                dcc.Dropdown(
                                                    id="genre", multi=True, value=[]
                                                ),
                                            ],
                                            md=4,
                                            sm=12,
                                        ),
                                        dbc.Col(
                                            [
                                                dbc.Label(
                                                    "Language",
                                                    html_for="language",
                                                ),
                                                dcc.Dropdown(
                                                    id="language", multi=True, value=[]
                                                ),
                                            ],
                                            md=4,
                                            sm=12,
                                        ),
                                        dbc.Col(
                                            [
                                                dbc.Label(
                                                    "Country",
                                                    html_for="country",
                                                ),
                                                dcc.Dropdown(
                                                    id="country", multi=True, value=[]
                                                ),
                                            ],
                                            md=4,
                                            sm=12,
                                        ),
                                    ]
                                ),
                                dbc.Row(
                                    dbc.Col(
                                        dbc.Button(
                                            "Clear Filters",
                                            id="clear-filters-btn",
                                            color="link",
                                            n_clicks=0,
                                        ),
                                        className="d-flex justify-content-end",
                                    )
                                ),
                            ],
                            gap=3,
                        )
                    ),
                    id="filter-collapse",
                    is_open=False,
                ),
            ]
        )
    ),
    id="filters",
)
