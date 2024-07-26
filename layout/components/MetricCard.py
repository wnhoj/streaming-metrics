import dash
import dash_bootstrap_components as dbc
from dash import html, dcc


class MetricCard(dbc.Card):
    def __init__(
        self,
        title,
        id,
    ):
        super().__init__(
            children=[
                html.H1("-", id={"type": "metric-value", "index": id}),
                html.P(title, id={"type": "metric-text", "index": id}),
            ],
            body=True,
            className="mb-3",
        )
