import dash_bootstrap_components as dbc
from dash import Dash, html

app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
    ],
)
server = app.server

app.layout = html.Div(
    [
        html.H1("Hello World")
    ],
)

if __name__ == "__main__":
    app.run()
