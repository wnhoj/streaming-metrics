import dash_bootstrap_components as dbc
from dash import html, dcc

with open("README.md", "r") as f:
    readme = f.read()

about = dbc.Modal(
    [dbc.ModalHeader(html.H1("About the App")), dbc.ModalBody(dcc.Markdown(readme, link_target="blank"))],
    id="about-modal",
    is_open=False,
    size="lg",
)
