import dash_bootstrap_components as dbc
from dash import html


def gen_report_header(title: str):
    width_px = "450px"
    report_header = dbc.Row([
        dbc.Col(html.Img(src="static/reghedge_logo_oxford_blue.png", style={"width": width_px}), style={"text-align": "center"}),
    ], align="center", justify="center", className="mb-1")
    report_title = dbc.Row([
        dbc.Col(html.Hr(style={"borderWidth": "4px"})),
        dbc.Col(html.H1(title, style={"font-size": "30px"},  className="text-center")),
        dbc.Col(html.Hr(style={"borderWidth": "4px"})),
    ], align="center", justify="center", className="mb-5")
    return [report_header, report_title]


def get_horizontal_line_row():
    return dbc.Row(
        dbc.Col(html.Hr(style={"borderWidth": "4px"}), width=12),
        className="my-4",  # Adds some vertical margin
    )
