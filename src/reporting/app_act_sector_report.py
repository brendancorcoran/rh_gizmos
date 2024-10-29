from datetime import datetime
from io import BytesIO

import dash
import dash_bootstrap_components as dbc
from dash import dcc, html, Output, Input
from dash.dependencies import State

from core.reporting.reporting_io import xlsx_sheets_io
from core.reporting.report_frags import gen_report_header, get_horizontal_line_row
from reporting.act_sector_report import generate_act_sectors_reports
from reporting.act_sector_report_main import logger

# IDS
ID_DOWNLOAD_ACTION = "download"
ID_XLSX_DOWNLOAD_SUBMIT = "download_button"
ID_GOVERNMENT_PUBLICATIONS_IDS = "government_publications_ids"
ID_OUTPUT_TEST = "out-all-types"
ID_GOVERNMENT_PUBLICATIONS_IDS_SUBMIT = "submit_button"


# Constants for layout_params keys
YAXIS_TITLE_KEY = "yaxis_title"
FONT_SIZE_KEY = "font_size"
FONT_COLOR_KEY = "font_color"
PLOT_BGCOLOR_KEY = "plot_bgcolor"
PAPER_BGCOLOR_KEY = "paper_bgcolor"
LINE_WIDTH_KEY = "line_width"
MARKER_SIZE_KEY = "marker_size"

BGCOLOR = "#0F1933"


def container_act_sector_report():
    dbc_act_sector_report_container = dbc.Container(
        children=[
            *gen_report_header("Act to Sector Report"),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Input(
                            id=ID_GOVERNMENT_PUBLICATIONS_IDS,
                            type="text",
                            placeholder="Enter Government Publication IDs",
                            style={
                                "width": "100%",
                                "padding": "10px",
                                "fontSize": "16px",
                                "marginRight": "10px",
                                "borderRadius": "5px",
                                "border": "1px solid #ccc",
                            },
                        ),
                        width=9,
                    ),
                    dbc.Col(
                        dbc.Button(
                            "Generate",
                            id=ID_GOVERNMENT_PUBLICATIONS_IDS_SUBMIT,
                            color="primary",
                            n_clicks=0,
                            style={"width": "100%"},
                        ),
                        width=3,
                    ),
                ],
                justify="end",
                className="mb-3",
            ),
            dcc.Download(id=ID_DOWNLOAD_ACTION),
            get_horizontal_line_row(),
        ],
        fluid=True,
    )
    return dbc_act_sector_report_container


def generate_act_sector_report(gp_ids: list[int]):
    act_sector_reports = generate_act_sectors_reports(gp_ids)
    buffer = BytesIO()
    xlsx_sheets_io(act_sector_reports, buffer)
    buffer.seek(0)  # Reset buffer position to the start
    return buffer.getvalue()


def main():
    external_stylesheets = [
        dbc.themes.SOLAR,
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css",
        "https://cdn.jsdelivr.net/npm/bootstrap-icons/font/bootstrap-icons.css",
        "https://unpkg.com/govicons@latest/css/govicons.min.css",
        "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.6.0/css/all.min.css",
    ]
    app = dash.Dash(
        __name__,
        external_stylesheets=external_stylesheets,
        suppress_callback_exceptions=True,
    )

    # App layout with navigation
    colors = {"background": "#0F1933", "color": "#FFFFFF"}

    app.layout = html.Div(
        style={
            "background-color": colors["background"],
            "color": colors["color"],
            "--bs-body-color": colors["color"],
        },
        children=[dcc.Location(id="url", refresh=False), html.Div(id="page-content")],
    )

    # Update page content based on the URL
    @app.callback(Output("page-content", "children"), [Input("url", "pathname")])
    def display_page(pathname):

        # strip leading "/"
        if pathname is not None and pathname.startswith("/"):
            pathname = pathname[1:]

        logger.info(f"Pathname: {pathname}")

        container_home = dbc.Container(
            children=[*gen_report_header("Act - Sector Report")]
        )
        container = container_act_sector_report()
        return container

    @app.callback(
        Output(ID_DOWNLOAD_ACTION, "data"),
        Input(ID_GOVERNMENT_PUBLICATIONS_IDS_SUBMIT, "n_clicks"),
        State(ID_GOVERNMENT_PUBLICATIONS_IDS, "value"),
        prevent_initial_call=True,
    )
    def handle_generate_report_submit(n_clicks, gp_ids):
        if gp_ids:
            # split the input value by comma, trim, and convert to int
            gp_ids = [int(x.strip()) for x in gp_ids.split(",")]

            report = generate_act_sector_report(gp_ids)
            current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"act_sector_report_{current_datetime}.xlsx"
            return dcc.send_bytes(report, report_filename)
        return None


    # Run the app
    app.run_server(host= '0.0.0.0',debug=False)


if __name__ == "__main__":
    main()