import dash_bootstrap_components as dbc
from dash import html, Output, Input, callback, State, dcc
import dash
import dash_ag_grid as dag
import ocha_stratus as stratus
from datetime import datetime, timedelta


NAVBAR_HEIGHT = 60
GUTTER = 15


app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)
server = app.server
app.title = "IPC Data Pipeline"


def disclaimer_modal():
    return dbc.Modal(
        [
            dbc.ModalHeader(
                dbc.ModalTitle("Disclaimer", className="header"),
                close_button=True,
            ),
            dbc.ModalBody(
                [
                    dcc.Markdown(
                        """
                        This is an internal tool under development. For any enquiries please
                        contact the OCHA Centre for Humanitarian Data via Hannah Ker or Giulia Martini at
                        [hannah.ker@un.org](mailto:hannah.ker@un.org) and [giulia.martini@un.org](mailto:giulia.martini@un.org).
                        """
                    ),
                ]
            ),
        ],
        id="modal",
        is_open=True,
        centered=True,
    )


def ag_grid():
    dataTypeDefinitions = {
        "percentage": {
            "extendsDataType": "number",
            "baseDataType": "number",
            "valueFormatter": {
                "function": "params.value == null ? '' :  d3.format(',.1%')(params.value)"
            },
        }
    }
    return dag.AgGrid(
        id="data-grid",
        style={
            "height": f"calc(100vh - {NAVBAR_HEIGHT + (GUTTER * 2)}px)",
            "margin": "15px",
            "width": f"calc(100vw - {GUTTER * 2}px)",
        },
        dashGridOptions={"dataTypeDefinitions": dataTypeDefinitions},
    )


def sidebar_controls():
    return html.Div(
        [
            html.Div(
                [
                    dcc.Markdown(
                        """
                    This application displays processed IPC data for use by CERF  to identify year-on-year changes
                    in food security across countries. See
                    [here](https://docs.google.com/document/d/15o6f5yPIl3p3sj7NNw2MoHg6f7DHzwtCPfKRlfU2PJE/edit?tab=t.0#heading=h.ieffsjdjd8lt)
                    for an overview of the methods and description of each column. Data is updated **daily**.
                    """,
                        style={"marginBottom": "7px"},
                    )
                ]
            ),
            html.Hr(),
            html.Div(
                [
                    html.P("Select severity level:"),
                    dbc.Select(
                        id="severity-dropdown",
                        options=["3", "3+", "4", "4+", "5"],
                        value="3+",
                        className="mb-3",
                    ),
                ]
            ),
            html.Div(
                [
                    dbc.Button("Download to CSV", color="primary", id="csv-button"),
                ],
                className="d-grid gap-2",
                style={"marginBottom": "10px"},
            ),
            html.Div(
                [
                    dbc.Button(
                        "Download Hunger Periods",
                        color="secondary",
                        id="reference-download-button",
                    ),
                    dcc.Download(id="reference-download"),
                ],
                className="d-grid gap-2",
            ),
            # Spacer div to push the disclaimer to the bottom
            html.Div(style={"flex": "1"}),
            html.Div(
                [
                    dcc.Markdown(
                        """
                        **Disclaimer**

                        This is an internal tool, not intended for public distribution or use.
                        """,
                    ),
                ],
                style={
                    "padding": "10px",
                    "backgroundColor": "#f8f9fa",
                },
            ),
        ],
        style={
            "width": "25vw",
            "padding": "20px",
            "backgroundColor": "#ffffff",
            "height": "calc(100vh - 60px)",
            "display": "flex",
            "flexDirection": "column",
        },
    )


def navbar(title):
    return dbc.Navbar(
        dbc.Container(
            [
                html.A(
                    dbc.Row(
                        [
                            dbc.Col(
                                dbc.NavbarBrand(
                                    title,
                                    className=["ms-2", "header", "bold"],
                                )
                            ),
                        ],
                        align="center",
                        className="g-0",
                    ),
                    href="https://centre.humdata.org/data-science/",
                    style={"textDecoration": "none"},
                ),
                dbc.Col(
                    html.Img(
                        src="assets/UN OCHA Logo_hor-blu660_white.png",
                        height=30,
                        style={
                            "position": "absolute",
                            "right": "140px",
                            "top": "15px",
                        },
                    ),
                ),
                dbc.Col(
                    html.Img(
                        src="assets/centreforHumdata_white_TransparentBG.png",
                        height=30,
                        style={
                            "position": "absolute",
                            "right": "15px",
                            "top": "15px",
                        },
                    ),
                ),
            ],
            fluid=True,
        ),
        style={
            "height": f"{NAVBAR_HEIGHT}px",
            "margin": "0px",
            "padding": "10px",
        },
        color="#1bb580ff",
        dark=True,
    )


layout = [
    navbar(title="IPC Data Pipeline"),
    disclaimer_modal(),
    html.Div(
        [sidebar_controls(), ag_grid()],
        style={"display": "flex", "flexDirection": "row"},
    ),
]


app.layout = html.Div(layout)


@callback(
    Output("data-grid", "exportDataAsCsv"),
    Output("data-grid", "csvExportParams"),
    Input("csv-button", "n_clicks"),
    State("severity-dropdown", "value"),
)
def export_data_as_csv(n_clicks, severity):
    now = datetime.now()
    now_formatted = now.strftime("%Y-%m-%d")
    if n_clicks:
        return True, {
            "fileName": f"annualized_ipc_conditions_{severity}_{now_formatted}_TEST.csv"
        }
    return False, {}


@callback(
    Output("reference-download", "data"),
    Input("reference-download-button", "n_clicks"),
    prevent_initial_call=True,
)
def download_hunger_period_reference(n_clicks):
    df = stratus.load_csv_from_blob(
        "ds-ufe-food-security/processed/reference_periods/cleaned_reference_periods.csv"
    )
    if n_clicks:
        return dcc.send_data_frame(df.to_csv, "reference_hunger_periods.csv")
    return dash.no_update


@callback(
    Output("data-grid", "rowData"),
    Output("data-grid", "columnDefs"),
    Input("severity-dropdown", "value"),
)
def load_data(severity):
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_formatted = yesterday.strftime("%Y-%m-%d")
    df = stratus.load_csv_from_blob(
        f"ds-ufe-food-security/processed/ipc_updates/annualized_ipc_summary_{severity}_{yesterday_formatted}.csv"
    )

    column_defs = [{"field": i} for i in df.columns]
    styled_column_defs = []
    for col_def in column_defs:
        if "Percentage" in col_def["field"]:
            col_def["cellStyle"] = {
                "function": "params.value && {'backgroundColor': 'rgb(242,100,90,' + params.value/1 + ')'}"
            }
            col_def["cellDataType"] = "percentage"
        elif "Change" in col_def["field"]:
            col_def["cellStyle"] = {
                "styleConditions": [
                    {
                        "condition": "params.value < 0",
                        "style": {"color": "#18998f", "fontWeight": "bold"},
                    },
                    {
                        "condition": "params.value > 0",
                        "style": {"color": "#c25048", "fontWeight": "bold"},
                    },
                ],
            }
            col_def["cellDataType"] = "percentage"
        elif "Overlap" in col_def["field"]:
            col_def["cellStyle"] = {
                "styleConditions": [
                    {
                        "condition": "params.value == 0",
                        "style": {"backgroundColor": "#f7a29c"},
                    },
                ],
            }
            col_def["cellDataType"] = "percentage"
        elif col_def["field"] == "Country":
            col_def["pinned"] = "left"
        elif "Number" in col_def["field"]:
            col_def["valueFormatter"] = {"function": "d3.format(',.0f')(params.value)"}

        styled_column_defs.append(col_def)
    return df.to_dict("records"), styled_column_defs


if __name__ == "__main__":
    app.run(debug=True)
