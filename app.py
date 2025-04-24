import dash_bootstrap_components as dbc
from dash import html, Output, Input, callback, State, dcc
import dash
import dash_ag_grid as dag
import ocha_stratus as stratus


NAVBAR_HEIGHT = 60
GUTTER = 15


app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)
server = app.server
app.title = "IPC Data Pipeline"


def ag_grid():
    return dag.AgGrid(
        id="data-grid",
        style={
            "height": f"calc(100vh - {NAVBAR_HEIGHT + (GUTTER * 2)}px)",
            "margin": "15px",
            "width": f"calc(100vw - {GUTTER * 2}px)",
        },
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
                    for an overview of the methods and description of each column.
                    """,
                        style={"marginBottom": "7px"},
                    )
                ]
            ),
            html.Div(
                [
                    html.P("Select severity level:"),
                    dbc.Select(
                        id="severity-dropdown",
                        options=["3+", "4", "5"],
                        value="3+",
                        className="mb-3",
                    ),
                ]
            ),
            html.Div(
                [
                    html.P("Select date updated:"),
                    dbc.Select(
                        id="date-dropdown",
                        # TODO: This is hard coded
                        options=["2025-04-24"],
                        value="2025-04-24",
                        className="mb-3",
                    ),
                ]
            ),
            html.Div(
                [
                    dbc.Button("Download to CSV", color="primary", id="csv-button"),
                ],
                className="d-grid gap-2",
            ),
            html.Hr(),
            html.Div(
                [
                    dcc.Markdown(
                        """
                    For reference, historical IPC data has been processed to estimate a single peak hunger period per country.
                    Overlap with these periods are reported in the `*_overlap` columns of the table visualized here.
                    """,
                        style={"marginBottom": "7px"},
                    )
                ]
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
        ],
        style={
            "width": "25vw",
            "padding": "20px",
            "backgroundColor": "#ffffff",
            "height": "calc(100vh - 60px)",
            "overflowY": "auto",
        },
    )


def navbar(title):
    return html.Div(
        [
            dbc.NavbarBrand(
                title,
                style={"margin": "0"},
                className=["ms-2", "header", "bold"],
            ),
        ],
        style={
            "backgroundColor": "#1bb580ff",
            "color": "white",
            "height": f"{NAVBAR_HEIGHT}px",
            "padding": "10px",
            "display": "flex",
            "justifyContent": "space-between",
            "alignItems": "center",
        },
    )


layout = [
    navbar(title="IPC Data Pipeline"),
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
    State("date-dropdown", "value"),
)
def export_data_as_csv(n_clicks, severity, date):
    if n_clicks:
        return True, {"fileName": f"annualized_ipc_conditions_{severity}_{date}.csv"}
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
    Input("date-dropdown", "value"),
)
def load_data(severity, date):
    df = stratus.load_csv_from_blob(
        f"ds-ufe-food-security/processed/ipc_updates/annualized_ipc_summary_2024_{severity}_{date}.csv"
    )

    column_defs = [{"field": i} for i in df.columns]
    styled_column_defs = []
    for col_def in column_defs:
        if "_percentage" in col_def["field"]:
            col_def["cellStyle"] = {
                "function": "params.value && {'backgroundColor': 'rgb(242,100,90,' + params.value/1 + ')'}"
            }
        elif "_change" in col_def["field"]:
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
        elif "_overlap" in col_def["field"]:
            col_def["cellStyle"] = {
                "styleConditions": [
                    {
                        "condition": "params.value == 0",
                        "style": {"backgroundColor": "#f7a29c"},
                    },
                ],
            }
        elif col_def["field"] == "Country":
            col_def["pinned"] = "left"

        styled_column_defs.append(col_def)
    return df.to_dict("records"), styled_column_defs


if __name__ == "__main__":
    app.run(debug=True)
