from dash import dcc, html, dash_table
import dash.dependencies as ddep
import dash_bootstrap_components as dbc

from app import app, db

# On récupère les noms ET les ids pour le dropdown
companies = db.df_query("SELECT id, name FROM companies")

tab2_layout = html.Div([
    html.H2("Données Brutes des Cours"),

    dbc.Row([
        dbc.Col([
            html.Label("Choisir une action :"),
            dcc.Dropdown(
                id='stock-dropdown',
                options=[],
                multi=False
            ),
        ], width=6),

        dbc.Col([
            html.Label("Période :"),
            dcc.DatePickerRange(
                id='date-range-picker-table',
                start_date_placeholder_text="Date de début",
                end_date_placeholder_text="Date de fin"
            ),
        ], width=6),
    ], className='my-3'),

    html.Hr(),

    dash_table.DataTable(
        id='stock-data-table',
        columns=[
            {"name": "début", "id": "open"},
            {"name": "fin", "id": "close"},
            {"name": "max", "id": "high"},
            {"name": "min", "id": "low"},
            {"name": "mean", "id": "mean"},
            {"name": "std", "id": "std"},
        ],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'left'},
        page_size=20,
    )
])
@app.callback(
    ddep.Output('stock-dropdown', 'options'),
    ddep.Input('tabs-example', 'active_tab')
)
def load_stock_options(active_tab):
    if active_tab != 'tab-2':
        return []

    df = db.df_query("""
        SELECT id, name
        FROM companies
        JOIN daystocks ON daystocks.cid = id
        GROUP BY id, name
    """)
    
    options = [{'label': row['name'], 'value': row['id']} for _, row in df.iterrows()]
    print("✅ Options 'dropdown' chargées dynamiquement :", options)
    return options

@app.callback(
    ddep.Output('stock-data-table', 'data'),
    ddep.Input('stock-dropdown', 'value'),
    ddep.Input('date-range-picker-table', 'start_date'),
    ddep.Input('date-range-picker-table', 'end_date')
)
def update_stock_data_table(selected_stock_id, start_date, end_date):
    if not selected_stock_id or not start_date or not end_date:
        return []

    query = """
        SELECT d.open, d.close, d.high, d.low, d.mean, d.std
        FROM daystocks d
        JOIN companies c ON d.cid = c.id
        WHERE c.id = '%s' AND d.date BETWEEN '%s' AND '%s'
    """

    df = db.df_query(query, (selected_stock_id, start_date, end_date))

    if df.empty:
        print("c'est vide")
        return []

    return df.fillna("").to_dict('records')

