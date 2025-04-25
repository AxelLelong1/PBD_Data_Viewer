import pandas as pd

from dash import dcc
from dash import html
import plotly.graph_objs as go
import dash.dependencies as ddep
import dash_bootstrap_components as dbc
import plotly.express as px

from app import app, db
companies = db.df_query("""
    SELECT companies.id AS cid, companies.name
    FROM companies
    INNER JOIN stocks ON stocks.cid = companies.id
    GROUP BY companies.id, companies.name
""")

# Debug : Vérifie les données récupérées
print("🔍 Companies disponibles :", companies)

tab1_layout = html.Div([
    html.H2("Analyse d'un Cours Boursiers"),

    dbc.Row([
        dbc.Col([
            html.Label("Choisir une action (pour chandeliers / bandes de Bollinger) :"),
            dcc.Dropdown(
                id='single-stock-dropdown',
                options=[],
                multi=False,
                placeholder="Sélectionne une action..."
            ),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            html.Label("Période :"),
            dcc.DatePickerRange(
                id='date-range-picker',
                start_date_placeholder_text="Date de début",
                end_date_placeholder_text="Date de fin"
            ),
        ], width=6),

        dbc.Col([
            html.Label("Type de graphique :"),
            dcc.RadioItems(
                id='chart-type-radio-single',
                options=[
                    {'label': 'Ligne', 'value': 'line'},
                    {'label': 'Chandeliers', 'value': 'candlestick'}
                ],
                value='line',
                labelStyle={'display': 'inline-block', 'margin-right': '10px'}
            ),
        ], width=6),
    ], className='my-3'),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='single-stock-graph')
        ])
    ]),

    html.Hr(),
    html.H2("Graphiques des Cours Boursiers"),
    dbc.Row([
        dbc.Col([
            html.Label("Sélectionnez une ou plusieurs actions :"),
            dcc.Dropdown(
                id='multi-stock-dropdown',
                options=[],
                multi=True
            ),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            html.Label("Période :"),
            dcc.DatePickerRange(
                id='date-range-picker-multi',
                start_date_placeholder_text="Date de début",
                end_date_placeholder_text="Date de fin"
            ),
        ], width=6),
    ], className='my-3'),

    html.Hr(),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='multi-stock-graph')
        ])
    ]),

    html.Hr(),
    html.H2("Graphiques Bollinger des Cours Boursiers"),
    dbc.Row([
        dbc.Col([
            html.Label("Sélectionnez une action :"),
            dcc.Dropdown(
                id='bollinger-stock-dropdown',
                options=[],
                multi=False,
            ),
        ], width=6),
    ], className='my-3'),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='bollinger-stock-graph')
        ])
    ]),
])
@app.callback(
    ddep.Output('single-stock-dropdown', 'options'),
    ddep.Output('multi-stock-dropdown', 'options'),
    ddep.Output('bollinger-stock-dropdown', 'options'),
    ddep.Input('tabs-example', 'active_tab')
)
def load_single_stock_options(active_tab):
    if active_tab != 'tab-1':
        return [],[], []

    df = db.df_query("""
        SELECT id, name
        FROM companies
        JOIN stocks ON stocks.cid = id
        GROUP BY id, name
    """)
    
    options = [{'label': row['name'], 'value': row['id']} for _, row in df.iterrows()]
    print("✅ Options 'dropdown' chargées dynamiquement :", options)
    return options, options, options

@app.callback(
    ddep.Output('single-stock-graph', 'figure'),
    ddep.Input('single-stock-dropdown', 'value'),
    ddep.Input('date-range-picker', 'start_date'),
    ddep.Input('date-range-picker', 'end_date'),
    ddep.Input('chart-type-radio-single', 'value'),
)
def update_single_stock_graph(selected_stock_id, start_date, end_date, chart_type):
    if not selected_stock_id or not start_date or not end_date:
        print(f"parametre empty.{selected_stock_id}, {start_date}, {end_date}")
        return go.Figure()  # Graph vide
    
    query = """
        SELECT date, cid, value, volume
        FROM stocks
        JOIN companies ON stocks.cid = companies.id
        WHERE companies.id = '%s'
        AND date BETWEEN '%s' AND '%s'
        ORDER BY date
    """
    df = db.df_query(query, (selected_stock_id, start_date, end_date))

    query = """
        SELECT d.date AS date, d.open AS open, d.close AS close, d.high AS high, d.low AS low
        FROM daystocks d
        JOIN companies c ON d.cid = c.id
        WHERE c.id = '%s' AND d.date BETWEEN '%s' AND '%s'
    """

    df2 = db.df_query(query, (selected_stock_id, start_date, end_date))

    if df.empty:
        print("Aucune donnée trouvée pour cette action et cette période.")
        return go.Figure()

    
    if chart_type == 'line':
        fig = px.line(df, x='date', y='value')
        fig.update_traces(line=dict(width=2))
        fig.update_layout(yaxis_type='log')

    else:
        fig = go.Figure(data=[go.Candlestick(x=df2['date'],
                                            open=df2['open'],
                                            high=df2['high'],
                                            low=df2['low'],
                                            close=df2['close'])])
        fig.update_layout(title=f'Chandeliers de {selected_stock_id}', yaxis_type='log')

    return fig
@app.callback(
    ddep.Output('multi-stock-graph', 'figure'),
    ddep.Input('multi-stock-dropdown', 'value'),
    ddep.Input('date-range-picker-multi', 'start_date'),
    ddep.Input('date-range-picker-multi', 'end_date')
)
def update_multi_stock_graph(selected_stocks_id, start_date, end_date):
    if not selected_stocks_id or not start_date or not end_date:
        return go.Figure()

    placeholders = ', '.join(['%s'] * len(selected_stocks_id))
    query = f"""
        SELECT stocks.date, companies.name, stocks.value, stocks.volume
        FROM stocks
        JOIN companies ON stocks.cid = companies.id
        WHERE companies.id IN ({placeholders})
        AND stocks.date BETWEEN '%s' AND '%s'
        ORDER BY stocks.date
    """
    params = selected_stocks_id + [start_date, end_date]
    df = db.df_query(query, tuple(params))

    if df.empty:
        return go.Figure()

    fig = px.line(df, x='date', y='value', color='name')
    fig.update_traces(line=dict(width=2))
    fig.update_layout(
        legend_title_text='Actions',
        yaxis_type='log',
        xaxis_title='Date',
        yaxis_title='Prix de Clôture',
        legend=dict(x=0, y=1, traceorder='normal', orientation='h'),
        xaxis_rangeslider_visible=True,
        xaxis_rangeslider=dict(
            visible=True,
            thickness=0.05,
            bgcolor='lightgrey',
            bordercolor='black',
            borderwidth=2
        ),
    )

    return fig
@app.callback(
    ddep.Output('bollinger-stock-graph', 'figure'),
    ddep.Input('bollinger-stock-dropdown', 'value')
)
def update_bollinger_stock_graph(selected_stock_id):
    print("selected_stock_id", selected_stock_id)
    if not selected_stock_id:
        return go.Figure()

    query = """
        SELECT date, cid, value, volume
        FROM stocks
        JOIN companies ON stocks.cid = companies.id
        WHERE companies.id = '%s'
        ORDER BY date
    """

    df = db.df_query(query, (selected_stock_id))

    if df.empty:
        print("Aucune donnée trouvée pour cette action.")
        return go.Figure()

    df['ma20'] = df['value'].rolling(window=20).mean()
    df['std20'] = df['value'].rolling(window=20).std()
    df['upper'] = df['ma20'] + 2 * df['std20']
    df['lower'] = df['ma20'] - 2 * df['std20']

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df['date'], y=df['value'],
        mode='lines', name='Cours', line=dict(width=2)
    ))

    fig.add_trace(go.Scatter(
        x=df['date'], y=df['ma20'],
        mode='lines', name='Moyenne mobile 20j', line=dict(dash='dot', color='blue')
    ))

    fig.add_trace(go.Scatter(
        x=df['date'], y=df['upper'],
        mode='lines', name='Bande Supérieure', line=dict(color='lightgray'),
        showlegend=True
    ))

    fig.add_trace(go.Scatter(
        x=df['date'], y=df['lower'],
        mode='lines', name='Bande Inférieure', line=dict(color='lightgray'),
        fill='tonexty',  # pour remplir entre les bandes
        fillcolor='rgba(200,200,200,0.2)'
    ))

    return fig