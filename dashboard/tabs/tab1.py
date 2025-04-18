import pandas as pd

from dash import dcc
from dash import html
import plotly.graph_objs as go
import dash.dependencies as ddep
import dash_bootstrap_components as dbc
import plotly.express as px

from app import app, db

tab1_layout = html.Div([
    html.H2("Analyse d'un Cours Boursiers"),

    dbc.Row([
        dbc.Col([
            html.Label("Choisir une action (pour chandeliers / bandes de Bollinger) :"),
            dcc.Dropdown(
                id='single-stock-dropdown',
                options=[{'label': name, 'value': name} for name in db.df_query("SELECT DISTINCT name FROM companies")['name']],
                multi=False
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
                id='chart-type-radio',
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
                options=[{'label': name, 'value': name} for name in db.df_query("SELECT DISTINCT name FROM companies")['name']],
                multi=True
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
                id='chart-type-radio',
                options=[
                    {'label': 'Ligne', 'value': 'line'},
                    {'label': 'Chandeliers', 'value': 'candlestick'}
                ],
                value='line',
                labelStyle={'display': 'inline-block', 'margin-right': '10px'}
            ),
        ], width=6),
    ], className='my-3'),

    html.Hr(),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='multi-stock-graph')
        ])
    ]),
])
@app.callback(
    ddep.Output('single-stock-graph', 'figure'),
    ddep.Input('single-stock-dropdown', 'value'),
    ddep.Input('date-range-picker', 'start_date'),
    ddep.Input('date-range-picker', 'end_date'),
    ddep.Input('chart-type-radio', 'value')
)
def update_single_stock_graph(selected_stock, start_date, end_date, chart_type):
    if selected_stock is None:
        return go.Figure()

    query = f"""
    SELECT date, open, close, high, low
    FROM stocks
    WHERE name = '{selected_stock}'
    AND date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    df = db.df_query(query)

    if chart_type == 'line':
        fig = px.line(df, x='date', y='close', title=f'Prix de Clôture de {selected_stock}')
        fig.update_traces(line=dict(width=2))
    else:
        fig = go.Figure(data=[go.Candlestick(x=df['date'],
                                             open=df['open'],
                                             high=df['high'],
                                             low=df['low'],
                                             close=df['close'])])
        fig.update_layout(title=f'Chandeliers de {selected_stock}')

    return fig
@app.callback(
    ddep.Output('multi-stock-graph', 'figure'),
    ddep.Input('multi-stock-dropdown', 'value'),
    ddep.Input('date-range-picker', 'start_date'),
    ddep.Input('date-range-picker', 'end_date')
)
def update_multi_stock_graph(selected_stocks, start_date, end_date):
    if not selected_stocks:
        return go.Figure()

    

    query = f"""
    SELECT date, name, close
    FROM stocks
    WHERE name IN ({', '.join([f"'{stock}'" for stock in selected_stocks])})
    AND date BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY date
    """
    df = db.df_query(query)

    fig = px.line(df, x='date', y='close', color='name', title='Prix de Clôture des Actions Sélectionnées')
    fig.update_traces(line=dict(width=2))
    fig.update_layout(legend_title_text='Actions')
    fig.update_xaxes(title_text='Date')
    fig.update_yaxes(title_text='Prix de Clôture')
    fig.update_layout(legend=dict(x=0, y=1, traceorder='normal', orientation='h'))
    fig.update_layout(xaxis_rangeslider_visible=True)
    fig.update_layout(xaxis_rangeslider=dict(visible=True, thickness=0.05))
    fig.update_layout(xaxis_rangeslider_thickness=0.05)
    fig.update_layout(xaxis_rangeslider_bordercolor='black')
    fig.update_layout(xaxis_rangeslider_borderwidth=2)
    fig.update_layout(xaxis_rangeslider_bgcolor='lightgrey')
    fig.update_layout(xaxis_rangeslider_activecolor='blue')
    fig.update_layout(xaxis_rangeslider_activebordercolor='blue')
    fig.update_layout(xaxis_rangeslider_activeborderwidth=2)
    fig.update_layout(xaxis_rangeslider_activebgcolor='lightgrey')