import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import psycopg2
import pandas as pd
from prophet import Prophet
import os
from datetime import datetime

# Create a Dash web app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Analitika transakcija"
app.config.suppress_callback_exceptions = True

# Define the connection parameters for your PostgreSQL warehouse
db_params = {
    'host': 'postgres_warehouse',
    'database': 'warehouse',
    'user': 'postgres',
    'password': 'Rea123Teo',
}



# Navigation layout
navbar = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Analitika", href="/analytics")),
        dbc.NavItem(dbc.NavLink("Predviđanje", href="/forecast")),
    ],
    brand="Nadzorna ploča analitike transakcija i predviđanja",
    brand_href="/",
    color="steelblue",
    dark=True,
)

# Define the layout of your Dash app
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    navbar,
    html.Div(id='page-content', style={'padding': '20px'})
])

conn = psycopg2.connect(**db_params)

# Fetch the distinct years from the time dimension
distinct_years_query = "SELECT DISTINCT year FROM time;"
distinct_years = pd.read_sql_query(distinct_years_query, conn)['year'].tolist()
distinct_years.sort()
# Close the database connection
conn.close()

analytics_layout = html.Div(children=[
    html.H1(children='Analitika transakcija'),

    html.Div([
        dcc.Dropdown(
            id='year-dropdown',
            options=[
                {'label': str(year), 'value': str(year)} for year in distinct_years
            ],
            value='2020',  # Default value
            clearable=False,
            multi=False,
            style={'width': '50%'}
        )
    ]),

    html.Hr(),

    dcc.Graph(
        id='total-sales-over-time',
        figure={
            'data': [],
            'layout': {
                'title': 'Broj transakcija tijekom vremena'
            }
        }
    ),

    html.Hr(),
    
    dcc.Graph(
        id='product-sales-distribution',
        figure={
            'data': [],
            'layout': {
                'title': '10 Najprodavanijih proizvoda po mjesecima'
            }
        }
    ),
     html.Hr(),

    dcc.Graph(
        id='monthly-transactions',
        figure={
            'data': [],
            'layout': {
                'title': 'Iznosi transakcija po mjesecima'
            }
        }
    )
])

forecast_layout = html.Div(children=[
    html.H1(children='Predviđanja'),

    html.Div([
        dcc.Dropdown(
            id='forecast-period-dropdown',
            options=[
                {'label': '30 dana', 'value': '30'},
                {'label': '6 mjeseci', 'value': '183'},
                {'label': 'Godina', 'value': '365'}
            ],
            value='30',  # Default value
            clearable=False,
            multi=False,
            style={'width': '50%'}
        )
    ]),

    html.Div(id='forecast-graph-container'),

    html.Div([
        dcc.Dropdown(
            id='product-dropdown',
            options=[],  # Options will be populated dynamically
            value=None,  # Default value will be None initially
            clearable=False,
            multi=False,
            style={'width': '50%'}
        )
    ]),

    html.Div(id='product-forecast-graph-container')
])

@app.callback(Output('page-content', 'children'), [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/analytics':
        return analytics_layout
    elif pathname == '/forecast':
        return forecast_layout
    else:
        return analytics_layout  # Default to analytics layout


# Update the total sales over time graph based on selected year
@app.callback(Output('total-sales-over-time', 'figure'), [Input('year-dropdown', 'value')])
def update_total_sales_graph(selected_year):
    conn = psycopg2.connect(**db_params)

    total_sales_query = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS total_sales_over_time AS
    SELECT t.id_time, tm.year, tm.month, COUNT(*) AS total_transactions
    FROM transactions t
    JOIN time tm ON t.id_time = tm.id_time
    GROUP BY t.id_time, tm.year, tm.month
    ORDER BY tm.year, tm.month;
    """
    with conn.cursor() as cursor:
        cursor.execute(total_sales_query, (selected_year,))

    query = f"""
    SELECT * FROM total_sales_over_time WHERE year = {selected_year};
    """

    df = pd.read_sql_query(query, conn)

    conn.close()

    return {
        'data': [
            {'x': pd.to_datetime(df[['year', 'month']].assign(day=1)), 'y': df['total_transactions'], 'type': 'bar', 'name': 'Total Transactions', 'marker': {'color': 'lightblue'}}
        ],
        'layout': {
            'title': f'Broj transakcija kroz {selected_year}. godinu',
            'xaxis': {'title': 'Datum'},
            'yaxis': {'title': 'Broj transakcija'}
        }
    }
    

# Define callback to update the product sales distribution graph
@app.callback(
    Output('product-sales-distribution', 'figure'),
    [Input('year-dropdown', 'value')]
)
def update_product_sales_distribution(selected_year):
    conn = psycopg2.connect(**db_params)
    
    product_sales_query = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS product_sales_distribution AS
    SELECT Product_name, SUM(Quantity) AS total_quantity
    FROM Transactions
    JOIN Products ON Transactions.ID_product = Products.ID_product
    JOIN Time ON Transactions.ID_time = Time.ID_time
    WHERE Time.year = %s
    GROUP BY Product_name
    ORDER BY total_quantity DESC
    LIMIT 10;
    """
    with conn.cursor() as cursor:
        cursor.execute(product_sales_query, (selected_year,))

    query = """
    SELECT * FROM product_sales_distribution;
    """

    df = pd.read_sql_query(query, conn)
    
    conn.close()
    
    return {
        'data': [
            {'x': df['product_name'], 'y': df['total_quantity'], 'type': 'bar', 'name': 'Product Sales',  'marker': {'color': 'steelblue'}}
        ],
        'layout': {
            'title': f'10 najprodavanijih proizvoda u {selected_year}. godini.',
            'xaxis': {
                'title': 'Proizvod',
                'automargin': True,
            },
            'yaxis': {'title': 'Prodana količina'}
        }
    }

# Define callback to update the monthly transactions graph
@app.callback(
    Output('monthly-transactions', 'figure'),
    [Input('year-dropdown', 'value')]
)
def update_monthly_transactions(selected_year):
    conn = psycopg2.connect(**db_params)

    monthly_transactions_query = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS monthly_transactions AS
    SELECT tm.year, tm.month, SUM(t.amount) AS total_amount
    FROM transactions t
    JOIN time tm ON t.id_time = tm.id_time
    WHERE tm.year = %s
    GROUP BY tm.year, tm.month
    ORDER BY tm.year, tm.month;
    """
    with conn.cursor() as cursor:
        cursor.execute(monthly_transactions_query, (selected_year,))

    query = """
    SELECT * FROM monthly_transactions;
    """
    
    df = pd.read_sql_query(query, conn)
    
    conn.close()

    return {
        'data': [
            {'x': df['month'], 'y': df['total_amount'], 'type': 'bar', 'name': 'Monthly Transactions Amount', 'marker': {'color': 'lightblue'}}
        ],
        'layout': {
            'title': f'Ukupni iznos transakcija po mjesecu u {selected_year}. godini',
            'xaxis': {'title': 'Mjesec'},
            'yaxis': {'title': 'Ukupni iznos'}
        }
    }


@app.callback(
    Output('product-dropdown', 'options'),
    Input('forecast-period-dropdown', 'value')
)
def set_product_dropdown_options(_):
    conn = psycopg2.connect(**db_params)
    top_products_query = """
    SELECT product_name
    FROM (
        SELECT p.product_name, SUM(t.quantity) AS total_quantity
        FROM transactions t
        JOIN products p ON t.id_product = p.id_product
        JOIN time tm ON t.id_time = tm.id_time
        GROUP BY p.product_name
        ORDER BY total_quantity DESC
        LIMIT 5
    ) AS top_products;
    """
    top_products_df = pd.read_sql_query(top_products_query, conn)
    conn.close()

    return [{'label': product, 'value': product} for product in top_products_df['product_name']]


@app.callback(
    Output('forecast-graph-container', 'children'),
    [Input('forecast-period-dropdown', 'value')]
)
def display_forecast_graph(forecast_period):
    forecast_file = f'/app/Dash/forecast_transactions_{forecast_period}_days.csv'
    if not os.path.isfile(forecast_file):
        return html.Div("Nema dostupnih predviđanja.")
    
    # Load forecast data from CSV file
    forecast_data = pd.read_csv(forecast_file)
    forecast_data['ds'] = pd.to_datetime(forecast_data['ds'])
    
    # Define the number of days for the last part of the forecast
    num_days = int(forecast_period)

    # Separate the last part of the forecast data
    last_days = forecast_data.tail(num_days)
    forecast_without_last_days = forecast_data.iloc[:-num_days]

    # Create a Plotly figure
    fig = {
        'data': [
            {'x': forecast_without_last_days['ds'], 'y': forecast_without_last_days['yhat'], 'type': 'line', 'name': 'Povijesni podaci', 'line': {'color': 'steelblue'}},
            {'x': last_days['ds'], 'y': last_days['yhat'], 'type': 'line', 'name': f'Predviđanje za idućih {num_days}', 'line': {'color': 'orange'}}
        ],
        'layout': {
            'title': f'Predviđanje zarade za idućih {num_days} dana',
            'xaxis': {'title': 'Datum'},
            'yaxis': {'title': 'Iznos transakcija'},
            'showlegend': True
        }
    }

    return dcc.Graph(
        id='forecast-graph',
        figure=fig
    )

@app.callback(
    Output('product-forecast-graph-container', 'children'),
    [Input('forecast-period-dropdown', 'value'),
     Input('product-dropdown', 'value')]
)
def display_product_forecast_graph(forecast_period, selected_product):
    if not selected_product:
        return html.Div("  Molimo odaberite proizvod.")

    forecast_file = f'/app/Dash/forecast_{selected_product.replace(" ", "_").lower()}_{forecast_period}_days.csv'
    if not os.path.isfile(forecast_file):
        return html.Div(f"Nema dostupnih predviđanja za  {selected_product} za sljedećih {forecast_period} dana.")


    # Load forecast data from CSV file
    forecast_data = pd.read_csv(forecast_file)
    forecast_data['ds'] = pd.to_datetime(forecast_data['ds'])

    # Define the number of days for the last part of the forecast
    num_days = int(forecast_period)

    # Separate the last part of the forecast data
    last_days = forecast_data.tail(num_days)
    forecast_without_last_days = forecast_data.iloc[:-num_days]

    # Create a Plotly figure
    fig = {
        'data': [
            {'x': forecast_without_last_days['ds'], 'y': forecast_without_last_days['yhat'], 'type': 'line', 'name': 'Povijesni podaci', 'line': {'color': 'steelblue'}},
            {'x': last_days['ds'], 'y': last_days['yhat'], 'type': 'line', 'name': f'Predviđanje za idućih {num_days}', 'line': {'color': 'orange'}}
        ],
        'layout': {
            'title': f'Predviđanje prodaje za idućih {num_days} dana za {selected_product}',
            'xaxis': {'title': 'Datum'},
            'yaxis': {'title': 'Prodana količina'},
            'showlegend': True
        }
    }

    return dcc.Graph(
        id='product-forecast-graph',
        figure=fig
    )

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050)
