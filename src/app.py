import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input,Output
import dash_table
import pandas as pd
from sqlalchemy import create_engine

user='test'
password='test'
host='ec2-18-190-120-167.us-east-2.compute.amazonaws.com'
port='5432'
db='my_db'
url='postgresql://{}:{}@{}:{}/{}'.format(user,password,host,port,db)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

colors = {
        'background': '#111111',
        'text': '#7FDBFF'
        }

con=create_engine(url)

server = app.server
q="select * from finali"
df=pd.read_sql(q,con)
#print (df[df['state']=='OH']['city'].unique())

app.layout = html.Div(children=[
    html.Div(children=[html.H1(children="BnB-Pay",
        style={
            'textAlign': 'center',
            "background": "yellow"})
        ]
        ),
    html.H4(children='Select State:'),
    dcc.Dropdown(id='state',options=[
            {'label':i, 'value':i} for i in df.state.unique()
        ], placeholder = 'Filter by state...'),
    html.H4(children='Select city:'),
    dcc.Dropdown(id='city', placeholder='Filter by city...'),
    html.H4(children='Select the # of bedrooms:'),
    dcc.Dropdown(id='bedrooms', options=[
        {'label':i, 'value':i} for i in df.bedrooms.unique()
        ], placeholder='Filter by bedrooms...'),
    #html.H4('bnb:',id='bnb')
    html.Table([
        html.Tr([html.Td('Average AirBnB rate:    $'), html.Td(id='bnb')]),
        html.Tr([html.Td('Average monthly mortgage:    $'), html.Td(id='mortgage')]),
        html.Tr([html.Td('%:    '), html.Td(id='percent')]),
        ])
])
@app.callback(Output('city', 'options'),
        [Input('state', 'value')])
def set_cities_options(value):
    df2=df[df.state == value]['city']
    return [{'label': i, 'value': i} for i in df2.unique()]

@app.callback([Output('bnb', 'children'),
    Output('mortgage', 'children'),
    Output('percent', 'children')],
        [Input('state', 'value'),
            Input('city', 'value'),
            Input('bedrooms', 'value')])
def final(state,city,bedrooms):
    bnb = df[(df.state == state) & (df.city == city) & (df.bedrooms == bedrooms)]['average']
    mortgage = df[(df.state == state) & (df.city == city) & (df.bedrooms == bedrooms)]['monthly_mortgage']
    percent = df[(df.state == state) & (df.city == city) & (df.bedrooms == bedrooms)]['%']
    return bnb,mortgage,percent
if __name__ == '__main__':
    app.run_server(debug=True, port=8050, host="ec2-18-190-120-167.us-east-2.compute.amazonaws.com")
                                                                                                            
