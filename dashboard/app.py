# dashboard/app.py
import os
import requests
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from dotenv import load_dotenv

load_dotenv()
AGGREGATOR_URL = os.getenv('AGGREGATOR_URL', 'http://aggregator:8001/metrics')

app = Dash(__name__)
server = app.server

app.layout = html.Div([
    html.H3("Parallel Sentiment Dashboard (Demo)"),
    html.Div(id="stats"),
    dcc.Interval(id="interval", interval=2000, n_intervals=0),
])

@app.callback(Output("stats", "children"), [Input("interval", "n_intervals")])
def update(n):
    try:
        r = requests.get(AGGREGATOR_URL, timeout=2)
        data = r.json()
        total = data.get("total", 0)
        counts = data.get("counts", {})
        avg = round(data.get("avg_score", 0), 3)
    except Exception as e:
        return html.Div(f"Error fetching metrics: {e}")
    return html.Div([
        html.P(f"Total recent processed messages: {total}"),
        html.P(f"Average confidence score: {avg}"),
        html.P(f"Counts: {counts}")
    ])


if __name__ == "__main__":
    app.run_server(host='0.0.0.0', port=8050)
