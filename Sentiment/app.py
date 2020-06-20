from analysis import *

sd = comprehend()
negative, mixed, neutral, positive = break_sentiment(sd)


import plotly.graph_objects as go
import dash
import dash_core_components as dcc
import dash_html_components as html

app = dash.Dash(__name__)
server = app.server

# fig = make_subplots(
#     rows=2, cols=1,
#     specs=[[{"type": "xy"}]],
#     subplot_titles=("r/wallstreetbets Sentiment Distribution")
# )


# fig.add_trace(go.Bar(x=fd["Word"], y=fd["Frequency"]), row=1, col=1)
# fig.add_trace(go.Bar(x=["Negative", "Mixed", "Neutral", "Positive"], y=[negative, mixed, neutral, positive]), row=2, col=1)




fig = go.Figure(go.Bar(x=["Negative", "Mixed", "Neutral", "Positive"], y=[negative, mixed, neutral, positive], ))

fig.update_layout(showlegend=False, title_text="r/wallstreetbets Sentiment Analysis",
                  )

# App layout
app.layout = html.Div([
    dcc.Graph(figure=fig)

], style={"height": "100"})

if __name__ == '__main__':
    app.run_server(debug=True)