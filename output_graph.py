from matplotlib import pyplot

from dataframes import get_df
from output_html import Query


def create_graph(q: Query):
    graph_type = q.output['graph']['type']
    legend = q.output['graph'].get('legend') == 'on'
    combo = q.output['graph']['combo']
    title = q.output['graph'].get('title')

    if graph_type == 'bar':
        grapher = pyplot.bar
    elif graph_type == 'line':
        grapher = pyplot.plot
    elif graph_type == 'scatter':
        grapher = pyplot.scatter
    else:
        raise ValueError("Graph type must be 'bar', 'line', or 'scatter'")

    df = get_df(q)
