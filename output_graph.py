from matplotlib import pyplot

from dataframes import get_unagg_track_df, get_agg_track_df
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

    if q.task == 'track':
        df = get_track_df(q)
    elif q.task == 'ratio':
        pass
    elif q.task == 'stats':
        pass
    else:
        print("Something went wrong")


def get_track_df(q):
    aggregation = q.data.get('aggregation')
    target = q.data.get('target')
    states = q.data.get('states')
    counties = q.data.get('counties')

    if not aggregation:
        if counties:
            df = get_unagg_track_df(q.data[0], q.task['track'], counties=counties)
        else:
            df = get_unagg_track_df(q.data[0], q.task['track'], states=states)
    else:
        if counties:
            df = get_agg_track_df(q.data[0], q.task['track'], target=target[0])
        else:
            df = get_agg_track_df(q.data[0], q.task['track'], target=aggregation)
