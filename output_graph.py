from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator

from dataframes import get_df
from output_html import Query


def create_graph(q: Query):
    graph_type = q.output['graph']['type']
    legend = q.output['graph'].get('legend') == 'on'
    combo = q.output['graph']['combo']
    title = q.output['graph'].get('title')

    fig, ax = plt.subplots()

    if graph_type == 'bar':
        grapher = ax.bar
    elif graph_type == 'line':
        grapher = ax.plot
    elif graph_type == 'scatter':
        grapher = ax.scatter
    else:
        raise ValueError("Graph type must be 'bar', 'line', or 'scatter'")

    df = get_df(q)

    grapher(df)
    ax.xaxis.set_major_locator(MultipleLocator(5))

    plt.xticks(rotation=90)
    if title:
        plt.title = title
    if legend:
        ax.legend()

    plt.show()
