import numpy as np
from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator

from dataframes import get_df
from output_html import Query


def create_graph(q: Query):
    graph_type = q.output['graph']['type']
    legend = q.output['graph'].get('legend') == 'on'
    combo = q.output['graph']['combo']
    title = q.output['graph'].get('title')
    df = get_df(q)

    fig, ax = plt.subplots()

    if graph_type == 'bar':
        grapher = ax.bar
        labels = np.arange(len(df.index))
        width = 1
        for i, col in enumerate(df.columns):
            offset = i * width / len(df.columns)
            grapher(labels - width / 2 + offset, height=df[col], width=width / len(df.columns), label=col)
        ax.set_xticklabels(df.index)
    elif graph_type == 'line':
        grapher = ax.plot
        grapher(df)
        ax.xaxis.set_major_locator(MultipleLocator(5))
    elif graph_type == 'scatter':
        grapher = ax.scatter
        grapher(df)
    else:
        raise ValueError("Graph type must be 'bar', 'line', or 'scatter'")



    plt.xticks(rotation=90)
    if title:
        plt.title = title
    if legend:
        ax.legend()

    plt.show()
