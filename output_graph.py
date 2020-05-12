import numpy as np
from matplotlib import pyplot as plt
from matplotlib.ticker import MultipleLocator

from constants import HEADERS
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
        group_width = .75
        col_width = group_width / len(df.columns)
        x = np.arange(len(df.index))
        for i, col in enumerate(df.columns):
            grapher(x - 0.5 + i * col_width + col_width / 2, df[col], width=col_width, label=col)
        plt.xticks(x, df.index)
        ax.xaxis.set_major_locator(MultipleLocator(5))
        ax.set_xticklabels([idx for n, idx in enumerate(df.index) if n % 5 == 0])
    elif graph_type == 'line':
        grapher = ax.plot
        grapher(df)
        ax.xaxis.set_major_locator(MultipleLocator(5))
    elif graph_type == 'scatter':
        grapher = ax.scatter
        x = np.arange(len(df.index))
        for i, col in enumerate(df.columns):
            grapher(x, df[col], label=col)
        plt.xticks(x, df.index)
        ax.xaxis.set_major_locator(MultipleLocator(5))
        ax.set_xticklabels([idx for n, idx in enumerate(df.index) if n % 5 == 0])
    else:
        raise ValueError("Graph type must be 'bar', 'line', or 'scatter'")

    plt.xticks(rotation=90)

    label = HEADERS.get(q.task.get('track')) or HEADERS[q.task['ratio']['numerator']] + ' to ' + HEADERS[q.task[
        'ratio']['denominator']] + " Ratio"
    plt.ylabel(label)
    if legend:
        plt.legend(df.columns)
    if title:
        ax.set_title(title)

    plt.show()
    plt.savefig('graph.png')
    return create_graph_html('graph.png')

def create_graph_html(name: str) -> str:
    return 
