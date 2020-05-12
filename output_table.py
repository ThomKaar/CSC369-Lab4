# Thomas Karr
# Wesley Benica
# Lab 4 - CSC369 - Spring 2020

import datetime

from constants import HEADERS
from dataframes import get_df
from output_html import Query


def get_table(q: Query) -> str:
    row = q.output['table'].get('row')
    title = q.output['table'].get('title')

    html_text = ''
    if title:
        html_text += f"<h2>{title}</h2>"

    df = get_df(q)

    if row == 'time':
        df = df.transpose()
    return html_text + df.to_html().replace('\n', '')


def date_to_str(date: int) -> str:
    year = date // 10000
    month = (date - year * 10000) // 100
    day = date - year * 10000 - month * 100
    return datetime.date(year, month, day).strftime('%a %b %d, %Y')


def create_table(q: Query) -> str:
    row = q.output['table'].get('row')
    col = q.output['table'].get('column')
    title = q.output['table'].get('title')

    html_text = '<body>'
    if title:
        html_text += f"<h2>{title}</h2>"
    res = html_text + "<table>" + create_stat_table_headers(q, row, col) + create_stat_table_rows(q) + "</table>"
    return res


def create_stat_table_rows(query: Query) -> str:
    table_rows = ''
    data = query.data.get('data')
    stats = query.task.get('stats')
    aggregation = query.task.get('aggregation')
    for e in data:
        table_row = ''
        table_row += f'<td>{e.get(aggregation)}</td>\n'
        for stat in stats:
            avgStat = "mean" + str(stat)
            stdDevStat = "stdDev" + str(stat)
            table_row += f'<td>{e.get(avgStat)}</td>'
            table_row += f'<td>{e.get(stdDevStat)}</td>\n'
        table_rows += table_row + '</tr>'
    return table_rows


def create_stat_table_headers(query: Query, rows: str, cols: str) -> str:
    header_rows = "<tr>"
    header_end = "</tr>"
    data = query.data.get('data')
    stats = query.task.get('stats')
    cols = []
    cols.append('avg')
    cols.append('stdDev')
    aggregation = query.task.get('aggregation')
    header_rows += f'<th> {aggregation}</th>'
    for stat in stats:
        header_row = ''

        header_row += f"<th>{HEADERS[cols[0]]} "
        header_row += f"{HEADERS[stat]}</th>"

        header_row += f"<th> {HEADERS[stat]} "
        header_row += f"{HEADERS[cols[1]]}</th>"

        header_rows += header_row
    header_rows += header_end
    return header_rows
