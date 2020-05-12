import datetime

from constants import HEADERS
from dataframes import get_df
from output_html import Query


def get_table(q: Query) -> str:
    row = q.output['table'].get('row')
    col = q.output['table'].get('column')
    title = q.output['table'].get('title')

    df = get_df(q)

    if row == 'time':
        df = df.transpose()
    return df.to_html().replace('\n', '')


def date_to_str(date: int) -> str:
    year = date // 10000
    month = (date - year * 10000) // 100
    day = date - year * 10000 - month * 100
    return datetime.date(year, month, day).strftime('%a %b %d, %Y')


def create_table_headers(query: Query, rows: str, cols: str) -> str:
    if query.task.get('stats') is not None:
        return create_stat_table_headers(query, rows, cols)

    aggregation_level = query.data.get('aggregation')
    target = query.data.get('target')
    states = query.data.get('states')
    counties = query.data.get('counties')
    num_data_cols = 1
    header_row = "<tr>"
    header_row += f'<th>{HEADERS[cols]}</th>'

    data_col_header = ''
    if aggregation_level:
        if target:
            if counties:
                data_col_header += f'<th>{target[0]}: ' + ', '.join(counties)
            else:
                data_col_header += f'<th>{aggregation_level}: ' + ', '.join(target)
        elif states:
            data_col_header += '<th>' + f"{', '.join(states)}"
        else:
            data_col_header += '<th>' + aggregation_level
        data_col_header += '</th></tr>'
    elif states:
        num_data_cols = len(states)
        for state in states:
            data_col_header += f'<th>{state}</th>'
        data_col_header += '</tr>'
    elif counties:
        num_data_cols = len(counties)
        for county in counties:
            data_col_header += f'<th>{county}</th>'
        data_col_header += '</tr>'

    if query.task.get('track'):
        data_header = HEADERS[query.task["track"]]
    else:
        n = HEADERS[query.task['ratio']['numerator']]
        d = HEADERS[query.task['ratio']['denominator']]
        data_header = n + ' to ' + d + " Ratio"
    header_row += f'<th colspan={num_data_cols}>{data_header}</th></tr>'
    header_row += '<tr><th></th>'
    header_row += data_col_header

    return header_row


def create_table_rows(query: Query, rows, cols) -> str:
    table_rows = ''
    if query.task.get('stats') is not None:
        table_rows = create_stat_table_rows(query, rows, cols)
        return table_rows
    for datum in query.data['data']:
        table_rows += f"<tr><td>"
        if cols == 'time':
            table_rows += date_to_str(datum['date'])
        table_rows += "</td>"
        if 'track' in query.task or 'ratio' in query.task:
            var = query.task['track'] if 'track' in query.task else 'ratio'
            if query.data.get('aggregation') and (query.data['aggregation'] in ['usa', 'fiftyStates'] or (
                    query.data['aggregation'] == 'state' and query.data.get('counties'))):
                table_rows += '<td>' + str(datum[f"{var}"] or 0) + '</td>'
            else:
                if query.data.get('counties'):
                    for county in query.data['counties']:
                        found = False
                        for daily_datum in datum['daily_data']:
                            if daily_datum['county'] == county:
                                found = True
                                table_rows += '<td>' + str(daily_datum[f"{var}" or 0]) + '</td>'
                        if not found:
                            table_rows += '<td>' + str(0) + '</td>'
                if query.data.get('states'):
                    for state in query.data['states']:
                        found = False
                        for daily_datum in datum['daily_data']:
                            if daily_datum['state'] == state:
                                found = True
                                table_rows += '<td>' + str(daily_datum[f"{var}"] or 0) + '</td>'
                        if not found:
                            table_rows += '<td>' + str(0) + '</td>'

        table_rows += "</tr>"
    return table_rows


def create_table(query: Query, row: str = None, col: str = None, title: str = None) -> str:
    html_text = '<body>'
    if title:
        html_text += f"<h2>{title}</h2>"
    res = html_text + "<table>" + create_table_headers(query, row, col) + create_table_rows(query, row,
        col) + "</table>"
    return res


def create_stat_table_rows(query: Query, rows, cols) -> str:
    table_rows = '';
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
        table_rows += table_row
    return table_rows


def create_stat_table_headers(query: Query, rows: str, cols: str) -> str:
    header_rows = "<tr>"
    header_end = "</tr>"
    data = query.data.get('data')
    stats = query.task.get('stats')
    cols = []
    cols.append('avg')
    cols.append('stdDev')
    aggregation = query.task.get('aggregation');
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
