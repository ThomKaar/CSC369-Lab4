import datetime
from typing import NamedTuple


def date_to_str(date: int) -> str:
    year = date // 10000
    month = (date - year * 10000) // 100
    day = date - year * 10000 - month * 100
    return datetime.date(year, month, day).strftime('%a %b %d, %Y')


HEADERS = {
    "death": "Cumulative Deaths",
    "positive": "Cumulative Positives",
    "time": "Date"
}


class Query(NamedTuple):
    task: dict
    output: dict
    data: dict


def create_table_headers(query: Query, rows: str, cols: str) -> str:
    header_row = "<tr>"
    header_row += f'<th>{HEADERS[cols]}</th>'
    header_row += f'<th>{HEADERS[query.task["track"]]}</th>'
    return header_row


def create_table_rows(query: Query, rows, cols) -> str:
    table_rows = ''
    for datum in query.data['data']:
        table_rows += f"<tr><td>"
        if cols == 'time':
            table_rows += date_to_str(datum['date'])
        table_rows += "</td><td>"
        if 'track' in query.task:
            table_rows += f"{datum.get(query.task['track']) or 0}"
        table_rows += "</td></tr>"
    return table_rows


def create_table(query: Query, row: str = None, col: str = None, title: str = None) -> str:
    html_text = ""
    if title:
        html_text += f"<h2>{title}</h2>"
    res = html_text + "<table>" + create_table_headers(query, row, col) + create_table_rows(query, row,
        col) + "</table>"
    return res
