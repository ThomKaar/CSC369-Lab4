from typing import NamedTuple


def get_css():
    return "<style>" \
           "th, td {" \
           "    border: 1px solid black;" \
           "}" \
           "td {" \
           "    font-family: monospace;" \
           "    padding: 6px 12px 6px 6px;" \
           "}" \
           "th {" \
           "    text-align: left;" \
           "    padding: 6px 12px 6px 6px" \
           "}" \
           "</style>"


def get_header():
    header = "<header>"
    header += get_css()
    header += "</header>"
    return header


def creat_html_page():
    page = ''
    page += get_header()


class Query(NamedTuple):
    task: dict
    output: dict
    data: dict
