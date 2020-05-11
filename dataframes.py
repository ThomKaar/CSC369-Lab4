from datetime import date

import pandas as pd

from output_html import Query


def get_df(q: Query) -> pd.DataFrame:
    if 'track' in q.task:
        df = get_track_df(q, q.task['track'])
    elif 'ratio' in q.task:
        df = get_track_df(q, 'ratio')
    elif 'stats' in q.task:
        df = get_stats_df(q)
    else:
        print("Something went wrong")
        raise ValueError

    return df


def get_track_df(q, track_var):
    aggregation = q.data.get('aggregation')
    target = q.data.get('target')
    states = q.data.get('states')
    counties = q.data.get('counties')

    if not aggregation:
        if counties:
            df = get_unagg_track_df(q.data, track_var, counties=counties)
        else:
            df = get_unagg_track_df(q.data, track_var, states=states)
    else:
        if counties:
            df = get_agg_track_df(q.data['data'], target=target[0])
        else:
            df = get_agg_track_df(q.data['data'], target=aggregation)

    return df


def get_unagg_track_df(data: dict, track_var: str, counties=None, states=None):
    dates = [int_to_date(datum['date']) for datum in data['data']]

    if counties:
        df = pd.DataFrame(index=dates, columns=counties)
    elif states:
        df = pd.DataFrame(index=dates, columns=states)
    else:
        raise ValueError

    for col in df.columns:
        df[col].values[:] = 0

    field = 'county' if counties else 'state'
    for d in data['data']:
        for dd in d['daily_data']:
            df[dd[field]][int_to_date(d['date'])] = dd.get(track_var) or 0

    return df


def get_agg_track_df(data: dict, target):
    # TODO add target as a title?
    df = pd.DataFrame(data)
    df['date'] = df['date'].apply(lambda date_int: int_to_date(date_int))
    return df.set_index('date')


def get_stats_df(q):
    return pd.DataFrame()


def int_to_date(date_int: int, short=False) -> str:
    year = date_int // 10000
    month = (date_int - year * 10000) // 100
    day = (date_int - year * 10000 - month * 100)
    if short:
        format = '%m/%d/%y'
    else:
        format = '%a, %b %d, %Y'
    return date(year, month, day).strftime(format)
