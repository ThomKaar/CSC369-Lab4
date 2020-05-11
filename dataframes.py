from datetime import date

import pandas as pd


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
            df[dd[field]][d['date']] = dd[track_var]

    return df


def get_agg_track_df(data: dict, track_var: str, target):
    df = pd.DataFrame(data['data'])
    return df.set_index('date')


def int_to_date(date_int: int) -> date:
    year = date_int // 10000
    month = (date_int - year * 10000) // 100
    day = (date_int - year * 10000 - month * 100)
    return date(year, month, day)
