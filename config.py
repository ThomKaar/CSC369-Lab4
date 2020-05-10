import json
from datetime import datetime, timedelta
from typing import List, Dict, Union


class Configuration:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config_data = json.load(f)

        self.refresh: bool = config_data.get('refresh')
        self.collection: str = config_data['collection']
        self.aggregation: Union[str, None] = config_data.get('aggregation')
        self.start: int = 0
        self.end: int = 0
        self.analysis: List[Dict] = config_data['analysis']
        self.output_file: Union[str, None] = config_data.get('Output')

        temp_target = config_data.get('target')
        # sets self.target to None if DNE, else makes sure target is an array
        self.target: Union[None, List[str]] = [temp_target] if (type(temp_target) == str) else temp_target

        temp_counties = config_data.get('counties')
        # sets self.counties to None if DNE, else makes sure target is an array
        self.counties: List[str] = temp_counties or [temp_counties] if (type(temp_counties) == str) else temp_counties
        self.counties = None if self.collection == 'covid' else self.counties

        self.check_validity()
        self.set_start_end(config_data)

    def check_validity(self):
        """verify that aggregations are valid for collection"""
        if self.collection == 'covid':
            if self.aggregation == 'county':
                raise ValueError("Covid data can't be aggregated by county")
        elif self.collection == 'states':
            if self.target and len(self.target) > 1:
                raise ValueError("States data must be filtered by a single state")
            elif self.aggregation in ['usa', 'fiftyStates']:
                raise ValueError("States data aggregation must be 'state' or 'county'")
        else:
            raise ValueError(f'Unknown collection {self.collection}')

        for a in self.analysis:
            if 'table' in a['output']:
                if (('track' in a['task'] or 'ratio' in a['task']) and not ('row' in a[
                    'output']['table'] and 'column' in a['output']['table'])):
                    raise ValueError(f'Track and Ratio tasks require both row and column specifications')
                if ('stats' in a['task'] and not ('row' in a['output']['table'] or 'column' in a['output']['table'])):
                    raise ValueError(f'Stats tasks require either row or column specification')

    def set_start_end(self, config_data):
        # get start/end range
        time = config_data.get('time')
        today = date_to_int(datetime.utcnow())
        if time:
            if time == 'today':
                self.start: int = today
                self.end: int = today
            elif time == 'yesterday':
                self.start: int = today - 1
                self.end: int = today - 1
            elif time == 'week':
                self.start = sub_seven_days(today)
                self.end = today
            elif time == 'month':
                self.start = today // 100 * 100 + 1
                self.end = today
            else:
                self.start = config_data['time']['start']
                self.end = config_data['time']['end']


def date_to_int(dt: datetime) -> int:
    return dt.year * 10000 + dt.month * 100 + dt.day


def sub_seven_days(date: int) -> int:
    y = date // 10000
    m = (date - y * 10000) // 100
    d = (date - y * 10000 - m * 100)
    dt = datetime(y, m, d) - timedelta(7)
    return date_to_int(dt)
