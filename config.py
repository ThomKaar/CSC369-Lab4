import json
from datetime import datetime
from typing import List, Dict, Union


class Configuration:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config_data = json.load(f)

        self.refresh: bool = config_data['refresh'] == 'true'
        self.collection: str = config_data['collection']
        self.aggregation: str = config_data['aggregation']
        self.start = 0
        self.end = 0
        self.target: Union[str, List[str]] = config_data['target']
        self.counties: Union[str, List[str]] = config_data.get('counties')
        self.analysis: List[Dict] = config_data['analysis']
        self.output_file: str = config_data['Output']

        if self.collection == 'covid':
            if self.aggregation == 'county':
                raise ValueError("Covid data can't be aggregated by county")
            if self.counties:
                raise ValueError("Covid data can't be filtered by county")
        else:
            if type(self.collection) == List:
                raise ValueError("States data must be filtered by a single state")

        time = config_data['time']
        today = Configuration.date_to_time(datetime.utcnow())
        if time == 'today':
            self.start: int = today
            self.end: int = today
        elif time == 'yesterday':
            self.start: int = today - 1
            self.end: int = today - 1
        elif time == 'week':
            self.start = today - 6
            self.end = today
        elif time == 'month':
            self.start = today // 100 * 100 + 1
            self.end = today
        else:
            self.start = config_data['time']['start']
            self.end = config_data['time']['end']

    def date_to_time(dt: datetime) -> int:
        return dt.year * 10000 + dt.month * 100 + dt.day
