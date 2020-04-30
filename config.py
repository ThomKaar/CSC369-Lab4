import json
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Union

NOT_STATES = ["AS", "GU", "MP", "VI"]

class Configuration:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config_data = json.load(f)

        self.refresh: bool = config_data.get('refresh') == 'true'
        self.collection: str = config_data['collection']
        self.aggregation: Union[str, None] = config_data.get('aggregation')
        self.start: int = 0
        self.end: int = 0
        self.analysis: List[Dict] = config_data['analysis']
        self.output_file: Union[str, None] = config_data.get('Output')

        temp_target = config_data.get('target')
        # sets self.target to None if not DNE, else makes sure target is an array
        self.target: Union[None, List[str]] = temp_target or (
            [temp_target] if (type(temp_target) == str) else temp_target)

        temp_counties = config_data.get('counties')
        # sets self.counties to None if not DNE, else makes sure target is an array
        self.counties: List[str] = temp_counties or [temp_counties] if (type(temp_counties) == str) else temp_counties

        self.check_validity()
        self.set_start_end(config_data)

        self.create_pipeline()

    def create_pipeline(self):
        self.pipeline = []
        self.pipeline.append(self.create_match_stage())
        self.pipeline.append(self.create_project_stage())

    def create_match_stage(self):
        res = defaultdict(dict)
        res = self.create_location_filter(res)
        res = self.create_date_filter(res)
        return dict(res)

    def create_project_stage(self):
        res = defaultdict(dict)
        res.update({"$project": {"_id": 0, "fips": 0}})
        return dict(res)

    def create_date_filter(self, res):
        pass

    def create_location_filter(self, res):
        if self.aggregation == 'fiftyStates':
            res["$match"].update({"state:": {"$notin": NOT_STATES}})
        elif self.aggregation == 'state':
            if self.collection == 'states':
                res["$match"].update({"state": self.target})
                if self.counties:
                    res["$match"].update({"county": {"$in": self.counties}})
            elif self.collection == 'covid':
                if self.aggregation in ['state', 'usa', None]:
                    res["$match"].update({"state": f"{self.target}"})
                elif self.aggregation == 'fiftyStates':
                    state_match = {"state": {"$notin": NOT_STATES, "$in": self.target}}
                    res["$match"].update({"Sstate": state_match})
        return res

    def check_validity(self):
        """verify that aggregations are valid for collection"""
        if self.collection == 'covid':
            if self.aggregation == 'county':
                raise ValueError("Covid data can't be aggregated by county")
        elif self.collection == 'states':
            if type(self.target) == list:
                raise ValueError("States data must be filtered by a single state")
            elif self.aggregation in ['usa', 'fiftyStates']:
                raise ValueError("States data aggregation must be 'state' or 'county'")
        else:
            raise ValueError(f'Unknown collection {self.collection}')

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
                self.start = today - 6
                self.end = today
            elif time == 'month':
                self.start = today // 100 * 100 + 1
                self.end = today
            else:
                self.start = config_data['time']['start']
                self.end = config_data['time']['end']


def date_to_int(dt: datetime) -> int:
    return dt.year * 10000 + dt.month * 100 + dt.day
