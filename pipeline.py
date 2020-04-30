from collections import defaultdict


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
