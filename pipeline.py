from collections import defaultdict

from config import Configuration

NOT_STATES = ["AS", "GU", "MP", "VI"]


def create_pipeline(test_config: Configuration):
    """
    :param test_config: Configuration that describes the desired query
    :return: the aggregation pipeline required to perform the query described by test_config
    """
    pipeline = []

    match = create_match_stage(test_config)
    if match:
        pipeline.append(match)

    project = create_project_stage(test_config)
    if project:
        pipeline.append(project)

    return pipeline


def create_match_stage(test_config: Configuration):
    """
    :param test_config: Configuration that describes the desired query
    :return: the match stage required to perform the query described by test_confgi
    """
    res = defaultdict(dict)
    res = create_location_filter(test_config, res)
    res = create_date_filter(test_config, res)
    return dict(res)


def create_project_stage(test_config: Configuration):
    """
    :param test_config: Configuration that describes the desired query
    :return: the project stage required to perform the query described by test_confgi
    """
    res = defaultdict(dict)
    res.update({"$project": {"_id": 0, "fips": 0}})
    return dict(res)


def create_date_filter(test_config: Configuration, res: defaultdict):
    """
    :param test_config: Configuration that describes the desired query
    :param res: the match stage to add the date filter to
    :return: the match stage with the date filter added in
    """
    return dict(res)


def create_location_filter(test_config: Configuration, res: defaultdict):
    """
    :param test_config: Configuration that describes the desired query
    :param res: the match stage to add the location filters to
    :return: the match stage with the location filters added in
    """
    if test_config.aggregation == 'fiftyStates':
        res["$match"].update({"state:": {"$notin": NOT_STATES}})
    elif test_config.aggregation == 'state':
        if test_config.collection == 'states':
            res["$match"].update({"state": test_config.target})
            if test_config.counties:
                res["$match"].update({"county": {"$in": test_config.counties}})
        elif test_config.collection == 'covid':
            if test_config.aggregation in ['state', 'usa', None]:
                res["$match"].update({"state": f"{test_config.target}"})
            elif test_config.aggregation == 'fiftyStates':
                state_match = {"state": {"$notin": NOT_STATES, "$in": test_config.target}}
                res["$match"].update({"Sstate": state_match})
    return res
