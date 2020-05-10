from collections import defaultdict

from config import Configuration

TERRITORIES = ["AS", "GU", "MP", "VI"]


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

    aggregation = create_facet_stage(test_config)
    if aggregation:
        pipeline.append(aggregation)

    # sort = create_sort_stage(test_config)
    # if sort:
    #     pipeline.append(sort)
    return pipeline


### Stages
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
    res["$project"].update({"_id": 0, "fips": 0, "hash": 0})
    return dict(res)


def create_facet_stage(test_config: Configuration):
    """
    :param test_config: Configuration that describes the desired query
    :return: the aggregation stage required to perform the query described by test_confgi
    """
    res = defaultdict()
    res["$facet"] = defaultdict(list)

    sort = create_sort_stage(test_config)

    for i, query in enumerate(test_config.analysis):
        grouping_stage = create_grouping_stage(test_config)
        if "ratio" in query['task']:
            numerator_ = query["task"]["ratio"]["numerator"]
            denominator_ = query["task"]["ratio"]["denominator"]

            if test_config.aggregation in ["usa", "fiftyStates"]:
                grouping_stage["$group"].update({
                    f'{numerator_}': {"$sum": f'${numerator_}'},
                    f'{denominator_}': {"$sum": f'${denominator_}'}
                })

            projection = {"$project": {}}
            projection["$project"].update({"date": 1})
            if test_config.aggregation in ["state", "county"]:
                projection["$project"].update({f'{test_config.aggregation}': 1})
            projection["$project"].update({
                "ratio": {
                    "$divide": [
                        f'${numerator_}',
                        f'${denominator_}'
                    ]
                }
            })
        elif "track" in query['task']:
            # TODO: Task - Track
            track_ = query['task']['track']
            if grouping_stage:
                grouping_stage['$group'].update({
                    track_ + "_total": {"$sum": f'${track_}'}
                })
                projection = {
                    "$project": {
                        "date": 1,
                        track_ + '_total': 1
                    }
                }
            else:
                projection = {
                    "$project": {
                        "date": 1,
                        track_: 1
                    }
                }
            if test_config.aggregation in ["state", "county"]:
                projection["$project"].update({f'{test_config.aggregation}': 1})
        else:
            # TODO: Task - Stats
            projection = None
            pass

        if grouping_stage:
            res["$facet"][str(i)].append(dict(grouping_stage))
        res["$facet"][str(i)].append(dict(projection))
        res["$facet"][str(i)].append(sort)

    res["$facet"] = dict(res["$facet"])
    return dict(res)


def create_grouping_stage(test_config):
    if test_config.aggregation in ['usa', "fiftyStates"]:
        grouping_stage = {
            "$group": {
                "_id": 1,
            }}
    elif test_config.collection == 'states' and test_config.aggregation == 'state':
        grouping_stage = {
            "$group": {
                "_id": "$state",
            }
        }
    elif test_config.collection == 'covid' and test_config.aggregation == 'country_wide':
        grouping_stage = {
            "$group": {
                "_id": 1,
            }
        }
    else:
        grouping_stage = defaultdict()
    return grouping_stage


# TODO: add sort stage? - this may be sufficient
def create_sort_stage(test_config: Configuration):
    """
    :param test_config: Configuration that describes the desired query
    :return: the sort stage required to perform the query described by test_confgi
    """
    res = defaultdict(dict)
    res["$sort"].update({"state": 1, "county": 1, "date": 1})
    return dict(res)


### Match filters
# TODO: filter by date
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
        state_match = {"$nin": TERRITORIES}
        if test_config.target:
            state_match.update({"$in": test_config.target})
        res["$match"].update({"state": state_match})
    elif test_config.target:
        res["$match"].update({"state": {"$in": test_config.target}})

    if test_config.aggregation in ['county', 'state']:
        if test_config.collection == 'states' and test_config.counties:
            res["$match"].update({"county": {"$in": test_config.counties}})

    return res


# this is only for the covid collection
def create_country_wide_aggregation(test_config: Configuration, res: defaultdict):
    """
    :param test_config: Configuration that describes the desired query
    :param res: the group stage to add the country filter
    :return: the group stage with the country aggregation
    """
    res = {"$group": {"_id": 1, "targetSum": {"$sum": test_config.target}}} 
    return dict(res)

