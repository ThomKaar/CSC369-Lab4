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
        projection = defaultdict(dict)
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
            var_to_track = query['task']['track']
            if test_config.aggregation == 'state':
                if test_config.collection == 'states':
                    grouping_stage['$group'].update({
                        var_to_track + "_total": {"$sum": f'${var_to_track}'}
                    })
                    projection['$project'].update(
                        {
                            "_id": 0,
                            "date": "$_id",
                            var_to_track + '_total': 1
                        }
                    )
                else:
                    grouping_stage['$group'].update({
                        "_id": "$state",
                        "data": {
                            "$push": {
                                "date": "$date",
                                var_to_track: f"${var_to_track}"
                            }
                        }
                    })
                    projection['$project'].update(
                        {
                            "_id": 0,
                            "state": "$_id",
                            "data": 1
                        }
                    )
            elif test_config.aggregation == 'county':
                grouping_stage['$group'].update({
                    "_id": "$county",
                    "data": {
                        "$push": {
                            "date": "$date",
                            var_to_track: {"$sum": f"${var_to_track}"}
                        }
                    }
                })
                projection['$project'].update(
                    {
                        "_id": 0,
                        "county": "$_id",
                        "data": 1
                    }
                )
            else:
                grouping_stage['$group'].update(
                    {
                        var_to_track: {"$sum": f"${var_to_track}"}
                    }
                )
                projection['$project'].update(
                    {
                        "_id": 0,
                        "date": "$_id",
                        var_to_track: 1
                    }
                )
        else:
            # TODO: Task - Stats
            projection = {
                "$project": query['task']['stats']
            }
            pass

        if grouping_stage:
            res["$facet"][str(i)].append(dict(grouping_stage))
        if projection:
            res["$facet"][str(i)].append(dict(projection))
        res["$facet"][str(i)].append(sort)

    res["$facet"] = dict(res["$facet"])
    return dict(res)


def create_grouping_stage(test_config):
    grouping_stage = defaultdict(dict)
    if test_config.aggregation in ['usa', "fiftyStates"]:
        grouping_stage["$group"].update(
            {
                "_id": "$date"
            }
        )
    elif test_config.aggregation == 'state' and test_config.collection == 'states':
        grouping_stage['$group'].update(
            {
                "_id": "$date",
            }
        )
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
def create_date_filter(test_config: Configuration, res: defaultdict):
    """
    :param test_config: Configuration that describes the desired query
    :param res: the match stage to add the date filter to
    :return: the match stage with the date filter added in
    """

    res["$match"].update({"$and": [{"date": {"$gte": test_config.start}}, {"date": {"$lte": test_config.end}}]});
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
