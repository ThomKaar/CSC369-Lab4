# Thomas Karr
# Wesley Benica
# Lab 4 - CSC369 - Spring 2020

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

    sort = create_sort_stage(test_config)
    if sort:
        pipeline.append(sort)

    project = create_project_stage(test_config)
    if project:
        pipeline.append(project)

    aggregation = create_facet_stage(test_config)
    if aggregation:
        pipeline.append(aggregation)

    return pipeline


# Stages
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
        unwind_regroup_stage = []
        projection_stage = defaultdict(dict)

        if "ratio" in query['task']:
            unwind_regroup_stage = task_ratio(
                test_config,
                query,
                grouping_stage,
                unwind_regroup_stage,
                projection_stage)

        elif "track" in query['task']:
            unwind_regroup_stage = task_track(
                test_config,
                query,
                grouping_stage,
                unwind_regroup_stage,
                projection_stage)

        elif "stats" in query['task']:
            projection_stage = {
                "$project": query['task']['stats']
            }
            
            unwind_regroup_stage = task_stats(
                test_config,
                query,
                grouping_stage,
                unwind_regroup_stage,
                projection_stage)

        if grouping_stage:
            res["$facet"][str(i)].append(dict(grouping_stage))
        if unwind_regroup_stage:
            res["$facet"][str(i)] += unwind_regroup_stage
        if projection_stage:
            res["$facet"][str(i)].append(dict(projection_stage))
        res["$facet"][str(i)].append(sort)

    res["$facet"] = dict(res["$facet"])

    return dict(res)


def create_grouping_stage(test_config):
    grouping_stage = defaultdict(dict)
    if test_config.aggregation in ['usa', "fiftyStates"] or (
            test_config.collection == 'states' and test_config.aggregation == 'state'):
        grouping_stage["$group"].update(
            {
                "_id": "$date"
            }
        )
    elif (test_config.collection == 'states' and test_config.aggregation == 'county') or (
            test_config.collection == 'covid' and test_config.aggregation == 'state'):
        grouping_stage['$group'].update(
            {
                "_id": f"$date"
            }
        )
    elif test_config.collection == 'covid' and test_config.aggregation == 'country_wide':
        grouping_stage = {
            "$group": {
                "_id": 1,
            }
        }
    else:
        print("something went wrong")
    return grouping_stage


def create_sort_stage(test_config: Configuration):
    """
    :param test_config: Configuration that describes the desired query
    :return: the sort stage required to perform the query described by test_confgi
    """
    res = defaultdict(dict)
    res["$sort"].update({"state": 1, "county": 1, "date": 1})
    return dict(res)


# Tasks
def task_track(test_config, query, grouping_stage, unwind_regroup_stage, projection_stage):
    var_to_track = query['task']['track']

    if test_config.aggregation in ['usa', "fiftyStates"] or (
            test_config.collection == 'states' and test_config.aggregation == 'state'):
        grouping_stage['$group'].update({
            var_to_track: {"$sum": f'${var_to_track}'}
        })

        unwind_regroup_stage.append({"$sort": {"_id": 1}})

        unwind_regroup_stage.append({
            "$group": {
                "_id": test_config.aggregation,
                "data": {"$push": {
                    "date": "$_id",
                    var_to_track: f"${var_to_track}"
                }}}})

        projection_stage['$project'].update({
            "_id": 0,
            "aggregation": "$_id",
            "data": 1,
        })

        if test_config.target:
            projection_stage['$project'].update({
                "target": test_config.target
            })

        if test_config.collection == 'states' and test_config.counties:
            projection_stage['$project'].update({
                "counties": test_config.counties
            })

    elif (test_config.aggregation == 'state' and test_config.collection == 'covid') or (
            test_config.aggregation == 'county' and test_config.collection == 'states'):
        grouping_stage['$group'].update({
            "daily_data": {"$push": {
                f"{test_config.aggregation}": f"${test_config.aggregation}",
                var_to_track: f"${var_to_track}"}}})

        unwind_regroup_stage.append({"$sort": {"_id": 1}})

        unwind_regroup_stage.append({
            "$group": {
                "_id": test_config.counties,
                "data": {
                    "$push": {
                        "date": "$_id",
                        "daily_data": "$daily_data"
                    }}}})

        projection_stage['$project'].update({
            "_id": 0,
            "data": 1})

        if test_config.aggregation == 'county':
            projection_stage['$project'].update({
                "counties": test_config.counties
            })
        else:
            projection_stage['$project'].update({
                "states": test_config.target
            })

    else:
        print("Trouble")

    return unwind_regroup_stage


def task_ratio(test_config, query, grouping_stage, unwind_regroup_stage, projection_stage):
    numerator_var = query["task"]["ratio"]["numerator"]
    denominator_var = query["task"]["ratio"]["denominator"]

    if test_config.aggregation in ['usa', "fiftyStates"] or (
            test_config.collection == 'states' and test_config.aggregation == 'state'):
        grouping_stage['$group'].update({
            f"{numerator_var}": {"$sum": f"${numerator_var}"},
            f"{denominator_var}": {"$sum": f"${denominator_var}"}
        })

        unwind_regroup_stage += [
            {'$addFields': {"ratio": {
                "$cond": {
                    "if": {"$gt": [f'${denominator_var}', 0]},
                    "then": {
                        "$divide": [
                            f'${numerator_var}',
                            f'${denominator_var}'
                        ]
                    },
                    "else": 0}}}},
            {'$sort': {"_id": 1}},
            {'$group': {
                "_id": test_config.aggregation,
                "data": {
                    "$push": {
                        "date": "$_id",
                        "ratio": "$ratio"}}}}]

        projection_stage['$project'].update({
            "_id": 0,
            "aggregation": "$_id",
            "data": 1
        })

        if test_config.target:
            projection_stage['$project'].update({
                "states": test_config.target
            })

        if test_config.collection == 'states' and test_config.counties:
            projection_stage['$project'].update({
                "counties": test_config.counties
            })

    elif (test_config.collection == 'covid' and test_config.aggregation == 'state') or (
            test_config.collection == 'states' and test_config.aggregation == 'county'):
        grouping_stage["$group"].update({
            "data": {"$push": {
                f"{test_config.aggregation}": f"${test_config.aggregation}",
                f'{numerator_var}': f'${numerator_var}',
                f'{denominator_var}': f'${denominator_var}'}}})

        unwind_regroup_stage += [{
            "$unwind": "$data"}, {
            "$addFields": {
                "data.ratio": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$gt": [f'$data.{denominator_var}', 0]},
                                {"$gt": [f'$data.{numerator_var}', 0]}]},
                        "then": {
                            "$divide": [
                                f'$data.{numerator_var}',
                                f'$data.{denominator_var}']},
                        "else": 0}}}}, {
            "$group": {
                "_id": "$_id",
                "daily_data": {
                    "$push": "$data"}}}, {
            "$project": {
                "_id": 0,
                f"date": "$_id",
                "daily_data.ratio": 1,
                f"daily_data.{test_config.aggregation}": 1}}, {
            "$sort": {"date": 1}}, {
            "$group": {
                "_id": test_config.counties if test_config.aggregation == 'county' else test_config.target,
                "data": {
                    "$push": {
                        "daily_data": "$daily_data",
                        "date": "$date"}}}}]

        projection_stage['$project'].update({
            "_id": 0,
            "data": 1})

        if test_config.aggregation == 'county':
            projection_stage['$project'].update({
                "counties": "$_id"
            })

        else:
            projection_stage['$project'].update({
                "states": "$_id"
            })

    else:
        print("Something went wrong here")

    return unwind_regroup_stage



# Task stats
def task_stats(test_config, query, grouping_stage, unwind_regroup_stage, projection_stage):
    statVars = []
    for i in range(len(projection_stage["$project"])):
        statVars.append(projection_stage["$project"][i])
    projection_stage["$project"] = {}
    if test_config.aggregation in ['usa', "fiftyStates"] or (
            test_config.collection == 'states' and test_config.aggregation == 'state'):
        
        for i in range(len(statVars)):
           grouping_stage['$group'].update({
                f"mean{statVars[i]}": {"$avg": f"${statVars[i]}"},
                f"stdDev{statVars[i]}": {"$stdDevPop": f"${statVars[i]}"}
                })
           projection_stage['$project'].update({
                "_id": 0,
                f"mean{statVars[i]}": 1,
                f"stdDev{statVars[i]}": 1
                })

        #perform aggregation for states
    elif (test_config.collection == 'covid' and test_config.aggregation == 'state') or (
            test_config.collection == 'states' and test_config.aggregation == 'county'):    
        grouping_stage['$group'].update({
           "_id": f'{test_config.aggregation}' 
        })
        for i in range(len(statVars)):
            grouping_stage['$group'].update({
                "_id": f"${test_config.aggregation}",
                f"mean{statVars[i]}": {"$avg": f"${statVars[i]}"},
                f"stdDev{statVars[i]}": {"$stdDevPop": f"${statVars[i]}"}
                })
            projection_stage['$project'].update({
                "_id": 0,
                f"{test_config.aggregation}": f"$_id",
                f"mean{statVars[i]}": 1,
                f"stdDev{statVars[i]}": 1
                })
    return unwind_regroup_stage


# Match filters
def create_date_filter(test_config: Configuration, res: defaultdict):
    """
    :param test_config: Configuration that describes the desired query
    :param res: the match stage to add the date filter to
    :return: the match stage with the date filter added in
    """
    if not (test_config.start == test_config.end == 0):
        res["$match"].update({"$and": [{"date": {"$gte": test_config.start}}, {"date": {"$lte": test_config.end}}]})
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
