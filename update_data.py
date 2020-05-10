import csv
import json
from collections import defaultdict
from io import StringIO
from typing import List, Dict

import httplib2
from pymongo import MongoClient
from pymongo.database import Database

COLL_COVID = 'covid'
COLL_STATES = 'states'
URL_COVID = 'https://covidtracking.com/api/v1/states/daily.json'
URL_STATES = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
JSON = List[Dict]


def get_db_connection(cred_file: str) -> Database:
    """
    returns a connection to the database described with cred_file
    :param cred_file: JSON file containing server (Optional, default='localhost', username, password, authDB,
    db (working database)
    :return: connection to the PyMongo Database
    """
    with open(cred_file, 'r') as f:
        auth_data = json.load(f)

    server = auth_data.get('server') or 'localhost'
    try:
        user = auth_data['username']
        password = auth_data['password']
        auth_db = auth_data['authDB']
    except KeyError:
        print('Authorization file is missing a required field: username, password, or authDB')
        raise

    client = MongoClient(
        host=server,
        username=user,
        password=password,
        authsource=auth_db
    )

    return client[auth_data['db']]


def add_cumulative_properties(states_data):
    states_data.sort(key=lambda datum: datum['date'])
    states_data.sort(key=lambda datum: datum['county'])
    prev_deaths = defaultdict(int)
    prev_positives = defaultdict(int)
    for datum in states_data:
        datum.update({'deathIncrease': int(datum['deaths']) - prev_deaths[datum['county']]})
        datum.update({'positiveIncrease': int(datum['cases']) - prev_positives[datum['county']]})
        prev_deaths[datum['county']] = int(datum['deaths'])
        prev_positives[datum['county']] = int(datum['cases'])
    print(states_data)


def update_collections(db: Database, refresh: bool) -> None:
    """
    If covid or states collections don't exist in db, downloads data, creates collections, and inserts data
    :param refresh: If True, download current data and update database
    :param db: PyMongo Database in which to update collections
    """
    collections = db.list_collection_names()

    if COLL_COVID not in collections or refresh:
        if refresh:
            db[COLL_COVID].drop()
        covid_data: JSON = get_covid_data()
        add_collection(db, COLL_COVID, covid_data)
        fix_covid_data(db, COLL_COVID)

    if COLL_STATES not in collections or refresh:
        if refresh:
            db[COLL_STATES].drop()
        states_data: JSON = get_states_data()
        add_cumulative_properties(states_data)
        add_collection(db, COLL_STATES, states_data)
        fix_states_data(db, COLL_STATES)


def get_covid_data() -> JSON:
    """
    :return: current COVID-19 by state data JSON
    """
    h = httplib2.Http('.cache', disable_ssl_certificate_validation=True)
    resp_headers, content = h.request(URL_COVID)
    content = content.decode('utf-8')
    return json.loads(content)


# TODO: fix so that properties match spec: "tests", "testIncrease", "hospitalization", "hospitalizationIncrease"
def get_states_data() -> JSON:
    """
    :return: current COVID-19 by county data JSON
    """
    h = httplib2.Http('.cache', disable_ssl_certificate_validation=True)
    resp_headers, content = h.request(URL_STATES)
    reader = csv.DictReader(StringIO(content.decode('utf-8')))
    return json.loads(json.dumps([row for row in reader]))


def add_collection(db: Database, collection: str, data: JSON) -> None:
    """
    adds data to collection in db
    :param db: database to contain collection
    :param collection: collection in which to insert data
    :param data: data to be inserted - List of JSON objects
    """
    db[collection].insert_many(data)


def fix_covid_data(db, COLL_COVID):
    pass


# TODO: Calculate deathIncrease and positiveIncrease
def fix_states_data(db, collection):
    """
    Converts dates in a database collection from YYYY-MM-DD to YYYYMMDD
    :param db: the database to fix dates
    :param collection: the collection to fix dates
    """
    pipeline = [
        {
            "$set": {
                "date": {
                    "$toInt": {
                        "$dateToString": {
                            "date": {
                                "$toDate": "$date"
                            },
                            "format": "%Y%m%d"
                        }
                    }
                },
                "positive": {"$toInt": "$cases"},
                "death": {"$toInt": "$deaths"}
            }
        },
        {
            "$out": "states"
        }
    ]

    db[collection].aggregate(pipeline)
