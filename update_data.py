import csv
import json
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


def update_collections(db: Database) -> None:
    """
    If covid or states collections don't exist in db, downloads data, creates collections, and inserts data
    :param db: PyMongo Database in which to update collections
    """
    collections = db.list_collection_names()

    if COLL_COVID not in collections:
        covid_data = get_covid_data()
        add_collection(db, COLL_COVID, covid_data)

    if COLL_STATES not in collections:
        ny_data = get_states_data()
        add_collection(db, COLL_STATES, ny_data)
        fix_dates(db, COLL_STATES)


def get_covid_data() -> JSON:
    """
    :return: current COVID-19 by state data JSON
    """
    h = httplib2.Http('.cache', disable_ssl_certificate_validation=True)
    resp_headers, content = h.request(URL_COVID)
    return content


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


def fix_dates(db, collection):
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
                }
            }
        },
        {
            "$out": "states"
        }
    ]

    db[collection].aggregate(pipeline)
