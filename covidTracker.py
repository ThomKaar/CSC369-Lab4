import argparse
import json
import csv
from io import StringIO

import httplib2
from pymongo import MongoClient
from pymongo.database import Database

JSON = str
COVID_DB = 'covid'
COUNTY_DB = 'states'
COVID_URL = 'https://covidtracking.com/api/v1/states/daily.json'
COUNTY_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'


def get_covid_data() -> JSON:
    h = httplib2.Http('.cache', disable_ssl_certificate_validation=True)
    resp_headers, content = h.request(COVID_URL)
    return content


def get_county_data() -> JSON:
    h = httplib2.Http('.cache', disable_ssl_certificate_validation=True)
    resp_headers, content = h.request(COUNTY_URL)
    reader = csv.DictReader(StringIO(content.decode('utf-8')))
    return json.loads(json.dumps([row for row in reader]))


def add_collection(db: Database, collection: str, data: JSON) -> None:
    db[collection].insert_many(data)


def get_db_connection(cred_file: str) -> Database:
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
    collections = db.list_collection_names()

    if COVID_DB not in collections:
        covid_data = get_covid_data()
        add_collection(db, COVID_DB, covid_data)

    if COUNTY_DB not in collections:
        ny_data = get_county_data()
        add_collection(db, COUNTY_DB, ny_data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-auth', type=str, required=False, default='credentials.json')
    parser.add_argument('-config', type=str, required=False, default='trackerConfig.json')
    args = parser.parse_args()

    db = get_db_connection(args.auth)
    update_collections(db)
    # with open(args.config, 'r') as f:
    #     config_data = json.load(f)
    #


if __name__ == '__main__':
    main()
