import argparse
import json
import httplib2
from pymongo import MongoClient

COVID_DB = 'covid'
NY_DB = 'states'
COVID_URL = 'https://covidtracking.com/api/v1/states/daily.json'

def get_covid_data():
    h = httplib2.Http('.cache', disable_ssl_certificate_validation=True)
    resp_headers, content = h.request(COVID_URL)
    print(content)


def add_collection(db, covid_data):
    pass


def get_ny_data():
    pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-auth', type=str, required=False, default='credentials.json')
    parser.add_argument('-config', type=str, required=False, default='trackerConfig.json')
    args = parser.parse_args()

    with open(args.auth, 'r') as f:
        auth_data = json.load(f)

    server = auth_data.get('server') or 'localhost'
    try:
        user = auth_data['username']
        password = auth_data['password']
        auth_db = auth_data['authDB']
    except KeyError:
        print('Authorization file is missing a required field: username, password, or authDB')
        return

    client = MongoClient(
        host=server,
        username=user,
        password=password,
        authsource=auth_db
    )

    db = client[auth_data['db']]

    collections = db.list_collection_names()
    if COVID_DB not in collections:
        covid_data = get_covid_data()
        add_collection(db, covid_data)
    if NY_DB not in collections:
        ny_data = get_ny_data()
        add_collection(db, ny_data)



    # with open(args.config, 'r') as f:
    #     config_data = json.load(f)
    #




if __name__ == '__main__':
    main()