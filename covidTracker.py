import argparse

from config import Configuration
from update_data import get_db_connection, update_collections


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-auth', type=str, required=False, default='credentials.json')
    parser.add_argument('-config', type=str, required=False, default='trackerConfig.json')
    args = parser.parse_args()

    try:
        test_config = Configuration(args.config)
    except ValueError as err:
        print(f'{type(err).__name__}: {err}')
        return

    db = get_db_connection(args.auth)
    update_collections(db, test_config.refresh)
    # Run analyses here
    print(test_config.pipeline)

    collection = db.covid if test_config.collection == 'covid' else db.states

    cursor = collection.aggregate(test_config.pipeline)

    for t in cursor:
        print(t)


if __name__ == '__main__':
    main()
