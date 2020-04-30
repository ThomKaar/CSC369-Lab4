import argparse

from config import Configuration
from pipeline import create_pipeline
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

    collection = db[test_config.collection]
    pipeline = create_pipeline(test_config)

    cursor = collection.aggregate(pipeline)

    for t in cursor:
        print(t)


if __name__ == '__main__':
    main()
