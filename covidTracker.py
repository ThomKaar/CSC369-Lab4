import argparse

from update_data import get_db_connection, update_collections


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-auth', type=str, required=False, default='credentials.json')
    parser.add_argument('-config', type=str, required=False, default='trackerConfig.json')
    args = parser.parse_args()

    db = get_db_connection(args.auth)
    update_collections(db)



if __name__ == '__main__':
    main()
