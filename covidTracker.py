import argparse
from pprint import pprint

from config import Configuration
from output_html import get_header
from output_table import create_table, Query
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

    with open('pipeline.json', 'w') as f:
        print(pipeline, file=f)

    result = collection.aggregate(pipeline).next()

    page = get_header()
    for n, t in enumerate(result):
        q = Query(
            task=test_config.analysis[n]['task'],
            output=test_config.analysis[n]['output'],
            data=result[t][0])
        if ('track' in q.task or 'ratio' in q.task) and 'table' in q.output:
            row = q.output['table'].get('row')
            col = q.output['table'].get('column')
            title = q.output['table'].get('title')
            page += create_table(q, row, col, title)

            with open(f'my.html', 'w') as f:
                print(page, file=f)
        else:
            with open(test_config.output_file, 'w') as f:
                pprint(result, f)

    print("done")


if __name__ == '__main__':
    main()
