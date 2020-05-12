import argparse
from pprint import pprint

from config import Configuration
from output_graph import create_graph
from output_html import get_header, Query
from output_table import get_table, create_table, Query
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

        if test_config.analysis[n]['task'].get('stats') is not None:
            test_config.analysis[n]['task'].update({"aggregation": test_config.aggregation})
            q = Query(
                task=test_config.analysis[n]['task'],
                output=test_config.analysis[n]['output'],
                data={"data": result[t]})
        else:
            q = Query(
                task=test_config.analysis[n]['task'],
                output=test_config.analysis[n]['output'],
                data=result[t][0])

        if ('track' in q.task or 'ratio' in q.task) and 'table' in q.output:
            row = q.output['table'].get('row')
            col = q.output['table'].get('column')
            title = q.output['table'].get('title')
            page += create_table(q, row, col, title)

        for key in q.output:
            if ('track' in q.task or 'ratio' in q.task) and key == 'table':
                page += get_table(q)
            if ('track' in q.task or 'ratio' in q.task) and key == 'graph':
                graph = create_graph(q)
                # page += graph or something like that
            else:
                with open(test_config.output_file, 'w') as f:
                    pprint(result, f)

    print("done")


if __name__ == '__main__':
    main()