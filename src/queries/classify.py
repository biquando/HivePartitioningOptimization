import json

def classify(queries: list[str], table_names: list[str]) -> dict[str, list[str]]:
    classes = {name: list() for name in table_names}

    for query in queries:
        tokens = query.split()
        for table_name in classes:
            if table_name in tokens:
                classes[table_name].append(query)
                break
    return classes

def main():
    with open('../schema.json', 'r') as f:
        schema = json.load(f)
        table_names = list(schema.keys())

    with open('all.json', 'r') as f:
        queries = json.load(f)
    classified_queries = classify(queries, table_names)

    for table_name in classified_queries:
        with open(f'{table_name}.json', 'w') as f:
            json.dump(classified_queries[table_name], f)

if __name__ == '__main__':
    main()
