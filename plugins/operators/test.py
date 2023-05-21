rules = {
        'row_count': {
            'query': 'SELECT COUNT(*) FROM {}',
            'operation': 'greater_than',
            'ref_value': 0
        },
        'null_count': {
            'query': 'SELECT COUNT(*) FROM {} WHERE {} IS NULL',
            'operation': 'less_than',
            'ref_value': 1
        }
    }

tables = {'songplays': ['songplay_id', 'start_time'],
          'users':     ['userid', 'firstname']}



for table in tables:

    for rule in rules:

        query_sql = rules.get(rule).get('query')
        operation = rules.get(rule).get('operation')
        ref_value = rules.get(rule).get('ref_value')

        queries = []

        if rule == 'row_count':
            queries = [query_sql.format(table)]

        elif rule == 'null_count':
            cols = tables.get(table)
            for col in cols:
                queries.append(query_sql.format(table, col))
        
        for query in queries:

            result = 5

            failed = False

            msg = f'{table :<12} | {rule :<12} | returned: {result :>4} | expected: {operation :<12} {ref_value :>4} | {query :<60}'

            if operation == 'greater_than':
                if result <= ref_value:
                    print(f'Failed -> {msg}')
                    failed = True
            if operation == 'less_than':
                if result >= ref_value:
                    print(f'Failed -> {msg}')
                    failed = True
            
            if not failed:
                print(f'Passed -> {msg}')