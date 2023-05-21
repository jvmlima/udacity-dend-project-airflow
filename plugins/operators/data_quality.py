from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 rules = '',
                 tables = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.rules = rules
        self.tables = tables

    def execute(self, context):

        # setup ---
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # run data quality checks --

        for table in self.tables:

            for rule in self.rules:

                query_sql = self.rules.get(rule).get('query')
                operation = self.rules.get(rule).get('operation')
                ref_value = self.rules.get(rule).get('ref_value')

                queries = []

                if rule == 'row_count':
                    queries = [query_sql.format(table)]

                elif rule == 'null_count':
                    cols = self.tables.get(table)
                    for col in cols:
                        queries.append(query_sql.format(table, col))
                
                for query in queries:

                    result = int(redshift_hook.get_first(query)[0])

                    msg = f'{table :<12} | {rule :<12} | returned: {result :>4} | expected: {operation :<12} {ref_value :>4} | {query :<60}'

                    if operation == 'greater_than':
                        if result <= ref_value:
                            raise AssertionError(f'Failed -> {msg}')
                        
                    if operation == 'less_than':
                        if result >= ref_value:
                            raise AssertionError(f'Failed -> {msg}')
                    
                    self.log.info(f'Passed -> {msg}')