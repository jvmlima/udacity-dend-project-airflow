from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    truncate_sql = 'DELETE FROM {};'

    insert_sql = """ 
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 table = '',
                 mode = 'append',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.mode = mode

    def execute(self, context):

        # setup ---
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # create table if not exists ---
        self.log.info(f'Creating {self.table} if not exists')
        redshift_hook.run(SqlQueries.create_table_queries.get(self.table))

        # delete data from table ---
        if self.mode == 'truncate-insert':
            self.log.info(f'Deleting data from {self.table}')
            redshift_hook.run(LoadDimensionOperator.truncate_sql.format(self.table))

        # insert into table ---
        self.log.info(f'Inserting into {self.table}')
        redshift_hook.run(LoadDimensionOperator.insert_sql.format(
            self.table,
            SqlQueries.insert_table_queries.get(self.table)))

        #  ---
        self.log.info(f'Successfully created {self.table}!')
