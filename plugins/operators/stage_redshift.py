from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    drop_sql = "DROP TABLE IF EXISTS {};"

    copy_sql = ("""
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {};
    """)


    @apply_defaults
    def __init__(self,
                 aws_credentials_id = 'aws_credentials',
                 redshift_conn_id = 'redshift',
                 table = '',
                 s3_bucket = '',
                 s3_prefix = '',
                 format_json = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.format_json = format_json

    def execute(self, context):

        # setup ---
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # drop table ---
        self.log.info(f'Dropping {self.table}')
        redshift_hook.run(StageToRedshiftOperator.drop_sql.format(self.table))

        # create table ---
        self.log.info(f'Creating {self.table}')
        redshift_hook.run(SqlQueries.create_table_queries.get(self.table))

        # copy into table ---
        self.log.info(f'Copying into {self.table}')
        redshift_hook.run(
            StageToRedshiftOperator.copy_sql.format(
            self.table, 
            f's3://{self.s3_bucket}/{self.s3_prefix}', 
            aws_credentials.access_key, 
            aws_credentials.secret_key,
            self.format_json)
            )

        #  ---
        self.log.info(f'Successfully staged {self.table}!')





