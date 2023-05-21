from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries


default_args = {
    'owner': 'jlima',
    'start_date': datetime.now() - timedelta(hours = 1, minutes = 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300, # 5 minutes
    'catchup': False,
    'email_on_retry': False
}

dag = DAG(
    'etl_s3_to_redshift',
    default_args = default_args,
    description = 'Load and transform data in Redshift with Airflow',
    schedule_interval = '@hourly'
    )

start_operator = EmptyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_prefix = 'log_data',
    format_json = "FORMAT AS JSON 's3://udacity-dend/log_json_path.json' REGION 'us-west-2'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    aws_credentials_id = 'aws_credentials',
    redshift_conn_id = 'redshift',
    table = 'staging_songs',
    s3_bucket = 'udacity-dend',
    s3_prefix = 'song_data/A/A/A',
    format_json = "FORMAT AS JSON 'auto' REGION 'us-west-2'"
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = 'redshift',
    table = 'songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'users',
    mode = 'truncate-insert' # 'append' or 'truncate-insert'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'songs',
    mode = 'truncate-insert' # 'append' or 'truncate-insert'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'artists',
    mode = 'truncate-insert' # 'append' or 'truncate-insert'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table = 'times',
    mode = 'truncate-insert' # 'append' or 'truncate-insert'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
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
    },
    tables = {'songplays': ['playid', 'start_time'],
              'users':     ['userid', 'first_name']}
)

end_operator = EmptyOperator(task_id='Stop_execution',  dag=dag)


# start_operator >> run_quality_checks >> end_operator

start_operator              >> stage_events_to_redshift
start_operator              >> stage_songs_to_redshift

stage_events_to_redshift    >> load_songplays_table
stage_songs_to_redshift     >> load_songplays_table

load_songplays_table        >> load_song_dimension_table
load_songplays_table        >> load_user_dimension_table
load_songplays_table        >> load_artist_dimension_table
load_songplays_table        >> load_time_dimension_table

load_song_dimension_table   >> run_quality_checks
load_user_dimension_table   >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table   >> run_quality_checks

run_quality_checks          >> end_operator
