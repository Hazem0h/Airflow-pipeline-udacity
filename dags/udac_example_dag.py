from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

# Setup the default args according to the project instructions
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2018, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

# TODO: What's the desired schedule?
dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1 # because start data is waaay behind today, so there'll be a lot of scheduled runs
        )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

################################################# Table Creation tasks ################################
create_staging_events_table_task = PostgresOperator(
    task_id = "create_staging_events_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = SqlQueries.staging_events_table_create
)

create_staging_songs_table_task = PostgresOperator(
    task_id = "create_staging_songs_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = SqlQueries.staging_songs_table_create
)

create_time_table_task = PostgresOperator(
    task_id = "create_time_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = SqlQueries.time_table_create
)

create_user_table_task = PostgresOperator(
    task_id = "create_users_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = SqlQueries.user_table_create
)

create_artist_table_task = PostgresOperator(
    task_id = "create_artists_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = SqlQueries.artist_table_create
)

create_song_table_task = PostgresOperator(
    task_id = "create_songs_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = SqlQueries.song_table_create
)

create_songplay_table_task = PostgresOperator(
    task_id = "create_songplays_table",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = SqlQueries.songplay_table_create
)

wait_for_table_creation_op = DummyOperator(
    task_id = "wait_for_table_creation",
    dag = dag
)

#################################################  S3 To Staging Tables Tasks ################################
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table = "staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data/{{ ds.year }}/{{ ds.month }}/{{ ds }}-events.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table = "staging_songs",
    s3_bucket = "udacity-dend",
    s3_key = "song_data"
)

###########################################  Loading to Fact and Dim Tables Tasks #########################
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    table_specific_sql = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "users",
    table_sepcific_sql = SqlQueries.user_table_insert,
    truncate_table = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "songs",
    table_sepcific_sql = SqlQueries.song_table_insert,
    truncate_table = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    table_specific_sql = SqlQueries.artist_table_insert,
    truncate_table = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time_table",
    table_specific_sql = SqlQueries.time_table_insert,
    truncate_table = True
)

######################################################## Quality Check ###########################################

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tests = [
        ('SELECT COUNT(*) FROM users WHERE user_id IS NULL', 0),
        ('SELECT COUNT(*) FROM songs WHERE song_id IS NULL', 0),
        ('SELECT COUNT(*) FROM artists WHERE artist_id IS NULL', 0),
        ('SELECT COUNT(*) FROM time_table WHERE start_time IS NULL', 0),
        ('SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL', 0)
    ]
)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag
)

# Define the dependencies of tasks
start_operator >> create_staging_events_table_task
start_operator >> create_staging_songs_table_task
start_operator >> create_time_table_task
start_operator >> create_user_table_task
start_operator >> create_artist_table_task
start_operator >> create_song_table_task
start_operator >> create_songplay_table_task

create_staging_events_table_task >> wait_for_table_creation_op
create_staging_songs_table_task >> wait_for_table_creation_op
create_time_table_task >> wait_for_table_creation_op
create_user_table_task >> wait_for_table_creation_op
create_artist_table_task >> wait_for_table_creation_op
create_song_table_task >> wait_for_table_creation_op
create_songplay_table_task >> wait_for_table_creation_op

wait_for_table_creation_op >> stage_events_to_redshift
wait_for_table_creation_op >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator


