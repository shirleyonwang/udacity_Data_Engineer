from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

'''
default_args = {
    'owner': 'wxl',
    'start_date': datetime(2020,9, 9),
    'depends_on_past': False,   
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('udac_data_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval= '@hourly', # or '0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    
    task_id='Stage_events',
    dag=dag,
    
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "s3://udacity-dend",
    s3_key = "log_data/2018/11",
    json_value='s3://udacity-dend/log_json_path.json'    
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "s3://udacity-dend",
    s3_key = "song_data/A/B/C",
  
    json_value='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    
    redshift_conn_id='redshift',
    aws_credentials_id = "aws_credentials",
    table_name='songplays',                
    append_data='True',
    
    dag=dag
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id = "aws_credentials",
    table_name='users', 
    sql = SqlQueries.user_table_insert,
    append_data='True',
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id = "aws_credentials",
    table_name='songs',     
    sql = SqlQueries.song_table_insert,
    append_data='True',
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id = "aws_credentials",
    table_name='artists',        
    sql = SqlQueries.artist_table_insert,
    append_data='True',
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    aws_credentials_id = "aws_credentials",
    table_name='time', 
    sql = SqlQueries.time_table_insert,
    append_data='True',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    aws_credentials_id = "aws_credentials",
    table_names = ['users','artists','songs','time'],  
    dq_checks=[
            {'table': 'users',
             'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null",
             'expected_result': 0},
            {'table': 'songs',
             'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
             'expected_result': 0}
        ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task >> [stage_events_to_redshift,stage_songs_to_redshift]  >> load_songplays_table >> [load_song_dimension_table,load_user_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks >>end_operator

'''
