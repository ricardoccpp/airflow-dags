from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from airflow.configuration import AIRFLOW_HOME

sql_path = AIRFLOW_HOME + ('/dags/ETL_Scripts/')

conn = PostgresHook('covid_db_postgres')
conn_engine = conn.get_sqlalchemy_engine()

script = open(sql_path + '01.extract_tables.sql', 'r').read()

conn_engine.execute(script)
