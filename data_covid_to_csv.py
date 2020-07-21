from datetime import timedelta
import codecs

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.configuration import AIRFLOW_HOME
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook

args = {
    'owner': 'Ricardo Tanaka',
    'start_date': days_ago(2),
}

def execute_test_sql(script_path, connection='covid_db_postgres'):
    query = open(script_path, 'r').read()
    conn = PostgresHook(connection)
    result_sql = conn.get_records(query)
    print(result_sql)
    return f'Script: {script_path} executed!'


with DAG(dag_id='data_covid_to_csv',
         default_args=args,
         schedule_interval='0 0 * * *',
         dagrun_timeout=timedelta(minutes=60),
         tags=['example']) as dag:

    # Dummy - Start DAG
    start_dag = BashOperator(
        task_id='start_dag',
        bash_command='echo Starting DAG...'
    )

    # Exams


    teste_sql = PythonOperator(
        task_id='task_teste_sql',
        python_callable=execute_test_sql,
        op_kwargs={'script_path': AIRFLOW_HOME + ('/dags/ETL_Scripts/teste_sql.sql')}
    )


    # load_tasks = [
    #     load_stg_exam_results_sirio,
    #     load_stg_exam_results_fleury,
    #     load_stg_exam_results_einstein,
    #     load_stg_patients_sirio,
    #     load_stg_patients_fleury,
    #     load_stg_patients_einstein,
    # ]
    # chain(*load_tasks)

    start_dag >> teste_sql