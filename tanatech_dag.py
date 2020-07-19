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

# copy data from local file to table
def copy_from_local_file(schema, table, local_path, delimiter=',', encoding='utf-8', truncate=True,
                   connection='covid_db_postgres'):
    conn = PostgresHook(connection)
    conn_engine = conn.get_sqlalchemy_engine()
    if encoding != 'utf-8':
        print('Starting encoding conversion...')
        targetFileName = 'teste.csv'
        BLOCKSIZE = 1048576  # or some other, desired size in bytes
        with codecs.open(local_path, 'r', encoding) as sourceFile:
            with codecs.open(targetFileName, 'w', 'utf-8') as targetFile:
                while True:
                    contents = sourceFile.read(BLOCKSIZE)
                    if not contents:
                        break
                    targetFile.write(contents)
        local_path = targetFileName
    if truncate:
        print('Truncating table...')
        conn_engine.execute(f'truncate table {schema}.{table};')
    print('Loading table...')
    sql = f"COPY {schema}.{table} FROM STDIN DELIMITERS '{delimiter}' csv header encoding 'utf-8'"
    conn.copy_expert(sql, filename=local_path)
    return f'Table: {table} loaded!'

# copy data from file to table
def copy_from_file(schema, table, local_path, delimiter=',', encoding='utf-8', truncate=True,
                   connection='covid_db_postgres'):
    conn = PostgresHook(connection)
    conn_engine = conn.get_sqlalchemy_engine()
    if truncate:
        print('Truncating table...')
        conn_engine.execute(f'truncate table {schema}.{table};')
    print('Loading table...')
    conn_engine.execute(f"""copy {schema}.{table}
                            from '{local_path}'
                            delimiters '{delimiter}' csv header encoding '{encoding}';
                            commit;""")
    return f'Table: {table} loaded!'

# Execute script writen in a sql file
def execute_script_file(script_path, connection='covid_db_postgres'):
    query = open(script_path, 'r').read()
    conn = PostgresHook(connection)
    conn_engine = conn.get_sqlalchemy_engine()
    conn_engine.execute(query)
    return f'Script: {script_path} executed!'

def execute_test_sql(script_path, connection='covid_db_postgres'):
    query = open(script_path, 'r').read()
    conn = PostgresHook(connection)
    result_sql = conn.get_records(query)
    print(result_sql)
    return f'Script: {script_path} executed!'


with DAG(dag_id='tanatech_dag',
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
    load_stg_exam_results_sirio = PythonOperator(
        task_id='load_table_stg_exam_results_sirio',
        python_callable=copy_from_local_file,
        op_kwargs={'schema': 'stage',
                   'table': 'stg_exam_results_sirio',
                   'local_path': AIRFLOW_HOME + ('/dags/input/hsl_lab_result_1.csv'),
                   'delimiter': '|',
                   'encoding': 'utf-8',
                   'truncate': True}
    )

    load_stg_exam_results_fleury = PythonOperator(
        task_id='load_stg_exam_results_fleury',
        python_callable=copy_from_local_file,
        op_kwargs={'schema': 'stage',
                   'table': 'stg_exam_results_fleury',
                   'local_path': AIRFLOW_HOME + ('/dags/input/Grupo_Fleury_Dataset_Covid19_Resultados_Exames.csv'),
                   'delimiter': '|',
                   'encoding': 'latin1',
                   'truncate': True}
    )

    load_stg_exam_results_einstein = PythonOperator(
        task_id='load_stg_exam_results_einstein',
        python_callable=copy_from_local_file,
        op_kwargs={'schema': 'stage',
                   'table': 'stg_exam_results_einstein',
                   'local_path': AIRFLOW_HOME + ('/dags/input/einstein_full_dataset_exames.csv'),
                   'delimiter': '|',
                   'encoding': 'utf-8',
                   'truncate': True}
    )

    # Patients
    load_stg_patients_sirio = PythonOperator(
        task_id='load_stg_patients_sirio',
        python_callable=copy_from_local_file,
        op_kwargs={'schema': 'stage',
                   'table': 'stg_patients_sirio',
                   'local_path': AIRFLOW_HOME + ('/dags/input/hsl_patient_1.csv'),
                   'delimiter': '|',
                   'encoding': 'utf-8',
                   'truncate': True}
    )

    load_stg_patients_fleury = PythonOperator(
        task_id='load_stg_patients_fleury',
        python_callable=copy_from_local_file,
        op_kwargs={'schema': 'stage',
                   'table': 'stg_patients_fleury',
                   'local_path': AIRFLOW_HOME + ('/dags/input/Grupo_Fleury_Dataset_Covid19_Pacientes.csv'),
                   'delimiter': '|',
                   'encoding': 'latin1',
                   'truncate': True}
    )

    load_stg_patients_einstein = PythonOperator(
        task_id='load_stg_patients_einstein',
        python_callable=copy_from_local_file,
        op_kwargs={'schema': 'stage',
                   'table': 'stg_patients_einstein',
                   'local_path': AIRFLOW_HOME + ('/dags/input/einstein_full_dataset_paciente.csv'),
                   'delimiter': '|',
                   'encoding': 'utf-8',
                   'truncate': True}
    )

    load_stg_desfecho_sirio = PythonOperator(
        task_id='load_stg_desfecho_sirio',
        python_callable=copy_from_local_file,
        op_kwargs={'schema': 'stage',
                   'table': 'stg_desfecho_sirio',
                   'local_path': AIRFLOW_HOME + ('/dags/input/hsl_desfecho_1.csv'),
                   'delimiter': '|',
                   'encoding': 'utf-8',
                   'truncate': True}
    )

    join_stage_exams = PythonOperator(
        task_id='join_stage_exams',
        python_callable=execute_script_file,
        op_kwargs={'script_path': AIRFLOW_HOME + ('/dags/ETL_Scripts/001_join_stage_data_exams.sql')}
    )

    join_stage_patients = PythonOperator(
        task_id='join_stage_patients',
        python_callable=execute_script_file,
        op_kwargs={'script_path': AIRFLOW_HOME + ('/dags/ETL_Scripts/002_join_stage_data_patients.sql')}
    )

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

    start_dag >> load_stg_exam_results_sirio
    start_dag >> load_stg_exam_results_fleury
    start_dag >> load_stg_exam_results_einstein
    start_dag >> load_stg_patients_sirio
    start_dag >> load_stg_patients_fleury
    start_dag >> load_stg_patients_einstein
    start_dag >> load_stg_desfecho_sirio

    load_stg_exam_results_sirio >> join_stage_exams
    load_stg_exam_results_fleury >> join_stage_exams
    load_stg_exam_results_einstein >> join_stage_exams

    load_stg_patients_sirio >> join_stage_patients
    load_stg_patients_fleury >> join_stage_patients
    load_stg_patients_einstein >> join_stage_patients

    join_stage_patients >> teste_sql