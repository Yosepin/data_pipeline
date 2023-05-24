import airflow
import datetime
from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat

default_args = {
    'owner': 'yosepinpakpahan',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 24, tzinfo=timezone(timedelta(hours=7))),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='endtoend_task',
    description='semoga berkah',
    schedule='@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'create_table_in_hive',
        bash_command = 'python3 /home/yosepinpakpahan/endproject.py'
    )

    task2 = BashOperator(
        task_id='dim1',
        bash_command='python3 /home/yosepinpakpahan/dim1.py'
    )

    task3 = BashOperator(
        task_id='dim2',
        bash_command='python3 /home/yosepinpakpahan/dim2.py'
    )

    task4 = BashOperator(
        task_id='dim3',
        bash_command='python3 /home/yosepinpakpahan/dim3.py'
    )    

    task5 = BashOperator(
        task_id='dim4',
        bash_command='python3 /home/yosepinpakpahan/dim4.py'
    )

    task6 = BashOperator(
        task_id='dim5',
        bash_command='python3 /home/yosepinpakpahan/dim5.py'
    )

    task7 = BashOperator(
        task_id='tbl_fact',
        bash_command='python3 /home/yosepinpakpahan/t1.py'
    )


    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7
