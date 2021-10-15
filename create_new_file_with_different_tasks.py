from datetime import datetime
import os
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
# from mysql_operations import insert_new_data

with DAG(
    dag_id='Create_files_with_DAGS',
    description='ETL DAG tutorial',
    schedule_interval=None,
    start_date=datetime(2021, 10, 15),
    catchup=False,
    tags=['demo'],
) as dag:

    def create_files(random_id):

        print("creating files")

        with open(f"/opt/airflow/dags/{random_id}_file.txt","w") as f:
            f.write(f"File {random_id}")


    create_files_task = PythonOperator(
        task_id="create_files-task-id",
        python_callable=create_files
    )
    
    
    for item_ in range(4):
        print("Create multiple tasks to generate a new file")
        task = PythonOperator(
            task_id='generate_file_' + str(item_),
            python_callable=create_files,
            op_kwargs={'random_id': item_},
        )
    
    
        create_files_task >> task
