from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import pandas as pd

@dag(schedule_interval=None, start_date=days_ago(1))
def migrate_mysql_to_postgres():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    @task
    def extract_from_mysql(table_name):
        """Extract data from MySQL."""
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        mysql_hook = MySqlHook("mysql_dibimbing").get_sqlalchemy_engine()

        with mysql_hook.connect() as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name}", con=conn)

        return df.to_dict(orient='records')  # Convert DataFrame to list of dictionaries

    @task
    def load_to_postgres(table_name, records):
        """Load data into PostgreSQL."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()
        df = pd.DataFrame(records) 

        with postgres_hook.connect() as conn:
            df.to_sql(name=table_name, con=conn, if_exists='replace', index=False)

    tables = ["clients", "departments", "employees", "invoices", "projects"]  

    for table in tables:
        data = extract_from_mysql(table)
        load_to_postgres(table, data) >> end_task

    start_task >> [extract_from_mysql(table) >> load_to_postgres(table, extract_from_mysql(table)) for table in tables] >> end_task

migrate_mysql_to_postgres()
