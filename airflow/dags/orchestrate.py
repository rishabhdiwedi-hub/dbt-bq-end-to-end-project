from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag
def orchestrate():

    @task
    def ingest_cdc():
        return "Ingesting CDC data..."

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_t"
    )

    silver_technical_test = BashOperator(
        task_id="silver_technical_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_t"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_obt_b"
    )

    silver_business_test = BashOperator(
        task_id="silver_business_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_obt_b"
    )

    gold_ephermeral = BashOperator(
        task_id="gold_ephermeral",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select gold/ephermeral"
    )

    gold_dimension = BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt snapshort'
    )

    gold_facts = BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow_walmart_project',
        bash_command='dbt run --select gold/fact'
    )


    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_test >> silver_business >> silver_business_test >> gold_ephermeral >> gold_dimension >> gold_facts

orchestrate_dag = orchestrate()



    




    

    

    