from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from datetime import datetime


with DAG(dag_id="ousontlesabstentions_france_bash", start_date=datetime(2025, 3, 19), schedule="0 0 * * *") as dag:

    op_download_geo_coord = BashOperator(
        task_id="task_download_geo_coord",
        bash_command="docker compose run -d datafeed --stage download_geo_coord",
    )

    op_download_datagouv_france2017 = BashOperator(
        task_id="task_download_datagouv_france2017",
        bash_command="docker compose run -d datafeed --stage download_datagouv_france2017",
    )

    op_download_datagouv_france2022 = BashOperator(
        task_id="task_download_datagouv_france2022",
        bash_command="docker compose run -d datafeed --stage download_datagouv_france2022",
    )

    op_download_opendatasoft_france2017 = BashOperator(
        task_id="task_download_opendatasoft_france2017",
        bash_command="docker compose run -d datafeed --stage download_opendatasoft_france2017",
    )

    op_download_opendatasoft_france2022 = BashOperator(
        task_id="task_download_opendatasoft_france2022",
        bash_command="docker compose run -d datafeed --stage download_opendatasoft_france2022",
    )

    op_insert_france2017 = BashOperator(
        task_id="task_insert_france2017",
        bash_command="docker compose run -d datafeed --stage france2017",
    )

    op_insert_france2022 = BashOperator(
        task_id="task_insert_france2022",
        bash_command="docker compose run -d datafeed --stage france2022",
    )

    chain([op_download_datagouv_france2017, op_download_opendatasoft_france2017, op_download_geo_coord] ,op_insert_france2017)
    chain([op_download_datagouv_france2022, op_download_opendatasoft_france2022, op_download_geo_coord] ,op_insert_france2022)

