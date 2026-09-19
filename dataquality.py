from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
)

GCP_PROJECT_ID = Variable.get("prod_gcp_project_id", default_var="dev_gcp_project_id")

default_args = {
    "owner": "hellboy   ",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bigquery_data_quality_checks_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["bigquery", "data_quality", "silver"],
) as dag:

    check_dim_track_duplicates = BigQueryCheckOperator(
        task_id="check_dim_track_duplicates",
        sql=f"""
            SELECT COUNT(*) = 0
            FROM (
                SELECT isrc
                FROM `{GCP_PROJECT_ID}.silver.dim_track`
                WHERE isrc IS NOT NULL
                GROUP BY isrc
                HAVING COUNT(*) > 1
            )
        """,
        use_legacy_sql=False,
    )

    check_dim_track_nulls = BigQueryCheckOperator(
        task_id="check_dim_track_nulls",
        sql=f"""
            SELECT COUNT(*) = 0
            FROM `{GCP_PROJECT_ID}.silver.dim_track`
            WHERE isrc IS NULL OR track_title IS NULL
        """,
        use_legacy_sql=False,
    )

    check_fact_video_duplicates = BigQueryCheckOperator(
        task_id="check_fact_video_duplicates",
        sql=f"""
            SELECT COUNT(*) = 0
            FROM (
                SELECT youtube_video_id
                FROM `{GCP_PROJECT_ID}.silver.fact_youtube_video`
                WHERE youtube_video_id IS NOT NULL
                GROUP BY youtube_video_id
                HAVING COUNT(*) > 1
            )
        """,
        use_legacy_sql=False,
    )

    check_fact_video_nulls = BigQueryCheckOperator(
        task_id="check_fact_video_nulls",
        sql=f"""
            SELECT COUNT(*) = 0
            FROM `{GCP_PROJECT_ID}.silver.fact_youtube_video`
            WHERE youtube_video_id IS NULL OR video_title IS NULL
        """,
        use_legacy_sql=False,
    )

    check_unlinked_facts_threshold = BigQueryValueCheckOperator(
        task_id="check_unlinked_facts_threshold",
        sql=f"""
            SELECT COUNTIF(track_sk IS NULL) / COUNT(*)
            FROM `{GCP_PROJECT_ID}.silver.fact_youtube_video`
        """,
        pass_value=0.20,
        tolerance=0.0,
        use_legacy_sql=False,
    )

    [
        check_dim_track_duplicates,
        check_dim_track_nulls,
        check_fact_video_duplicates,
        check_fact_video_nulls,
    ] >> check_unlinked_facts_threshold
