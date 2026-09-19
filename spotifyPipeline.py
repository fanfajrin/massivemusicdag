import json
import re
from datetime import datetime, timedelta
import musicbrainzngs
from google.cloud import storage

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

GCP_PROJECT_ID = Variable.get("prod_gcp_project_id", default_var="dev_gcp_project_id")
GCS_BUCKET_NAME = Variable.get("prod_gcs_bucket_name", default_var="dev_gcs_bucket_name")
POSTGRES_CONN_ID = Variable.get("prod_postgres_conn_id", default_var="dev_postgres")

musicbrainzngs.set_useragent(
    app="AirflowMedallionPipeline", version="1.0", contact="admin@company.com"
)

default_args = {
    "owner": "hellboy",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def ingest_musicbrainz_to_gcs(**kwargs):
    bucket_name = GCS_BUCKET_NAME
    pg_conn_id = POSTGRES_CONN_ID

    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    sql_query = "SELECT isrc_code FROM isrc_source_table WHERE status = 'PENDING';"
    records = pg_hook.get_records(sql_query)

    if not records:
        print("No pending ISRCs found in PostgreSQL. Skipping execution.")
        return

    isrc_list = [row[0] for row in records]
    print(f"Retrieved {len(isrc_list)} pending ISRC(s) from Postgres: {isrc_list}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    records_to_upload = []

    for isrc in isrc_list:
        try:
            mb_result = musicbrainzngs.get_recordings_by_isrc(
                isrc, includes=["artists", "releases"]
            )
            for rec in mb_result.get("isrc", {}).get("recording-list", []):
                artists = [
                    ac.get("artist", {}).get("name")
                    for ac in rec.get("artist-credit", [])
                    if "artist" in ac
                ]
                artist_str = ", ".join(filter(None, artists))

                releases = rec.get("release-list", [])
                if releases:
                    for rel in releases:
                        records_to_upload.append({
                            "isrc": isrc,
                            "recording_title": rec.get("title"),
                            "artist_name": artist_str,
                            "album": rel.get("title"),
                            "release_date": rel.get("date"),
                            "duration_ms": rec.get("length"),
                            "source_system": "MUSICBRAINZ",
                            "ingested_at": datetime.utcnow().isoformat(),
                        })
                else:
                    records_to_upload.append({
                        "isrc": isrc,
                        "recording_title": rec.get("title"),
                        "artist_name": artist_str,
                        "album": None,
                        "release_date": None,
                        "duration_ms": rec.get("length"),
                        "source_system": "MUSICBRAINZ",
                        "ingested_at": datetime.utcnow().isoformat(),
                    })
        except Exception as e:
            print(f"MusicBrainz lookup failed for ISRC {isrc}: {e}")

    if records_to_upload:
        execution_date = kwargs.get("ds")
        blob_name = f"bronze/musicbrainz/{execution_date}/mb_data_{int(datetime.utcnow().timestamp())}.json"
        blob = bucket.blob(blob_name)
        
        json_lines = "\n".join([json.dumps(record) for record in records_to_upload])
        blob.upload_from_string(json_lines, content_type="application/json")
        print(f"Successfully uploaded raw MusicBrainz payload to gs://{bucket_name}/{blob_name}")

        formatted_isrcs = "', '".join(isrc_list)
        update_sql = f"UPDATE isrc_source_table SET status = 'PROCESSED' WHERE isrc_code IN ('{formatted_isrcs}');"
        pg_hook.run(update_sql)


with DAG(
    dag_id="musicbrainz_gcs_medallion_pipeline",
    default_args=default_args,
    schedule_interval="5 4 * * *",
    catchup=False,
    tags=["postgres", "gcs", "bigquery", "medallion", "musicbrainz"],
) as dag:

    task_ingest_gcs = PythonOperator(
        task_id="ingest_musicbrainz_to_gcs",
        python_callable=ingest_musicbrainz_to_gcs,
        provide_context=True,
    )

    task_run_silver_sp = BigQueryExecuteQueryOperator(
        task_id="run_silver_stored_procedure",
        sql="CALL `{{ var.value.gcp_project_id }}.silver.sp_transform_bronze_to_silver`();",
        use_legacy_sql=False,
    )

    task_run_gold_sp = BigQueryExecuteQueryOperator(
        task_id="run_gold_stored_procedure",
        sql="CALL `{{ var.value.gcp_project_id }}.gold.sp_transform_silver_to_gold`();",
        use_legacy_sql=False,
    )

    task_ingest_gcs >> task_run_silver_sp >> task_run_gold_sp
