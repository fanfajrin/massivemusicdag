import json
import re
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.cloud import storage

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

GCP_PROJECT_ID = Variable.get("prod_gcp_project_id", default_var="dev_gcp_project_id")
GCS_BUCKET_NAME = Variable.get("prod_gcs_bucket_name", default_var="dev_gcs_bucket_name")
YOUTUBE_API_KEY = Variable.get("prod_youtube_api_key", default_var="dev_YOUTUBE_API_KEY")
POSTGRES_CONN_ID = Variable.get("prod_postgres_conn_id", default_var="dev_postgres")

default_args = {
    "owner": "hellboy",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def parse_title_for_song_and_artist(video_title):
    clean_title = re.sub(
        r"[\(\[\{].*?(official\vert{}music\vert{}video\vert{}audio\vert{}lyric\vert{}hd\vert{}4k).*?[\)\]\}]",
        "",
        video_title,
        flags=re.IGNORECASE,
    ).strip()
    if "-" in clean_title:
        parts = clean_title.split("-", 1)
        return parts[1].strip(), parts[0].strip()
    return clean_title, None


def ingest_youtube_to_gcs(**kwargs):
    bucket_name = GCS_BUCKET_NAME
    yt_api_key = YOUTUBE_API_KEY
    pg_conn_id = POSTGRES_CONN_ID

    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    sql_query = "SELECT video_id, video_code FROM youtube_list WHERE status = 'PENDING';"
    records = pg_hook.get_records(sql_query)

    if not records:
        print("No pending Video IDs found in PostgreSQL. Skipping execution.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    youtube = build("youtube", "v3", developerKey=yt_api_key)

    records_to_upload = []
    processed_video_ids = []

    for row in records:
        video_id = row[0]
        video_code = row[1] if len(row) > 1 else None

        try:
            yt_res = youtube.videos().list(
                part="snippet", id=video_id
            ).execute()
            items = yt_res.get("items", [])
            
            if items:
                snippet = items[0]["snippet"]
                video_title = snippet.get("title", "")
                song_title, artist = parse_title_for_song_and_artist(video_title)

                records_to_upload.append({
                    "video_code": video_code,
                    "video_id": video_id,
                    "video_title": video_title,
                    "song_title": song_title,
                    "artist_name": artist or snippet.get("channelTitle"),
                    "channel_id": snippet.get("channelId"),
                    "channel_title": snippet.get("channelTitle"),
                    "source_system": "YOUTUBE",
                    "ingested_at": datetime.utcnow().isoformat(),
                })
                processed_video_ids.append(video_id)
        except Exception as e:
            print(f"YouTube video lookup failed for Video ID {video_id}: {e}")

    if records_to_upload:
        execution_date = kwargs.get("ds")
        blob_name = f"bronze/youtube/{execution_date}/yt_data_{int(datetime.utcnow().timestamp())}.json"
        blob = bucket.blob(blob_name)
        
        json_lines = "\n".join([json.dumps(record) for record in records_to_upload])
        blob.upload_from_string(json_lines, content_type="application/json")
        print(f"Successfully uploaded raw YouTube payload to gs://{bucket_name}/{blob_name}")

        formatted_video_ids = "', '".join(processed_video_ids)
        update_sql = f"UPDATE youtube_list SET status = 'PROCESSED' WHERE video_id IN ('{formatted_video_ids}');"
        pg_hook.run(update_sql)


with DAG(
    dag_id="youtube_gcs_medallion_pipeline",
    default_args=default_args,
    schedule_interval="5 4 * * *",
    catchup=False,
    tags=["postgres", "gcs", "bigquery", "medallion", "youtube"],
) as dag:

    task_ingest_gcs = PythonOperator(
        task_id="ingest_youtube_to_gcs",
        python_callable=ingest_youtube_to_gcs,
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
