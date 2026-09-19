CREATE OR REPLACE PROCEDURE `pr0jectN4me.silver.sp_transform_bronze_to_silver`()
BEGIN
  MERGE `pr0jectN4me.silver.dim_track` T
  USING (
    SELECT DISTINCT
      isrc
      ,recording_title AS track_title
      ,album AS album_title
      ,PARSE_DATE('%Y-%m-%d', release_date) AS release_date
      ,SAFE_CAST(duration_ms AS INT64) AS duration_ms
      ,etc
    FROM `pr0jectN4me.bronze.ext_gcs_raw_json`
    WHERE source_system = 'MUSICBRAINZ' AND isrc IS NOT NULL
    --i dont have premium spotify
  ) S
  ON T.isrc = S.isrc
  WHEN MATCHED THEN
    UPDATE SET 
      track_title = S.track_title,
      album_title = S.album_title,
      release_date = S.release_date,
      duration_ms = S.duration_ms,
      updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (isrc, track_title, album_title, release_date, duration_ms, updated_at)
    VALUES (S.isrc, S.track_title, S.album_title, S.release_date, S.duration_ms, CURRENT_TIMESTAMP());

  MERGE `pr0jectN4me.silver.dim_youtube_channel` T
  USING (
    SELECT DISTINCT
      channel_id AS youtube_channel_id
      ,channel_title
      ,etc
    FROM `pr0jectN4me.bronze.ext_gcs_raw_json`
    WHERE source_system = 'YOUTUBE' AND channel_id IS NOT NULL
  ) S
  ON T.youtube_channel_id = S.youtube_channel_id
  WHEN MATCHED THEN
    UPDATE SET channel_title = S.channel_title
  WHEN NOT MATCHED THEN
    INSERT (youtube_channel_id, channel_title)
    VALUES (S.youtube_channel_id, S.channel_title);

  INSERT INTO `pr0jectN4me.silver.fact_youtube_video` (youtube_video_id, track_sk, channel_sk, video_title, ingested_at)
  SELECT 
    b.video_id AS youtube_video_id
    ,dt.track_sk
    ,dc.channel_sk
    ,b.video_title
    ,CURRENT_TIMESTAMP() AS ingested_at
  FROM `pr0jectN4me.bronze.ext_gcs_raw_json` b
  LEFT JOIN `pr0jectN4me.silver.dim_track` dt ON b.isrc = dt.isrc
  LEFT JOIN `pr0jectN4me.silver.dim_youtube_channel` dc ON b.channel_id = dc.youtube_channel_id
  WHERE b.source_system = 'YOUTUBE'
    AND b.video_id NOT IN (SELECT youtube_video_id FROM `pr0jectN4me.silver.fact_youtube_video`);
END;
