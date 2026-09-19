CREATE OR REPLACE TABLE `pr0jectN4me.gold.fact_music_asset_performance` AS
  SELECT 
    t.isrc,
    ,t.track_title
    ,a.artist_name AS primary_artist
    ,EXTRACT(YEAR FROM t.release_date) AS release_year
    ,COUNT(DISTINCT v.youtube_video_id) AS youtube_video_count
    ,COUNT(DISTINCT v.channel_sk) AS youtube_channel_count
    ,IF(COUNT(v.youtube_video_id) > 0, TRUE, FALSE) AS is_matched_to_youtube
    ,CURRENT_TIMESTAMP() AS last_refreshed_at
    ,etc
  FROM `your_project.silver.dim_track` t
  LEFT JOIN `your_project.silver.dim_artist` a ON t.artist_sk = a.artist_sk
  LEFT JOIN `your_project.silver.fact_youtube_video` v ON t.track_sk = v.track_sk
  GROUP BY 1, 2, 3, 4;
END;
