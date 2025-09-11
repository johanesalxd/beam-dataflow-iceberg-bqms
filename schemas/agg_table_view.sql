CREATE OR REPLACE VIEW `{full_view_name}` AS
SELECT
  window_start,
  window_end,
  ride_status,
  passenger_count
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER(
      PARTITION BY window_start, ride_status
      ORDER BY processing_time DESC
    ) as rn
  FROM
    `{full_source_table_name}`
)
WHERE
  rn = 1
