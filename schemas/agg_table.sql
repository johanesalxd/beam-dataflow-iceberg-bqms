SELECT
    ride_status,
    SUM(passenger_count) as passenger_count,
    MAX(processing_time) as processing_time
FROM PCOLLECTION
GROUP BY
    ride_status
