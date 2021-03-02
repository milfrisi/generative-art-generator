set hive.query.name=hotel_search_search_engine_detail;

set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.tez.bucket.pruning=true;
set hive.tez.dynamic.partition.pruning=true;

WITH cte_hotel_search_search_engine_detail AS (
    SELECT
        *
    FROM
        ${gobblin_streams_db}.hsc_searchenginerequestlog_proto
	WHERE
		ymd = ${crunchDate}
		AND metadata.request_id.value NOT RLIKE('^0+$')
),

cte_hotel_search_search_engine_detail_transformed AS (
    SELECT
        metadata.request_id.value AS poll_request_id,
        FROM_UNIXTIME(header.unix_timestamp) AS poll_request_timestamp,
        header.sender_id AS poll_request_sender_id,
        c_test AS ctest_id_list,
        client_type,
        accommodation_criteria.limit AS page_limit,
        COALESCE(accommodation_criteria.offset,0) AS new_offset,
        COALESCE(channel.search_engine_marketing.stay_period_source.value,0) AS stay_period_source_id,
        IF(
            COALESCE(channel.search_engine_marketing.stay_period_source.value,0)<20,
            1,
            0
        ) AS is_standard_date,
        ROW_NUMBER() OVER (PARTITION BY metadata.request_id.value ORDER BY header.unix_timestamp DESC) AS row_num,
        ymd
    FROM
        cte_hotel_search_search_engine_detail
)

INSERT OVERWRITE TABLE ${trivago_stage_db}.hotel_search_search_engine_detail
PARTITION(ymd)
SELECT
    poll_request_id,
    poll_request_timestamp,
    poll_request_sender_id,
    ctest_id_list,
    client_type,
    page_limit,
    CAST((new_offset/page_limit) AS INT)+1 AS page_number,
    stay_period_source_id,
    is_standard_date,
    ymd
FROM
    cte_hotel_search_search_engine_detail_transformed
WHERE
    row_num=1
;
