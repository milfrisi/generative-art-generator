set hive.query.name=hotel_search_price_impression;

set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.tez.bucket.pruning=true;
set hive.tez.dynamic.partition.pruning=true;

WITH cte_hotel_search_price_impression AS (
	SELECT
		*
	FROM
	    ${gobblin_streams_db}.price_impression_log_proto
	WHERE
		 ymd = ${crunchDate}
		 AND last_poll_request_id NOT RLIKE('^0+$')
		 AND tracking_id<>'trvPrerendererBotNoTraking'
),

cte_hotel_search_price_impression_transformed AS (
	SELECT
	    request_id AS price_impression_id,
		last_poll_request_id AS poll_request_id,
		session_id,
		tracking_id,
		FROM_UNIXTIME(header.time) AS poll_request_timestamp,
		header.senderid AS poll_request_sender_id,
		IF(log_type='IMPRESSION','SCROLL_IMPRESSION',log_type) AS impression_type,
		clicked_item2partner_id,
		clicked_unique_price_id,
		visible_items AS visible_item_id_list,
		ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY header.time DESC) AS row_num,
		ymd
	FROM
	    cte_hotel_search_price_impression

)

INSERT OVERWRITE TABLE ${trivago_stage_db}.hotel_search_price_impression
PARTITION(ymd)
SELECT
    price_impression_id,
	poll_request_id,
	session_id,
	tracking_id,
	poll_request_timestamp,
	poll_request_sender_id,
	impression_type,
	clicked_item2partner_id,
	clicked_unique_price_id,
	visible_item_id_list,
	ymd
FROM
    cte_hotel_search_price_impression_transformed
WHERE
	row_num=1
;
