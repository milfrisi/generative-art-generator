DROP TABLE IF EXISTS {{ db }}.hotel_search_price_impression;

CREATE EXTERNAL TABLE {{ db }}.hotel_search_price_impression(
	price_impression_id STRING COMMENT 'UUID generated to identify CLICKOUT and IMPRESSION events',
	poll_request_id STRING COMMENT 'UUID generated per request on the main service',
	session_id STRING COMMENT 'Trivago raw session_id',
	tracking_id STRING COMMENT 'ID from user cookies',
	poll_request_timestamp TIMESTAMP COMMENT 'transaction timestamp of the event log',
	poll_request_sender_id STRING COMMENT 'data source identifier of the event log',
	impression_type STRING COMMENT 'impression event type triggered (SCROLL_IMPRESSION,CLICKOUT)',
	clicked_item2partner_id BIGINT COMMENT 'item2partner_id of the clicked. Only populated for impression_type=CLICKOUT.',
	clicked_unique_price_id BIGINT COMMENT 'item2partner_id of the clicked. Only populated for impression_type=CLICKOUT.',
    visible_item_id_list ARRAY<INT> COMMENT 'list of item_id in the user\'s viewport when the 3 second scroll impression event got triggered'
)
PARTITIONED BY (ymd INT)
CLUSTERED BY (poll_request_id) INTO 16 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://nameservice1/user/{{ user }}/db/{{ project }}.db/hotel_search_price_impression'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

MSCK REPAIR TABLE {{ db }}.hotel_search_price_impression;
