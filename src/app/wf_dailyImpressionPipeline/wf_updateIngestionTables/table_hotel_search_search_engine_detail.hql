DROP TABLE IF EXISTS {{ db }}.hotel_search_search_engine_detail;

CREATE EXTERNAL TABLE {{ db }}.hotel_search_search_engine_detail(
	poll_request_id STRING COMMENT 'UUID generated per request on the main service',
	poll_request_timestamp TIMESTAMP COMMENT 'transaction timestamp of the event log',
	poll_request_sender_id STRING COMMENT 'data source identifier of the event log',
	ctest_id_list ARRAY<INT> COMMENT 'active ctest_ids in the session',
	client_type STRING COMMENT 'client used to access trivago (MOBILE_APP,WEB_APP)',
	page_limit INT COMMENT 'max number of items displayed per page',
    page_number INT COMMENT 'current page of search result',
	stay_period_source_id INT COMMENT 'https://admin.trivago.com/pageid/page_detail/493',
    is_standard_date TINYINT COMMENT 'stay_period_source_id between 0-19, when user has no calendar interaction'
)
PARTITIONED BY (ymd INT)
CLUSTERED BY (poll_request_id) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://nameservice1/user/{{ user }}/db/{{ project }}.db/hotel_search_search_engine_detail'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

MSCK REPAIR TABLE {{ db }}.hotel_search_search_engine_detail;