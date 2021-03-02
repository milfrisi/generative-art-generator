DROP TABLE IF EXISTS {{ db }}.hotel_search_concept_search_query;

CREATE EXTERNAL TABLE {{ db }}.hotel_search_concept_search_query(
	poll_request_id STRING COMMENT 'UUID generated per request on the main service',
	origin_request_id STRING COMMENT 'First poll_request_id',
	parent_poll_request_id STRING COMMENT 'Poll_request_id of the main list search. Populated only for slideout searches',
	tracking_id STRING  COMMENT 'ID from user cookies',
	poll_request_timestamp TIMESTAMP COMMENT 'transaction timestamp of the event log',
	poll_request_sender_id STRING COMMENT 'data source identifier of the event log',
	platform_code STRING COMMENT 'platform the user is using from gateway logs',
	language_code STRING COMMENT 'language the user is using (EN,DE)',
	language_script_code STRING COMMENT 'dialect or script codes for specific languages',
	region_code STRING COMMENT 'region code is subset of platform_code or locale_code',
	currency_code STRING COMMENT 'ISO4217 CODE (GBP,EUR,USD)',
	check_in_date INT COMMENT 'start date of stay in YYYYMMDD format',
	check_out_date INT COMMENT 'end date of stay in YYYYMMDD format',
	max_price_per_night INT COMMENT 'price filter used in user currency',
	total_adult_count INT COMMENT 'adult count filter used',
	total_child_count INT COMMENT 'chilren count filter used', 
	room_count INT COMMENT 'room count filter used',
	first_sort_criterion_type STRING COMMENT 'sort filter used (distance,rating)',
	second_sort_criterion_type STRING COMMENT 'sort filter used (distance,rating)',
	uiv_list array<struct<nsid:struct<ns:int,id:int>,weight:int,required:boolean>> COMMENT 'user intent vector http://git.trivago.trv/projects/TRIV/repos/nsids/browse',
	top_item_id INT COMMENT 'populated when item search',
	poi_flag TINYINT COMMENT 'populated when point of interest search (1,0)',
	destination_id INT COMMENT 'ID for destination. See destination.destination table',
	parent_item_id INT COMMENT 'clicked-in item_id that triggered to open the slideout',
	created_at_nanoseconds BIGINT COMMENT 'transaction nanosecond of the event log',
	concept_search_type STRING COMMENT 'type of concept search done (RADIUS_SEARCH_EMULATOR,CONCEPT_SEARCH_EMULATOR)'
)
PARTITIONED BY (ymd INT)
CLUSTERED BY (poll_request_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://nameservice1/user/{{ user }}/db/{{ project }}.db/hotel_search_concept_search_query'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

MSCK REPAIR TABLE {{ db }}.hotel_search_concept_search_query;
