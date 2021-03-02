DROP TABLE IF EXISTS {{ db }}.hotel_search_concept_search_result;

CREATE EXTERNAL TABLE {{ db }}.hotel_search_concept_search_result(
	poll_request_id STRING COMMENT 'UUID generated per request on the main service',
	origin_request_id STRING COMMENT 'First poll_request_id',
	poll_request_timestamp TIMESTAMP COMMENT 'transaction timestamp of the event log',
	poll_request_sender_id STRING COMMENT 'data source identifier of the event log',
	search_result_status STRING COMMENT 'indicates if search result was final or not (DONE,PENDING)',
	price_position_selector_description STRING COMMENT 'service description which determined the price sorting',
	item_position_selector_description STRING COMMENT 'service description which determined the item sorting',
	search_result_item_list array<struct<
		item_exposure_type:string,
		item_displayed_rank:int,
		item_id:int,
		item_price_list:array<struct<
			price_exposure_type:string,
			price_displayed_rank:int,
			price_id:bigint,
			partner_id:int,
			bucket_id:int,
			eurocent_price:int,
			displayed_price:int,
			item2partner_id:bigint,
			base_cpc:int,
			modified_cpc:int,
			priority_cpc:int,
			free_cancellation_flag:int,
			breakfast_included_flag:int,
			meal_plan_type:string,
			price_placement_type:string,
			payment_option_type:string,
			blocking_reason_id:int,
			trv_exclusive_deal_flag:int
		>>
	>>,
	currency_code STRING COMMENT 'ISO4217 CODE (GBP,EUR,USD)'
)
PARTITIONED BY (ymd INT)
CLUSTERED BY (poll_request_id) INTO 256 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://nameservice1/user/{{ user }}/db/{{ project }}.db/hotel_search_concept_search_result'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

MSCK REPAIR TABLE {{ db }}.hotel_search_concept_search_result;
