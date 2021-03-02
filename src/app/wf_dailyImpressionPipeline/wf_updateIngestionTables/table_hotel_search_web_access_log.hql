DROP TABLE IF EXISTS {{ db }}.hotel_search_web_access_log;

CREATE EXTERNAL TABLE {{ db }}.hotel_search_web_access_log(
	poll_request_id STRING COMMENT 'UUID generated per request on the main service',
	tracking_id STRING COMMENT 'ID from user cookies',
	poll_request_timestamp TIMESTAMP COMMENT 'transaction timestamp of the event log',
	poll_request_sender_id STRING COMMENT 'data source identifier of the event log',
	bot_detection_type STRING COMMENT 'Online bot detection (BOT,USER,NULL)',
	geo_location_code STRING COMMENT 'location of the user from gateway logs',
	request_method_type STRING COMMENT 'RESTful http methods (GET,PUT)',
	request_uri_description STRING COMMENT 'Web uri description',
	request_protocol_type STRING COMMENT 'Web protocol used',
	http_status_code INT COMMENT 'Indicates reponses ex 200=Succesful'
	
)
PARTITIONED BY (ymd INT)
CLUSTERED BY (poll_request_id) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://nameservice1/user/{{ user }}/db/{{ project }}.db/hotel_search_web_access_log'
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

MSCK REPAIR TABLE {{ db }}.hotel_search_web_access_log;
