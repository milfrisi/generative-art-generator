set hive.query.name=hotel_search_concept_search_result;

set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.tez.bucket.pruning=true;
set hive.tez.dynamic.partition.pruning=true;

CREATE TEMPORARY MACRO transformPriceList(
	`key` int,
    val  struct<
        advertiser_id:int,
        price_per_night:int,
        price_id:bigint,
        advertiser_bucket_id:int,
        price_attribute:array<struct<ns:int,id:int>>,
        price_restriction:string,
        bid_storage_override_cpc:struct<
            base_cpc:int,
            modified_cpc:int
        >,
        advertiser_group_id:int,
        advertiser_connection_id:bigint,
        price_per_stay:bigint,
        eurocent_price_per_night:int,
        display_attribute:array<string>,
        rate_id:int,
        value_for_money:struct<
            type:struct<ns:int,id:int>,
            score:int
        >
    >,
    price_exposure_type_input string
)
named_struct(
    "price_exposure_type",price_exposure_type_input,
    "price_displayed_rank",`key`+1,
    "price_id",val.price_id,
    "partner_id",val.advertiser_id,
	"bucket_id",val.advertiser_bucket_id,
	"eurocent_price",val.eurocent_price_per_night,
	"displayed_price",val.price_per_night,
	"item2partner_id",val.advertiser_connection_id,
	"base_cpc",val.bid_storage_override_cpc.base_cpc,
	"modified_cpc",val.bid_storage_override_cpc.modified_cpc,
	"priority_cpc",CAST(NULL AS INT),
	"free_cancellation_flag",
	    CASE
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',412).id[0] = 1 THEN 1
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',412).id[0] = 2 THEN 0
	        ELSE NULL
        END,
	"breakfast_included_flag",
	    CASE
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',411).id[0] IN (2,3,4,5) THEN 1
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',411).id[0] = 1 THEN 0
	        ELSE NULL
        END,
	"meal_plan_type",
	    CASE
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',411).id[0] = 1 THEN 'ROOM_ONLY'
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',411).id[0] = 2 THEN 'BREAKFAST_INCLUDED'
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',411).id[0] = 3 THEN 'HALF_BOARD'
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',411).id[0] = 4 THEN 'FULL_BOARD'
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',411).id[0] = 5 THEN 'ALL_INCLUSIVE'
	        ELSE NULL
        END,
	"price_placement_type",
	    CASE
            WHEN array_contains(val.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_CHAMPION_DEAL')     THEN 'CHAMPION'
            WHEN array_contains(val.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_WORST_DEAL')        THEN 'STRIKE_THROUGH'
            WHEN array_contains(val.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_ALTERNATIVE_DEAL')  THEN 'MID_FIELD'
            WHEN array_contains(val.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_MINIMUM_DEAL')      THEN 'ELIGIBLE_MIN_OPT'
            WHEN array_contains(val.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_TOP_DEAL')          THEN 'TOP_DEAL'
            WHEN array_contains(val.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_DIRECT_CONNECT')    THEN 'DIRECT_CONNECT'
            WHEN array_contains(val.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_EXPRESS_BOOKING')   THEN 'EXPRESS_BOOKING'
            WHEN array_contains(val.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_SEM_RATE')          THEN 'SEM_RATE'
            ELSE NULL
	    END,
	"payment_option_type",
	    CASE
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',413).id[0] = 1 THEN 'PREPAID'
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',413).id[0] = 2 THEN 'POSTPAID'
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',413).id[0] = 3 THEN 'INSTALLMENT'
	        WHEN trv_udf.filterArray(val.price_attribute,'ns',413).id[0] = 4 THEN 'DEPOSIT'
	        ELSE NULL
        END,
	"blocking_reason_id",CAST(NULL AS INT),
	"trv_exclusive_deal_flag",CAST(NULL AS INT)
);

CREATE TEMPORARY MACRO transformItemList(
	`key` int,
	val struct<
            id:int,
            deal:array<
                struct<
                    advertiser_id:int,
                    price_per_night:int,
                    price_id:bigint,
                    advertiser_bucket_id:int,
                    price_attribute:array<struct<ns:int,id:int>>,
                    price_restriction:string,
                    bid_storage_override_cpc:struct<
                        base_cpc:int,
                        modified_cpc:int
                    >,
                    advertiser_group_id:int,
                    advertiser_connection_id:bigint,
                    price_per_stay:bigint,
                    eurocent_price_per_night:int,
                    display_attribute:array<string>,
                    rate_id:int,
                    value_for_money:struct<
                        type:struct<ns:int,id:int>,
                        score:int
                    >
                >
            >
        >,
    item_exposure_type_input string
)
named_struct(
    "item_exposure_type",item_exposure_type_input,
    "item_displayed_rank",`key`+1,
    "item_id",val.id,
    "item_price_list",trv_udf.map_collection("transformPriceList",trv_udf.array_to_map(val.deal),"visible")
);


WITH cte_hotel_search_concept_search_result_transformed AS (
	SELECT
	request_id.value AS poll_request_id,
	origin_request_id.value as origin_request_id,
	FROM_UNIXTIME(header.unix_timestamp) AS poll_request_timestamp,
	header.sender_id AS poll_request_sender_id,
	CASE WHEN result_metadata.status IS NULL
		THEN 'PENDING'
		ELSE 'DONE'
	END AS search_result_status,
	COALESCE(result_metadata.price_position_selector,'FALLBACK_PRICE_POSITIONING') AS price_position_selector_description,
	COALESCE(result_metadata.item_position_selector,'MARKETPLACE_ITEM_POSITIONING') AS item_position_selector_description,
	trv_udf.map_collection("transformItemList",trv_udf.array_to_map(accommodation),"visible") AS search_result_item_list,
	request_parameters.currency AS currency_code,
	ymd
FROM
    ${gobblin_streams_db}.hsc_searchresultlog_proto
WHERE
	ymd = ${crunchDate}
	AND request_id.value NOT RLIKE('^0+$')
),

cte_hotel_search_concept_search_result_deduplicated AS (
    SELECT
		*,
		ROW_NUMBER() OVER (
            PARTITION BY poll_request_id
            ORDER BY poll_request_timestamp DESC,SIZE(search_result_item_list) DESC) AS row_num
	FROM
		cte_hotel_search_concept_search_result_transformed
)

INSERT OVERWRITE TABLE ${trivago_stage_db}.hotel_search_concept_search_result
PARTITION(ymd)
SELECT
	poll_request_id,
	origin_request_id,
	poll_request_timestamp,
	poll_request_sender_id,
	search_result_status,
	price_position_selector_description,
	item_position_selector_description,
	search_result_item_list,
	currency_code,
	ymd
FROM cte_hotel_search_concept_search_result_deduplicated
WHERE
	row_num=1
;
