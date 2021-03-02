set hive.query.name=hotel_search_concept_search_query;

set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.tez.bucket.pruning=true;
set hive.tez.dynamic.partition.pruning=true;

WITH cte_hotel_search_concept_search_query AS (
	SELECT
		header,
		metadata,
		created_at_nanoseconds,
		search_poll,
		concept_deals_search,
		accommodation_deals_search,
		accommodation_deals_explorer,
		discover_deals_search,
		UPPER(type) AS type,
		ymd
	FROM
		${gobblin_streams_db}.hsg_deals_search_request_logs_v3_proto
	WHERE
		ymd = ${crunchDate}
		AND metadata.tracking_id.value <>'trvPrerendererBotNoTraking'
	    AND metadata.request_id.value NOT RLIKE('^0+$')
),

cte_hotel_search_concept_search_query_transformed AS (
	SELECT
	   	metadata.request_id.value as poll_request_id,
	   	search_poll.origin_request_id.value AS origin_request_id,
		accommodation_deals_search.search_parameters.concept_deal_search_request_id.value AS parent_poll_request_id,
		metadata.tracking_id.value as tracking_id,
		FROM_UNIXTIME(header.unix_timestamp) AS poll_request_timestamp,
		header.sender_id AS poll_request_sender_id,
	    UPPER(
			CASE
				WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.platform.code
				WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.platform.code
				WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.platform.code
				WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.platform.code
				ELSE NULL
			END
	    ) AS platform_code,
	    UPPER(
			CASE
				WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.language_tag.language
				WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.language_tag.language
				WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.language_tag.language
				WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.language_tag.language
				ELSE NULL
			END
	    ) AS language_code,

	    UPPER(
			CASE
				WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.language_tag.script
				WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.language_tag.script
				WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.language_tag.script
				WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.language_tag.script
				ELSE NULL
			END
	    ) AS language_script_code,

		CASE
			WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.language_tag.region
			WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.language_tag.region
			WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.language_tag.region
			WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.language_tag.region
			ELSE NULL
		END AS region_code,

		CASE
			WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.currency
			WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.currency.code
			WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.currency.code
			WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.currency.code
			ELSE NULL
		END AS currency_code,

		CASE
			WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.check_in_date
			WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.check_in_date
			ELSE NULL
		END AS check_in_date,

		CASE
			WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.check_out_date
			WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.check_out_date
			ELSE NULL
		END AS check_out_date,

		CASE
			-- temporary discover_deals_search hotfix while waiting for a fix on max_price_per_night from source not to be max int32 value when user doesn't specify max price
			WHEN
				concept_deals_search.common_parameters.deal_criteria.max_price_per_night = 2147483647
				OR accommodation_deals_explorer.search_parameters.deal_criteria.max_price_per_night = 2147483647
				OR discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.max_price_per_night = 2147483647
			THEN
				NULL
			WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.max_price_per_night
			WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.max_price_per_night
			WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.max_price_per_night
			ELSE NULL
		END AS max_price_per_night,

	    brickhouse.sum_array(
			CASE
				WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.room.adult_count
				WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.room.adult_count
				WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.rooms.adult_count
				WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.rooms.adult_count
				ELSE NULL
			END
	    ) AS total_adult_count,

	    SIZE(
	        brickhouse.array_flatten(
				CASE
					WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.room.child_age
					WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.room.child_age
					WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.rooms.child_age
					WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.rooms.child_age
					ELSE NULL
				END
	        )
	    ) AS total_child_count,

	    SIZE(
			CASE
				WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.deal_criteria.room
				WHEN type = 'ACCOMMODATION_DEALS_SEARCH' THEN accommodation_deals_search.search_parameters.deal_criteria.room
				WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.deal_criteria.rooms
				WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.deal_criteria.rooms
				ELSE NULL
			END
	    ) AS room_count,
    
	    COALESCE(concept_deals_search.common_parameters.sort_criterion.type[0], UPPER(concept_deals_search.common_parameters.sort_criterion.criterion[0])) AS first_sort_criterion_type,
	    COALESCE(concept_deals_search.common_parameters.sort_criterion.type[1], UPPER(concept_deals_search.common_parameters.sort_criterion.criterion[1])) AS second_sort_criterion_type,

		CASE
			WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.user_intent_vector.user_intent
			WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.user_intent_vector.user_intent
			WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.user_intent_vector.user_intent
			ELSE NULL
		END AS uiv_list,

	    concept_deals_search.common_parameters.top_accommodation_id AS top_item_id,

	    IF(ARRAY_CONTAINS(CASE
			WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.user_intent_vector.user_intent.nsid.ns
			WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.user_intent_vector.user_intent.nsid.ns
			WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.user_intent_vector.user_intent.nsid.ns
			ELSE NULL
		END, 500) = TRUE, 1, 0) AS poi_flag,

	     trv_udf.filterArray(
			CASE
				WHEN type = 'CONCEPT_DEALS_SEARCH' THEN concept_deals_search.common_parameters.user_intent_vector.user_intent.nsid
				WHEN type = 'ACCOMMODATION_DEALS_EXPLORER' THEN accommodation_deals_explorer.search_parameters.user_intent_vector.user_intent.nsid
				WHEN type = 'DISCOVER_DEALS_SEARCH' THEN discover_deals_search.concept_search_for_accommodations_parameters.user_intent_vector.user_intent.nsid
				ELSE NULL
			END,
            'ns', 200
        ).id[0] AS destination_id,
		accommodation_deals_search.search_parameters.accommodation.id AS parent_item_id,
	    created_at_nanoseconds,

		CASE
			WHEN type = 'CONCEPT_DEALS_SEARCH' AND SIZE(concept_deals_search.accommodation_id) > 0 THEN "ACCOMMODATION_LIST_SEARCH_EMULATOR"
			WHEN type = 'CONCEPT_DEALS_SEARCH' AND concept_deals_search.bounding_box IS NOT NULL THEN "BOUNDING_BOX_SEARCH_EMULATOR"
			WHEN type = 'CONCEPT_DEALS_SEARCH' AND concept_deals_search.circle IS NOT NULL THEN "RADIUS_SEARCH_EMULATOR"
			WHEN type = 'CONCEPT_DEALS_SEARCH' AND SIZE(concept_deals_search.accommodation_id) = 0 AND concept_deals_search.bounding_box IS NULL AND concept_deals_search.circle IS NULL THEN "CONCEPT_SEARCH_EMULATOR"
			ELSE type
		END AS concept_search_type,
	    ROW_NUMBER() OVER (PARTITION BY metadata.request_id.value ORDER BY IF(type='SEARCH_POLL',0,1) DESC, header.unix_timestamp DESC, created_at_nanoseconds DESC) AS row_num,
		ymd
	FROM
		cte_hotel_search_concept_search_query
)

INSERT OVERWRITE TABLE ${trivago_stage_db}.hotel_search_concept_search_query
PARTITION(ymd)
SELECT
	poll_request_id,
	origin_request_id,
	parent_poll_request_id,
	tracking_id,
	poll_request_timestamp,
	poll_request_sender_id,
	IF (platform_code = 'SHABAKA', 'AA', platform_code) AS platform_code,
	language_code,
	language_script_code,
	region_code,
	currency_code,
	check_in_date,
	check_out_date,
	max_price_per_night,
	total_adult_count,
	total_child_count,
	room_count,
	first_sort_criterion_type,
	second_sort_criterion_type,
	uiv_list,
	top_item_id,
	poi_flag,
	destination_id,
	parent_item_id,
	created_at_nanoseconds,
	concept_search_type,
	ymd
FROM
    cte_hotel_search_concept_search_query_transformed
WHERE
	row_num=1;
