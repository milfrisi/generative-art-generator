set hive.query.name=marketplace_exposure_algorithm_result;

set price_data_type=struct<unique_price_id:bigint,bucket_id:int,priority_cpc:int,blocking_reason:int,display_price:int,ctr_log_values:array<struct<feature:string,value:double>>,partner_id:int,eurocent_price:int,breakfast_included:boolean,free_cancellation:boolean,item_2_partner_id:bigint,trv_exclusive_deal:boolean,meal_plan:string,base_cpc:int,modified_cpc:int,property_group_id:int,price_placement:string,payment_option:array<string>,trv_sponsored_listing:boolean,predicted_ctr:float,ad_relevance_gain:float,cpa_campaign:int,cpa_input:float,c2b_bayes:float,modified_c2b_bayes:float,estimated_vpc:float,predicted_c2b:float,vpc_modifier:float>;
set modified_price_data_type_string='struct<price_exposure_type:string,price_displayed_rank:int,price_id:bigint,partner_id:int,bucket_id:int,eurocent_price:int,displayed_price:int,item2partner_id:bigint,base_cpc:int,modified_cpc:int,priority_score:int,free_cancellation_flag:int,breakfast_included_flag:int,meal_plan_type:string,price_placement_type:string,payment_option_type:array<string>,blocking_reason_id:int,trv_exclusive_deal_flag:int,trv_sponsored_listing_flag:int,ctr_log_values:array<struct<feature:string,value:double>>,property_group_id:int,predicted_ctr:float,ad_relevance_gain:float,cpa_campaign:int,cpa_input:float,c2b_bayes:float,modified_c2b_bayes:float,estimated_vpc:float,sponsored_listing_base_cpc:int,sponsored_listing_modified_cpc:int,sponsored_listing_priority_cpc:int,is_commissioned_sponsored_listing:boolean,applied_commission:float,predicted_c2b:float,vpc_modifier:float>';

set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.tez.bucket.pruning=true;
set hive.tez.dynamic.partition.pruning=true;

CREATE TEMPORARY MACRO check_sponsored_listing(input struct<display_attribute:array<string>>, carry_on_result boolean)
IF(
	ARRAY_CONTAINS(input.display_attribute,'PROTO_DEAL_DISPLAY_ATTRIBUTE_SPONSORED_DEAL'),
	true,				-- return true and set it to 'carry_on_result'
	carry_on_result		-- return the current value and set it to carry_on_result
);

CREATE TEMPORARY MACRO filter_sponsored_listing_item(sl_hotels struct<item_id:int>, item_id_list array<int>)
ARRAY_CONTAINS(item_id_list, sl_hotels.item_id)
;

CREATE TEMPORARY MACRO transform_sponsored_listing_price(
	`key` int,
	val struct<unique_price_id:bigint,item_2_partner_id:bigint,partner_id:int,is_winner:boolean,sponsored_listing_base_cpc:int,sponsored_listing_modified_cpc:int,sponsored_listing_priority_cpc:int,is_commissioned_sponsored_listing:boolean,applied_commission:float,modified_cpc:int,property_group_id:int,display_price:int,eurocent_price:int,meal_plan:string,ctr_log_values:array<struct<feature:string,value:double>>,predicted_ctr:float>,
	price_exposure_type_input string
)
named_struct(
	'price_exposure_type',price_exposure_type_input,
	'price_displayed_rank',`key`+1,
	'price_id',val.unique_price_id,
	'partner_id',val.partner_id,
	'bucket_id',CAST(NULL AS INT),
	'eurocent_price',val.eurocent_price,
	'displayed_price',val.display_price,
	'item2partner_id',val.item_2_partner_id,
	'base_cpc',CAST(NULL AS INT),
	'modified_cpc',val.modified_cpc,
	'priority_score',val.sponsored_listing_priority_cpc,
	'free_cancellation_flag',CAST(NULL AS INT),
	'breakfast_included_flag',if(val.meal_plan<>'NO_PLAN_SPECIFIED',1,0),
	'meal_plan_type',val.meal_plan,
	'price_placement_type','CHAMPION',
	'payment_option_type',ARRAY(cast(NULL as STRING)),
	'blocking_reason_id',CAST(NULL AS INT),
	'trv_exclusive_deal_flag',CAST(NULL AS INT),
	'trv_sponsored_listing_flag',1,
	'ctr_log_values',val.ctr_log_values,
	'property_group_id',val.property_group_id,
	'predicted_ctr',val.predicted_ctr,
	'ad_relevance_gain',CAST(NULL AS FLOAT),
	'cpa_campaign',CAST(NULL AS INT),
	'cpa_input',CAST(NULL AS FLOAT),
	'c2b_bayes',CAST(NULL AS FLOAT),
	'modified_c2b_bayes',CAST(NULL AS FLOAT),
	'estimated_vpc',CAST(NULL AS FLOAT),
	'sponsored_listing_base_cpc',val.sponsored_listing_base_cpc,
	'sponsored_listing_modified_cpc',val.sponsored_listing_modified_cpc,
	'sponsored_listing_priority_cpc',val.sponsored_listing_priority_cpc,
	'is_commissioned_sponsored_listing',val.is_commissioned_sponsored_listing,
	'applied_commission',val.applied_commission,
	'predicted_c2b',CAST(NULL AS FLOAT),
	'vpc_modifier',CAST(NULL AS FLOAT)
);

CREATE TEMPORARY MACRO transform_meta_listing_price(
	`key` int,
	val struct<unique_price_id:bigint,bucket_id:int,priority_cpc:int,blocking_reason:int,display_price:int,ctr_log_values:array<struct<feature:string,value:double>>,partner_id:int,eurocent_price:int,breakfast_included:boolean,free_cancellation:boolean,item_2_partner_id:bigint,trv_exclusive_deal:boolean,meal_plan:string,base_cpc:int,modified_cpc:int,property_group_id:int,price_placement:string,payment_option:array<string>,trv_sponsored_listing:boolean,predicted_ctr:float,ad_relevance_gain:float,cpa_campaign:int,cpa_input:float,c2b_bayes:float,modified_c2b_bayes:float,estimated_vpc:float,predicted_c2b:float,vpc_modifier:float>,
	price_exposure_type_input string
)
named_struct(
	'price_exposure_type',price_exposure_type_input,
	'price_displayed_rank',`key`+1,
	'price_id',val.unique_price_id,
	'partner_id',val.partner_id,
	'bucket_id',val.bucket_id,
	'eurocent_price',val.eurocent_price,
	'displayed_price',val.display_price,
	'item2partner_id',val.item_2_partner_id,
	'base_cpc',val.base_cpc,
	'modified_cpc',val.modified_cpc,
	'priority_score',val.priority_cpc,
	'free_cancellation_flag',if(val.free_cancellation=true,1,0),
	'breakfast_included_flag',if(val.breakfast_included=true,1,0),
	'meal_plan_type',val.meal_plan,
	'price_placement_type',val.price_placement,
	'payment_option_type',val.payment_option,
	'blocking_reason_id',val.blocking_reason,
	'trv_exclusive_deal_flag',if(val.trv_exclusive_deal=true,1,0),
	'trv_sponsored_listing_flag',if(val.trv_sponsored_listing=true,1,0),
	'ctr_log_values',val.ctr_log_values,
	'property_group_id',val.property_group_id,
	'predicted_ctr',val.predicted_ctr,
	'ad_relevance_gain',val.ad_relevance_gain,
	'cpa_campaign',val.cpa_campaign,
	'cpa_input',val.cpa_input,
	'c2b_bayes',val.c2b_bayes,
	'modified_c2b_bayes',val.modified_c2b_bayes,
	'estimated_vpc',val.estimated_vpc,
	'sponsored_listing_base_cpc',CAST(NULL AS INT),
	'sponsored_listing_modified_cpc',CAST(NULL AS INT),
	'sponsored_listing_priority_cpc',CAST(NULL AS INT),
	'is_commissioned_sponsored_listing',CAST(NULL AS BOOLEAN),
	'applied_commission',CAST(NULL AS FLOAT),
	'predicted_c2b',val.predicted_c2b,
	'vpc_modifier',val.vpc_modifier
);

CREATE TEMPORARY MACRO transform_blocked_item_list(
	`key` int,
	val struct<
		item_id:int,
		ctr_log_values:array<struct<feature:string,value:double>>,
		visible_prices:array<${hiveconf:price_data_type}>,
		blocked_prices:array<${hiveconf:price_data_type}>,
		non_visible_prices:array<${hiveconf:price_data_type}>,
		ad_relevance_reserve_gain:float,
		champion_choice:string,
		bid_mod_reserve_factor:float
	>,
	item_exposure_type_input string,
	auction_id_input string
)
named_struct(
	'item_exposure_type',item_exposure_type_input,
	'item_displayed_rank',`key`+1,
	'item_id',val.item_id,
	'relevance_score',cast(NULL AS int),
	'top_deal_item_flag',CAST(NULL AS INT),
	'item_price_list',brickhouse.combine(
		 COALESCE(
			trv_udf.map_collection(
				'transform_meta_listing_price',
				trv_udf.array_to_map(val.visible_prices),
				'VISIBLE'
			),
			trv_udf.empty_collection_of_type(trv_udf.type_from_string(${hiveconf:modified_price_data_type_string}))
		) ,
		COALESCE(
			trv_udf.map_collection(
				'transform_meta_listing_price',
				trv_udf.array_to_map(val.non_visible_prices),
				'NON_VISIBLE'
			),
			trv_udf.empty_collection_of_type(trv_udf.type_from_string(${hiveconf:modified_price_data_type_string}))
		) ,
		COALESCE(
			trv_udf.map_collection(
				'transform_meta_listing_price',
				trv_udf.array_to_map(val.blocked_prices),
				'BLOCKED'
			),
			trv_udf.empty_collection_of_type(trv_udf.type_from_string(${hiveconf:modified_price_data_type_string}))
		)
	),
	'auction_id',auction_id_input,
	'ctr_log_values',val.ctr_log_values,
	'ad_relevance_reserve_gain',val.ad_relevance_reserve_gain,
	'champion_choice',val.champion_choice,
	'ir_predicted_ctr',CAST(NULL AS DOUBLE),
	'bid_mod_reserve_factor',val.bid_mod_reserve_factor
);

WITH cte_hsr_ranking_logs_proto AS (
	SELECT
		*,
		ROW_NUMBER() OVER (
			PARTITION BY
				request_id
			ORDER BY
				header.time DESC,
				total_ranked_hotels_count DESC
		) AS rownumber
	FROM
		${gobblin_streams_db}.hsr_ranking_logs_proto
	WHERE
		ymd = ${crunchDate}
		AND request_id NOT RLIKE('^0+$')
		AND request_id<>'00000000000000000000000000000001'
),

cte_mpe_ea_optimization_results_deduplicated AS (
	SELECT
		ea.header AS header,
		request_id.value AS request_id,
		ea.visible_hotels,
		ea.blocked_hotels,
		search_result_log.origin_request_id.value AS original_request_id,
		trv_udf.array_to_map(search_result_log.accommodation) AS accommodation_map,
		search_result_log.accommodation AS accommodation,
		ea.bid_storage_call_status,
		ea.platform,
		auction_id,
		ea.sl_hotels,
		ea.experiments,
		ROW_NUMBER() OVER (
			PARTITION BY
				--auction_id,
				-- Add this for next PR to handle multiple auction_ids
				request_id.value
			ORDER BY
				ea.header.time DESC,
				size(ea.visible_hotels) DESC)
		AS rownumber
	FROM ${trivago_stage_db}.mpe_ea_optimization_results
	LATERAL VIEW OUTER
		EXPLODE(auction_log_with_ranked_items_by_auction_id) kv AS auction_id, ea
	WHERE
		ymd = ${crunchDate}
),

cte_ea_ranking_joined AS (
	SELECT
		ea.*,
		trv_udf.filter_collection(
			"filter_sponsored_listing_item",
			ea.sl_hotels,
			sl_log.sl_auctions.item_id
		) AS sl_hotel_detail,
		ranking.ranked_hotels
	FROM
		cte_mpe_ea_optimization_results_deduplicated ea
	LEFT JOIN
		cte_hsr_ranking_logs_proto ranking
	ON
		ea.request_id = ranking.request_id
		AND ea.rownumber = ranking.rownumber
	WHERE
		ea.rownumber = 1
),

cte_infer_visible_hotel_positions as (
	SELECT
		header,
		request_id,
		original_request_id,
		bid_storage_call_status,
		platform,
		hotel_details.id as item_id,
		ranked_hotels[hotel_pos].relevance_score,
		ranked_hotels[hotel_pos].predicted_ctr as ir_predicted_ctr,
		hotel_pos+1 as hotel_position,
		hotel_details.auction_id.value as auction_id,
		trv_udf.reduce_collection('check_sponsored_listing',hotel_details.deal,false) as is_sponsored_listing,
		IF(
			trv_udf.reduce_collection('check_sponsored_listing',hotel_details.deal,false),
			trv_udf.filterArray(sl_hotel_detail,'item_id',hotel_details.id),
			NULL
		)[0] as sl_hotel_details,
		
		IF(
			!trv_udf.reduce_collection('check_sponsored_listing',hotel_details.deal,false),
			trv_udf.filterArray(visible_hotels,'item_id',hotel_details.id),
			NULL
		)[0] as meta_hotel_details,
		 trv_udf.map_collection('transform_blocked_item_list',trv_udf.array_to_map(blocked_hotels),'BLOCKED',auction_id) AS blocked_hotels,
		 experiments
	FROM
		cte_ea_ranking_joined
	LATERAL VIEW POSEXPLODE(accommodation) accm AS hotel_pos, hotel_details
	-- WHERE
	--	 hotel_details.auction_id.value = auction_id
	-- Add this for next PR to handle multiple auction_ids
),

cte_exp_algo_log_hotels_visible_hotels AS (
	SELECT
		header,
		request_id,
		original_request_id,
		bid_storage_call_status,
		platform,
		item_id,
		relevance_score,
		ir_predicted_ctr,
		hotel_position,
		auction_id,
		blocked_hotels,
		COALESCE(
			IF(
				is_sponsored_listing,
				trv_udf.map_collection(
					'transform_sponsored_listing_price',
					trv_udf.array_to_map(
					trv_udf.filterArray(sl_hotel_details.sl_prices,'is_winner',true)
			),
					'VISIBLE'
				),
				trv_udf.map_collection(
					'transform_meta_listing_price',
					trv_udf.array_to_map(meta_hotel_details.visible_prices),
					'VISIBLE'
				)
			),
			trv_udf.empty_collection_of_type(trv_udf.type_from_string(${hiveconf:modified_price_data_type_string}))
		) AS visible_prices,
		COALESCE(
			trv_udf.map_collection(
					'transform_meta_listing_price',
					trv_udf.array_to_map(meta_hotel_details.non_visible_prices),
					'NON_VISIBLE'
			),
			trv_udf.empty_collection_of_type(trv_udf.type_from_string(${hiveconf:modified_price_data_type_string}))
		) AS non_visible_prices,
		COALESCE(
			trv_udf.map_collection(
				'transform_meta_listing_price',
				trv_udf.array_to_map(meta_hotel_details.blocked_prices),
				'BLOCKED'
			),
			trv_udf.empty_collection_of_type(trv_udf.type_from_string(${hiveconf:modified_price_data_type_string}))			
		) AS blocked_prices,
		is_sponsored_listing,
		meta_hotel_details.ad_relevance_reserve_gain AS ad_relevance_reserve_gain,
		meta_hotel_details.champion_choice AS champion_choice,
		meta_hotel_details.bid_mod_reserve_factor AS bid_mod_reserve_factor,
		experiments
	FROM
		cte_infer_visible_hotel_positions
),

cte_infer_EA AS (
	SELECT
		header,
		request_id,
		original_request_id,
		bid_storage_call_status,
		platform,
		brickhouse.collect(
			named_struct(
				'item_exposure_type', 'VISIBLE',
				'item_displayed_rank',hotel_position,
				'item_id',item_id,
				'relevance_score',relevance_score,
				'top_deal_item_flag',CAST(NULL AS INT),
				'item_price_list',
				brickhouse.combine(
					visible_prices,
					blocked_prices,
					non_visible_prices
				),
				'auction_id',auction_id,
				'ctr_log_values',array(named_struct('feature',cast(NULL AS STRING),'value',cast(NULL AS DOUBLE))),
				'ad_relevance_reserve_gain',ad_relevance_reserve_gain,
				'champion_choice',champion_choice,
				'ir_predicted_ctr',ir_predicted_ctr,
				'bid_mod_reserve_factor',bid_mod_reserve_factor
			)
		) AS visible_hotels,
		trv_udf.first(blocked_hotels) AS blocked_hotels,
		trv_udf.first(experiments) AS experiments
	FROM
		cte_exp_algo_log_hotels_visible_hotels
	GROUP BY
		header,
		request_id,
		original_request_id,
		bid_storage_call_status,
		platform
),

cte_marketplace_exposure_algorithm_result_transformed AS (
	SELECT
		request_id AS poll_request_id,
		original_request_id AS origin_request_id,
		NULL AS tracking_id,
		from_unixtime(header.time) AS poll_request_timestamp,
		header.senderid AS poll_request_sender_id,
		trv_udf.sortArrayItem(visible_hotels,"item_displayed_rank") AS visible_item_list,
		blocked_hotels AS blocked_item_list,
		bid_storage_call_status,
		experiments,
		${crunchDate} AS ymd
	FROM
		cte_infer_EA
)

INSERT OVERWRITE TABLE ${trivago_stage_db}.marketplace_exposure_algorithm_result
PARTITION(ymd)
SELECT
	poll_request_id,
	origin_request_id,
	tracking_id,
	poll_request_timestamp,
	poll_request_sender_id,
	visible_item_list,
	blocked_item_list,
	bid_storage_call_status,
	experiments,
	ymd
FROM
	cte_marketplace_exposure_algorithm_result_transformed
;