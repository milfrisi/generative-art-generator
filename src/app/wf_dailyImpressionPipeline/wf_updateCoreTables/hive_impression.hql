set hive.query.name=impression;

set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.tez.bucket.pruning=true;
set hive.tez.dynamic.partition.pruning=true;

WITH cte_valid_poll_request_ids AS (
   SELECT
        poll_request_id,
        COUNT(1) OVER (PARTITION BY tracking_id) AS count_tracking_id
    FROM
        ${trivago_stage_db}.hotel_search_result
    WHERE
        ymd = ${crunchDate}
),

cte_hotel_search_result AS (
    SELECT
        hsr.*
    FROM
        ${trivago_stage_db}.hotel_search_result hsr
    INNER JOIN 
        cte_valid_poll_request_ids valid
    ON 
        hsr.poll_request_id = valid.poll_request_id
        AND valid.count_tracking_id < 3000 --3000 poll_request_ids per day is around 2 searches per minute
    WHERE
        hsr.ymd = ${crunchDate}
),

cte_scroll_impression AS (
     SELECT 
        poll_request_id,
        impression_type,
        named_struct(
			'item_id',item_id,
            'event_count',COUNT(DISTINCT price_impression_id)
        ) scroll_impression_detail
    FROM 
        ${trivago_stage_db}.hotel_search_price_impression im
        LATERAL VIEW OUTER EXPLODE(im.visible_item_id_list) vil AS item_id
    WHERE 
        ymd=${crunchDate}
        AND impression_type='SCROLL_IMPRESSION'
    GROUP BY    
        poll_request_id,
        impression_type,
        item_id
),

cte_scroll_impression_flattened AS (
     SELECT 
        poll_request_id,
        impression_type,
        brickhouse.collect(scroll_impression_detail) AS scroll_impression_item_id_list
    FROM 
       cte_scroll_impression
    GROUP BY    
        poll_request_id,
        impression_type
    
),

cte_clickouts AS (
    SELECT
        co.poll_request_id,
        'CLICKOUT' AS impression_type,
        named_struct(
    		'item2partner_id',co.item2partner_id,
            'event_count',COUNT(DISTINCT co.request_id),
            'item_id',CAST(co.item_id AS INT),
            'partner_id',CAST(co.partner_id AS INT),
            'clickout_source',co.clickout_source
        ) clickout_detail
    FROM 
        ${trivago_analytic_db}.session_stats_master AS ssm
        LATERAL VIEW EXPLODE (co_log_entries) co_log AS co
    WHERE
        ymd = ${crunchDate}
        AND co.page_id = 8001
        AND ssm.is_core
    GROUP BY 
        co.poll_request_id,
        co.item2partner_id,
        co.item_id,
        co.partner_id,
        co.clickout_source
),

cte_clickouts_flattened AS (
     SELECT 
        poll_request_id,
        impression_type,
        brickhouse.collect(clickout_detail) AS clickout_item2partner_id_list
    FROM 
       cte_clickouts
    GROUP BY    
        poll_request_id,
        impression_type
),

cte_hotel_search_price_impression_flattened AS ( 
    SELECT
        COALESCE(co.poll_request_id,si.poll_request_id) AS poll_request_id,
        IF(si.poll_request_id IS NOT NULL,1,0) AS scroll_impression_event_flag,
        IF(co.poll_request_id IS NOT NULL,1,0) AS clickout_event_flag,
        IF(
            si.poll_request_id IS NOT NULL AND co.poll_request_id IS NOT NULL,
            ARRAY(si.impression_type,co.impression_type),
            ARRAY(COALESCE(si.impression_type,co.impression_type))
        ) AS impression_type_list,
        si.scroll_impression_item_id_list,
        co.clickout_item2partner_id_list
    FROM
        cte_clickouts_flattened co
    FULL OUTER JOIN
        cte_scroll_impression_flattened si 
    ON
        co.poll_request_id = si.poll_request_id
),

cte_session_stats_master_co_log AS (
    SELECT
        co.request_id AS clickout_id,
        ssm.session_date_id,
        ssm.session_id,
        ssm.identifier_data.tracking_id_raw AS tracking_id,
        ssm.locale AS locale_code,
        ssm.agent_id,
        ssm.tags,
        ssm.crawler_id,
        ssm.is_core,
        ssm.test_group_set,
        ssm.control_group_set,
        ssm.test_group_set_php,
        ssm.control_group_set_php,
        ssm.release_string,
        ssm.is_app,
        ssm.session_timestamp
    FROM 
        ${trivago_analytic_db}.session_stats_master AS ssm
        LATERAL VIEW EXPLODE (co_log_entries) co_log AS co
    WHERE
        ymd = ${crunchDate}
        AND co.page_id = 8001
),

cte_session_stats_master_clickout AS (
    SELECT
        prim.poll_request_id,
        ssm.session_date_id,
        ssm.session_id,
        ssm.tracking_id,
        ssm.locale_code,
        ssm.agent_id,
        ssm.tags,
        ssm.crawler_id,
        ssm.is_core,
        ssm.test_group_set,
        ssm.control_group_set,
        ssm.test_group_set_php,
        ssm.control_group_set_php,
        ssm.release_string,
        ssm.is_app,
        ROW_NUMBER() OVER (
            PARTITION BY prim.poll_request_id
            ORDER BY ssm.session_timestamp DESC
        ) AS row_num
    FROM 
        ${trivago_stage_db}.hotel_search_price_impression prim
    LEFT JOIN
        cte_session_stats_master_co_log ssm
    ON 
        prim.price_impression_id = ssm.clickout_id  
    WHERE 
        ymd = ${crunchDate}
        AND prim.impression_type = 'CLICKOUT'
),

cte_session_stats_master_poll_request AS (
    SELECT
        page_log_detail_map[193] AS poll_request_id,
        session_date_id,
        session_id,
        identifier_data.tracking_id_raw AS tracking_id,
        locale AS locale_code,
        agent_id,
        tags,
        crawler_id,
        is_core,
        test_group_set,
        control_group_set,
        test_group_set_php,
        control_group_set_php,
        release_string,
        is_app,
        ROW_NUMBER() OVER (
            PARTITION BY page_log_detail_map[193] 
            ORDER BY session_timestamp DESC
        ) AS row_num
    FROM 
        ${trivago_analytic_db}.session_stats_master
    WHERE 
        ymd = ${crunchDate}
        AND page_log_detail_map[193] IS NOT NULL
),

cte_session_stats_master_tracking_id AS (
    SELECT
        session_date_id,
        session_id,
        identifier_data.tracking_id_raw AS tracking_id,
        locale AS locale_code,
        agent_id,
        tags,
        crawler_id,
        is_core,
        test_group_set,
        control_group_set,
        test_group_set_php,
        control_group_set_php,
        release_string,
        is_app,
        ROW_NUMBER() OVER (
            PARTITION BY identifier_data.tracking_id_raw 
            ORDER BY crawler_id ASC,session_timestamp DESC
        ) AS row_num
    FROM 
        ${trivago_analytic_db}.session_stats_master
    WHERE 
        ymd = ${crunchDate}
)

INSERT OVERWRITE TABLE ${trivago_analytic_db}.impression 
PARTITION(ymd)
SELECT
    hsr.poll_request_id,
    hsr.origin_request_id,
    COALESCE(ssm_click.session_date_id,ssm_poll.session_date_id,ssm_tid.session_date_id) AS session_date_id,
    COALESCE(ssm_click.session_id,ssm_poll.session_id,ssm_tid.session_id) AS session_id,
    hsr.tracking_id,
    hsr.platform_code,
    COALESCE(ssm_click.locale_code,ssm_poll.locale_code,ssm_tid.locale_code) AS locale_code,
    hsr.language_code,
    hsr.language_script_code,
    hsr.region_code,
    hsr.currency_code,
    hsr.check_in_date,
    hsr.check_out_date,
    hsr.max_price_per_night,
    hsr.total_adult_count,
    hsr.total_child_count, 
    hsr.room_count,
    hsr.first_sort_criterion_type,
    hsr.second_sort_criterion_type,
    hsr.uiv_list,
    hsr.top_item_id,
    hsr.poi_flag,
    hsr.destination_id,
    hsr.concept_search_type,
    hsr.search_result_status,
    hsr.price_position_selector_description,
    hsr.item_position_selector_description,
    hsr.visible_item_list,
    hsr.blocked_item_list,
    hsr.bid_storage_call_status,
    hsr.geo_location_code,
    hsr.bot_detection_type,
    hsr.page_limit,
    hsr.page_number,
    prim.impression_type_list,
    COALESCE(prim.scroll_impression_event_flag,0),
    prim.scroll_impression_item_id_list,
    COALESCE(prim.clickout_event_flag,0),
    prim.clickout_item2partner_id_list,
    COALESCE(ssm_click.agent_id,ssm_poll.agent_id,ssm_tid.agent_id) AS agent_id,
    COALESCE(ssm_click.tags,ssm_poll.tags,ssm_tid.tags) AS tags,
    COALESCE(ssm_click.crawler_id,ssm_poll.crawler_id,ssm_tid.crawler_id) AS crawler_id,
    COALESCE(ssm_click.is_core,ssm_poll.is_core,ssm_tid.is_core,FALSE) AS is_core,
    COALESCE(ssm_click.test_group_set,ssm_poll.test_group_set,ssm_tid.test_group_set) AS test_group_set,
    COALESCE(ssm_click.control_group_set,ssm_poll.control_group_set,ssm_tid.control_group_set) AS control_group_set,
    COALESCE(ssm_click.test_group_set_php,ssm_poll.test_group_set_php,ssm_tid.test_group_set_php) AS test_group_set_php,
    COALESCE(ssm_click.control_group_set_php,ssm_poll.control_group_set_php,ssm_tid.control_group_set_php) AS control_group_set_php,
    COALESCE(ssm_click.release_string,ssm_poll.release_string,ssm_tid.release_string) AS release_string,
    COALESCE(ssm_click.is_app,ssm_poll.is_app,ssm_tid.is_app) AS is_app,
    CASE 
        WHEN ssm_click.session_date_id IS NOT NULL THEN 'CLICKOUT_ID'
        WHEN ssm_poll.session_date_id IS NOT NULL THEN 'POLL_REQUEST_ID'
        WHEN ssm_tid.session_date_id IS NOT NULL THEN 'TRACKING_ID'
        ELSE NULL
    END AS session_source_type,
    hsr.stay_period_source_id,
    hsr.is_standard_date,
    hsr.poll_request_timestamp,
    hsr.poll_request_sender_id,
    hsr.experiments,
    hsr.ymd
FROM 
    cte_hotel_search_result hsr
    
LEFT JOIN
    cte_hotel_search_price_impression_flattened prim
ON 
    hsr.poll_request_id = prim.poll_request_id

LEFT JOIN
    cte_session_stats_master_clickout ssm_click
ON 
    COALESCE(hsr.poll_request_id,hsr.origin_request_id) = ssm_click.poll_request_id 
    AND ssm_click.row_num = 1

LEFT JOIN
    cte_session_stats_master_poll_request ssm_poll
ON 
    COALESCE(hsr.origin_request_id,hsr.poll_request_id) = ssm_poll.poll_request_id
    AND ssm_poll.row_num = 1

LEFT JOIN
    cte_session_stats_master_tracking_id ssm_tid
ON 
    hsr.tracking_id = ssm_tid.tracking_id
    AND ssm_tid.row_num = 1
;