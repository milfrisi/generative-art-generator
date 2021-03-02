set hive.query.name=hotel_search_result;

set hive.vectorized.execution.reduce.groupby.enabled=true;
set hive.vectorized.execution.reduce.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.cbo.enable=true;
set hive.tez.bucket.pruning=true;
set hive.tez.dynamic.partition.pruning=true;

WITH cte_rejected_tracking_ids AS (
    SELECT DISTINCT
        tracking_id_raw as tracking_id
    FROM
        ${trivago_analytic_db}.page_log_master_rejected
    WHERE
        ymd=${crunchDate}
),

cte_hotel_search_concept_search_query AS (
    SELECT
        hsq.*
    FROM
        ${trivago_stage_db}.hotel_search_concept_search_query hsq
    LEFT JOIN
        cte_rejected_tracking_ids rej
    ON
        hsq.tracking_id = rej.tracking_id
    WHERE
        hsq.ymd = ${crunchDate}
        AND rej.tracking_id IS NULL
        AND COALESCE(hsq.origin_request_id,hsq.poll_request_id) <> '594aac44-5c26-400b-923d-1e328127d6a9' --BDQ-647 hotfix + BDQ-663 updated hotfix with NULL SAFE
),

cte_hotel_search_concept_search_result AS (
    SELECT
        *
    FROM
        ${trivago_stage_db}.hotel_search_concept_search_result
    WHERE
        ymd = ${crunchDate}
),

cte_marketplace_exposure_algorithm_result AS (
    SELECT
        *
    FROM
        ${trivago_stage_db}.marketplace_exposure_algorithm_result mpe
    WHERE
        ymd = ${crunchDate}
),

cte_hotel_search_access_log AS (
    SELECT
        *
    FROM
        ${trivago_stage_db}.hotel_search_web_access_log
    WHERE
        ymd = ${crunchDate}
),

cte_hotel_search_search_engine_detail AS (
    SELECT
        *
    FROM
        ${trivago_stage_db}.hotel_search_search_engine_detail
    WHERE
        ymd = ${crunchDate}
),

cte_hotel_search_price_impression as (
    SELECT 
        DISTINCT
        poll_request_id
    FROM
        ${trivago_stage_db}.hotel_search_price_impression
    WHERE
        ymd = ${crunchDate}
),

cte_hotel_search_concept_search_query_origin AS (
     SELECT
        poll_request_id,
        tracking_id,
        poll_request_timestamp,
        poll_request_sender_id,
        platform_code,
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
        created_at_nanoseconds,
        concept_search_type,
        ymd
    FROM
        cte_hotel_search_concept_search_query hs_csq
    WHERE
        concept_search_type <> 'SEARCH_POLL'
    CLUSTER BY
        poll_request_id
),

cte_hotel_search_concept_search_query_transformed AS (
    SELECT
        hs_csq.poll_request_id as poll_request_id,
        hs_csq.origin_request_id as origin_request_id,
        hs_csq.tracking_id as tracking_id,
        hs_csq.poll_request_timestamp as poll_request_timestamp,
        hs_csq.poll_request_sender_id as poll_request_sender_id,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.platform_code,hs_csq_origin.platform_code) AS platform_code,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.language_code,hs_csq_origin.language_code) AS language_code,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.language_script_code,hs_csq_origin.language_script_code) AS language_script_code,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.region_code,hs_csq_origin.region_code) AS region_code,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.currency_code,hs_csq_origin.currency_code) AS currency_code,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.check_in_date,hs_csq_origin.check_in_date) AS check_in_date,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.check_out_date,hs_csq_origin.check_out_date) AS check_out_date,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.max_price_per_night,hs_csq_origin.max_price_per_night) AS max_price_per_night,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.total_adult_count,hs_csq_origin.total_adult_count) AS total_adult_count,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.total_child_count,hs_csq_origin.total_child_count) AS total_child_count,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.room_count,hs_csq_origin.room_count) AS room_count,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.first_sort_criterion_type,hs_csq_origin.first_sort_criterion_type) AS first_sort_criterion_type,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.second_sort_criterion_type,hs_csq_origin.second_sort_criterion_type) AS second_sort_criterion_type,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.uiv_list,hs_csq_origin.uiv_list) AS uiv_list,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.top_item_id,hs_csq_origin.top_item_id) AS top_item_id,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.poi_flag,hs_csq_origin.poi_flag) AS poi_flag,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.destination_id,hs_csq_origin.destination_id) AS destination_id,
        hs_csq.created_at_nanoseconds as created_at_nanoseconds,
        IF(hs_csq.concept_search_type<>'SEARCH_POLL',hs_csq.concept_search_type,hs_csq_origin.concept_search_type) AS concept_search_type,
        hs_csq.ymd
    FROM
        cte_hotel_search_concept_search_query hs_csq
    LEFT JOIN
        cte_hotel_search_concept_search_query_origin hs_csq_origin
    ON
        COALESCE(hs_csq.origin_request_id,rand(100)) = hs_csq_origin.poll_request_id
    CLUSTER BY
        poll_request_id
),

cte_hotel_search_concept_search_result_filtered AS (
    SELECT
        hsr.*
    FROM
        cte_hotel_search_concept_search_result hsr
    LEFT JOIN
        cte_hotel_search_price_impression im
    ON
        hsr.poll_request_id = im. poll_request_id
    WHERE
        hsr.search_result_status='DONE'
        OR (hsr.search_result_status='PENDING' AND im.poll_request_id IS NOT NULL)
)

INSERT OVERWRITE TABLE ${trivago_stage_db}.hotel_search_result
PARTITION(ymd)
SELECT
    hs_csq.poll_request_id,
    hs_csq.origin_request_id,
    hs_csq.tracking_id,
    hs_csq.platform_code,
    hs_csq.language_code,
    hs_csq.language_script_code,
    hs_csq.region_code,
    hs_csq.currency_code,
    hs_csq.check_in_date,
    hs_csq.check_out_date,
    hs_csq.max_price_per_night,
    hs_csq.total_adult_count,
    hs_csq.total_child_count,
    hs_csq.room_count,
    hs_csq.first_sort_criterion_type,
    hs_csq.second_sort_criterion_type,
    hs_csq.uiv_list,
    hs_csq.top_item_id,
    hs_csq.poi_flag,
    hs_csq.destination_id,
    hs_csq.concept_search_type,
    hs_csr.search_result_status,
    hs_csr.price_position_selector_description,
    hs_csr.item_position_selector_description,
    mpe_ea.visible_item_list,
	mpe_ea.blocked_item_list,
	mpe_ea.bid_storage_call_status,
    hs_al.geo_location_code,
    hs_al.bot_detection_type,
    hs_sed.page_limit,
    hs_sed.page_number,
    hs_sed.stay_period_source_id,
    hs_sed.is_standard_date,
    hs_csq.poll_request_timestamp,
    hs_csq.poll_request_sender_id,
    mpe_ea.experiments,
    ${crunchDate} AS ymd

FROM
    cte_hotel_search_concept_search_query_transformed hs_csq

INNER JOIN
    cte_marketplace_exposure_algorithm_result mpe_ea
ON
    hs_csq.poll_request_id=mpe_ea.poll_request_id

INNER JOIN 
    cte_hotel_search_concept_search_result_filtered hs_csr
ON
    mpe_ea.poll_request_id = hs_csr.poll_request_id

LEFT JOIN 
    cte_hotel_search_access_log hs_al
ON
    hs_csq.poll_request_id=hs_al.poll_request_id

LEFT JOIN
    cte_hotel_search_search_engine_detail hs_sed
ON
    COALESCE(hs_csq.origin_request_id,hs_csq.poll_request_id) = hs_sed.poll_request_id
;