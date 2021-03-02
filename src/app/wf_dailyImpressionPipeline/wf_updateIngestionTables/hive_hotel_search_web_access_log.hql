WITH cte_hotel_search_web_access_log AS (
    SELECT 
        *
    FROM
        ${gobblin_streams_db}.hsw_access_logs_proto   
    WHERE 
        ymd = ${crunchDate}
        AND tracking_id<>'trvPrerendererBotNoTraking'
	    AND unique_id NOT RLIKE('^0+$')
),

cte_hotel_search_web_access_log_transformed AS (
    SELECT
        unique_id as poll_request_id,
        tracking_id,
        FROM_UNIXTIME(header.time) AS poll_request_timestamp,
        header.senderid AS poll_request_sender_id,
        CASE 
            WHEN lower(headers['X-Trv-Gwg'].value) = 'u' THEN 'USER'
            WHEN lower(headers['X-Trv-Gwg'].value) = 'b' THEN 'BOT'
            ELSE NULL
        END AS bot_detection_type,
        UPPER(headers['X-Trv-Client-Geo-Location'].value) AS geo_location_code,
        req_first_line.method AS request_method_type,
        req_first_line.uri AS request_uri_description,
        req_first_line.protocol AS request_protocol_type,
        status as http_status_code,
        ${crunchDate} AS ymd,
        ROW_NUMBER() OVER (
            PARTITION BY unique_id 
            ORDER BY IF(status=200,1,0) DESC,header.time DESC
        ) AS row_num
    FROM
        cte_hotel_search_web_access_log
)

INSERT OVERWRITE TABLE ${trivago_stage_db}.hotel_search_web_access_log
PARTITION(ymd)
SELECT
    poll_request_id,
    tracking_id,
    poll_request_timestamp,
    poll_request_sender_id,
    bot_detection_type,
    geo_location_code,
    request_method_type,
    request_uri_description,
    request_protocol_type,
    http_status_code,
    ymd
FROM 
    cte_hotel_search_web_access_log_transformed
WHERE
    row_num=1;