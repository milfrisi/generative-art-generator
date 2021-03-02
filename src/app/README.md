# Impression Data Pipeline

## Data Pipeline Details

* Schedule - Daily 12:15am UTC

* Dependencies
  1. trivago_analytic.session_stats_master Day-1
  2. gobblin_streams.mpe_ea_optimization_results_proto Day-1

* Producer - Marketplace Intelligence
* Consumers
  1. Advertiser Intelligence
  2. Trivago Intelligence
  3. Bidding Intelligence
  4. Auction and Ranking Teams

## Source Tables
##### `gobblin_streams.marketplace_exposure_algorithm_optimization`
* Producer: Marketplace Auction Team
* Contact Person: Alexander Volkman
* Description: Data containing search result coming from the exposure algorithm

#####  `gobblin_streams.price_impression_log_proto`
* Producer: Data Engineering
* Contact Person: Heiko Hilbert
* Description: Data containing impression events
  1. SCROLL_IMPRESSION - User scrolls and stops for 3 seconds and pixel threshold is met. Logs all item_ids in viewport
  2. CLICKOUT - User clicks and advertiser link. Logs item2partner_id clicked

##### `gobblin_streams.hsc_searchresultlog_proto`
* Producer: Hotel Search - Core Search
* Contact Person: Rupesh Patayane
* Description: Data containing search result but can be from EA or not if API fails.

##### `gobblin_streams.hsw_service_concept_search_v2_proto`
* Producer: Hotel Search - Gateway
* Contact Person: Werner Moraes
* Description: Data containing search queries and details like check_in_date, check_out_date, type of concept search, number of rooms

##### `gobblin_streams.hsw_access_logs_proto`
* Producer: Hotel Search - Gateway
* Contact Person: Werner Moraes
* Description: Data containing details for user or bot and geo_location

##### `gobblin_streams.hsc_searchenginerequestlog_proto`
* Producer: Hotel Search - Core Search
* Contact Person: Rupesh Patayane
* Description: Data containing page number and page limit

##### `trivago_analytic.page_log_master_rejected`
* Producer: Data Engineering
* Contact Person: Dirk Brodersen
* Description: Rejected crawlers on initial processed and before loading to session_stats_master

##### `trivago_analytic.session_stats_master`
* Producer: Data Engineering
* Contact Person: Dirk Brodersen
* Description: trivago's main session detail table

## Target Tables
* `trivago_stage.marketplace_exposure_algorithm_result`
* `trivago_stage.hotel_search_price_impression`
* `trivago_stage.hotel_search_concept_search_result`
* `trivago_stage.hotel_search_concept_search_query`
* `trivago_stage.hotel_search_search_engine_detail`
* `trivago_stage.hotel_search_web_access_log`
* `trivago_stage.hotel_search_result`
* `trivago_analytic.impression`

## DAG

To add airflow link once done
