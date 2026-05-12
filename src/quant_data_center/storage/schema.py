"""DuckDB schema for the quant data center control plane."""

CONTROL_SCHEMA = "qdc_meta"
SILVER_SCHEMA = "qdc_silver"

CONTROL_SCHEMA_SQL = """
create schema if not exists qdc_meta;

create table if not exists qdc_meta.job_run (
  job_id varchar primary key,
  job_type varchar not null,
  status varchar not null,
  dataset varchar,
  source_id varchar,
  universe varchar,
  start_date date,
  end_date date,
  start_at timestamp not null,
  end_at timestamp,
  parameters_json varchar not null default '{}',
  error_message varchar,
  created_at timestamp not null
);

create table if not exists qdc_meta.backfill_task (
  task_id varchar primary key,
  dataset varchar not null,
  source_id varchar not null,
  universe varchar,
  start_date date not null,
  end_date date not null,
  symbol_batch_json varchar not null default '[]',
  status varchar not null,
  attempt_count integer not null default 0,
  last_error varchar,
  created_at timestamp not null,
  updated_at timestamp not null
);

create table if not exists qdc_meta.dataset_watermark (
  dataset varchar not null,
  source_id varchar not null,
  universe varchar not null default '',
  min_date date,
  max_date date,
  last_success_at timestamp,
  last_job_id varchar,
  updated_at timestamp not null,
  primary key (dataset, source_id, universe)
);

create table if not exists qdc_meta.source_object (
  object_id varchar primary key,
  job_id varchar,
  dataset varchar not null,
  source_id varchar not null,
  layer varchar not null,
  uri varchar not null,
  content_hash varchar,
  size_bytes bigint,
  observed_at timestamp not null,
  created_at timestamp not null
);

create table if not exists qdc_meta.quality_issue (
  issue_id varchar primary key,
  job_id varchar,
  dataset varchar,
  source_id varchar,
  severity varchar not null,
  issue_type varchar not null,
  status varchar not null,
  entity_key varchar,
  message varchar not null,
  observed_value varchar,
  created_at timestamp not null
);

create table if not exists qdc_meta.crawler_source (
  source_id varchar primary key,
  source_type varchar not null,
  dataset varchar not null,
  base_url varchar not null,
  enabled boolean not null,
  robots_url varchar,
  robots_status varchar not null,
  terms_review_status varchar not null,
  copyright_policy varchar not null,
  rate_limit_per_minute integer not null,
  min_delay_seconds double not null,
  max_retry integer not null,
  parser_version varchar not null,
  notes varchar,
  updated_at timestamp not null
);

create table if not exists qdc_meta.crawl_task (
  task_id varchar primary key,
  source_id varchar not null,
  dataset varchar not null,
  crawl_date date not null,
  partition_key varchar not null,
  request_json varchar not null default '{}',
  status varchar not null,
  attempt_count integer not null default 0,
  last_error varchar,
  created_at timestamp not null,
  updated_at timestamp not null
);

create table if not exists qdc_meta.crawl_run (
  run_id varchar primary key,
  source_id varchar,
  dataset varchar,
  crawl_date date,
  status varchar not null,
  planned_count integer not null default 0,
  success_count integer not null default 0,
  failed_count integer not null default 0,
  document_count integer not null default 0,
  raw_object_count integer not null default 0,
  start_at timestamp not null,
  end_at timestamp,
  parameters_json varchar not null default '{}',
  error_message varchar,
  created_at timestamp not null
);
"""

CONTROL_TABLES = [
    "job_run",
    "backfill_task",
    "dataset_watermark",
    "source_object",
    "quality_issue",
    "crawler_source",
    "crawl_task",
    "crawl_run",
]

SILVER_SCHEMA_SQL = """
create schema if not exists qdc_silver;

create table if not exists qdc_silver.stock_basic (
  instrument varchar primary key,
  symbol varchar not null,
  exchange varchar not null,
  name varchar,
  list_date date,
  delist_date date,
  is_active boolean,
  industry varchar,
  source_id varchar not null,
  updated_at timestamp not null
);

create table if not exists qdc_silver.universe_constituent (
  universe varchar not null,
  snapshot_date date not null,
  instrument varchar not null,
  symbol varchar not null,
  exchange varchar not null,
  name varchar,
  weight double,
  source_id varchar not null,
  updated_at timestamp not null,
  primary key (universe, snapshot_date, instrument)
);

create table if not exists qdc_silver.trade_calendar (
  calendar_id varchar not null,
  trade_date date not null,
  is_open boolean not null,
  pre_trade_date date,
  next_trade_date date,
  source_id varchar not null,
  updated_at timestamp not null,
  primary key (calendar_id, trade_date)
);

create table if not exists qdc_silver.daily_bar (
  trade_date date not null,
  instrument varchar not null,
  open double,
  high double,
  low double,
  close double,
  pre_close double,
  volume double,
  amount double,
  vwap double,
  source_id varchar not null,
  updated_at timestamp not null,
  primary key (trade_date, instrument)
);

create table if not exists qdc_silver.adj_factor (
  trade_date date not null,
  instrument varchar not null,
  adj_factor double,
  factor_type varchar,
  source_id varchar not null,
  updated_at timestamp not null,
  primary key (trade_date, instrument)
);

create table if not exists qdc_silver.price_limit (
  trade_date date not null,
  instrument varchar not null,
  limit_up double,
  limit_down double,
  prev_close double,
  limit_rule varchar,
  source_id varchar not null,
  updated_at timestamp not null,
  primary key (trade_date, instrument)
);

create table if not exists qdc_silver.trade_status (
  trade_date date not null,
  instrument varchar not null,
  trade_status varchar not null,
  halt_reason varchar,
  source_update_time date,
  source_id varchar not null,
  updated_at timestamp not null,
  primary key (trade_date, instrument)
);

create table if not exists qdc_silver.announcement (
  announcement_id varchar primary key,
  publish_date date not null,
  instrument varchar not null,
  title varchar not null,
  url varchar,
  source_id varchar not null,
  updated_at timestamp not null
);

create table if not exists qdc_silver.news (
  news_id varchar primary key,
  publish_date date not null,
  instrument varchar not null,
  title varchar not null,
  url varchar,
  source_id varchar not null,
  updated_at timestamp not null
);

create table if not exists qdc_silver.daily_news_factor (
  trade_date date not null,
  instrument varchar not null,
  news_count double not null,
  news_sentiment_mean double not null,
  news_positive_count double not null,
  news_negative_count double not null,
  news_growth_count double not null,
  news_risk_count double not null,
  news_financing_count double not null,
  news_weighted_sentiment_sum double not null,
  news_importance_sum double not null,
  news_contract_count double not null,
  news_buyback_count double not null,
  news_shareholder_change_count double not null,
  news_regulatory_count double not null,
  news_litigation_count double not null,
  news_performance_count double not null,
  source_id varchar not null,
  updated_at timestamp not null,
  primary key (trade_date, instrument)
);

create table if not exists qdc_silver.daily_announcement_factor (
  trade_date date not null,
  instrument varchar not null,
  announcement_count double not null,
  announcement_growth_count double not null,
  announcement_risk_count double not null,
  announcement_financing_count double not null,
  announcement_operation_count double not null,
  announcement_sentiment_mean double not null,
  announcement_positive_count double not null,
  announcement_negative_count double not null,
  announcement_weighted_sentiment_sum double not null,
  announcement_importance_sum double not null,
  announcement_contract_count double not null,
  announcement_buyback_count double not null,
  announcement_shareholder_change_count double not null,
  announcement_regulatory_count double not null,
  announcement_litigation_count double not null,
  announcement_performance_count double not null,
  source_id varchar not null,
  updated_at timestamp not null,
  primary key (trade_date, instrument)
);
"""

SILVER_TABLES = [
    "stock_basic",
    "universe_constituent",
    "trade_calendar",
    "daily_bar",
    "adj_factor",
    "price_limit",
    "trade_status",
    "announcement",
    "news",
    "daily_news_factor",
    "daily_announcement_factor",
]

SILVER_SCHEMA_MIGRATIONS = {
    "daily_news_factor": {
        "news_sentiment_mean": "double default 0",
        "news_positive_count": "double default 0",
        "news_negative_count": "double default 0",
        "news_growth_count": "double default 0",
        "news_risk_count": "double default 0",
        "news_financing_count": "double default 0",
        "news_weighted_sentiment_sum": "double default 0",
        "news_importance_sum": "double default 0",
        "news_contract_count": "double default 0",
        "news_buyback_count": "double default 0",
        "news_shareholder_change_count": "double default 0",
        "news_regulatory_count": "double default 0",
        "news_litigation_count": "double default 0",
        "news_performance_count": "double default 0",
    },
    "daily_announcement_factor": {
        "announcement_growth_count": "double default 0",
        "announcement_risk_count": "double default 0",
        "announcement_financing_count": "double default 0",
        "announcement_operation_count": "double default 0",
        "announcement_sentiment_mean": "double default 0",
        "announcement_positive_count": "double default 0",
        "announcement_negative_count": "double default 0",
        "announcement_weighted_sentiment_sum": "double default 0",
        "announcement_importance_sum": "double default 0",
        "announcement_contract_count": "double default 0",
        "announcement_buyback_count": "double default 0",
        "announcement_shareholder_change_count": "double default 0",
        "announcement_regulatory_count": "double default 0",
        "announcement_litigation_count": "double default 0",
        "announcement_performance_count": "double default 0",
    },
}
