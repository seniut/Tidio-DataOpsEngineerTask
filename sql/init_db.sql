DROP TABLE IF EXISTS customer_visits;

CREATE TABLE IF NOT EXISTS customer_visits (
    ad_bucket VARCHAR(255),
    ad_type VARCHAR(255),
    ad_source VARCHAR(255),
    schema_version VARCHAR(255),
    ad_campaign_id VARCHAR(255),
    ad_keyword VARCHAR(255),
    ad_group_id VARCHAR(255),
    ad_creative VARCHAR(255)
);
