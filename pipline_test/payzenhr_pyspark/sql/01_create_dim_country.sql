USE payzen_dw;
CREATE TABLE IF NOT EXISTS dim_country
(
    sk_country UInt32,
    bk_country_id UInt32,
    country_name Nullable(String),
    country_name_ar Nullable(String),
    country_code Nullable(String),
    country_phone_code Nullable(String)
)
ENGINE = ReplacingMergeTree
ORDER BY sk_country;