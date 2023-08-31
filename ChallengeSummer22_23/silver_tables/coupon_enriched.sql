-- Databricks notebook source
CREATE TABLE IF NOT EXISTS coupons.coupon_enriched 
  USING PARQUET
  LOCATION "/user/hive/warehouse/coupons/coupon_enriched" 
  AS 
    SELECT C.coupon_upc, product_id, C.campaign, household_key, day,
      date_format(dateAdd(day, R.day, '2021-01-01'), 'yyyy-MM-dd') AS trans_date,
      CASE
        WHEN day IS NULL THEN false
        WHEN day IS NOT NULL THEN true
    END AS is_coupon_redeemed
    FROM coupons.coupon C LEFT JOIN coupons.coupon_redempt R ON R.COUPON_UPC = C.COUPON_UPC
