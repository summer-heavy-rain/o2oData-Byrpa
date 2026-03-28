-- =============================================================================
-- 固定成本维表（《房租水电成本》Excel 导入）
-- =============================================================================
-- 执行一次即可；与 ODS 贴源表无关，不参与 RPA 宏建表。
-- 聚合脚本：sql/o2o_daily_store_aggregate.sql 中的 fixed_enriched 依赖本表。
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_ods_store_operating_cost (
  store_label TEXT PRIMARY KEY,
  province TEXT,
  city TEXT,
  daily_fixed_cost NUMERIC NOT NULL
);

COMMENT ON TABLE dim_ods_store_operating_cost IS 'O2O 门店日均固定成本（房租水电人工等），与 dim_store.store_short_name 对齐';
