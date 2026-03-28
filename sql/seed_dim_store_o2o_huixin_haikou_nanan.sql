-- =============================================================================
-- 补全 dim_store_o2o：华信店 / 海口店 / 南安店（美团买药 + 淘宝闪购）
-- 与 ODS 导出门店名、platform_store_id 一致；执行多次安全（ON CONFLICT 更新店名）。
-- =============================================================================

INSERT INTO dim_store_o2o (
  platform_store_id, store_name, platform, license_no, store_short_name,
  platform_store_short, store_nature, business_status,
  company_name, company_short, legal_person
) VALUES
-- 华信店（善培臣）
(
  '27152216', '善培臣医疗器械（华信店）', '美团买药',
  '91370112MAEBJ00Y43', '华信店', '美团买药华信店', '直营', '在营',
  '济南善培臣医疗器械有限公司', '济南善培臣', NULL
),
(
  '1301456589', '善培臣医疗器械(华信店)', '淘宝闪购',
  '91370112MAEBJ00Y43', '华信店', '淘宝闪购华信店', '直营', '在营',
  '济南善培臣医疗器械有限公司', '济南善培臣', NULL
),
-- 海口店（加盟）
(
  '29959330', '小猴快跑医疗器械（海口店）', '美团买药',
  '91460000MAELQXLU2K', '海口店', '美团买药海口店', '加盟', '在营',
  '海口捷旺通医疗器械有限公司', '海口捷旺通', NULL
),
(
  '1312130675', '小猴快跑医疗器械(海口店)', '淘宝闪购',
  '91460000MAELQXLU2K', '海口店', '淘宝闪购海口店', '加盟', '在营',
  '海口捷旺通医疗器械有限公司', '海口捷旺通', NULL
),
-- 南安店（加盟）
(
  '29971866', '小猴快跑医疗器械（南安店）', '美团买药',
  '91350583MAER1G2768', '南安店', '美团买药南安店', '加盟', '在营',
  '华瑞（南安市）医疗器械有限公司', '南安华瑞', NULL
),
(
  '1311896420', '小猴快跑医疗器械(南安店)', '淘宝闪购',
  '91350583MAER1G2768', '南安店', '淘宝闪购南安店', '加盟', '在营',
  '华瑞（南安市）医疗器械有限公司', '南安华瑞', NULL
)
ON CONFLICT (platform_store_id) DO UPDATE SET
  store_name           = EXCLUDED.store_name,
  store_short_name     = EXCLUDED.store_short_name,
  platform_store_short = EXCLUDED.platform_store_short,
  license_no           = EXCLUDED.license_no,
  store_nature         = EXCLUDED.store_nature,
  business_status      = EXCLUDED.business_status,
  company_name         = EXCLUDED.company_name,
  company_short        = EXCLUDED.company_short,
  legal_person         = EXCLUDED.legal_person,
  updated_at           = NOW();
