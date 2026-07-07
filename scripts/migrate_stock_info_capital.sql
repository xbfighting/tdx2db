-- issue #45: stock_info 新增股本/日期列（换手率支持 + 上市日期）
-- PostgreSQL / MySQL / SQLite 通用；新库无需执行（create_all 自动带列）。
-- 执行后重跑 `tdx2db stock-list --db-only` 填充数据。

ALTER TABLE stock_info ADD COLUMN zgb FLOAT;           -- 总股本（万股）
ALTER TABLE stock_info ADD COLUMN ltag FLOAT;          -- 流通A股（万股）
ALTER TABLE stock_info ADD COLUMN capital_date DATE;   -- 股本数据更新日
ALTER TABLE stock_info ADD COLUMN list_date DATE;      -- 上市日期

-- 换手率(%) = daily_data.volume / stock_info.ltag
--   （volume 单位为手、ltag 单位为万股，比值恰为百分数）
-- 注意：股本为当前快照，股本变动点之前的历史换手率会失真。
