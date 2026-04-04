-- 迁移脚本：为 daily_data 表新增前复权因子列
-- 执行一次即可，若列已存在则不报错（IF NOT EXISTS）
--
-- 使用方式：
--   psql -d <dbname> -f scripts/migrate_add_adj_factor.sql
--   mysql -u <user> -p <dbname> < scripts/migrate_add_adj_factor.sql
--
-- 说明：
--   adj_factor = 1.0 表示该行是原始价格（无复权或当天最新价）
--   adj_factor < 1.0 表示历史数据已向前调整（价格已乘以该因子）

-- PostgreSQL
ALTER TABLE daily_data ADD COLUMN IF NOT EXISTS adj_factor FLOAT DEFAULT 1.0;

-- MySQL（不支持 IF NOT EXISTS，如报列已存在错误可忽略）
-- ALTER TABLE daily_data ADD COLUMN adj_factor FLOAT DEFAULT 1.0;
