-- 增量同步约束脚本
-- 为各数据表添加唯一约束，确保 (code, datetime) 组合唯一
-- 执行前请先备份数据库

-- ============================================
-- 第一步：清理重复数据（如有）
-- ============================================

-- 清理 daily_data 重复记录，保留 id 最大的（日线使用 date 字段）
DELETE FROM daily_data a USING daily_data b
WHERE a.id < b.id AND a.code = b.code AND a.date = b.date;

-- 清理 minute5_data 重复记录
DELETE FROM minute5_data a USING minute5_data b
WHERE a.id < b.id AND a.code = b.code AND a.datetime = b.datetime;

-- 清理 minute15_data 重复记录
DELETE FROM minute15_data a USING minute15_data b
WHERE a.id < b.id AND a.code = b.code AND a.datetime = b.datetime;

-- 清理 minute30_data 重复记录
DELETE FROM minute30_data a USING minute30_data b
WHERE a.id < b.id AND a.code = b.code AND a.datetime = b.datetime;

-- 清理 minute60_data 重复记录
DELETE FROM minute60_data a USING minute60_data b
WHERE a.id < b.id AND a.code = b.code AND a.datetime = b.datetime;

-- 清理 block_stock_relation 重复记录
DELETE FROM block_stock_relation a USING block_stock_relation b
WHERE a.id < b.id AND a.block_code = b.block_code AND a.code = b.code;

-- ============================================
-- 第二步：添加唯一约束
-- ============================================

-- daily_data 表：(code, date) 唯一（日线使用 date 字段）
ALTER TABLE daily_data
ADD CONSTRAINT uq_daily_code_date UNIQUE (code, date);

-- minute5_data 表
ALTER TABLE minute5_data
ADD CONSTRAINT uq_minute5_code_datetime UNIQUE (code, datetime);

-- minute15_data 表
ALTER TABLE minute15_data
ADD CONSTRAINT uq_minute15_code_datetime UNIQUE (code, datetime);

-- minute30_data 表
ALTER TABLE minute30_data
ADD CONSTRAINT uq_minute30_code_datetime UNIQUE (code, datetime);

-- minute60_data 表
ALTER TABLE minute60_data
ADD CONSTRAINT uq_minute60_code_datetime UNIQUE (code, datetime);

-- block_stock_relation 表：(block_code, code) 唯一
ALTER TABLE block_stock_relation
ADD CONSTRAINT uq_block_code UNIQUE (block_code, code);

-- stock_info 表：code 已有唯一索引，无需额外操作

-- ============================================
-- 验证约束（可选）
-- ============================================

-- 查看所有约束
-- SELECT conname, conrelid::regclass, pg_get_constraintdef(oid)
-- FROM pg_constraint
-- WHERE conname LIKE 'uq_%';
