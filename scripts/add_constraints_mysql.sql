-- 增量同步约束脚本（MySQL 版）
-- 为各数据表添加唯一约束，确保 (code, date/datetime) 组合唯一
-- 仅老用户需要：v0.2.0 起表由 SQLAlchemy 模型自动携带约束，新建库无需执行本脚本
-- 执行前请先备份数据库
--
-- 注意：无约束期间 INSERT IGNORE 不报错，重复数据可能已大量累积，
-- 第一步的去重删除可能影响大量行，务必先备份

-- ============================================
-- 第一步：清理重复数据（如有）
-- ============================================

DELETE a FROM daily_data a
JOIN daily_data b ON a.code = b.code AND a.date = b.date AND a.id < b.id;

DELETE a FROM minute5_data a
JOIN minute5_data b ON a.code = b.code AND a.datetime = b.datetime AND a.id < b.id;

DELETE a FROM minute15_data a
JOIN minute15_data b ON a.code = b.code AND a.datetime = b.datetime AND a.id < b.id;

DELETE a FROM minute30_data a
JOIN minute30_data b ON a.code = b.code AND a.datetime = b.datetime AND a.id < b.id;

DELETE a FROM minute60_data a
JOIN minute60_data b ON a.code = b.code AND a.datetime = b.datetime AND a.id < b.id;

DELETE a FROM block_stock_relation a
JOIN block_stock_relation b ON a.block_code = b.block_code AND a.code = b.code AND a.id < b.id;

-- ============================================
-- 第二步：添加唯一约束（命名与 SQLAlchemy 模型一致）
-- ============================================

ALTER TABLE daily_data
ADD CONSTRAINT uq_daily_code_date UNIQUE (code, date);

ALTER TABLE minute5_data
ADD CONSTRAINT uq_minute5_code_datetime UNIQUE (code, datetime);

ALTER TABLE minute15_data
ADD CONSTRAINT uq_minute15_code_datetime UNIQUE (code, datetime);

ALTER TABLE minute30_data
ADD CONSTRAINT uq_minute30_code_datetime UNIQUE (code, datetime);

ALTER TABLE minute60_data
ADD CONSTRAINT uq_minute60_code_datetime UNIQUE (code, datetime);

ALTER TABLE block_stock_relation
ADD CONSTRAINT uq_block_code UNIQUE (block_code, code);

-- stock_info 表：code 已有唯一索引，无需额外操作

-- ============================================
-- 验证约束（可选）
-- ============================================

-- SELECT TABLE_NAME, CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS
-- WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME LIKE 'uq_%';
