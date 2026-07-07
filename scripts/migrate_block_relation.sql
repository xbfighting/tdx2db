-- issue #39: block_stock_relation 表结构变更
--   新增 block_type 列；唯一约束从 (block_code, code) 改为 (block_type, block_name, code)；
--   移除 name 列（板块文件不含股票名称，与"行情表不存名称"契约一致）。
--
-- 该表在本次变更前从未有 CLI 写入路径（功能未实现），正常情况下为空表，直接重建：
-- PostgreSQL / MySQL / SQLite 通用。执行前请确认表中没有你自行写入的数据。

DROP TABLE IF EXISTS block_stock_relation;

-- 之后运行任意写库命令（如 tdx2db blocks --db-only）会自动按新结构建表。
