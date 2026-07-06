---
name: tdx2db-query
description: 查询 tdx2db 同步的 A 股行情数据库（daily_data / minute*_data）。取日线分钟线、均线截面筛选、检查数据新鲜度/完整性、排查同步问题时使用。
---

# tdx2db 数据库查询手册

数据库由 `tdx2db sync` 写入（PostgreSQL / MySQL / SQLite，连接配置见项目 `.env`）。查询前先读 `AGENTS.md` 的 schema 与陷阱清单；本 skill 提供可直接套用的操作模板。

## 第一步永远是确认数据状态

```bash
tdx2db status --json
```

关注三点：`daily_data.latest` 是否为预期交易日；`rows`/`codes` 是否符合常识（全市场约 4000+ 只）；`warnings` 是否为空。任何分析在数据不新鲜/不完整时都是白做。

## SQL 模板

**单票日线（含缠论均线族）**
```sql
SELECT date, open, high, low, close, volume, amount,
       ma5, ma13, ma21, ma34, ma55, ma89, ma144, ma233
FROM daily_data
WHERE code = :code6      -- 6 位纯数字，如 '600036'，不带 sh/sz 前缀！
  AND date BETWEEN :start AND :end
ORDER BY date;
```

**某日全市场截面（成熟股票）**
```sql
SELECT code, close, ma5, ma13, ma21, ma34, ma55, ma89, ma144, ma233
FROM daily_data
WHERE date = :d AND ma233 IS NOT NULL;   -- ma233 非空 ≈ 上市满一年，天然剔除次新股
```

**最新可用交易日**
```sql
SELECT MAX(date) FROM daily_data WHERE ma5 IS NOT NULL;
```

**分钟线**（表按周期选：minute5_data / minute15_data / minute30_data / minute60_data）
```sql
SELECT datetime, open, high, low, close, volume, amount
FROM minute30_data
WHERE code = :code6 AND datetime BETWEEN :start AND :end
ORDER BY datetime;
```

**周线/月线**：无独立表，取日线后 pandas `resample('W')`/`resample('ME')` 聚合（open=first, high=max, low=min, close=last, volume/amount=sum）。

**覆盖度自检**（跑批前防"同步不完整静默算子集"）
```sql
SELECT
  (SELECT COUNT(*) FROM daily_data WHERE date = (SELECT MAX(date) FROM daily_data)) AS today_n,
  (SELECT COUNT(*) FROM daily_data
   WHERE date = (SELECT MAX(date) FROM daily_data
                 WHERE date < (SELECT MAX(date) FROM daily_data))) AS prev_n;
-- |today_n - prev_n| / prev_n > 5% ⇒ 疑似同步不完整，先重跑 tdx2db sync 再分析
```

## 硬规则

1. **code 一律 6 位纯数字**。`sh600036` / `sz000001` 是 `stock_info` 和文件名的格式，行情表里没有。
2. **停牌日显式处理**：`WHERE date = :d` 查不到就是停牌，需要回退到前一交易日时显式写，禁止用 `<= :d ORDER BY date DESC LIMIT 1` 静默错配。
3. **价格不复权**。除权除息造成的跳空是数据特征不是 bug。
4. **股票名称不在库里**：`stock_info.name` 是占位符。需要名称/板块时用通达信导出 CSV（code 先 `zfill(6)`），或直接用 code。
5. **统计结果踩到极端值（0%/100%/历史新低）先复核口径再下结论**。

## 排查同步问题

| 症状 | 检查 | 处理 |
|------|------|------|
| 某票查无数据 | 收录范围：仅深 000/001/002/300/301、沪 60/688 | 北交所/ETF/指数不收录，属预期 |
| 分钟表行数远少于日线 | `status` 的 warnings | `tdx2db minutes --db-only --incremental` 自愈 |
| 当日行数骤降 | 覆盖度自检 SQL | 通达信补做盘后下载 → 重跑 sync |
| 增量后仍缺历史 | 该票是否新入库（新股全历史应自动补） | 用 `tdx2db daily --code XXX --db-only` 单票重同步
