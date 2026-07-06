# 贡献指南

感谢你的兴趣。这是个小而专注的工具，贡献规则也尽量简短。

## 不接受的改动（数据契约）

下游有按现有口径消费数据的量化管道，以下契约不会改变，涉及它们的 PR 无法合并（历史讨论见 PR #6）：

- **表结构与字段单位**：现有表的列、volume/amount 单位不变
- **code 格式**：`stock_info.code` 带前缀（`sz000001`），行情表 code 为 6 位纯数字
- **复权口径**：默认不复权，不提供内置复权选项（见 issue #2）

## 欢迎的方向

- 北交所支持（`vipdoc/bj/`）
- SMB / 网络路径读取
- 换手率等新增字段（见 issue #1，good first issue）
- stock_info 真实股票名称读取
- bug 修复、错误信息改进、文档补全

不确定方向是否合适？先开 issue 讨论，避免白写。

## PR 要求

- 一个 PR 只做一件事
- `pytest tests/` 通过，CI 三矩阵（3.9/3.10/3.11）绿
- 新增行为附带测试；bug 修复附带能复现原问题的回归测试
- commit message 说清楚"为什么"，不只是"改了什么"

## 开发环境

```bash
pip install -e '.[all]'     # 可编辑安装，含 postgres/mysql 驱动，并创建 tdx2db 命令
cp .env.example .env        # 跑单元测试不需要真实 TDX 数据和数据库
pytest tests/
```

架构说明见 README"开发与贡献"一节；更详细的模块职责、数据流和历史坑记录在 `CLAUDE.md`（AI 辅助开发时尤其有用）。
