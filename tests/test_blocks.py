"""板块解析与入库测试（issue #39）

合成 fixture 覆盖四条数据链：infoharbor（概念/指数/风格，含 8 字节截断名、
空板块、spblock 补齐）、tdxhy×tdxzs3（行业多级）、base.dbf×tdxzs（地区）。
格式样本与真实文件逐字段一致（2026-07-07 实测验证）。
"""
import struct

import pandas as pd
import pytest
from sqlalchemy import text

from tdx2db.blocks import (
    collect_block_relations,
    parse_base_dbf_dy,
    parse_infoharbor,
    parse_spblock,
    parse_tdxhy,
    parse_zs_cfg,
)
from tdx2db.config import config
from tdx2db.storage import DataStorage


def _write_gbk(path, content):
    path.write_bytes(content.encode('gbk'))


DEFAULT_DBF_FIELDS = [('SC', 2), ('GPDM', 6), ('DY', 4)]


def _make_dbf(path, rows, fields=None):
    """最小 DBF 构造器，与真实 base.dbf 同构

    Args:
        rows: 每行一个 tuple，顺序与 fields 一致
        fields: [(字段名, 长度), ...]，默认 SC/GPDM/DY
    """
    fields = fields or DEFAULT_DBF_FIELDS
    hlen = 32 + 32 * len(fields) + 1
    rlen = 1 + sum(length for _, length in fields)
    buf = bytearray()
    buf += struct.pack('<B3BIHH20x', 0x03, 26, 7, 7, len(rows), hlen, rlen)
    for name, flen in fields:
        buf += struct.pack('<11sc4xB15x', name.encode(), b'C', flen)
    buf += b'\x0d'
    for row in rows:
        rec = b' '
        for value, (_, flen) in zip(row, fields):
            rec += str(value).ljust(flen).encode()
        buf += rec
    path.write_bytes(bytes(buf))


@pytest.fixture
def hq_cache(tmp_path):
    """合成完整的 T0002/hq_cache 目录"""
    hq = tmp_path / 'T0002' / 'hq_cache'
    hq.mkdir(parents=True)

    # infoharbor：截断名概念 + 空指数板块（由 spblock 补）+ 空板块（跳过）
    _write_gbk(hq / 'infoharbor_block.dat', (
        '#GN_人形机器,2,880948,20170314,20260702,,\n'
        '0#000016,1#600036\n'
        '#ZS_中证500,0,,20050408,,,\n'
        '#FG_专精特新,1,880735,20210802,20260114,,\n'
        '0#301111\n'
        '#GN_空板块,0,,20200101,,,\n'
    ))

    # spblock：补中证500 + 独立的特殊板块
    _write_gbk(hq / 'spblock.dat', (
        '#中证500\n0000063\n1600000\n'
        '#融资融券\n0000001\n'
    ))

    # tdxzs：地区定义(类别3) + 880948 全名还原
    _write_gbk(hq / 'tdxzs.cfg', (
        '黑龙江|880201|3|1|0|1\n'
        '新疆板块|880202|3|1|0|2\n'
        '人形机器人|880948|4|1|0|GN948\n'
    ))

    # tdxzs3：研究行业(类别12) 三级
    _write_gbk(hq / 'tdxzs3.cfg', (
        '煤炭|881001|12|1|0|X10\n'
        '煤炭开采|881002|12|1|0|X1001\n'
        '动力煤|881003|12|1|1|X100101\n'
    ))

    # tdxhy：000552 → X100101（应命中三级各一行）
    _write_gbk(hq / 'tdxhy.cfg', (
        '0|000552|T010101|||X100101\n'
        '1|600000|T1001|||X9999\n'   # X9999 无匹配 key，不产生行业行
    ))

    # base.dbf：000711→DY=1(黑龙江)，600036→DY=2(新疆)，888888→DY=99(无映射)
    _make_dbf(hq / 'base.dbf', [
        ('0', '000711', '1'),
        ('1', '600036', '2'),
        ('0', '888888', '99'),
    ])
    return tmp_path


class TestParsers:
    def test_infoharbor(self, hq_cache):
        sections = parse_infoharbor(hq_cache / 'T0002' / 'hq_cache' / 'infoharbor_block.dat')
        by_name = {s['name']: s for s in sections}
        assert by_name['人形机器']['type'] == '概念'
        assert by_name['人形机器']['block_code'] == '880948'
        assert by_name['人形机器']['codes'] == {'000016', '600036'}
        assert by_name['中证500']['codes'] == set()          # 空板块
        assert by_name['专精特新']['type'] == '风格'

    def test_spblock(self, hq_cache):
        boards = parse_spblock(hq_cache / 'T0002' / 'hq_cache' / 'spblock.dat')
        assert boards['中证500'] == {'000063', '600000'}     # 去市场前缀
        assert boards['融资融券'] == {'000001'}

    def test_tdxhy_and_zs_cfg(self, hq_cache):
        hq = hq_cache / 'T0002' / 'hq_cache'
        assert parse_tdxhy(hq / 'tdxhy.cfg')['000552'] == 'X100101'
        industry = parse_zs_cfg(hq / 'tdxzs3.cfg', '12')
        assert industry['X1001'] == ('煤炭开采', '881002')
        area = parse_zs_cfg(hq / 'tdxzs.cfg', '3')
        assert area['1'] == ('黑龙江', '880201')

    def test_base_dbf(self, hq_cache):
        dy = parse_base_dbf_dy(hq_cache / 'T0002' / 'hq_cache' / 'base.dbf')
        assert dy == {'000711': '1', '600036': '2', '888888': '99'}


class TestCollect:
    def test_all_chains(self, hq_cache):
        df = collect_block_relations(hq_cache)
        key = df.set_index(['block_type', 'block_name'])

        # 概念：截断名经 880948 还原为全名
        gn = df[(df.block_type == '概念') & (df.block_name == '人形机器人')]
        assert set(gn.code) == {'000016', '600036'}
        assert '人形机器' not in set(df.block_name)

        # 指数：infoharbor 空板块由 spblock 补齐
        zs = df[(df.block_type == '指数') & (df.block_name == '中证500')]
        assert set(zs.code) == {'000063', '600000'}

        # 特殊：spblock 独有板块
        sp = df[(df.block_type == '特殊') & (df.block_name == '融资融券')]
        assert set(sp.code) == {'000001'}

        # 行业：X100101 命中三级各一行，block_level 标注层级
        hy = df[(df.block_type == '行业') & (df.code == '000552')]
        assert set(hy.block_name) == {'煤炭', '煤炭开采', '动力煤'}
        assert set(hy.block_code) == {'881001', '881002', '881003'}
        assert dict(zip(hy.block_name, hy.block_level)) == {'煤炭': 1, '煤炭开采': 2, '动力煤': 3}

        # 非行业类型 block_level 为空
        assert df[df.block_type != '行业'].block_level.isna().all()
        # 无匹配 key 的 X9999 不产生行
        assert df[(df.block_type == '行业') & (df.code == '600000')].empty

        # 地区：DY 映射，未定义 DY 跳过
        area = df[df.block_type == '地区']
        assert set(zip(area.block_name, area.code)) == {('黑龙江', '000711'), ('新疆板块', '600036')}

        # 空板块被跳过
        assert '空板块' not in set(df.block_name)

    def test_missing_files_degrade(self, tmp_path):
        (tmp_path / 'T0002' / 'hq_cache').mkdir(parents=True)
        df = collect_block_relations(tmp_path)
        assert df.empty  # 全缺失 → 空 DataFrame，不抛异常

    def test_unreadable_file_degrades_per_chain(self, hq_cache):
        """文件存在但不可读（#44 同款：SMB 下被运行中的通达信锁定）→
        该链跳过，其余链正常。用目录冒充文件触发 OSError，跨平台可复现"""
        hq = hq_cache / 'T0002' / 'hq_cache'
        (hq / 'infoharbor_block.dat').unlink()
        (hq / 'infoharbor_block.dat').mkdir()   # read_text 目录 → OSError

        df = collect_block_relations(hq_cache)
        assert '概念' not in set(df.block_type)        # 该链降级
        assert {'行业', '地区', '特殊'} <= set(df.block_type)  # 其余链不受影响


class TestSnapshotReplace:
    def test_save_is_full_replace(self, tmp_path, monkeypatch):
        """板块关系是快照语义：重写完全替换，不累积"""
        monkeypatch.setattr(config, 'db_type', 'sqlite')
        storage = DataStorage(db_url='sqlite://', csv_path=str(tmp_path))

        df1 = pd.DataFrame(
            [('概念', '880948', '人形机器人', c) for c in ('000016', '600036', '000063')],
            columns=['block_type', 'block_code', 'block_name', 'code'],
        )
        storage.save_block_relation(df1, to_csv=False)
        df2 = df1.iloc[:2]  # 成分调出一只
        storage.save_block_relation(df2, to_csv=False)

        with storage.engine.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM block_stock_relation")).scalar()
        assert n == 2

    def test_failed_replace_keeps_old_snapshot(self, tmp_path, monkeypatch):
        """替换写入失败必须整体回滚，保留旧快照（不能变成空表）"""
        monkeypatch.setattr(config, 'db_type', 'sqlite')
        storage = DataStorage(db_url='sqlite://', csv_path=str(tmp_path))

        good = pd.DataFrame(
            [('概念', '880948', '人形机器人', '000016')],
            columns=['block_type', 'block_code', 'block_name', 'code'],
        )
        _, ok = storage.save_block_relation(good, to_csv=False)
        assert ok

        bad = good.assign(no_such_column='x')  # 未知列 → INSERT 必然失败
        _, ok = storage.save_block_relation(bad, to_csv=False)
        assert not ok

        with storage.engine.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM block_stock_relation")).scalar()
        assert n == 1  # 旧快照仍在，未被 DELETE 清空