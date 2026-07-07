"""板块数据解析模块（issue #39）

四条数据链，均为通达信本地文件（盘后数据下载时自动更新）：

- 概念/指数/风格: T0002/hq_cache/infoharbor_block.dat（文本 GBK）
- 行业:          tdxhy.cfg（个股 X 码）× tdxzs3.cfg 类别 12（881 研究行业，多级前缀匹配）
- 指数补充/特殊:  spblock.dat（中证500/1000 等跨市场指数成分仅此处有）
- 地区:          base.dbf 的 DY 字段 × tdxzs.cfg 类别 3

历史注记：旧版通达信的二进制 block_zs/gn/fg.dat 在新版已不存在，
pytdx BlockReader 不适用；以上文件均自行解析，无新增依赖。
格式均经真实文件与导出 CSV 全量比对验证（2026-07-07，issue #39）。
"""
import struct
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

from .logger import logger

# infoharbor section 前缀 → block_type
_IH_TYPE = {'GN': '概念', 'ZS': '指数', 'FG': '风格'}


def parse_infoharbor(path: Path) -> List[dict]:
    """解析 infoharbor_block.dat

    格式：``#GN_板块名,成员数,880码,创建日,更新日,,`` + 成员行 ``市场#6位code,...``
    板块名按 GBK 8 字节截断（如"人形机器人"→"人形机器"），
    调用方应用 880 码从 tdxzs/tdxzs3 还原全名。

    Returns:
        list[dict]: {'type', 'name', 'block_code'(可为 None), 'codes': set}
    """
    sections: List[dict] = []
    cur: Optional[dict] = None
    for line in path.read_text(encoding='gbk', errors='replace').splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith('#'):
            head = line[1:].split(',')
            if '_' not in head[0]:
                cur = None
                continue
            typ, name = head[0].split('_', 1)
            cur = {
                'type': _IH_TYPE.get(typ, typ),
                'name': name,
                'block_code': head[2] if len(head) > 2 and head[2] else None,
                'codes': set(),
            }
            sections.append(cur)
        elif cur is not None:
            for member in line.split(','):
                if '#' in member:
                    cur['codes'].add(member.split('#', 1)[1].strip())
    return sections


def parse_zs_cfg(path: Path, category: str) -> Dict[str, Tuple[str, str]]:
    """解析 tdxzs.cfg / tdxzs3.cfg，按类别过滤

    行格式：``板块名|880码|类别|级别|_|key``。
    类别实测分布：2=TDX行业(T码key) 3=地区(数字key) 4=概念 5=风格 12=研究行业(X码key)

    Returns:
        dict: key -> (板块名, 板块代码)
    """
    out: Dict[str, Tuple[str, str]] = {}
    for line in path.read_text(encoding='gbk', errors='replace').splitlines():
        p = line.strip().split('|')
        if len(p) >= 6 and p[2] == category and p[5]:
            out[p[5]] = (p[0], p[1])
    return out


def parse_block_code_names(path: Path) -> Dict[str, str]:
    """tdxzs/tdxzs3: 板块代码 -> 全名（用于还原 infoharbor 的 8 字节截断名）"""
    out: Dict[str, str] = {}
    for line in path.read_text(encoding='gbk', errors='replace').splitlines():
        p = line.strip().split('|')
        if len(p) >= 2 and p[1]:
            out[p[1]] = p[0]
    return out


def parse_tdxhy(path: Path) -> Dict[str, str]:
    """解析 tdxhy.cfg：6位code -> X行业码

    行格式：``市场|code|T码|||X码``
    """
    out: Dict[str, str] = {}
    for line in path.read_text(encoding='gbk', errors='replace').splitlines():
        p = line.strip().split('|')
        if len(p) >= 6 and p[1] and p[5]:
            out[p[1]] = p[5]
    return out


def parse_spblock(path: Path) -> Dict[str, Set[str]]:
    """解析 spblock.dat：``#板块名`` + 每行一个 ``市场前缀+6位code``"""
    boards: Dict[str, Set[str]] = {}
    cur: Optional[str] = None
    for line in path.read_text(encoding='gbk', errors='replace').splitlines():
        line = line.strip()
        if line.startswith('#'):
            cur = line[1:]
            boards[cur] = set()
        elif line and cur is not None and len(line) == 7:
            boards[cur].add(line[1:])
    return boards


def parse_base_dbf_dy(path: Path) -> Dict[str, str]:
    """解析 base.dbf，仅取 GPDM(股票代码)/DY(地域码) 两列

    标准 DBF：头 32 字节（记录数@4、头长@8、记录长@10），
    之后每 32 字节一个字段描述（名 11B + 类型 1B + 4B + 长度 1B），0x0D 结束。
    """
    data = path.read_bytes()
    nrec = struct.unpack_from('<I', data, 4)[0]
    hlen, rlen = struct.unpack_from('<HH', data, 8)

    fields = []
    offset = 1  # 每条记录首字节是删除标记
    p = 32
    while p < len(data) and data[p] != 0x0D:
        name = data[p:p + 11].split(b'\x00')[0].decode('ascii', 'replace')
        flen = data[p + 16]
        fields.append((name, offset, flen))
        offset += flen
        p += 32
    fmap = {n: (o, l) for n, o, l in fields}
    if 'GPDM' not in fmap or 'DY' not in fmap:
        raise ValueError(f"base.dbf 缺少 GPDM/DY 字段，实际字段: {sorted(fmap)[:12]}")

    dm_o, dm_l = fmap['GPDM']
    dy_o, dy_l = fmap['DY']
    out: Dict[str, str] = {}
    for i in range(nrec):
        rec = data[hlen + i * rlen: hlen + (i + 1) * rlen]
        if len(rec) < rlen or rec[0:1] == b'*':  # 删除标记
            continue
        code = rec[dm_o:dm_o + dm_l].decode('gbk', 'replace').strip()
        dy = rec[dy_o:dy_o + dy_l].decode('gbk', 'replace').strip()
        if code:
            out[code] = dy
    return out


def collect_block_relations(tdx_path) -> pd.DataFrame:
    """汇总四条数据链为板块-个股关系 DataFrame

    单链文件缺失时 WARNING 跳过该类，不阻塞其他链。

    Returns:
        DataFrame: 列 block_type / block_code / block_name / code
    """
    hq = Path(tdx_path) / 'T0002' / 'hq_cache'
    rows: List[Tuple[str, Optional[str], str, str]] = []

    # 板块代码 -> 全名（还原 infoharbor 截断名）
    fullname: Dict[str, str] = {}
    for fname in ('tdxzs.cfg', 'tdxzs3.cfg'):
        f = hq / fname
        if f.exists():
            fullname.update(parse_block_code_names(f))

    # --- 概念/指数/风格（infoharbor）+ 指数补充（spblock） ---
    ih_file = hq / 'infoharbor_block.dat'
    sp_file = hq / 'spblock.dat'
    sections = parse_infoharbor(ih_file) if ih_file.exists() else []
    sp_boards = parse_spblock(sp_file) if sp_file.exists() else {}
    if not ih_file.exists():
        logger.warning(f"缺少 {ih_file}，跳过概念/指数/风格板块")
    if not sp_file.exists():
        logger.warning(f"缺少 {sp_file}，中证500 等跨市场指数成分将缺失")

    ih_names = set()
    for s in sections:
        ih_names.add(s['name'])
        # infoharbor 空板块（如中证500）优先用 spblock 同名成分补齐
        codes = s['codes'] or sp_boards.get(s['name'], set())
        if not codes:
            continue
        name = fullname.get(s['block_code'], s['name']) if s['block_code'] else s['name']
        rows.extend((s['type'], s['block_code'], name, c) for c in codes)

    # spblock 中未被 infoharbor 收录的板块（融资融券等）归为"特殊"
    for name, codes in sp_boards.items():
        if name not in ih_names:
            rows.extend(('特殊', None, name, c) for c in codes)

    # --- 行业（tdxhy X 码 × tdxzs3 类别 12，多级前缀匹配各入一行） ---
    hy_file = hq / 'tdxhy.cfg'
    zs3_file = hq / 'tdxzs3.cfg'
    if hy_file.exists() and zs3_file.exists():
        key2board = parse_zs_cfg(zs3_file, '12')
        keys = sorted(key2board, key=len, reverse=True)
        for code, x in parse_tdxhy(hy_file).items():
            for k in keys:
                if x.startswith(k):
                    name, bcode = key2board[k]
                    rows.append(('行业', bcode, name, code))
    else:
        logger.warning(f"缺少 {hy_file.name}/{zs3_file.name}，跳过行业板块")

    # --- 地区（base.dbf DY × tdxzs 类别 3） ---
    dbf_file = hq / 'base.dbf'
    zs_file = hq / 'tdxzs.cfg'
    if dbf_file.exists() and zs_file.exists():
        dy2board = parse_zs_cfg(zs_file, '3')
        for code, dy in parse_base_dbf_dy(dbf_file).items():
            if dy in dy2board:
                name, bcode = dy2board[dy]
                rows.append(('地区', bcode, name, code))
    else:
        logger.warning(f"缺少 {dbf_file.name}/{zs_file.name}，跳过地区板块")

    df = pd.DataFrame(rows, columns=['block_type', 'block_code', 'block_name', 'code'])
    df = df.drop_duplicates(subset=['block_type', 'block_name', 'code'])
    if not df.empty:
        counts = df.groupby('block_type')['block_name'].nunique().to_dict()
        logger.info(f"板块解析完成: {len(df)} 条关系，板块数 {counts}")
    return df
