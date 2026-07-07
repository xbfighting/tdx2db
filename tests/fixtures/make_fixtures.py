"""生成 tests/fixtures/ 下的二进制解析 fixture（issue #34）。

fixture 是确定性合成字节，格式与通达信实际文件逐字段一致：
- .day  32 字节/条：<IIIIIfII date, open, high, low, close(价×100 整型), amount(f32), volume, 保留
- .lc5  32 字节/条：<HHfffffII date((年-2004)*2048+月*100+日), time(0点起分钟数),
        open/high/low/close/amount(f32), volume(u32), 保留

价格/成交额选用二进制可精确表示的值（×.25/.5/.75），断言可用 ==。
重新生成：python tests/fixtures/make_fixtures.py
真实文件切片（数据盘可用时）：dd if=<vipdoc>/sh/lday/sh688001.day of=sh688001.day bs=32 count=5
"""
import struct
from pathlib import Path

HERE = Path(__file__).parent

# 与 tests/test_reader.py 的断言一一对应，改动需同步
DAY_RECORDS_SZ = [
    # (date, open, high, low, close, amount, volume)——open 等为 ×100 整型
    (20260605, 1050, 1075, 1025, 1050, 5250000.0, 500000),
    (20260608, 1050, 1100, 1050, 1075, 5375000.0, 400000),
]
DAY_RECORDS_688 = [
    (20260605, 5000, 5150, 4950, 5075, 25375000.0, 300000),
]
LC5_RECORDS = [
    # (year, month, day, hour, minute, open, high, low, close, amount, volume)
    (2026, 6, 5, 9, 35, 10.5, 10.75, 10.25, 10.5, 2100000.0, 200000),
    (2026, 6, 5, 9, 40, 10.5, 10.5, 10.25, 10.25, 1025000.0, 100000),
]


def write_day(path: Path, records) -> None:
    with open(path, 'wb') as f:
        for date, o, h, l, c, amount, vol in records:
            f.write(struct.pack('<IIIIIfII', date, o, h, l, c, amount, vol, 0))


def write_lc5(path: Path, records) -> None:
    with open(path, 'wb') as f:
        for year, month, day, hour, minute, o, h, l, c, amount, vol in records:
            date_num = (year - 2004) * 2048 + month * 100 + day
            time_num = hour * 60 + minute
            f.write(struct.pack('<HHfffffII', date_num, time_num, o, h, l, c, amount, vol, 0))


if __name__ == '__main__':
    write_day(HERE / 'sz000001.day', DAY_RECORDS_SZ)
    write_day(HERE / 'sh688001.day', DAY_RECORDS_688)
    write_lc5(HERE / 'sz000001.lc5', LC5_RECORDS)
    print(f'fixtures written to {HERE}')
