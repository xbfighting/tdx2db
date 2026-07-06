"""配置管理模块

负责加载和管理程序的配置参数，包括：
- 通达信数据路径
- 数据库连接信息
- 输出CSV路径
- 其他配置选项
"""

import os
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

# 加载.env文件中的环境变量
load_dotenv()

# Windows 常见通达信安装位置（盘符 × 目录名，探测顺序即优先级）
_TDX_DRIVES = ('C:/', 'D:/', 'E:/')
_TDX_DIRNAMES = ('new_tdx', 'zd_zsone', 'tdx', 'new_jyplug')


def default_tdx_candidates() -> List[str]:
    """Windows 下的默认探测候选路径列表"""
    return [drive + name for drive in _TDX_DRIVES for name in _TDX_DIRNAMES]


def detect_tdx_path(candidates: Optional[List[str]] = None) -> Optional[str]:
    """探测通达信安装目录：目录下存在 vipdoc 子目录即视为有效，返回首个命中。

    仅应在 TDX_PATH 未显式配置时调用。candidates 为 None 时使用 Windows
    默认候选；非 Windows 平台不做盘符探测（挂载路径因人而异），返回 None。
    """
    if candidates is None:
        if os.name != 'nt':
            return None
        candidates = default_tdx_candidates()
    for cand in candidates:
        if (Path(cand) / 'vipdoc').is_dir():
            return str(cand)
    return None


class Config:
    """配置类"""

    tdx_path: str
    csv_output_path: str
    db_type: str
    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str
    db_batch_size: int
    use_tqdm: bool

    def __init__(self) -> None:
        """初始化配置"""
        # 通达信安装路径
        self.tdx_path = os.getenv('TDX_PATH', '')

        # CSV输出路径
        self.csv_output_path = os.getenv('CSV_OUTPUT_PATH', 'output')

        # 数据库配置
        self.db_type = os.getenv('DB_TYPE', 'postgresql')
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME', 'tdx_data')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', '')
        self.db_batch_size = int(os.getenv('DB_BATCH_SIZE', '10000'))

        # 是否使用进度条
        self.use_tqdm = os.getenv('USE_TQDM', 'True').lower() == 'true'

    @property
    def database_url(self):
        """获取数据库连接URL

        用 URL.create 而非 f-string 拼接：密码含 @ : / 等字符时不会解析错乱，
        且 URL 对象在日志/异常中自动掩码密码。
        """
        from sqlalchemy import URL

        if self.db_type == 'postgresql':
            drivername = 'postgresql'
        elif self.db_type == 'mysql':
            drivername = 'mysql+pymysql'
        elif self.db_type == 'sqlite':
            return URL.create('sqlite', database=f"{self.db_name}.db")
        else:
            raise ValueError(f"不支持的数据库类型: {self.db_type}")

        try:
            port = int(self.db_port)
        except (TypeError, ValueError):
            raise ValueError(f"DB_PORT 配置无效（应为数字）: {self.db_port!r}，请检查 .env")

        return URL.create(
            drivername,
            username=self.db_user,
            password=self.db_password or None,
            host=self.db_host,
            port=port,
            database=self.db_name,
        )

# 创建全局配置实例
config = Config()
