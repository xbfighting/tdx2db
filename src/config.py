"""配置管理模块

负责加载和管理程序的配置参数，包括：
- 通达信数据路径
- 数据库连接信息
- 输出CSV路径
- 其他配置选项
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# 加载.env文件中的环境变量
load_dotenv()


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
        """获取数据库连接URL"""
        if self.db_type == 'postgresql':
            return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        elif self.db_type == 'mysql':
            return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        elif self.db_type == 'sqlite':
            return f"sqlite:///{self.db_name}.db"
        else:
            raise ValueError(f"不支持的数据库类型: {self.db_type}")

# 创建全局配置实例
config = Config()
