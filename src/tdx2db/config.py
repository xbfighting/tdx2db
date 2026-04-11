import os
from dotenv import load_dotenv

load_dotenv()


class Config:
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
    download_url: str
    smb_enabled: bool
    smb_host: str
    smb_share: str
    smb_user: str
    smb_password: str
    smb_tdx_path: str
    smb_port: int

    def __init__(self) -> None:
        self.tdx_path = os.getenv('TDX_PATH', '')
        self.csv_output_path = os.getenv('CSV_OUTPUT_PATH', 'output')
        self.db_type = os.getenv('DB_TYPE', 'sqlite')
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME', 'tdx_data')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', '')
        self.db_batch_size = int(os.getenv('DB_BATCH_SIZE', '10000'))
        self.use_tqdm = os.getenv('USE_TQDM', 'True').lower() == 'true'
        self.download_url = os.getenv('TDX_DOWNLOAD_URL', '')
        self.smb_enabled = os.getenv('SMB_ENABLED', 'false').lower() == 'true'
        self.smb_host = os.getenv('SMB_HOST', '')
        self.smb_share = os.getenv('SMB_SHARE', '')
        self.smb_user = os.getenv('SMB_USER', '')
        self.smb_password = os.getenv('SMB_PASSWORD', '')
        self.smb_tdx_path = os.getenv('SMB_TDX_PATH', '')
        self.smb_port = int(os.getenv('SMB_PORT', '445'))

    @property
    def database_url(self) -> str:
        if self.db_type == 'postgresql':
            return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        elif self.db_type == 'mysql':
            return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        elif self.db_type == 'sqlite':
            return f"sqlite:///{self.db_name}.db"
        else:
            raise ValueError(f"不支持的数据库类型: {self.db_type}")


config = Config()
