import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

import requests
from tqdm import tqdm

from .config import config
from .logger import logger

DEFAULT_DOWNLOAD_URL = 'https://data.tdx.com.cn/vipdoc/hsjday.zip'


def download_zip(url: str, dest_path: Path, chunk_size: int = 1024 * 1024) -> None:
    """流式下载 ZIP 文件，支持 tqdm 进度条。"""
    try:
        response = requests.get(url, stream=True, timeout=(10, 300))
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"下载失败: {e}") from e

    total = int(response.headers.get('Content-Length', 0)) or None
    desc = Path(url).name

    try:
        if config.use_tqdm:
            with tqdm(total=total, unit='B', unit_scale=True, desc=desc) as bar:
                with open(dest_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        f.write(chunk)
                        bar.update(len(chunk))
        else:
            logger.info(f"正在下载 {desc}...")
            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
    except Exception:
        dest_path.unlink(missing_ok=True)
        raise


def extract_zip(zip_path: Path, extract_dir: Path) -> Path:
    """解压 ZIP 文件，返回内部 hsjday/ 子目录路径（作为 vipdoc_path 使用）。"""
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"文件不是合法的 ZIP 格式: {zip_path}")

    with zipfile.ZipFile(zip_path) as zf:
        bad = zf.testzip()
        if bad:
            raise ValueError(f"ZIP 文件损坏，首个问题文件: {bad}")

        # 安全检查：过滤路径穿越
        for member in zf.infolist():
            if member.filename.startswith('/') or '..' in member.filename:
                raise ValueError(f"ZIP 包含不安全路径: {member.filename}")

        logger.info("正在解压数据包...")
        # ZIP 内路径可能使用 Windows 反斜杠（如 sh\lday\sh000001.day）
        # Python zipfile 在 macOS/Linux 上不会自动转换，需手动处理
        for member in zf.infolist():
            normalized = member.filename.replace('\\', '/')
            dest = extract_dir / Path(normalized)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not normalized.endswith('/'):
                with zf.open(member) as src, open(dest, 'wb') as dst:
                    dst.write(src.read())

    # 兼容两种结构：
    # 1. {sh,sz,bj}/lday/*.day  （根目录直接是市场目录，实际情况）
    # 2. hsjday/{sh,sz,bj}/lday/*.day  （有顶层目录）
    if (extract_dir / 'sh').exists() or (extract_dir / 'sz').exists() or (extract_dir / 'bj').exists():
        return extract_dir
    vipdoc_path = extract_dir / 'hsjday'
    if vipdoc_path.exists():
        return vipdoc_path
    raise FileNotFoundError("解压后未找到预期的市场目录（sh/sz/bj），请检查 ZIP 包结构")


@contextmanager
def download_and_extract(
    url: Optional[str] = None,
    keep_tmp: bool = False,
) -> Generator[Path, None, None]:
    """
    下载并解压 TDX 日线数据包，yield vipdoc_path（即 hsjday/ 目录）。

    退出时若 keep_tmp=False 自动删除临时目录。

    用法：
        with download_and_extract() as vipdoc_path:
            reader = TdxDataReader(vipdoc_path=str(vipdoc_path))
    """
    target_url = url or config.download_url or DEFAULT_DOWNLOAD_URL
    tmp_dir = Path(tempfile.mkdtemp(prefix='tdx_hsjday_'))
    zip_path = tmp_dir / 'hsjday.zip'

    try:
        logger.info(f"开始下载: {target_url}")
        download_zip(target_url, zip_path)
        logger.info("下载完成，开始解压...")
        vipdoc_path = extract_zip(zip_path, tmp_dir)
        zip_path.unlink(missing_ok=True)  # 解压后释放 ZIP 占用的磁盘空间
        logger.info(f"解压完成: {vipdoc_path}")
        yield vipdoc_path
    finally:
        if keep_tmp:
            logger.info(f"临时目录已保留: {tmp_dir}")
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)
