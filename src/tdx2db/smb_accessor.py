"""SMB 网络文件访问封装。

依赖：smbprotocol（pip install smbprotocol）
"""
import os
import tempfile
from typing import List, Optional

import smbclient
import smbclient.path

from .logger import logger


class SmbAccessor:
    """封装对 SMB 共享目录的只读访问。

    UNC 路径格式：\\\\host\\share\\tdx_path\\vipdoc\\...
    """

    def __init__(
        self,
        host: str,
        share: str,
        tdx_path: str = '',
        username: Optional[str] = None,
        password: Optional[str] = None,
        port: int = 445,
    ) -> None:
        self.host = host
        self.share = share.strip('\\/')
        self._tdx_rel = tdx_path.strip('\\/')
        self._username = username
        self._password = password
        self._port = port
        self._registered = False

    # ── 上下文管理器 ──────────────────────────────────────────────────────────

    def __enter__(self) -> 'SmbAccessor':
        self._register()
        return self

    def __exit__(self, *_) -> None:
        self._unregister()

    def _register(self) -> None:
        if not self._registered:
            smbclient.register_session(
                self.host,
                username=self._username,
                password=self._password,
                port=self._port,
            )
            self._registered = True
            logger.debug(f"SMB 会话已建立: {self.host}:{self._port}")

    def _unregister(self) -> None:
        if self._registered:
            try:
                smbclient.reset_connection_cache()
            except Exception:
                pass
            self._registered = False

    # ── 路径构建 ──────────────────────────────────────────────────────────────

    def _unc(self, *parts: str) -> str:
        """构建 UNC 路径字符串。

        示例（tdx_path='TDX'）：
            _unc('vipdoc', 'sz', 'lday') → '\\\\host\\share\\TDX\\vipdoc\\sz\\lday'
        示例（tdx_path=''）：
            _unc('vipdoc') → '\\\\host\\share\\vipdoc'
        """
        segments = [self.host, self.share]
        if self._tdx_rel:
            segments.append(self._tdx_rel)
        segments.extend(p.strip('\\/') for p in parts if p)
        return '\\\\' + '\\'.join(segments)

    @property
    def vipdoc_unc(self) -> str:
        return self._unc('vipdoc')

    @property
    def gbbq_unc(self) -> str:
        return self._unc('T0002', 'hq_cache', 'gbbq')

    def lday_dir_unc(self, market: str) -> str:
        return self._unc('vipdoc', market, 'lday')

    def day_file_unc(self, market: str, filename: str) -> str:
        return self._unc('vipdoc', market, 'lday', filename)

    # ── 核心 I/O 操作 ─────────────────────────────────────────────────────────

    def exists(self, unc_path: str) -> bool:
        try:
            return smbclient.path.exists(unc_path)
        except Exception:
            return False

    def list_files(self, unc_dir: str, suffix: str = '') -> List[str]:
        """列出目录下的文件名（不含路径），可按后缀过滤。"""
        try:
            entries = smbclient.listdir(unc_dir)
            if suffix:
                return [e for e in entries if e.endswith(suffix)]
            return entries
        except Exception as e:
            logger.warning(f"SMB 列目录失败 {unc_dir}: {e}")
            return []

    def read_bytes(self, unc_path: str) -> bytes:
        with smbclient.open_file(unc_path, mode='rb') as f:
            return f.read()

    def download_to_tmp(self, unc_path: str, suffix: str = '.day') -> str:
        """将远程文件下载到本地临时文件，返回临时文件路径字符串。

        调用方负责删除临时文件（使用 try/finally）。
        """
        data = self.read_bytes(unc_path)
        tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        try:
            tmp.write(data)
            tmp.flush()
            tmp.close()
            return tmp.name
        except Exception:
            tmp.close()
            os.unlink(tmp.name)
            raise
