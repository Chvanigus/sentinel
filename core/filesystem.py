import os
import shutil

import paramiko

from core.logging import get_logger
from core.utils import check_create_folder


class LocalFS:
    """Локальные операции с файлами."""

    def __init__(self):
        self.logger = get_logger()

    def copy(self, src: str, dst_dir: str) -> str:
        check_create_folder(dst_dir)
        dst = shutil.copy(src, dst_dir)
        self.logger.info(f"Локальное копирование: {src} → {dst}")
        return dst

    def delete(self, path: str):
        if os.path.exists(path):
            os.remove(path)
            self.logger.info(f"Локальный файл удалён: {path}")


class RemoteFS:
    """SFTP-операции с удалённым сервером."""

    def __init__(self):
        self.logger = get_logger()

    def copy(self, src: str, host: str, port: int, user: str, pwd: str,
             dst: str):
        self.logger.info(f"SFTP: {src} → {user}@{host}:{dst}")
        transport = paramiko.Transport((host, port))
        try:
            transport.connect(username=user, password=pwd)
            sftp = paramiko.SFTPClient.from_transport(transport)
            check_create_folder(os.path.dirname(dst))
            sftp.put(src, dst)
        finally:
            transport.close()

    def delete(self, host: str, port: int, user: str, pwd: str, path: str):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(host, port, user, pwd)
            client.exec_command(f"rm -f {path}")
            self.logger.info(f"Удалён удалённый файл: {path}")
        finally:
            client.close()
