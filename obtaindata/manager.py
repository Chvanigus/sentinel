from .auth import AuthManager
from .catalog import CatalogClient


# from .download import SafeDownloader

class Sentinel2Manager:
    """Менеджер для поиска и скачивания снимков SENTINEL-2."""

    def __init__(self, out_dir):
        self.auth = AuthManager()
        self.catalog = CatalogClient(self.auth)
        # self.downloader = SafeDownloader(self.auth, out_dir)

    def find(self, tiles, start=None, end=None, maxcc=100, limit=None):
        prods = self.catalog.search_s2(
            tiles, start, end, limit=limit
        )
        return prods

    # def download(self, prod_list):
    #     return [self.downloader.download(p) for p in prod_list]
