"""Архивирование снимков: перемещение и обновление GeoServer."""
import os
from datetime import datetime

from tqdm import tqdm

from core import settings
from satgeo.client import GeoServerClient


class ArchiveMover:
    """
    Класс архивирования снимков: перемещение и обновление хранилища.
    Перемещает спутниковые снимки из активного хранилища в архив и
    обновляет пути на Geoserver.
    """

    def __init__(self, start_date, end_date, year=None):
        self.client = GeoServerClient(
            service_url=f"http://{settings.RMHOST}:{settings.TSPORT}/geoserver/rest/",
            username=settings.TSUSER,
            password=settings.TSPASSWORD
        )
        self.fs = FileSystemManager()
        self.start = start_date
        self.end = end_date
        self.year = year or datetime.today().year
        self.ws = "sentinel"
        self.old_dir = f"/opt/geoware/SENTINEL{self.year}"
        self.new_dir = f"/mnt/map/geoware/SENTINEL{self.year}"

    def move_images(self):
        """
        Перемещение снимков из активного хранилища - в архив.
        """
        stores = self.client.get_stores(self.ws)
        for store in tqdm(stores, desc="Архивирование снимков"):
            name = store.name
            if extract_year(name) == str(self.year) and in_range(name,
                                                                 self.start,
                                                                 self.end):
                old_path = os.path.join(self.old_dir, f"{name}.tif")
                new_path = os.path.join(self.new_dir, f"{name}.tif")
                self.client.update_store_path(name, new_path)
                self.fs.delete_remote(old_path)

    def execute(self):
        """Вызов метода перемещения."""
        self.move_images()


def execute_mover(startdate, enddate, year):
    """Вызов класс перемещения снимков в архив."""
    mover = ArchiveMover(startdate, enddate, year)
    mover.execute()
