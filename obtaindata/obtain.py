"""Поиск и скачивание спутниковых снимков Sentinel."""

from datetime import datetime

from sentinelsat import SentinelAPI


class GetData:
    """Класс поиска и скачивания снимков."""
    api = SentinelAPI(
        "ilmem542@gmail.com",
        "mypkat-fuqja0-taGqeh",
        "https://apihub.copernicus.eu/apihub"
    )

    def __init__(self, download_dir) -> None:
        self.__download_dir = download_dir

    def search(self,
               ll_lat: float = None,
               ll_lon: float = None,
               ur_lat: float = None,
               ur_lon: float = None,
               start_date: datetime.date = None,
               end_date: datetime.date = None, ) -> None:
        """
        Метод поиска снимков.
        Происходит это через запрос в базу данных о координатах зоны охвата
        геометрии полей в каждом из хозяйств и выгрузки этих данных в скрипт.

        :param ll_lat: Lower left latitude - нижняя левая широта (координата)
                       необходимой зоны поиска снимка.
        :param ll_lon: Lower left longitude - нижняя левая долгота (координата)
                       необходимой зоны поиска снимка.
        :param ur_lat: Upper right latitude - правая верхняя широта
                       (координата) необходимой зоны поиска снимка.
        :param ur_lon: Upper right longitude - правая верхняя долгота
                       (координата) необходимой зоны поиска снимка.
        :param start_date: Начальная дата поиска снимков
        :param end_date: Конечная дата поиска снимков
        :return: Словарь, содержащий в себе информацию о каждом снимке.
        """


def obtain_data():
    data = GetData(download_dir='/home/sysop/sentinel/downloads/')
    data.search()

    # scenes = {}
    # query_kwargs = {'date': (start_date, end_date),
    #                 'platformname': 'Sentinel-2',
    #                 'cloudcoverpercentage': (cloud_min, cloud_max)}
    #
    # footprint = None
    # if ll_lat and ll_lon and ur_lat and ur_lon:
    #     footprint = f'POLYGON (({ur_lon} {ll_lat}, {ur_lon} {ur_lat}, ' \
    #                 f'{ll_lon} {ur_lat}, {ll_lon} {ll_lat}, {ur_lon} {ll_lat}))'
    #
    # pp = self.__api.query(footprint, **query_kwargs)
    # scenes.update(pp)
    #
    # return scenes


#     def download(self, scenes: list) -> None:
#         """Метод скачивания снимков, которые содержатся в переданном
#         списке словарей.
#         :param scenes: Список словарей с данными о каждом снимке
#         """
#         self.__api.download_all(products=scenes,
#                                 directory_path=self.__download_dir,
#                                 max_attempts=2,
#                                 checksum=True)
#
#
# def obtain_data(start_date: datetime.date = db.get_last_date_from_layer(),
#                 end_date: datetime.date = datetime.now() + timedelta(days=1),
#                 year: int = datetime.now().year,
#                 cloud_max: int = settings.CLOUD_MAX,
#                 download: bool = False) -> None:
#     """Основная функция управления скриптом."""
#     gd = GetData(
#         settings.USER_NAME,
#         settings.PASSWORD,
#         settings.API_URL,
#         settings.DOWNLOAD_DIR
#     )
#
#     field_groups = db.get_field_groups()
#     field_groups.sort()
#
#     all_scenes = {}
#
#     for group in tqdm(field_groups):
#         if group == 2:
#             continue
#         lats_lons = db.get_bounds_lats_lons(year=year,
#                                             field_group=group,
#                                             dstype=settings.DESTSRID_OBTAIN)
#
#         scenes = gd.search(ll_lon=lats_lons[0],
#                            ll_lat=lats_lons[1],
#                            ur_lon=lats_lons[2],
#                            ur_lat=lats_lons[3],
#                            start_date=start_date,
#                            end_date=end_date,
#                            cloud_max=cloud_max)
#
#         all_scenes.update(scenes)
#         all_scenes = OrderedDict(sorted(
#             all_scenes.items(),
#             key=lambda t: t[1]['ingestiondate']
#         ))
#
#     # Словарь для выборки нужных снимков
#     new_scenes = {}
#
#     if all_scenes.items():
#         print('Найденные снимки:')
#         for i, scene in all_scenes.items():
#             if scene.get('tileid'):
#                 if any(x in scene.get('tileid', []) for x in settings.TILES):
#                     new_scenes[i] = scene
#
#         for i, scene in new_scenes.items():
#             print(f'Дата: {scene["ingestiondate"].strftime("%d.%m.%y")} | '
#                   f'Квадрат: {scene["tileid"]} | '
#                   f'Облачность: {scene["cloudcoverpercentage"]}% | '
#                   f'Чексумма = {i} | '
#                   f'Размер файла: {scene["size"]}')
#     else:
#         print('Поиск снимков не дал результатов')
#         sys.exit(-1)
#
#     if download:
#         gd.download(list(new_scenes.keys()))
#
#     print('Поиск снимков завершён')


if __name__ == '__main__':
    obtain_data()
