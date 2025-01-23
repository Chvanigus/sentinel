""" Скрипт публикации снимков на сервере"""

import datetime
import sys
from os import walk
from os.path import join
from shutil import copy

import psycopg2
from fabric.api import env, put
from geoserver.catalog import Catalog, FailedRequestError

from core import settings
from core.publishing.utils import (
    _get_session, _get_geoserver_wms_service_url,
    _get_geoserver_url, _get_geoserver_rest_service_url,
    _get_geoserver_gwc_service_url
)
from core.utils import check_create_folder, split_file_name, get_basename
from db.connect_data import DSL
from db.data_class import Layer
from db.db_class import get_postgis_worker

hiders_xml = {
    "Content-type": "application/xml",
    "Accept": "application/xml"
}

session = _get_session()
url = _get_geoserver_url()
service_wms_url = _get_geoserver_wms_service_url(url)
service_url = _get_geoserver_rest_service_url(url)
service_gwc_url = _get_geoserver_gwc_service_url(url)


def _create_catalog():
    try:
        return Catalog(
            service_url, username=settings.TSUSER,
            password=settings.TSPASSWORD
        )

    except Exception as e:
        print(f"Невозможно создать каталог. Ошибка: {e}.")
        sys.exit(1)


def _create_workspace(catalog):
    try:
        workspace = catalog.get_workspace(settings.WORKSPACE)
        if not workspace:
            workspace = catalog.create_workspace(settings.WORKSPACE)

        return workspace

    except Exception as e:
        print(f"Невозможно создать рабочее пространство. Ошибка: {e}.")
        sys.exit(1)


def _get_store(catalog, workspace, store_name, file_path):
    try:
        store = catalog.get_store(name=store_name, workspace=workspace)

        if store:
            _url = service_url + f"workspaces/{settings.WORKSPACE}/coveragestores/{store_name}.html"
            params = {"recurse": "true", "purge": "all"}

            r = session.delete(_url, params=params, headers=hiders_xml)
            if r.status_code < 200 or r.status_code > 299:
                print(f"Unable to remove coverage store. GeoServer error code: '{r.status_code}'.")
                sys.exit(1)

            store = _create_store(catalog, workspace, store_name, file_path)

    except FailedRequestError:
        store = _create_store(catalog, workspace, store_name, file_path)
    except Exception as e:
        print(
            "Unable to get coverage store. An error has occurred: '{}'.".format(
                e))
        sys.exit(1)

    return store


def _create_store(catalog, workspace, store_name, file_path):
    try:
        store = catalog.create_coveragestore2(name=store_name,
                                              workspace=workspace)
        store.type = "GeoTIFF"
        store.url = "file://{}".format(file_path)
        catalog.save(store)
        store = catalog.get_store(name=store_name, workspace=workspace)

        return store
    except Exception as e:
        print(
            "Unable to create coverage store. An error has occurred: '{}'.".format(
                e))
        sys.exit(1)


def _create_covrage(catalog, store, coverage_name):
    layer = catalog.get_layer(coverage_name)

    if not layer:
        _url = service_url + "workspaces/{}/coveragestores/{}/coverages".format(
            store.workspace.name, store.name)
        data = """<coverage>
                        <name>{}</name>
                        <title>{}</title>
                        <srs>EPSG:{}</srs>
                        <parameters>
                            <entry>
                            <string>SUGGESTED_TILE_SIZE</string>
                            <string>{},{}</string>
                            </entry>
                        </parameters>
                     </coverage>""".format(coverage_name, coverage_name,
                                           settings.DESTSRID,
                                           settings.TILE_SIZE,
                                           settings.TILE_SIZE)

        r = session.post(_url, data=data, headers=hiders_xml)
        if r.status_code < 200 or r.status_code > 299:
            print(
                "Unable to create coverage. GeoServer coverage error code: '{}'.".format(
                    r.status_code))
            sys.exit(1)

        layer = catalog.get_layer(coverage_name)

    return layer


def _gwc_seeding(layer_name, workspace_name):
    print("Configuring of GeoServer GWC.")

    _url = service_gwc_url + f"seed/{workspace_name}:{layer_name}"
    data = f"""<seedRequest>
                     <name>{workspace_name}:{layer_name}</name>
                     <srs>
                        <number>{settings.DESTSRID}</number>
                     </srs>
                     <zoomStart>{settings.ZOOM_START}</zoomStart>
                     <zoomStop>{settings.ZOOM_STOP}</zoomStop>
                     <format>image/png8</format>
                     <type>reseed</type>
                     <threadCount>4</threadCount>
                </seedRequest>"""

    print(_url)

    r = session.post(_url, data=data, headers=hiders_xml)
    if r.status_code < 200 or r.status_code > 299:
        print(
            "Unable to generate layer geo web cache. GeoServer geo web cache error code: '{}'.".format(
                r.status_code))
        sys.exit(1)


# def _copy_to_remote(folder_absolute_path, file_path):
#     print(f"Копирование файла "{file_path}" на удаленный сервер.")
# 
#     try:
#         env.host_string = "{}@{}:{}".format(settings.RMUSER, settings.RMHOST,
#                                             settings.SSH_PORT)
#         env.user = settings.TSUSER
#         if settings.RMPASSWORD:
#             env.password = settings.RMPASSWORD
# 
#         print("Remote host string: '{}'.".format(env.host_string))
#         print("Image absolute path: '{}'.".format(folder_absolute_path))
# 
#         file_path = put(file_path, folder_absolute_path)
# 
#         return file_path[0]
# 
#     except Exception as e:
#         print(
#             "Unable to copy a file to remote server. An error has occurred: '{}'.".format(
#                 e))
#         sys.exit(1)


def _copy_to_local(folder_absolute_path, file_path):
    print(f"Copying file '{file_path}' to the local tile server's directory.")

    try:
        check_create_folder(folder_absolute_path)
        file_path = copy(file_path, folder_absolute_path)
        return file_path

    except Exception as e:
        print("Unable to copy a file to the tile server directory. An error has occurred: '{}'.".format(e))
        sys.exit(1)


def _public_layer(catalog, workspace, file_path, folder_absolute_path):
    file_name = get_basename(file_path).split(".")[0]
    file_path = _copy_to_local(folder_absolute_path, file_path)

    print("Configure a GeoServer image layer.")

    store = _get_store(
        catalog=catalog,
        workspace=workspace,
        store_name=file_name,
        file_path=file_path
    )
    layer = _create_covrage(catalog, store, file_name)

    return store, layer


def public():
    """ Публикация снимков"""
    catalog = _create_catalog()
    workspace = _create_workspace(catalog)

    for (dir_path, dir_names, file_names) in walk(settings.PROCESSED_DIR):

        # layers_group_name = None
        if "__group__" in file_names:
            file_names.remove("__group__")

        folder_absolute_path = join(settings.IMAGE_DIR)

        for file_name in file_names:
            store, layer = _public_layer(
                catalog, workspace,
                join(dir_path, file_name),
                folder_absolute_path
            )

            if layer:
                (
                    date, image_set, resolution, field_group_id, field_id,
                    layer_name, satellite
                ) = split_file_name(layer.name)

                date = datetime.datetime.strptime(date, "%d_%m_%Y").date()

                if image_set == "rgb":
                    image_set = "visual"

                with psycopg2.connect(**DSL) as pg_conn:
                    pw = get_postgis_worker(pg_conn)

                    obj_layer = Layer(
                        name="sentinel:" + layer_name,
                        date=date,
                        set=image_set,
                        resolution=resolution,
                        agroid=field_group_id,
                        satellite=satellite,
                    )
                    pw.insert_layer(layer=obj_layer)

                if settings.USE_GWC:
                    _gwc_seeding(layer.name, workspace.name)

    print("Загрузка файлов на удаленный сервер завершена")


if __name__ == "__main__":
    public()
