""" Скрипт публикации снимков на сервере"""

import datetime
import sys
from os import walk
from os.path import join
from shutil import copy

import requests
from fabric.api import env, put, run
from geoserver.catalog import Catalog, FailedRequestError

import dboperator as db
import settings
from utils import check_create_folder, get_filename, split_file_name

hiders_xml = {
    "Content-type": "application/xml",
    "Accept": "application/xml"
}

session = requests.Session()
session.auth = (settings.TSUSER, settings.TSPASSWORD)
url = 'http://{}:{}/geoserver'.format(settings.RMHOST, settings.TSPORT)
service_wms_url = url + '/wms/'
service_url = url + '/rest/'
service_gwc_url = url + '/gwc/rest/'


def _create_catalog():
    try:
        return Catalog(service_url, username=settings.TSUSER, password=settings.TSPASSWORD)

    except Exception as e:
        print(f'Невозможно создать каталог. Ошибка: {e}.')
        sys.exit(1)


def _create_workspace(catalog):
    try:
        workspace = catalog.get_workspace(settings.WORKSPACE)
        if not workspace:
            workspace = catalog.create_workspace(settings.WORKSPACE)

        return workspace

    except Exception as e:
        print(f'Невозможно создать рабочее пространство. Ошибка: {e}.')
        sys.exit(1)


def _get_store(catalog, workspace, store_name, file_path):
    try:
        store = catalog.get_store(name=store_name, workspace=workspace)

        if store:
            # Delete coverage store.
            url = service_url + 'workspaces/{}/coveragestores/{}'.format(settings.WORKSPACE, store_name)
            # 'recurse': 'true' - remove coverages inside this coverage store.
            # 'purge': 'all' - purge everything related to that store (metadata and granules).
            params = {'recurse': 'true', 'purge': 'all'}
            r = session.delete(url, params=params, headers=hiders_xml)
            if r.status_code < 200 or r.status_code > 299:
                print('Unable to remove coverage store. GeoServer error code: "{}".'.format(r.status_code))
                sys.exit(1)

            store = _create_store(catalog, workspace, store_name, file_path)

    # Store doesn't exist.
    except FailedRequestError:
        store = _create_store(catalog, workspace, store_name, file_path)
    except Exception as e:
        print("Unable to get coverage store. An error has occurred: '{}'.".format(e))
        sys.exit(1)

    return store


def _create_store(catalog, workspace, store_name, file_path):
    try:
        store = catalog.create_coveragestore2(name=store_name, workspace=workspace)
        store.type = "GeoTIFF"
        store.url = 'file://{}'.format(file_path)
        catalog.save(store)
        store = catalog.get_store(name=store_name, workspace=workspace)

        return store
    except Exception as e:
        print("Unable to create coverage store. An error has occurred: '{}'.".format(e))
        sys.exit(1)


def _create_covrage(catalog, store, coverage_name):
    layer = catalog.get_layer(coverage_name)

    if not layer:
        url = service_url + 'workspaces/{}/coveragestores/{}/coverages'.format(store.workspace.name, store.name)
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
                     </coverage>""".format(coverage_name, coverage_name, settings.DESTSRID, settings.TILE_SIZE,
                                           settings.TILE_SIZE)

        r = session.post(url, data=data, headers=hiders_xml)
        if r.status_code < 200 or r.status_code > 299:
            print("Unable to create coverage. GeoServer coverage error code: '{}'.".format(r.status_code))
            sys.exit(1)

        layer = catalog.get_layer(coverage_name)

    return layer


def _gwc_seeding(layer_name, workspace_name):
    print("Configuring of GeoServer GWC.")

    url = service_gwc_url + 'seed/{}:{}.xml'.format(workspace_name, layer_name)
    data = """<seedRequest>
                     <name>{}:{}</name>
                     <srs>
                        <number>{}</number>
                     </srs>
                     <zoomStart>{}</zoomStart>
                     <zoomStop>{}</zoomStop>
                     <format>image/png8</format>
                     <type>reseed</type>
                     <threadCount>4</threadCount>
                </seedRequest>""".format(workspace_name, layer_name, settings.DESTSRID, settings.ZOOM_START,
                                         settings.ZOOM_STOP)

    r = session.post(url, data=data, headers=hiders_xml)
    if r.status_code < 200 or r.status_code > 299:
        print("Unable to generate layer geo web cache. GeoServer geo web cache error code: '{}'.".format(
                r.status_code))
        sys.exit(1)


def _copy_to_remote(folder_absolute_path, file_path):
    print("Copying file '{}' to the remote server.".format(file_path))

    try:
        env.host_string = '{}@{}:{}'.format(settings.RMUSER, settings.RMHOST, settings.SSH_PORT)
        env.user = settings.TSUSER
        if settings.RMPASSWORD:
            env.password = settings.RMPASSWORD

        print("Remote host string: '{}'.".format(env.host_string))
        print("Image absolute path: '{}'.".format(folder_absolute_path))

        run('mkdir -p {}'.format(folder_absolute_path))

        file_path = put(file_path, folder_absolute_path)

        return file_path[0]

    except Exception as e:
        print("Unable to copy a file to remote server. An error has occurred: '{}'.".format(e))
        sys.exit(1)


def _copy_to_local(folder_absolute_path, file_path):
    print("Copying file '{}' to the local tile server's directory.".format(file_path))

    try:
        check_create_folder(folder_absolute_path)
        file_path = copy(file_path, folder_absolute_path)

        return file_path

    except Exception as e:
        print("Unable to copy a file to the tile server directory. An error has occurred: '{}'.".format(e))
        sys.exit(1)


def _public_layer(catalog, workspace, file_path, folder_absolute_path):
    file_name = get_filename(file_path)

    if settings.RMHOST and settings.RMHOST != 'localhost' and settings.RMHOST != '127.0.0.1':
        file_path = _copy_to_remote(folder_absolute_path, file_path)
    else:
        file_path = _copy_to_local(folder_absolute_path, file_path)

    print("Configure a GeoServer image layer.")

    store = _get_store(catalog, workspace, file_name, file_path)
    layer = _create_covrage(catalog, store, file_name)

    return store, layer


def public():
    """ Публикация снимков"""
    catalog = _create_catalog()
    workspace = _create_workspace(catalog)

    print('Запуск функции загрузки изображений на сервер')

    for (dir_path, dir_names, file_names) in walk(settings.PROCESSED_DIR):

        # layers_group_name = None
        if '__group__' in file_names:
            file_names.remove('__group__')

        folder_absolute_path = join(settings.IMAGE_DIR)

        print(folder_absolute_path)

        for file_name in file_names:
            store, layer = _public_layer(catalog, workspace, join(dir_path, file_name), folder_absolute_path)

            if layer:
                date, image_set, resolution, field_group_id, field_id, layer_name, satellite = split_file_name(layer.name)

                date = datetime.datetime.strptime(date, "%d_%m_%Y").date()

                if image_set == 'rgb':
                    image_set = 'visual'
                db.insert_layer(date=date, set_=image_set,
                                resolution=resolution,
                                agroid=field_group_id,
                                field_id=field_id,
                                name=workspace.name + ':' + layer_name,
                                satellite=satellite,
                                isgrouplayer=False)

                if settings.USE_GWC:
                    _gwc_seeding(layer.name, workspace.name)

    print('Загрузка файлов на удаленный сервер завершена')


if __name__ == '__main__':
    public()
