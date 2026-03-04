"""Настройки GeoServer."""
import os

GS_DATA_ROOT = "/mnt/map/geoware"
GS_DATA_DIR = "/opt/geoserver_data/geoware"

GS_HOST = os.environ.get("GS_HOST", "192.168.0.19")
GS_WORKSPACE = os.environ.get("GS_WORKSPACE", "sentinel")
GS_USERNAME = os.environ.get("GS_USERNAME", "admin")
GS_PASSWORD = os.environ.get("GS_PASSWORD", "geoserver2026")
GS_USE_MOSAIC = True