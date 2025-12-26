from datetime import date

import requests

from .auth import AuthManager


class CatalogClient:
    """Управление каталогом данных."""
    BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

    def __init__(self, auth: AuthManager):
        self.auth = auth

    def search_s2(
            self, tiles,
            start: date = None,
            end: date = None,
            limit=100
    ):
        fl = [
            f"Collection/Name eq 'SENTINEL-2'",
            "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq 'S2MSI2A') or ".join([f"contains(Name,'{t}')" for t in tiles])
        ]

        if start:
            fl.append(f"ContentDate/Start ge {start.isoformat()}T00:00:00Z")
        if end:
            fl.append(f"ContentDate/Start le {end.isoformat()}T23:59:59Z")

        params = {
            "$filter": " and ".join(fl),
            "$top": limit,
            "$orderby": "ContentDate/Start desc"
        }
        headers = {"Authorization": f"Bearer {self.auth.get_token()}"}
        r = requests.get(self.BASE, headers=headers, params=params,
                         timeout=(5, 30))
        r.raise_for_status()
        return r.json().get("value", [])
