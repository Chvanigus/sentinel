"""URLs для Geoserver."""

from core import settings


class GeoServerUrls:
    """Класс для формирования URL'ов для работы с GeoServer."""

    def __init__(self):
        self._base = f"http://{settings.RMHOST}:{settings.TSPORT}/geoserver"

    def rest(self) -> str:
        """REST-url."""
        return f"{self._base}/rest"

    def gwc(self) -> str:
        """GWC-url."""
        return f"{self._base}/gwc/rest"

    def workspace(self, name: str) -> str:
        """Workspace-url."""
        return f"{self.rest()}/workspaces/{name}"

    def coverage_store(self, workspace: str, store: str) -> str:
        """coverage-store-url."""
        return f"{self.workspace(workspace)}/coveragestores/{store}"

    def coverage(self, workspace: str, store: str, layer: str) -> str:
        """coverage-url."""
        return f"{self.coverage_store(workspace, store)}/coverages/{layer}"

    def style(self, name: str) -> str:
        """style-url."""
        return f"{self.rest()}/styles/{name}"
