from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from core import settings


class AuthManager:
    """Менеджер соединения и авторизации по токену."""
    TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"

    def __init__(self):
        self._token = None
        self._client_id = settings.SH_CLIENT_ID
        self._client_secret = settings.SH_CLIENT_SECRET

    def get_token(self):
        """Получения токена для скачивания снимков."""
        client = BackendApplicationClient(client_id=self._client_id)
        oauth = OAuth2Session(client=client)
        oauth.trust_env = False

        token = oauth.fetch_token(
            token_url=self.TOKEN_URL,
            client_secret=self._client_secret, include_client_id=True,
        )

        resp = oauth.get(
            "https://sh.dataspace.copernicus.eu/configuration/v1/wms/instances"
        )
        print(resp.content)

        self._token = token
        return self._token


