import base64
import os
import pathlib
import tempfile
from datetime import datetime, timedelta
from typing import Final
import aiohttp


class ClusterAccessConfiguration:
    CLUSTER_PORT: Final[int] = 6443
    VPN_PORT: Final[int] = 30000
    VPN_USER: Final[str] = "cluster-user"
    TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory()
    KUBE_CONFIG_FILE_NAME: Final[str] = "kubeconfig.yaml"
    CONNECTION_TIMEOUT_IN_MINUTES: Final[int] = 5

    def __init__(self):
        self._cluster_host = (
            pathlib.Path(os.environ["KUBERNETES_CONFIG"]) / "host-source-dns-name"
        ).read_text()
        self._vpn_api_key = (
            pathlib.Path(os.environ["KUBERNETES_CONFIG"]) / "vpn-token"
        ).read_text()

        self._kubernetes_config_file = (
            pathlib.Path(ClusterAccessConfiguration.TEMPORARY_DIRECTORY.name)
            / ClusterAccessConfiguration.KUBE_CONFIG_FILE_NAME
        )
        self._kubernetes_config_file.write_text(
            base64.b64decode(
                (
                    pathlib.Path(os.environ["KUBERNETES_CONFIG"])
                    / "kubernetes-config-file"
                ).read_text()
            ).decode("utf-8")
        )
        self._node_access_token = (
            pathlib.Path(os.environ["KUBERNETES_CONFIG"]) / "k3s-node-token"
        ).read_text()
        self._redis_url = (
            f'redis://:{os.environ["REDIS_PASSWORD"]}@{os.environ["REDIS_IP"]}/'
        )

    @staticmethod
    def get_time_for_pre_auth():
        utc_now = datetime.utcnow()
        utc_plus_five = utc_now + timedelta(
            minutes=ClusterAccessConfiguration.CONNECTION_TIMEOUT_IN_MINUTES
        )
        formatted_time = utc_plus_five.isoformat() + "Z"
        return formatted_time

    def get_cluster_host(self) -> str:
        return self._cluster_host

    def get_kubernetes_config_file(self) -> str:
        return str(self._kubernetes_config_file.absolute())

    async def get_vpn_join_token_key(self) -> str:
        headers = {"Authorization": f"Bearer {self._vpn_api_key}"}
        data = {
            "user": self.VPN_USER,
            "reusable": False,
            "ephemeral": False,
            "expiration": ClusterAccessConfiguration.get_time_for_pre_auth,
            "aclTags": ["string"],
        }
        async with aiohttp.ClientSession() as session:
            response = await session.post(
                url=f"http://{self._cluster_host}:{self.VPN_PORT}/api/v1/preauthkey",
                headers=headers,
                json=data,
            )
            return (await response.json())["preAuthKeys"]["key"]

    def get_node_access_token(self) -> str:
        return self._node_access_token

    def get_redis_url(self) -> str:
        return self._redis_url

if __name__ == '__main__':
    print(ClusterAccessConfiguration.get_time_for_pre_auth())