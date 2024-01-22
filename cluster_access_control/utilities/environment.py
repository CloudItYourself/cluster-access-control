import base64
import os
import pathlib
import tempfile
import time
from typing import Final
import aiohttp
import yaml


class ClusterAccessConfiguration:
    CLUSTER_PORT: Final[int] = 6443
    VPN_PORT: Final[int] = 30000
    VPN_USER: Final[str] = "cluster-user"
    TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory()
    KUBE_CONFIG_FILE_NAME: Final[str] = "kubeconfig.yaml"

    def __init__(self):
        self._cluster_host = (pathlib.Path(os.environ["KUBERNETES_CONFIG"]) / "host-source-dns-name").read_text()
        self._vpn_api_key = (pathlib.Path(os.environ["KUBERNETES_CONFIG"]) / "vpn-token").read_text()

        self._kubernetes_config_file = pathlib.Path(
            ClusterAccessConfiguration.TEMPORARY_DIRECTORY.name) / ClusterAccessConfiguration.KUBE_CONFIG_FILE_NAME
        self._kubernetes_config_file.write_text(
            base64.b64decode((pathlib.Path(os.environ["KUBERNETES_CONFIG"]) / "kubernetes-config-file").read_text()).decode('utf-8'))
        self._node_access_token = (pathlib.Path(os.environ["KUBERNETES_CONFIG"]) / "k3s-node-token").read_text()
        self._redis_url = f'redis://:{os.environ["REDIS_PASSWORD"]}@{os.environ["REDIS_IP"]}/'

    def get_cluster_host(self) -> str:
        return self._cluster_host

    def get_kubernetes_config_file(self) -> str:
        return str(self._kubernetes_config_file.absolute())

    async def get_vpn_join_token_key(self) -> str:
        headers = {"Authorization": f"Bearer {self._vpn_api_key}"}
        body = {"user": self.VPN_USER}
        async with aiohttp.ClientSession() as session:
            response = await session.post(
                url="https://httpbin.org/post", data=body, headers=headers
            )
            return (await response.json())["key"]

    def get_node_access_token(self) -> str:
        return self._node_access_token

    def get_redis_url(self) -> str:
        return self._redis_url
