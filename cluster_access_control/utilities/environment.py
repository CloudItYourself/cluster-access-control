import base64
import os
import pathlib
import tempfile
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
        print(f"XD::::::::: {list(pathlib.Path(os.environ['KUBERNETES_CONFIG']).rglob("*"))}")
        with pathlib.Path(os.environ["KUBERNETES_CONFIG"]).open("r") as file:
            configurations = yaml.safe_load(file)
        self._cluster_host = configurations["host-source-dns-name"]
        self._vpn_api_key = configurations["vpn-token"]

        self._kubernetes_config_file = pathlib.Path(
            ClusterAccessConfiguration.TEMPORARY_DIRECTORY.name) / ClusterAccessConfiguration.KUBE_CONFIG_FILE_NAME
        self._kubernetes_config_file.write_text(
            base64.b64decode(configurations['kubernetes-config-file']).decode('utf-8'))
        self._node_access_token = configurations["k3s-node-token"]
        self._redis_url = f'redis://{os.environ["REDIS_PASSWORD"]}@{os.environ["REDIS_IP"]}/'

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
