import os
import pathlib
from typing import Final
import aiohttp
import yaml


class ClusterAccessConfiguration:
    CLUSTER_PORT: Final[int] = 6443
    VPN_PORT: Final[int] = 30000
    VPN_USER: Final[str] = "cluster-user"

    def __init__(self):
        self._cluster_host = os.environ["CLUSTER_HOST"]
        self._kubernetes_config_file = pathlib.Path(os.environ["KUBERNETES_CONFIG"])
        with self._kubernetes_config_file.open("r") as file:
            prime_service = yaml.safe_load(file)
        self._vpn_api_key = os.environ["VPN_API_KEY"]
        self._node_access_token = os.environ["K3S_NODE_TOKEN"]
        self._redis_url = os.environ["REDIS_URL"]

    def get_cluster_host(self) -> str:
        return self._cluster_host

    def get_kubernetes_config_file(self) -> str:
        return self._kubernetes_config_file

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
