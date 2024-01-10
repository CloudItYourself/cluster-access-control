import os
from typing import Final


class ClusterAccessConfiguration:
    CLUSTER_PORT: Final[int] = 6443
    VPN_PORT: Final[int] = 30000
    VPN_USER: Final[str] = 'cluster-user'

    def __init__(self):
        self._cluster_host = os.environ['CLUSTER_HOST']
        self._kubernetes_client_auth = os.environ['KUBERNETES_AUTH']
        self._vpn_api_key = os.environ['VPN_API_KEY']
        self._node_access_token = os.environ['K3S_NODE_TOKEN']

    def get_cluster_host(self) -> str:
        return self._cluster_host

    def get_kubernetes_api_join_key(self) -> str:
        return self._kubernetes_client_auth

    def get_vpn_join_token_key(self) -> str:
        return self._vpn_api_key  # TODO: get an actual connection token token here

    def get_node_access_token(self) -> str:
        return self._node_access_token
