import os
from typing import Final

import requests


class ClusterAccessConfiguration:
    CLUSTER_PORT: Final[int] = 6443
    VPN_PORT: Final[int] = 30000
    VPN_USER: Final[str] = 'cluster-user'

    def __init__(self):
        self._cluster_host = os.environ['CLUSTER_HOST']
        self._kubernetes_cert = os.environ['KUBERNETES_CERT']
        self._kubernetes_key = os.environ['KUBERNETES_KEY']
        self._vpn_api_key = os.environ['VPN_API_KEY']
        self._node_access_token = os.environ['K3S_NODE_TOKEN']

        self._node_join_token = os.environ['K3S_NODE_TOKEN']

    def get_cluster_host(self) -> str:
        return self._cluster_host

    def get_kubernetes_key(self) -> str:
        return self._kubernetes_key

    def get_kubernetes_cert(self) -> str:
        return self._kubernetes_cert

    def get_node_join_token(self) -> str:
        return self._node_join_token

    def get_vpn_join_token_key(self) -> str:
        headers = {
            'Authorization': f'Bearer {self._vpn_api_key}'
        }
        body = {
            'user': self.VPN_USER
        }
        return requests.post(f'http://{self._cluster_host}:{self.VPN_PORT}/api/v1/preauthkey', headers=headers,
                             json=body).json()['key']

    def get_node_access_token(self) -> str:
        return self._node_access_token
