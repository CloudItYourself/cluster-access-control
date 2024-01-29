import time
from typing import Final
from cluster_access_control.node_cleaner.node_cleaner import NodeCleaner

from fastapi import APIRouter
from starlette.exceptions import HTTPException
from ciy_backend_libraries.api.cluster_access.v1.node_registrar import (
    NodeDetails,
    RegistrationDetails,
)
from cluster_access_control.utilities.environment import ClusterAccessConfiguration


class NodeRegistrar:
    NODE_REGISTER_COOLDOWN_IN_SECONDS: Final[int] = 10  # 5 minutes

    def __init__(self, node_cleaner: NodeCleaner):
        self.router = APIRouter()
        self._node_cleaner = node_cleaner
        self._environment = ClusterAccessConfiguration()
        self.router.add_api_route(
            "/api/v1/node_token", self.request_token, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/node_keepalive/{node_id}",
            self.node_keepalive_message,
            methods=["PUT"],
        )
        self.router.add_api_route(
            "/api/v1/node_exists/{node_name}",
            self.is_node_online,
            methods=["GET"],
        )
        self._registered_nodes = dict()

    async def request_token(self, node_details: NodeDetails) -> RegistrationDetails:
        hashed_node_details = str(node_details)
        if (
                hashed_node_details in self._registered_nodes
                and time.time() - self._registered_nodes[hashed_node_details]
                < NodeRegistrar.NODE_REGISTER_COOLDOWN_IN_SECONDS
        ):
            raise HTTPException(
                status_code=429,
                detail=f"Error! node with name: {node_details.name} and id: {node_details.id} was registered lately",
            )

        self._registered_nodes[hashed_node_details] = time.time()
        return RegistrationDetails(
            k8s_ip=self._environment.get_cluster_host(),
            k8s_port=ClusterAccessConfiguration.CLUSTER_PORT,
            k8s_token=self._environment.get_node_access_token(),
            vpn_ip=self._environment.get_cluster_host(),
            vpn_port=ClusterAccessConfiguration.VPN_PORT,
            vpn_token=await self._environment.get_vpn_join_token_key(),
        )

    def node_keepalive_message(self, node_id: str, node_details: NodeDetails):
        self._node_cleaner.update_node_keepalive(node_id, str(node_details))

    def is_node_online(self, node_name: str):
        if node_name in self._node_cleaner.get_online_nodes():
            return True
        raise HTTPException(status_code=404)
