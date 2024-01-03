import time
from typing import Final, Union

from fastapi import APIRouter
from starlette.exceptions import HTTPException

from cluster_manager.api_messages.v1.node_registrar import NodeDetails, RegistrationDetails


class NodeRegistrar:
    NODE_REGISTER_COOLDOWN_IN_SECONDS: Final[int] = 300  # 5 minutes

    def __init__(self):
        self.router = APIRouter()
        self.router.add_api_route("/api/v1/node_token", self.request_token, methods=["POST"])
        self._registered_nodes = dict()

    async def request_token(self, node_details: NodeDetails) -> RegistrationDetails:
        if (node_details in self._registered_nodes and
                time.time() - self._registered_nodes[node_details] < NodeRegistrar.NODE_REGISTER_COOLDOWN_IN_SECONDS):
            raise HTTPException(
                status_code=429,
                detail=f"Error! node with name: {node_details.name} and id: {node_details.id} was registered lately",
            )
        # TODO: generate token and respond with details
        self._registered_nodes[node_details] = time.time()
        return RegistrationDetails(k8s_ip="127.0.0.1",
                                   k8s_port=6443,
                                   k8s_token="abcdef",
                                   vpn_ip="127.0.0.1",
                                   vpn_port=30000,
                                   vpn_token="abdasdasdsa")
