import time
from typing import Final, Union

from fastapi import APIRouter
from starlette.exceptions import HTTPException

from cluster_manager.api_messages.v1.kubernetes_access import KubernetesAccessResponse
from cluster_manager.api_messages.v1.node_registrar import NodeDetails, RegistrationDetails


class ClusterAccess:
    def __init__(self):
        self.router = APIRouter()
        self.router.add_api_route("/api/v1/cluster_access", self.cluster_access, methods=["GET"])

    async def cluster_access(self, requesting_uuid: str) -> KubernetesAccessResponse:
        return KubernetesAccessResponse(k8s_ip="127.0.0.1",
                                        k8s_port=6634,
                                        k8s_token="agvdads")
