import time
from typing import Final, Union

from fastapi import APIRouter
from starlette.exceptions import HTTPException
from cluster_manager.api_messages.v1.kubernetes_access import KubernetesAccessResponse
from cluster_manager.api_messages.v1.node_registrar import NodeDetails, RegistrationDetails
from cluster_manager.utilities.environment import ClusterAccessConfiguration


class ClusterAccess:
    def __init__(self):
        self.router = APIRouter()
        self._environment = ClusterAccessConfiguration()
        self.router.add_api_route("/api/v1/cluster_access", self.cluster_access, methods=["GET"])

    async def cluster_access(self) -> KubernetesAccessResponse:
        return KubernetesAccessResponse(k8s_ip=self._environment.get_cluster_host(),
                                        k8s_port=ClusterAccessConfiguration.CLUSTER_PORT,
                                        k8s_token=self._environment.get_kubernetes_api_join_key())
