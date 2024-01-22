import pathlib

from fastapi import APIRouter

from cluster_access_control.utilities.environment import ClusterAccessConfiguration
from ciy_backend_libraries.api.cluster_access.v1.kubernetes_access import (
    KubernetesAccessResponse,
)


class ClusterAccess:
    def __init__(self):
        self.router = APIRouter()
        self._environment = ClusterAccessConfiguration()
        self.router.add_api_route(
            "/api/v1/cluster_access", self.cluster_access, methods=["GET"]
        )

    async def cluster_access(self) -> KubernetesAccessResponse:
        return KubernetesAccessResponse(
            k8s_config_file=pathlib.Path(self._environment.get_kubernetes_config_file()).read_text()
        )
