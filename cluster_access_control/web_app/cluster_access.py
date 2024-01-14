from fastapi import APIRouter

from cluster_access_control.utilities.environment import ClusterAccessConfiguration
from tpc_backend_libraries.api.cluster_access.v1.kubernetes_access import KubernetesAccessResponse


class ClusterAccess:
    def __init__(self):
        self.router = APIRouter()
        self._environment = ClusterAccessConfiguration()
        self.router.add_api_route("/api/v1/cluster_access", self.cluster_access, methods=["GET"])

    async def cluster_access(self) -> KubernetesAccessResponse:
        return KubernetesAccessResponse(k8s_ip=self._environment.get_cluster_host(),
                                        k8s_port=ClusterAccessConfiguration.CLUSTER_PORT,
                                        k8s_cert=self._environment.get_kubernetes_cert(),
                                        k8s_key=self._environment.get_kubernetes_key())
