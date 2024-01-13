from fastapi import APIRouter

from cluster_access_control.node_cleaner.node_cleaner import NodeCleaner
from cluster_access_control.utilities.environment import ClusterAccessConfiguration
from tpc_backend_libraries.api.cluster_access.v1.kubernetes_access import KubernetesAccessResponse


class ClusterAccess:
    def __init__(self, node_cleaner: NodeCleaner):
        self.router = APIRouter()
        self._node_cleaner = node_cleaner
        self._environment = ClusterAccessConfiguration()
        self.router.add_api_route("/api/v1/cluster_access", self.cluster_access, methods=["GET"])
        self.router.add_api_route("/api/v1/node_keepalive/{node_id}", self.node_keepalive_message, methods=["PUT"])

    async def cluster_access(self) -> KubernetesAccessResponse:
        return KubernetesAccessResponse(k8s_ip=self._environment.get_cluster_host(),
                                        k8s_port=ClusterAccessConfiguration.CLUSTER_PORT,
                                        k8s_cert=self._environment.get_kubernetes_cert(),
                                        k8s_key=self._environment.get_kubernetes_key())

    async def node_keepalive_message(self, node_id: str):
        self._node_cleaner.update_node_keepalive(node_id)