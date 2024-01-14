import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Final

from kubernetes import client
from kubernetes.client import V1DeleteOptions, V1Eviction
from redis import Redis
from pottery import RedisDict, Redlock
from threading import Lock
from cluster_access_control.utilities.environment import ClusterAccessConfiguration


class NodeCleaner:
    NODE_CLEANING_DICT_NAME: Final[str] = "node_keepalive_dict"
    NODE_CLEANING_LOCK_NAME: Final[str] = 'node_keepalive_lock'
    NODE_TIMEOUT_IN_SECONDS: Final[float] = 1.5

    def __init__(self):
        self._environment = ClusterAccessConfiguration()
        self._redis_client = Redis.from_url(f"{self._environment.get_redis_url()}/0")
        self._keepalive_nodes_dict = RedisDict(redis=self._redis_client, name=self.NODE_CLEANING_DICT_NAME)
        self._redlock = Redlock(key=self.NODE_CLEANING_LOCK_NAME, masters={self._redis_client})
        self._lock = Lock()
        self._thread_pool = ThreadPoolExecutor()

        configuration = client.Configuration()
        configuration.cert_file = self._environment.get_kubernetes_cert()
        configuration.key_file = self._environment.get_kubernetes_key()
        configuration.host = f'https://{self._environment.get_cluster_host()}:{ClusterAccessConfiguration.CLUSTER_PORT}'

        self._kube_client = client.CoreV1Api(client.ApiClient(configuration))

    def update_node_keepalive(self, node_name: str):
        with self._redlock:
            self._keepalive_nodes_dict[node_name] = datetime.utcnow().timestamp()

    def delete_stale_nodes(self):
        while True:
            time.sleep(self.NODE_TIMEOUT_IN_SECONDS)

            with self._lock:
                nodes = self._kube_client.list_node().items

            for node in nodes:
                node_name = node.metadata.name
                with self._redlock:
                    node_exists = node_name in self._keepalive_nodes_dict

                if not node_exists:
                    time.sleep(self.NODE_TIMEOUT_IN_SECONDS)  # allow for a timeout between node connection and keepalive

                with self._redlock:
                    if node not in self._keepalive_nodes_dict or \
                            datetime.utcnow().timestamp() - self._keepalive_nodes_dict[
                        node_name] > self.NODE_TIMEOUT_IN_SECONDS:
                        self._keepalive_nodes_dict.pop(node_name, None)
                        self._thread_pool.submit(self.clean_up_node, node_name, node.status.conditions[-1].type == "Ready")

    def cordon_and_drain(self, node_name: str):
        body = {
            "spec": {
                "unschedulable": True,
            }
        }
        self._kube_client.patch_node(node_name, body)
        pods = self._kube_client.list_pod_for_all_namespaces(
            field_selector='spec.nodeName={}'.format(node_name)).items

        non_daemonset_pods = [pod for pod in pods if
                              not pod.metadata.owner_references or pod.metadata.owner_references[0].kind != 'DaemonSet']

        for pod in non_daemonset_pods:
            eviction = V1Eviction(metadata=client.V1ObjectMeta(name=pod.metadata.name))
            self._kube_client.create_namespaced_pod_eviction(name=pod.metadata.name,
                                                             namespace=pod.metadata.namespace,
                                                             body=eviction)

    def taint_node(self, node_name: str):
        body = {
            "spec": {
                "taints": [
                    {
                        "effect": "NoExecute",
                        "key": "node.kubernetes.io/out-of-service",
                        "value": "nodeshutdown"
                    }
                ]
            }
        }
        self._kube_client.patch_node(node_name, body)

    def clean_up_node(self, node_name: str, node_ready: bool):
        with self._lock:
            if node_ready:  # graceful shutdown
                self.cordon_and_drain(node_name)
            else:  # ungraceful shutdown
                self.taint_node(node_name)
            self._kube_client.delete_node(name=node_name)
