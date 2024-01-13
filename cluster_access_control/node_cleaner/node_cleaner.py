from typing import Final

from kubernetes import client
from redis import Redis
from pottery import RedisDict, Redlock

from cluster_access_control.utilities.environment import ClusterAccessConfiguration


class NodeCleaner:
    NODE_CLEANING_DICT_NAME: Final[str] = "node_keepalive_dict"
    NODE_CLEANING_LOCK_NAME: Final[str] = 'node_keepalive_lock'

    def __init__(self):
        self._environment = ClusterAccessConfiguration()
        self._redis_client = Redis.from_url(f"{self._environment.get_redis_url()}/0")
        self._keepalive_nodes_dict = RedisDict({}, redis=self._redis_client, name=self.NODE_CLEANING_DICT_NAME)
        self._lock = Redlock(key=self.NODE_CLEANING_LOCK_NAME, masters={self._redis_client})

        configuration = client.Configuration()
        configuration.cert_file = self._environment.get_kubernetes_cert()
        configuration.key_file = self._environment.get_kubernetes_key()
        configuration.host = f'https://{self._environment.get_cluster_host()}:{ClusterAccessConfiguration.CLUSTER_PORT}'

        self._kube_client = client.CoreV1Api(client.ApiClient(configuration))

    def update_node_keepalive(self, node_name: str):
        with self._lock:
            self._keepalive_nodes_dict[node_name] = time.time()
        pass
