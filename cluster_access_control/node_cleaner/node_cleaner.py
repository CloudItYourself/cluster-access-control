import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Final, List

import kubernetes
from kubernetes import client
from kubernetes.client import V1Eviction
from redis import Redis
from pottery import RedisDict, Redlock
from threading import Lock
from cluster_access_control.utilities.environment import ClusterAccessConfiguration


class NodeCleaner:
    NODE_CLEANING_DICT_NAME: Final[str] = "node_keepalive_dict"
    NODE_NAME_TO_ID: Final[str] = "node_name_to_current_id"
    NODE_TIMEOUT_IN_SECONDS: Final[float] = 3

    def __init__(self):
        self._environment = ClusterAccessConfiguration()
        self._redis_client = Redis.from_url(f"{self._environment.get_redis_url()}/0")
        self._keepalive_nodes_dict = RedisDict(
            redis=self._redis_client, name=self.NODE_CLEANING_DICT_NAME
        )
        self._node_name_to_id = RedisDict(
            redis=self._redis_client, name=self.NODE_NAME_TO_ID
        )
        self._thread_pool = ThreadPoolExecutor()
        kubernetes.config.load_kube_config(
            config_file=self._environment.get_kubernetes_config_file()
        )
        self._prev_nodes_list = list()
        self._kube_client = client.CoreV1Api(
            kubernetes.config.new_client_from_config(config_file=self._environment.get_kubernetes_config_file()))
        self._deletion_kube_client = client.CoreV1Api(
            kubernetes.config.new_client_from_config(config_file=self._environment.get_kubernetes_config_file()))

    def update_node_keepalive(self, node_id: str, node_name: str):
        self._node_name_to_id[node_id] = node_name
        self._keepalive_nodes_dict[node_name] = datetime.utcnow().timestamp()

    def delete_stale_nodes(self):
        stale_node_deletion_client = client.CoreV1Api(
            kubernetes.config.new_client_from_config(config_file=self._environment.get_kubernetes_config_file()))
        grace_period_nodes = dict()
        while True:
            time.sleep(self.NODE_TIMEOUT_IN_SECONDS)
            try:
                nodes = stale_node_deletion_client.list_node().items

                for node in nodes:
                    if "ciy.persistent_node" not in node.metadata.labels:
                        node_name = node.metadata.name
                        if node_name not in self._node_name_to_id and node_name not in grace_period_nodes:
                            grace_period_nodes[node_name] = datetime.now()
                            continue

                        elif node_name not in self._node_name_to_id and node_name in grace_period_nodes:
                            if (datetime.now() - grace_period_nodes[node_name]).seconds > 30:
                                self._thread_pool.submit(
                                    self.clean_up_node,
                                    node.metadata.name,
                                    node.status.conditions[-1].type == "Ready",
                                )
                                grace_period_nodes.pop(node_name)
                            continue

                        node_keepalive_name = self._node_name_to_id[node_name]

                        node_exists = node_keepalive_name in self._keepalive_nodes_dict

                        if not node_exists and node_name not in grace_period_nodes:
                            grace_period_nodes[node_name] = datetime.now()
                        elif node_name in grace_period_nodes and (
                                datetime.now() - grace_period_nodes[node_name]).seconds < 30:
                            pass
                        else:
                            latest_keepalive = self._keepalive_nodes_dict[node_keepalive_name]
                            if (
                                    not node_exists
                                    or (datetime.utcnow().timestamp() - latest_keepalive) > self.NODE_TIMEOUT_IN_SECONDS
                            ):
                                self._keepalive_nodes_dict.pop(node_keepalive_name, None)

                                self._thread_pool.submit(
                                    self.clean_up_node,
                                    node.metadata.name,
                                    node.status.conditions[-1].type == "Ready",
                                )
                                grace_period_nodes.pop(node_name)
            except Exception as e:  # TODO IMPROVE ME
                print(f"Failed to clean up nodes.. error: {e}")

    def cordon_and_drain(self, node_name: str):
        body = {
            "spec": {
                "unschedulable": True,
            }
        }
        self._deletion_kube_client.patch_node(node_name, body)
        pods = self._deletion_kube_client.list_pod_for_all_namespaces(
            field_selector="spec.nodeName={}".format(node_name)
        ).items

        non_daemonset_pods = [
            pod
            for pod in pods
            if not pod.metadata.owner_references
               or pod.metadata.owner_references[0].kind != "DaemonSet"
        ]

        for pod in non_daemonset_pods:
            eviction = V1Eviction(metadata=client.V1ObjectMeta(name=pod.metadata.name))
            self._deletion_kube_client.create_namespaced_pod_eviction(
                name=pod.metadata.name, namespace=pod.metadata.namespace, body=eviction
            )

    def taint_node(self, node_name: str):
        body = {
            "spec": {
                "taints": [
                    {
                        "effect": "NoExecute",
                        "key": "node.kubernetes.io/out-of-service",
                        "value": "nodeshutdown",
                    }
                ]
            }
        }
        self._deletion_kube_client.patch_node(node_name, body)

    def clean_up_node(self, node_name: str, node_ready: bool):
        if node_ready:  # graceful shutdown
            self.cordon_and_drain(node_name)
        else:  # ungraceful shutdown
            self.taint_node(node_name)
        self._deletion_kube_client.delete_node(name=node_name)

    def get_online_nodes(self) -> List[str]:
        try:
            self._prev_nodes_list = [node.metadata.name for node in self._kube_client.list_node().items]
        except:
            pass  # TODO: check this thing

        return self._prev_nodes_list
