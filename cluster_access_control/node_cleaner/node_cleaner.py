import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Final, Set

import kubernetes
from kubernetes import client
from kubernetes.client import V1Eviction
from redis import Redis
from pottery import Redlock, RedisSet
from cluster_access_control.utilities.environment import ClusterAccessConfiguration


class NodeCleaner:
    NODE_KEEPALIVE_PREFIX: Final[str] = "node-keepalive-prefix"
    NODE_CLEANING_LOCK_NAME: Final[str] = "cluster-access-cleanup-lock"
    CONNECTED_NODE_SET: Final[str] = "connected-nodes-set"
    CONNECTED_NODE_SET_TIME: Final[str] = "connected-nodes-set-time"
    CONNECTED_NODE_LOCK: Final[str] = "connected-nodes-lock"
    NODE_TIMEOUT_IN_SECONDS: Final[int] = 3
    READY_NODE_CHECK_PERIOD_IN_SECONDS: Final[int] = 5

    def __init__(self):
        self._environment = ClusterAccessConfiguration()
        self._redis_client = Redis.from_url(f"{self._environment.get_redis_url()}/0")

        self._redlock = Redlock(
            key=self.NODE_CLEANING_LOCK_NAME,
            masters={self._redis_client},
            auto_release_time=100,
        )

        self._connected_node_lock = Redlock(
            key=self.CONNECTED_NODE_LOCK,
            masters={self._redis_client},
            auto_release_time=50,
        )

        self._thread_pool = ThreadPoolExecutor()
        kubernetes.config.load_kube_config(
            config_file=self._environment.get_kubernetes_config_file()
        )

        self._current_node_set = RedisSet(
            redis=self._redis_client, key=NodeCleaner.CONNECTED_NODE_SET
        )

        self._kube_client = client.CoreV1Api(
            kubernetes.config.new_client_from_config(
                config_file=self._environment.get_kubernetes_config_file()
            )
        )
        self._deletion_kube_client = client.CoreV1Api(
            kubernetes.config.new_client_from_config(
                config_file=self._environment.get_kubernetes_config_file()
            )
        )

    def update_node_keepalive(self, node_id: str):
        self._redis_client.setex(
            f"{NodeCleaner.NODE_KEEPALIVE_PREFIX}-{node_id}",
            NodeCleaner.NODE_TIMEOUT_IN_SECONDS,
            str(datetime.utcnow().timestamp()),
        )

    def get_kube_client(self):
        try:
            kube_client = client.CoreV1Api(
                kubernetes.config.new_client_from_config(
                    config_file=self._environment.get_kubernetes_config_file()
                )
            )
            return kube_client
        except Exception as e:
            print("Killing due to kubernetes failure")
            sys.exit(-1)

    def delete_stale_nodes(self):
        stale_node_deletion_client = self.get_kube_client()

        grace_period_nodes = set()
        while True:
            time.sleep(self.NODE_TIMEOUT_IN_SECONDS)
            try:
                nodes = stale_node_deletion_client.list_node().items
                with self._redlock:
                    for node in nodes:
                        if "ciy.persistent_node" not in node.metadata.labels:
                            node_name = node.metadata.name
                            node_exists = (
                                    self._redis_client.get(
                                        f"{NodeCleaner.NODE_KEEPALIVE_PREFIX}-{node_name}"
                                    )
                                    is not None
                            )

                            if not node_exists:
                                if node_name not in grace_period_nodes:
                                    grace_period_nodes.add(node_name)
                                else:
                                    print("Deleting node due to grace period expiry")
                                    #TODO: add postgres notification
                                    self._thread_pool.submit(
                                        self.clean_up_node,
                                        node.metadata.name,
                                        node.status.conditions[-1].type == "Ready",
                                    )

                                    grace_period_nodes.remove(node_name)
                            else:
                                if node_name in grace_period_nodes:
                                    grace_period_nodes.remove(node_name)

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

    def get_online_nodes(self) -> Set[str]:
        with self._connected_node_lock:
            last_connected_node_timestamp = self._redis_client.get(
                NodeCleaner.CONNECTED_NODE_SET_TIME
            )
            if (
                    last_connected_node_timestamp is None
                    or (
                    datetime.utcnow()
                    - datetime.fromtimestamp(float(last_connected_node_timestamp))
            ).total_seconds()
                    > NodeCleaner.READY_NODE_CHECK_PERIOD_IN_SECONDS
            ):
                current_node_list = {
                    node.metadata.name
                    for node in self._kube_client.list_node().items
                    if node.status.conditions[-1].type == "Ready"
                }

                self._current_node_set.clear()
                self._current_node_set.update(current_node_list)
                self._redis_client.set(
                    NodeCleaner.CONNECTED_NODE_SET_TIME,
                    str(datetime.utcnow().timestamp()),
                )
            else:
                current_node_list = self._current_node_set.to_set()

        return current_node_list

    def gracefully_kill_node(self, node_name: str):
        self._thread_pool.submit(self.clean_up_node, node_name, True)
