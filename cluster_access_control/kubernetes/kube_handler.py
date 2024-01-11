import base64
import json
import logging
import string
import random
import time
from typing import Final, Dict, Optional

import kubernetes
from kubernetes import client
from kubernetes.client import ApiException

from cluster_access_control.utilities.environment import ClusterAccessConfiguration
from cluster_access_control.utilities.messages import PodDetails, NamespaceDetails


class KubeHandler:
    POD_MAX_STARTUP_TIME_IN_MINUTES: Final[int] = 6
    POD_DELETION_TIME_IN_MINUTES: Final[int] = 1
    K3S_MAX_STARTUP_TIME_IN_SECONDS: Final[int] = 360

    def __init__(self):
        self._environment = ClusterAccessConfiguration()

        configuration = client.Configuration()
        configuration.cert_file = self._environment.get_kubernetes_cert()
        configuration.key_file = self._environment.get_kubernetes_key()
        configuration.host = f'https://{self._environment.get_cluster_host()}:{ClusterAccessConfiguration.CLUSTER_PORT}'

        self._kube_client = client.CoreV1Api(client.ApiClient(configuration))

    @staticmethod
    def generate_random_string(length: int) -> str:
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))

    def verify_pod_successful_startup(self, pod_name: str, namespace: str) -> bool:
        start_time = time.time()
        while time.time() - start_time < KubeHandler.POD_MAX_STARTUP_TIME_IN_MINUTES * 60:
            try:
                api_response = self._kube_client.read_namespaced_pod(pod_name, namespace=namespace)
                if api_response.status.phase != 'Pending':
                    return api_response.status.phase == "Running"
                time.sleep(0.2)
            except ApiException as e:
                logging.error(f"Exception when calling CoreV1Api->read_namespaced_pod: {e}")
                return False
        return False

    def run_pod(self, image_name: str, version: str, environment: Dict[str, str], namespace: str) -> Optional[str]:
        generated_image_name = f'{image_name}-{self.generate_random_string(10)}'
        full_image_name = f'{image_name}:{version}'
        container = client.V1Container(
            name=generated_image_name,
            image=full_image_name,
            env=[client.V1EnvVar(name=key, value=value) for key, value in environment.items()]
        )

        pod_spec = client.V1PodSpec(
            restart_policy='Never',
            containers=[container]
        )

        pod_manifest = client.V1Pod(
            metadata=client.V1ObjectMeta(name=generated_image_name),
            spec=pod_spec
        )

        try:
            self._kube_client.create_namespaced_pod(body=pod_manifest, namespace=namespace)
            startup_successful = self.verify_pod_successful_startup(pod_name=generated_image_name, namespace=namespace)

            if not startup_successful:
                logging.error(f"Pod creation was not successful.")
                self.delete_pod(pod_name=generated_image_name, namespace=namespace)
                return None
            return generated_image_name

        except ApiException as e:
            logging.error(f"Exception when calling CoreV1Api->create_namespaced_pod: {e}")
            return None

    def create_namespace(self, namespace: str) -> bool:
        try:
            namespaces = self._kube_client.list_namespace()
            if namespace in [n.metadata.name for n in namespaces.items]:
                return True

            api_response = self._kube_client.create_namespace(
                client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)))
            return api_response.status == "Success"
        except ApiException as e:
            logging.error(f"Exception when calling CoreV1Api->create_namespace: {e}")
            return False

    def delete_pod(self, pod_name: str, namespace: str) -> bool:
        try:
            self._kube_client.delete_namespaced_pod(pod_name, namespace)
            start_time = time.time()
            while time.time() - start_time < KubeHandler.POD_DELETION_TIME_IN_MINUTES * 60:
                try:
                    self._kube_client.read_namespaced_pod(pod_name, namespace)
                    time.sleep(0.2)
                except ApiException as e:
                    return True
            return False
        except ApiException as e:
            logging.error(f"Exception when calling CoreV1Api->delete_namespaced_pod: {e}")
            return False

    def delete_all_pods_in_namespace(self, namespace: str) -> bool:
        try:
            self._kube_client.delete_collection_namespaced_pod(namespace)
            start_time = time.time()
            while time.time() - start_time < KubeHandler.POD_DELETION_TIME_IN_MINUTES * 60:
                pods = self._kube_client.list_namespaced_pod(namespace)
                if len(pods.items) == 0:
                    return True
                time.sleep(0.2)
            return False
        except ApiException as e:
            logging.error(f"Exception when calling CoreV1Api->delete_collection_namespaced_pod: {e}")
            return False

    def get_namespace_details(self, namespace: str):
        try:
            namespace_item_details = []
            custom_object_api = client.CustomObjectsApi()
            resp = custom_object_api.list_cluster_custom_object('metrics.k8s.io', 'v1beta1', 'pods')
            for item in resp['items']:
                if item['metadata']['namespace'] == namespace:
                    current_metrics = PodDetails(pod_name=item['metadata']['name'],
                                                 cpu_utilization=item['containers'][0]['usage']['cpu'],
                                                 memory_utilization=item['containers'][0]['usage']['memory'],
                                                 measurement_window=item['window'])
                    namespace_item_details.append(current_metrics)
            return NamespaceDetails(pod_details=namespace_item_details)
        except ApiException as e:
            logging.error(f"Exception when calling CoreV1Api->list_cluster_custom_object: {e}")
            return None

    def pre_load_image(self, image_name: str, version: str, namespace: str, url: str, user: str, access_key: str) -> bool:
        secret_name = f'{namespace}-{user}'
        app_name = f'{namespace}-{image_name.split("/")[-1]}-prepuller'
        ds_name = f'{namespace}-{image_name.split("/")[-1]}-ds'
        if not self._secret_exists(secret_name=secret_name, namespace=namespace):
            try:
                self._create_namespaced_secret(secret_name=secret_name, namespace=namespace, url=url,
                                               access_key=access_key)
            except ApiException as e:
                logging.error(f"Exception when calling CoreV1Api->create_namespaced_secret: {e}")
                return False

        daemonset_spec = client.V1DaemonSetSpec(
            selector=client.V1LabelSelector(match_labels={"app": app_name}),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": app_name}),
                spec=client.V1PodSpec(
                    image_pull_secrets=[client.V1LocalObjectReference(name=secret_name)],
                    init_containers=[
                        client.V1Container(
                            name=app_name,
                            image=f"{image_name}:{version}",
                            command=["/bin/sh", "-c", "'true'"]
                        )
                    ],
                    containers=[
                        client.V1Container(
                            name="pause",
                            image="gcr.io/google_containers/pause"
                        )
                    ]
                )
            )
        )

        daemonset = client.V1DaemonSet(
            api_version="apps/v1",
            kind="DaemonSet",
            metadata=client.V1ObjectMeta(name=ds_name),
            spec=daemonset_spec
        )
        self._delete_ds_if_exists(ds_name=ds_name, namespace=namespace)

        app_api = kubernetes.client.AppsV1Api()
        try:
            app_api.create_namespaced_daemon_set(namespace=namespace, body=daemonset)
        except ApiException as e:
            logging.error(f"Exception when calling app_api->create_namespaced_daemon_set: {e}")
            return False

        return True

    def _create_namespaced_secret(self, secret_name: str, namespace: str, url: str, access_key: str):
        docker_config = {
            "auths": {
                url: {
                    "username": "usr",
                    "password": access_key,
                    "auth": base64.b64encode(f"usr:{access_key}".encode()).decode()
                }
            }
        }

        self._kube_client.create_namespaced_secret(
            namespace=namespace,
            body=client.V1Secret(
                metadata=client.V1ObjectMeta(name=secret_name),
                type='kubernetes.io/dockerconfigjson',
                data={".dockerconfigjson": base64.b64encode(json.dumps(docker_config).encode()).decode()}
            )
        )

    def _secret_exists(self, secret_name: str, namespace: str) -> bool:
        try:
            return self._kube_client.read_namespaced_secret(secret_name, namespace) is not None
        except ApiException as e:
            return False

    def _delete_ds_if_exists(self, ds_name: str, namespace: str) -> None:
        try:
            app_api = kubernetes.client.AppsV1Api()
            app_api.delete_namespaced_daemon_set(ds_name, namespace)
        except ApiException as e:
            pass


if __name__ == '__main__':
    xd = KubeHandler()
    hi = xd.get_namespace_details('monitoring')
