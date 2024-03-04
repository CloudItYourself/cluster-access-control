import datetime
import time
from typing import Final

from redis.client import Redis

from cluster_access_control.database_usage_statistics.postgres_handling import PostgresHandler
from cluster_access_control.node_cleaner.node_cleaner import NodeCleaner

from fastapi import APIRouter
from starlette.exceptions import HTTPException
from ciy_backend_libraries.api.cluster_access.v1.node_registrar import (
    NodeDetails,
    RegistrationDetails,
)
from cluster_access_control.utilities.environment import ClusterAccessConfiguration
from cluster_access_control.utilities.redis_utils import redis_test_and_set


class NodeRegistrar:
    NODE_REGISTER_COOLDOWN_IN_SECONDS: Final[int] = 10

    def __init__(self, node_cleaner: NodeCleaner, postgres_handler: PostgresHandler):
        self.router = APIRouter()
        self._node_cleaner = node_cleaner
        self._postgres_handler = postgres_handler
        self._environment = ClusterAccessConfiguration()
        self._redis_client = Redis.from_url(f"{self._environment.get_redis_url()}/0")
        self.router.add_api_route(
            "/api/v1/node_token", self.request_token, methods=["POST"]
        )
        self.router.add_api_route(
            "/api/v1/node_keepalive/{node_id}",
            self.node_keepalive_message,
            methods=["PUT"],
        )
        self.router.add_api_route(
            "/api/v1/node_exists/{node_name}",
            self.is_node_online,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/api/v1/node_exists/{node_name}",
            self.request_graceful_shutdown,
            methods=["POSTS"],
        )
        self._registered_nodes = dict()

    async def request_token(self, node_details: NodeDetails) -> RegistrationDetails:
        hashed_node_details = str(node_details)
        if (
                hashed_node_details in self._registered_nodes
                and time.time() - self._registered_nodes[hashed_node_details]
                < NodeRegistrar.NODE_REGISTER_COOLDOWN_IN_SECONDS
        ):
            raise HTTPException(
                status_code=429,
                detail=f"Error! node with name: {node_details.name} and id: {node_details.id} was registered lately",
            )
        self._postgres_handler.register_node(str(node_details))

        self._registered_nodes[hashed_node_details] = time.time()
        return RegistrationDetails(
            k8s_ip=self._environment.get_cluster_host(),
            k8s_port=ClusterAccessConfiguration.CLUSTER_PORT,
            k8s_token=self._environment.get_node_access_token(),
            vpn_ip=self._environment.get_cluster_host(),
            vpn_port=ClusterAccessConfiguration.VPN_PORT,
            vpn_token=await self._environment.get_vpn_join_token_key(),
        )

    def node_keepalive_message(self, node_id: str):
        self._node_cleaner.update_node_keepalive(node_id)
        current_time = datetime.datetime.utcnow()

        if redis_test_and_set(self._redis_client, node_id,
                              f"{node_id}_{current_time.weekday()}_{PostgresHandler.get_seconds_since_midnight(current_time)}",
                              PostgresHandler.SECONDS_PER_CHECK_IN * 2):
            self._postgres_handler.update_node(node_id, current_time)

    def is_node_online(self, node_name: str):
        if node_name in self._node_cleaner.get_online_nodes():
            return True
        raise HTTPException(status_code=404)

    def request_graceful_shutdown(self, node_name: str):
        if node_name not in self._node_cleaner.get_online_nodes():
            raise HTTPException(status_code=404)

        self._node_cleaner.gracefully_kill_node(node_name)
