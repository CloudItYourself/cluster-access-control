import time
from typing import Final, Union

from fastapi import APIRouter
from starlette.exceptions import HTTPException
from cluster_manager.api_messages.v1.kubernetes_access import KubernetesAccessResponse
from cluster_manager.api_messages.v1.node_registrar import NodeDetails, RegistrationDetails
from cluster_manager.api_messages.v1.temporary_deployment import TemporaryDeploymentRequest


class TemporaryDeployment:
    def __init__(self):
        self.router = APIRouter()
        self.router.add_api_route("/api/v1/temporary_deployment", self.temporary_deployment_request, methods=["PUT"])

    async def temporary_deployment_request(self, deployment_request: TemporaryDeploymentRequest) -> bool:
        return True
