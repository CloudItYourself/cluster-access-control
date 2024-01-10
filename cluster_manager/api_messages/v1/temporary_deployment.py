from pydantic import BaseModel


class TemporaryDeploymentRequest(BaseModel):
    deployment_yaml: str
    erasure_timeout_in_seconds: int


class TemporaryDeploymentRefresh(BaseModel):
    id: str
