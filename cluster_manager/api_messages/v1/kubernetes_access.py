from pydantic import BaseModel

class KubernetesAccessResponse(BaseModel):
    k8s_ip: str
    k8s_port: int
    k8s_token: str
