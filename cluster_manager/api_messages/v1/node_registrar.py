from pydantic import BaseModel


class NodeDetails(BaseModel):
    name: str
    id: str

    def __hash__(self):
        return hash(self.name + self.id)


class RegistrationDetails(BaseModel):
    k8s_ip: str
    k8s_port: int
    k8s_token: str

    vpn_ip: str
    vpn_port: int
    vpn_token: str
