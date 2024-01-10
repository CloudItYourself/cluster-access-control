import uvicorn
from fastapi import FastAPI

from cluster_manager.web_app.cluster_access import ClusterAccess
from cluster_manager.web_app.node import NodeRegistrar
from cluster_manager.web_app.temporary_deployment import TemporaryDeployment


def main():
    app = FastAPI()
    node_api = NodeRegistrar()
    cluster_access_api = ClusterAccess()
    temporary_registration_api = TemporaryDeployment()
    app.include_router(node_api.router)
    app.include_router(cluster_access_api.router)
    app.include_router(temporary_registration_api.router)

    uvicorn.run(app, host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
