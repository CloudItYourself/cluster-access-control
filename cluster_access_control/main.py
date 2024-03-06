import threading

import uvicorn
from fastapi import FastAPI

from cluster_access_control.database_usage_statistics.postgres_handling import PostgresHandler
from cluster_access_control.node_cleaner.node_cleaner import NodeCleaner
from cluster_access_control.utilities.response_caching import initialize_fastapi_cache
from cluster_access_control.web_app.cluster_access import ClusterAccess
from cluster_access_control.web_app.node import NodeRegistrar
from cluster_access_control.web_app.node_statistics import NodeStatistics


def main():
    app = FastAPI()
    initialize_fastapi_cache()
    postgres_handler = PostgresHandler()
    node_cleaner = NodeCleaner(postgres_handler)
    node_cleaner_thread = threading.Thread(target=node_cleaner.delete_stale_nodes)
    node_cleaner_thread.start()

    node_api = NodeRegistrar(node_cleaner, postgres_handler)
    statistics_api = NodeStatistics(postgres_handler)
    cluster_access_api = ClusterAccess()
    app.include_router(node_api.router)
    app.include_router(cluster_access_api.router)
    app.include_router(statistics_api.router)
    uvicorn.run(app, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
