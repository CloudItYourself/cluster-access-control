import platform
import sys

import uvicorn
from fastapi import FastAPI

import subprocess
import os

from cluster_manager.web_app.node import NodeRegistrar


def main():
    print("=" * 40, "System Information", "=" * 40)
    uname = platform.uname()
    print(f"System: {uname.system}")
    print(f"Node Name: {uname.node}")
    print(f"Release: {uname.release}")
    print(f"Version: {uname.version}")
    print(f"Machine: {uname.machine}")
    print(f"Processor: {uname.processor}")

    app = FastAPI()
    node_api = NodeRegistrar()
    app.include_router(node_api.router)
    uvicorn.run(app, host='0.0.0.0', port=8080)


if __name__ == '__main__':
    main()
