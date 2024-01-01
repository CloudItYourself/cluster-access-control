from setuptools import setup, find_packages

setup(
    name="cluster_manager",
    version="0.0.1",
    packages=find_packages(),
    install_requires=['python-socketio', 'kubernetes']  # Todo: load requirements.txt
)
