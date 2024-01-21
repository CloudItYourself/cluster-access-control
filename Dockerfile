FROM registry.gitlab.com/ronen48/ciy/python-311:latest

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN pip3 install keyring
RUN echo "glpat-3zqVQwKxwU_Qsvc_8fw8" | keyring set https://gitlab.com/api/v4/projects/54080196/packages/pypi ci-python-package-user

RUN pip3 install --extra-index-url=https://gitlab.com/api/v4/projects/54080196/packages/pypi ci-python-package-user --keyring-provider /usr/src/app

RUN chmod -R +777 /tmp

ENTRYPOINT python3 ./cluster_access_control/main.py
