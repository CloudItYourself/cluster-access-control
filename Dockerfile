FROM registry.gitlab.com/ronen48/ciy/python-311:latest

RUN mkdir -p /usr/src/app
COPY . /usr/src/app
WORKDIR /usr/src/app
RUN pip3 install --extra-index-url=https://ci-python-package-user:glpat-3zqVQwKxwU_Qsvc_8fw8@gitlab.com/api/v4/projects/54080196/packages/pypi/simple -r /usr/src/app/reqruirements.txt
RUN pip3 install --extra-index-url=https://ci-python-package-user:glpat-3zqVQwKxwU_Qsvc_8fw8@gitlab.com/api/v4/projects/54080196/packages/pypi/simple .

RUN chmod -R +777 /tmp

EXPOSE 8080

ENTRYPOINT python3 ./cluster_access_control/main.py
