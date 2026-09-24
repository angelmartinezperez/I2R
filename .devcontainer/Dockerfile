# Based on https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/resource-providers/standalone/docker/
# We need flink for the flink run.
FROM flink:2.2.1

# apt-get gives an error if the user is not root due to missing permissions.
USER root

COPY pyflink/dependencies/* /opt/flink/lib/

# Install python3, pip3, git and docker.
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev git docker.io && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# From https://github.com/microsoft/vscode-dev-containers/blob/main/containers/python-3/.devcontainer/Dockerfile
# This installs pyflink. We use --break-system-packages because we do not need venvs inside the container.
COPY requirements.txt /tmp/pip-tmp/
RUN pip3 --disable-pip-version-check --no-cache-dir install --break-system-packages -r /tmp/pip-tmp/requirements.txt \
    && rm -rf /tmp/pip-tmp
