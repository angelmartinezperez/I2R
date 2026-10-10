# Intended to be run from root instead of the devcontainer.
docker build --file .devcontainer/Dockerfile --build-context . --tag=i2r_devcontainer
docker run -it --rm --name i2r_devcontainer \
    -v i2r_paimon:/tmp/paimon \
    -v $(pwd):/workspaces/I2R \
    -v /var/run/docker.sock:/var/run/docker.sock \
    --network i2r-network \
    i2r_devcontainer