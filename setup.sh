docker network inspect i2r-network >/dev/null 2>&1 ||
    docker network create i2r-network

docker volume inspect i2r_paimon >/dev/null 2>&1 ||
    docker volume create i2r_paimon
