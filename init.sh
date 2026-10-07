docker compose down

# Reset the database.
rm -fr /tmp/paimon/*

# remove-orphans is for those cases where I modify the compose file.
docker compose up -d --no-recreate --remove-orphans

# router example: produce and store in data lake.
python ./producers/router.py > /dev/null

python ./src/init.py