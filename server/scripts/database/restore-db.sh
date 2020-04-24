#!/bin/bash
ENV_FILE="server/scripts/docker/development/db.env"
COMPOSE_FILE="docker-compose-dev.yaml"
source $ENV_FILE

FILE_NAME=`ls -t1 data/develpment/db-backup |  head -n 1`
echo "restoring last generated backup"
docker-compose -f $COMPOSE_FILE exec db pg_restore --no-owner --clean -U $POSTGRES_USER -d $POSTGRES_DB /db-backup/$FILE_NAME
