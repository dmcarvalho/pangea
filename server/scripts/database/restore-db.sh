#!/bin/bash
ENV_FILE="server/scripts/docker/development/db.env"
COMPOSE_FILE="docker-compose-dev.yaml"
source $ENV_FILE

FILE_NAME=`ls -t1 data/develpment/db-backup |  head -n 1`
echo "Stopping APP"
docker-compose -f $COMPOSE_FILE stop django

echo "Dropping Database"
docker-compose -f $COMPOSE_FILE exec db dropdb -U $POSTGRES_USER $POSTGRES_DB

echo "Creating Database"
docker-compose -f $COMPOSE_FILE exec db createdb -U $POSTGRES_USER $POSTGRES_DB

echo "Restoring last generated backup"
docker-compose -f $COMPOSE_FILE exec db pg_restore --no-owner -U $POSTGRES_USER -d $POSTGRES_DB /db-backup/$FILE_NAME

echo "Restarting APP"
docker-compose -f $COMPOSE_FILE start django