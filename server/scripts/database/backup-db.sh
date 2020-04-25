#!/bin/bash
ENV_FILE="server/scripts/docker/development/db.env"
COMPOSE_FILE="docker-compose-dev.yaml"
source $ENV_FILE
docker-compose -f $COMPOSE_FILE exec db pg_dump -Ft -U $POSTGRES_USER -d $POSTGRES_DB -f "./db-backup/$POSTGRES_DB-`date +%Y-%m-%d-%H.%M.%S`.dump"
