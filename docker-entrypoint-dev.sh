#!/bin/bash

set -e

until PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER $PANGEA_DB_NAME -c '\q'; do
  echo "Postgres is unavailable - sleeping"
  sleep 1
done



# Building migrations
echo "Building migrations"
python manage.py makemigrations

# Apply database migrations
echo "Apply database migrations"
python manage.py migrate

# Creating admin
echo "Creating admin"
python -m django_createsuperuser "$PANGEA_ADM_USER" "$PANGEA_ADM_PASS"

# Start server
echo "Starting server"
python manage.py runserver 0.0.0.0:8000

