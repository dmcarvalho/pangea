#!/bin/bash

set -e

until PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER $PANGEA_DB_NAME -c '\q'; do
  echo "Postgres is unavailable - sleeping"
  sleep 5
done

PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER $PANGEA_DB_NAME -c 'CREATE SCHEMA IF NOT EXISTS imported_data;' -c '\q';
PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER $PANGEA_DB_NAME -c 'CREATE SCHEMA IF NOT EXISTS layers_published;' -c '\q';

echo "Building migrations"
python manage.py makemigrations

echo "Apply database migrations"
python manage.py migrate

echo "Adding generalizationparams data"
PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER $PANGEA_DB_NAME -f data/pangea_admin_generalizationparams.sql

echo "Adding functions"
PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER $PANGEA_DB_NAME -f data/functions.sql

echo "Creating admin"
python -m django_createsuperuser "$PANGEA_ADM_USER" "$PANGEA_ADM_PASS"

echo "Collect static"
python manage.py collectstatic --no-input 

echo "Starting server"
gunicorn --workers=3 pangea.wsgi:application -b 0.0.0.0:8000
