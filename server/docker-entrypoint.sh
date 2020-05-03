#!/bin/bash

set -e



until PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER postgres -c '\q'; do
  echo "Postgres is unavailable - sleeping"
  sleep 5
done

if PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER -lqt | cut -d \| -f 1 | grep -qw $PANGEA_DB_NAME; then
  echo 'DB already exist!'
else
  echo "Creating DB"
  PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER -c "CREATE DATABASE $PANGEA_DB_NAME;"

  echo "Creating Spatial Extension"
  PGPASSWORD=$PANGEA_DB_PASS psql -h $PANGEA_DB_HOST -p $PANGEA_DB_PORT -U $PANGEA_DB_USER $PANGEA_DB_NAME -c "CREATE EXTENSION postgis;"

  echo "Creatin Schemas"
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

fi

echo "Collect static"
python manage.py collectstatic --no-input 

echo "Starting server"
gunicorn --workers=3 pangea.wsgi:application --log-level=debug --error-logfile /code/logs/gunicorn.errors --log-file /code/logs/gunicorn.log -b 0.0.0.0:8000