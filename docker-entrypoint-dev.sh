#!/bin/bash


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

