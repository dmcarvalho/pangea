FROM python:3.7.7-slim-buster

RUN apt-get update && apt-get install -y --no-install-recommends python3-dev gcc g++ postgresql-server-dev-all postgresql-client libgdal-dev

RUN mkdir /code
WORKDIR /code

COPY . .

RUN pip install -r requirements.txt

