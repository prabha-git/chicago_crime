FROM python:3.7-slim

ENV PYTHONBUFFERED True


COPY requirements.txt ./


RUN pip install -r requirements.txt

ENV APP_HOME /APP_HOME

WORKDIR $APP_HOME 

COPY . ./


CMD exec gunicorn --bind :$PORT --workers 1 --threads 2 --timeout 0 main:app