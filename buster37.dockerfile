FROM python:3.7-slim-buster

RUN apt-get update && apt-get -y install git

COPY requirements.txt /
RUN pip install --upgrade pip && pip install -r requirements.txt
