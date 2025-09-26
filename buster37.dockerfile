FROM python:3.7-slim-buster

RUN sed -i 's|deb.debian.org/debian|archive.debian.org/debian|g' /etc/apt/sources.list \
 && sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list \
 && apt-get update \
 && apt-get -y install git \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /
RUN pip install --upgrade pip && pip install -r requirements.txt
