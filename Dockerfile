FROM python:3.14-alpine

RUN apk add --no-cache \
    openssl-dev \
    libffi-dev \
    make \
    gcc \
    musl-dev \
    libxml2-dev \
    libxslt-dev \
    git \
    tzdata

ENV TZ Asia/Shanghai

WORKDIR /opt/htdocs/evascrapy
COPY pyproject.toml .
RUN pip install --no-cache-dir .
COPY . .

EXPOSE 6000
CMD python start.py
