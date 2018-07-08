FROM python:alpine3.6

#ADD debian.sources.list    /etc/apt/sources.list
#RUN echo "deb-src http://security.debian.org/ jessie/updates main contrib non-free" >> /etc/apt/sources.list
RUN apk add --no-cache openssl-dev libffi-dev make gcc musl-dev libxml2-dev libxslt-dev
#RUN mkdir -p ~/.pip && echo "[global]\nindex-url = http://mirrors.aliyun.com/pypi/simple/\n[install]\ntrusted-host = mirrors.aliyun.com" > ~/.pip/pip.conf

COPY . /opt/htdocs/crawler_appstores
WORKDIR /opt/htdocs/crawler_appstores
RUN pip install -r requirements.txt

EXPOSE 6000
CMD python start.py
