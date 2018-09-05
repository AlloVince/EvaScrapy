import hashlib
import pathlib
import oss2
import ssl
import json
from io import BytesIO
from kafka import KafkaProducer
from minio import Minio

from evascrapy.items import RawHtmlItem


class HtmlFilePipeline(object):
    @staticmethod
    def url_to_filepath(url, root_path: str, depth=0):
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        chunk_size = 2
        hash_chunks = [url_hash[i:i + chunk_size] for i in range(0, len(url_hash), chunk_size)]
        return [
            '/'.join([root_path] + [i for i in hash_chunks[0:int(depth)]]),
            '%s.html' % ''.join([i for i in hash_chunks[int(depth):]])
        ]

    def process_item(self, item: RawHtmlItem, spider) -> RawHtmlItem:
        [filepath, filename] = HtmlFilePipeline.url_to_filepath(
            item['url'],
            '/'.join([spider.settings['APP_STORAGE_ROOT_PATH'], spider.name, spider.settings['APP_TASK']]),
            spider.settings['APP_STORAGE_DEPTH']
        )
        pathlib.Path(filepath).mkdir(parents=True, exist_ok=True)
        with open('%s/%s' % (filepath, filename), 'w+') as f:
            f.write(item.to_string())
        return item


class AliyunOssPipeline(object):
    _oss_bucket = None

    def get_oss_bucket(self, settings: dict) -> oss2.Bucket:
        if self._oss_bucket:
            return self._oss_bucket

        auth = oss2.Auth(settings['OSS_ACCESS_KEY_ID'], settings['OSS_ACCESS_KEY_SECRET'])
        self._oss_bucket = oss2.Bucket(auth, settings['OSS_ENDPOINT'], settings['OSS_BUCKET'])
        return self._oss_bucket

    def process_item(self, item: RawHtmlItem, spider) -> RawHtmlItem:
        [filepath, filename] = HtmlFilePipeline.url_to_filepath(
            item['url'],
            '/'.join([spider.settings['APP_STORAGE_ROOT_PATH'], spider.name, spider.settings['APP_TASK']]),
            spider.settings['APP_STORAGE_DEPTH']
        )
        self.get_oss_bucket(spider.settings).put_object('%s/%s' % (filepath, filename), item.to_string())
        return item


class AwsS3Pipeline(object):
    _client = None

    def get_client(self, settings) -> Minio:
        if self._client:
            return self._client

        client = Minio(
            settings['AWS_S3_ENDPOINT'],
            access_key=settings['AWS_S3_ACCESS_KEY'],
            secret_key=settings['AWS_S3_ACCESS_SECRET'],
            region=settings['AWS_S3_REGION'],
            secure=settings['AWS_S3_ACCESS_SECURE']
        )
        self._client = client
        return client

    def process_item(self, item: RawHtmlItem, spider) -> RawHtmlItem:
        [filepath, filename] = HtmlFilePipeline.url_to_filepath(
            item['url'],
            '/'.join([spider.settings['APP_STORAGE_ROOT_PATH'], spider.name, spider.settings['APP_TASK']]),
            spider.settings['APP_STORAGE_DEPTH']
        )
        content = BytesIO(item.to_string().encode())
        self.get_client(spider.settings).put_object(spider.settings['AWS_S3_DEFAULT_BUCKET'],
                                                    '%s/%s' % (filepath, filename),
                                                    content,
                                                    content.getbuffer().nbytes)
        return item


class KafkaPipeline(object):
    _kafka_producer = None

    @staticmethod
    def item_to_kafka_message(item: RawHtmlItem, spider) -> bytes:
        [filepath, filename] = HtmlFilePipeline.url_to_filepath(
            item['url'],
            '/'.join([spider.settings['APP_STORAGE_ROOT_PATH'], spider.name, spider.settings['APP_TASK']]),
            spider.settings['APP_STORAGE_DEPTH']
        )
        command = json.dumps({
            'messageId': '123',
            'queueName': spider.settings['KAFKA_TOPIC'],
            'content': {
                'name': 'etl:%s' % spider.name,
                'spec': {
                    'storage': spider.settings['APP_STORAGE'],
                    'uri': '/'.join([filepath, filename])
                }
            },
            'command': 'etl:%s --storage=%s --uri=%s' % (
                spider.name, spider.settings['APP_STORAGE'], '/'.join([filepath, filename]))
        })
        return bytes(command, encoding='utf8')

    def get_producer(self, settings):
        if self._kafka_producer:
            return self._kafka_producer

        if settings['KAFKA_SSL_ENABLE']:
            context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(settings['KAFKA_SASL_CA_CERT_LOCATION'])
            self._kafka_producer = KafkaProducer(
                bootstrap_servers=settings['KAFKA_SERVER_STRING'].split(','),
                sasl_mechanism=settings['KAFKA_SASL_MECHANISM'],
                ssl_context=context,
                security_protocol=settings['KAFKA_SECURITY_PROTOCOL'],
                api_version=(0, 10),
                retries=5,
                sasl_plain_username=settings['KAFKA_SASL_PLAIN_USERNAME'],
                sasl_plain_password=settings['KAFKA_SASL_PLAIN_PASSWORD']
            )
        else:
            self._kafka_producer = KafkaProducer(
                bootstrap_servers=settings['KAFKA_SERVER_STRING'].split(','),
                api_version=(0, 10),
                retries=5,
            )
        return self._kafka_producer

    def process_item(self, item: RawHtmlItem, spider) -> RawHtmlItem:
        msg = KafkaPipeline.item_to_kafka_message(item, spider)
        future = self.get_producer(spider.settings).send(spider.settings['KAFKA_TOPIC'],
                                                         msg)
        future.get()
        return item
