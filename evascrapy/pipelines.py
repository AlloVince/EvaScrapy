import pathlib
import oss2
import ssl
import os
from urllib.parse import urlparse
from io import BytesIO
from kafka import KafkaProducer
from minio import Minio
from mns.account import Account
from mns.queue import Message
from nats.aio.client import Client as NATS
from evascrapy.items import (
    QueueBasedItem,
    RawTextItem,
    TorrentFileItem,
    render_nats_message,
    url_to_filepath,
)
import logging
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)

class LocalFilePipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.crawler = crawler
        return pipeline

    def process_item(self, item: QueueBasedItem) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        spider = self.crawler.spider
        filepath = item.to_filepath(spider)
        pathlib.Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)
        mode = 'w+' if isinstance(item, RawTextItem) else 'wb+'
        with open(filepath, mode) as f:
            f.write(
                item.to_string() if isinstance(item, RawTextItem) else item.to_bytes()
            )
        logger.info('Stored item at %s', filepath)
        return item


class AliyunOssPipeline(object):
    _oss_bucket = None

    def get_oss_bucket(self, settings: dict) -> oss2.Bucket:
        if self._oss_bucket:
            return self._oss_bucket

        auth = oss2.Auth(settings['OSS_ACCESS_KEY_ID'], settings['OSS_ACCESS_KEY_SECRET'])
        self._oss_bucket = oss2.Bucket(auth, settings['OSS_ENDPOINT'], settings['OSS_BUCKET'], connect_timeout=3.0)
        return self._oss_bucket

    def process_item(self, item: QueueBasedItem, spider) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        filepath = item.to_filepath(spider)
        self.get_oss_bucket(
            spider.settings
        ).put_object(
            key=filepath,
            data=item.to_string() if isinstance(item, RawTextItem) else item.to_bytes()
        )
        logger.info('Stored item at oss://%s/%s', spider.settings['OSS_BUCKET'], filepath)
        return item


class AwsS3Pipeline(object):
    _client = None

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline.crawler = crawler
        return pipeline

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

    def process_item(self, item: QueueBasedItem) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        spider = self.crawler.spider
        content = BytesIO(item.to_string().encode()) if isinstance(item, RawTextItem) else BytesIO(item.to_bytes())
        filepath = item.to_filepath(spider)
        self.get_client(
            spider.settings
        ).put_object(
            bucket_name=spider.settings['AWS_S3_DEFAULT_BUCKET'],
            object_name=filepath,
            data=content,
            length=content.getbuffer().nbytes,
            metadata=item.get_meta(),
        )
        logger.info(
            'Stored item at s3://%s/%s',
            spider.settings['AWS_S3_DEFAULT_BUCKET'],
            filepath,
        )
        if isinstance(item, TorrentFileItem) and spider.settings.getbool('S3_DUPEFILTER_ENABLED'):
            marker_root = spider.settings.get('S3_DUPEFILTER_ROOT_PATH')
            if not marker_root:
                raise RuntimeError(
                    'S3_DUPEFILTER_ROOT_PATH is required when S3DupeFilter is enabled'
                )
            marker_path, marker_name = url_to_filepath(
                item['url'],
                marker_root,
                depth=spider.settings.getint('S3_DUPEFILTER_DEPTH'),
                extension='json',
            )
            self.get_client(spider.settings).put_object(
                bucket_name=spider.settings['AWS_S3_DEFAULT_BUCKET'],
                object_name='/'.join([marker_path, marker_name]),
                data=BytesIO(b'{}'),
                length=2,
                content_type='application/json',
            )
        return item


class KafkaPipeline(object):
    _kafka_producer = None

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

    def process_item(self, item: QueueBasedItem, spider) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        future = self.get_producer(
            spider.settings
        ).send(
            spider.settings['KAFKA_TOPIC'],
            item.to_kafka_message(spider)
        )
        future.get()
        return item


class AliyunMnsPipeline(object):
    _mns_producer = None

    def get_producer(self, settings):
        if self._mns_producer:
            return self._mns_producer

        account = Account(
            host=settings['MNS_ACCOUNT_ENDPOINT'],
            access_id=settings['MNS_ACCESSKEY_ID'],
            access_key=settings['MNS_ACCESSKEY_SECRET'],
            logger=logging.getLogger('evascrapy.pipelines.AliyunMnsPipeline'),
            debug=False,
        )
        self._mns_producer = account.get_queue(settings['MNS_QUEUE_NAME'])
        return self._mns_producer

    def process_item(self, item, spider) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        msg = Message(item.to_mns_message(spider))
        future = self.get_producer(spider.settings)
        future.send_message(msg)
        return item


class NatsPipeline(object):
    _nats_producer = None

    async def get_producer(self, settings):
        if self._nats_producer:
            return self._nats_producer

        producer = NATS()
        await producer.connect(
            servers=settings['NATS_SERVER_LIST'],
            connect_timeout=settings['NATS_CONNECT_TIMEOUT'],
            allow_reconnect=settings['NATS_ALLOW_RECONNECT'],
            max_reconnect_attempts=settings['NATS_MAX_RECONNECT_ATTEMPTS'],
        )
        self._nats_producer = producer
        return producer

    async def process_item(self, item, spider) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        producer = await self.get_producer(spider.settings)
        await producer.publish(
            spider.settings['NATS_SUBJECT'],
            render_nats_message(
                spider.settings['NATS_MESSAGE_TEMPLATE'], item, spider
            ).encode(),
        )
        return item


class ElasticDupePipeline(object):
    _es = None

    def get_elastic(self, settings) -> Elasticsearch:
        if self._es:
            return self._es

        url = urlparse(settings['TORRENT_FILE_ELASTIC_DUPE_URL'])
        http_auth = (url.username, url.password) if url.username and url.password else None
        self._es = Elasticsearch(
            url.geturl(),
            basic_auth=http_auth,
        )

        return self._es

    def process_item(self, item, spider) -> QueueBasedItem or None:
        if not isinstance(item, TorrentFileItem):
            return item

        if self.get_elastic(spider.settings).exists(
                index=spider.settings['TORRENT_FILE_ELASTIC_DUPE_INDICE'],
                id=item.get_info_hash(),
        ):
            logger.info('Torrent item %s ignored by pipelines.ElasticDupePipeline', item.get_info_hash())
            return None

        return item
