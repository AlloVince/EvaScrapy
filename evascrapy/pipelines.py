import pathlib
import oss2
import ssl
import os
from io import BytesIO
from kafka import KafkaProducer
from minio import Minio
from mns.account import Account
from mns.queue import Message
from evascrapy.items import QueueBasedItem, RawTextItem
import urllib3

# default timeout for minio
urllib3.Timeout.DEFAULT_TIMEOUT = 5.0


class LocalFilePipeline(object):
    def process_item(self, item: QueueBasedItem, spider) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        filepath = item.to_filepath(spider)
        pathlib.Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)
        mode = 'w+' if isinstance(item, RawTextItem) else 'wb+'
        with open(filepath, mode) as f:
            f.write(
                item.to_string() if isinstance(item, RawTextItem) else item.to_bytes()
            )
        return item


class AliyunOssPipeline(object):
    _oss_bucket = None

    def get_oss_bucket(self, settings: dict) -> oss2.Bucket:
        if self._oss_bucket:
            return self._oss_bucket

        auth = oss2.Auth(settings['OSS_ACCESS_KEY_ID'], settings['OSS_ACCESS_KEY_SECRET'])
        self._oss_bucket = oss2.Bucket(auth, settings['OSS_ENDPOINT'], settings['OSS_BUCKET'])
        return self._oss_bucket

    def process_item(self, item: QueueBasedItem, spider) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        self.get_oss_bucket(
            spider.settings
        ).put_object(
            key=item.to_filepath(spider),
            data=item.to_string() if isinstance(item, RawTextItem) else item.to_bytes()
        )
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

    def process_item(self, item: QueueBasedItem, spider) -> QueueBasedItem:
        if not isinstance(item, QueueBasedItem):
            return item

        content = BytesIO(item.to_string().encode()) if isinstance(item, RawTextItem) else BytesIO(item.to_bytes())
        self.get_client(
            spider.settings
        ).put_object(
            bucket_name=spider.settings['AWS_S3_DEFAULT_BUCKET'],
            object_name=item.to_filepath(spider),
            data=content,
            length=content.getbuffer().nbytes,
            metadata=item.get_meta(),
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
            settings['MNS_ACCOUNT_ENDPOINT'],
            settings['MNS_ACCESSKEY_ID'],
            settings['MNS_ACCESSKEY_SECRET']
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
