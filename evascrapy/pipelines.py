import hashlib
import pathlib
import oss2
import ssl
import json
import random
import string
import bencode
from scrapy.pipelines.files import FilesPipeline
from scrapy.http import Request, Response
from io import BytesIO
from kafka import KafkaProducer
from minio import Minio
from mns.account import Account
from mns.queue import Message
from evascrapy.items import RawHtmlItem
from scrapy.utils.python import to_bytes

class TorrentFilePipeLine(FilesPipeline):
    @staticmethod
    def hash_to_filepath(hash: str, root_path: str, depth: int = 0, extension: str = 'torrent'):
        chunk_size = 2
        hash_chunks = [hash[i:i + chunk_size] for i in range(0, len(hash), chunk_size)]
        return [
            '/'.join([root_path] + [i for i in hash_chunks[0:int(depth)]]),
            '%s.%s' % (''.join([i for i in hash_chunks[int(depth):]]), extension)
        ]

    def file_path(self, request: Request, response: Response = None, info=None):
        if not isinstance(request, Request):
            url = request
        else:
            url = request.url
        if not hasattr(self.file_key, '_base'):
            return self.file_key(url)

        if not response:
            url_hash = hashlib.sha1(to_bytes(url)).hexdigest()
            return 'urlhash/%s%s' % (url_hash, '.torrent')

        torrent = bencode.bdecode(response.body).get('info')
        info_hash = hashlib.sha1(bencode.bencode(torrent)).hexdigest()
        spider = info.spider
        [filepath, filename] = TorrentFilePipeLine.hash_to_filepath(
            info_hash,
            '',
            spider.settings['APP_TORRENT_PIPELINE_DEPTH']
        )

        return '%s/%s' % (filepath, filename)


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
        if not isinstance(item, RawHtmlItem):
            return item

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
        if not isinstance(item, RawHtmlItem):
            return item

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
        if not isinstance(item, RawHtmlItem):
            return item

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
            'messageId': ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
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
        if not isinstance(item, RawHtmlItem):
            return item

        msg = KafkaPipeline.item_to_kafka_message(item, spider)
        future = self.get_producer(spider.settings).send(spider.settings['KAFKA_TOPIC'],
                                                         msg)
        future.get()
        return item


class AliyunMnsPipeline(object):
    _mns_producer = None

    @staticmethod
    def item_to_mns_message(item, spider) -> bytes:
        [filepath, filename] = HtmlFilePipeline.url_to_filepath(
            item['url'],
            '/'.join([spider.settings['APP_STORAGE_ROOT_PATH'], spider.name, spider.settings['APP_TASK']]),
            spider.settings['APP_STORAGE_DEPTH']
        )
        command = json.dumps({
            'messageId': ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
            'queueName': spider.settings['MNS_QUEUE_NAME'],
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
        return command

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

    def process_item(self, item, spider):
        if not isinstance(item, RawHtmlItem):
            return item

        msg = Message(AliyunMnsPipeline.item_to_mns_message(item, spider))
        future = self.get_producer(spider.settings)
        future.send_message(msg)
        return item
