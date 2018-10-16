import pathlib
import oss2
import ssl
import os
from io import BytesIO
from kafka import KafkaProducer
from minio import Minio
from mns.account import Account
from mns.queue import Message
from evascrapy.items import RawTextItem, TorrentFileItem


class TorrentFilePipeLine(object):
    @staticmethod
    def hash_to_filepath(hash: str, root_path: str, depth: int = 0, extension: str = 'torrent'):
        chunk_size = 2
        hash_chunks = [hash[i:i + chunk_size] for i in range(0, len(hash), chunk_size)]
        return [
            '/'.join([root_path] + [i for i in hash_chunks[0:int(depth)]]),
            '%s.%s' % (''.join([i for i in hash_chunks[int(depth):]]), extension)
        ]

    def process_item(self, item: TorrentFileItem, spider) -> TorrentFileItem:
        if not isinstance(item, TorrentFileItem):
            return item

        [filepath, filename] = TorrentFilePipeLine.hash_to_filepath(
            item.get_info_hash(),
            '/'.join([spider.settings['TORRENT_FILE_PIPELINE_ROOT_PATH']]),
            spider.settings['TORRENT_FILE_PIPELINE_DEPTH']
        )
        print(filepath, filename)
        pathlib.Path(filepath).mkdir(parents=True, exist_ok=True)
        with open('/'.join([filepath, filename]), 'wb') as f:
            f.write(item['body'])

        return item


# class TorrentFilePipeLine(FilesPipeline):
#     @staticmethod
#     def hash_to_filepath(hash: str, root_path: str, depth: int = 0, extension: str = 'torrent'):
#         chunk_size = 2
#         hash_chunks = [hash[i:i + chunk_size] for i in range(0, len(hash), chunk_size)]
#         return [
#             '/'.join([root_path] + [i for i in hash_chunks[0:int(depth)]]),
#             '%s.%s' % (''.join([i for i in hash_chunks[int(depth):]]), extension)
#         ]
#
#     def file_path(self, request: Request, response: Response = None, info=None):
#         if not isinstance(request, Request):
#             url = request
#         else:
#             url = request.url
#         if not hasattr(self.file_key, '_base'):
#             return self.file_key(url)
#
#         if not response:
#             url_hash = hashlib.sha1(to_bytes(url)).hexdigest()
#             return 'urlhash/%s%s' % (url_hash, '.torrent')
#
#         torrent = bencode.bdecode(response.body).get('info')
#         info_hash = hashlib.sha1(bencode.bencode(torrent)).hexdigest()
#         spider = info.spider
#         [filepath, filename] = TorrentFilePipeLine.hash_to_filepath(
#             info_hash,
#             '',
#             spider.settings['TORRENT_FILE_PIPELINE_DEPTH']
#         )
#
#         return '%s/%s' % (filepath, filename)
#

class HtmlFilePipeline(object):
    def process_item(self, item: RawTextItem, spider) -> RawTextItem:
        if not isinstance(item, RawTextItem):
            return item

        filepath = item.to_filepath(spider)
        pathlib.Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w+') as f:
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

    def process_item(self, item: RawTextItem, spider) -> RawTextItem:
        if not isinstance(item, RawTextItem):
            return item

        self.get_oss_bucket(
            spider.settings
        ).put_object(
            item.to_filepath(spider), item.to_string()
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

    def process_item(self, item: RawTextItem, spider) -> RawTextItem:
        if not isinstance(item, RawTextItem):
            return item

        content = BytesIO(item.to_string().encode())
        self.get_client(
            spider.settings
        ).put_object(
            spider.settings['AWS_S3_DEFAULT_BUCKET'],
            item.to_filepath(spider),
            content,
            content.getbuffer().nbytes
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

    def process_item(self, item: RawTextItem, spider) -> RawTextItem:
        if not isinstance(item, RawTextItem):
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

    def process_item(self, item, spider):
        if not isinstance(item, RawTextItem):
            return item

        msg = Message(item.to_mns_message(spider))
        future = self.get_producer(spider.settings)
        future.send_message(msg)
        return item
