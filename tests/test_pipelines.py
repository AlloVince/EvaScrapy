import json
import hashlib
import pathlib
import tempfile
from unittest.mock import MagicMock, patch

import bencode
import pytest

from evascrapy.items import (
    url_to_filepath,
    hash_to_filepath,
    random_id,
    get_command,
    RawJsonItem,
    RawHtmlItem,
    BinaryFileItem,
    TorrentFileItem,
    QueueBasedItem,
)
from evascrapy.pipelines import (
    LocalFilePipeline,
    ElasticDupePipeline,
    NatsPipeline,
    KafkaPipeline,
    AliyunMnsPipeline,
    AwsS3Pipeline,
    AliyunOssPipeline,
)


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

class TestUrlToFilepath:
    def test_depth_0(self):
        result = url_to_filepath('https://avnpc.com', 'dl', 0)
        assert result == ['dl', 'e64a7949178724d29183923ec58179fb.html']

    def test_depth_1(self):
        result = url_to_filepath('https://avnpc.com', 'dl', 1)
        assert result == ['dl/e6', '4a7949178724d29183923ec58179fb.html']

    def test_depth_3(self):
        result = url_to_filepath('https://avnpc.com', 'dl', 3)
        assert result == ['dl/e6/4a/79', '49178724d29183923ec58179fb.html']

    def test_custom_extension(self):
        result = url_to_filepath('https://example.com', 'data', 1, 'json')
        assert result == ['data/c9', '84d06aafbecf6bc55569f964148ea3.json']


class TestHashToFilepath:
    def test_depth_0(self):
        result = hash_to_filepath(
            'e64a7949178724d29183923ec58179fb', 'dl', 0
        )
        assert result == ['dl', 'e64a7949178724d29183923ec58179fb.torrent']

    def test_depth_3(self):
        result = hash_to_filepath(
            'e64a7949178724d29183923ec58179fb', 'dl', 3
        )
        assert result == ['dl/e6/4a/79', '49178724d29183923ec58179fb.torrent']

    def test_custom_extension(self):
        result = hash_to_filepath('abc123', 'root', 1, 'txt')
        assert result[1].endswith('.txt')


class TestRandomId:
    def test_length(self):
        rid = random_id()
        assert len(rid) == 10

    def test_alphanumeric(self):
        rid = random_id()
        assert rid.isupper()
        assert rid.isalnum()

    def test_multiple_calls_unique(self):
        ids = {random_id() for _ in range(100)}
        assert len(ids) == 100


class TestGetCommand:
    def make_spider(self, storage='file'):
        spider = MagicMock()
        spider.settings = {'APP_STORAGE': storage}
        spider.name = 'test_spider'
        return spider

    def test_basic_structure(self):
        spider = self.make_spider()
        cmd = json.loads(get_command('etl:test_spider', 'my_queue', 'dl/foo.html', spider))

        assert cmd['queueName'] == 'my_queue'
        assert cmd['content']['name'] == 'etl:test_spider'
        assert cmd['content']['spec']['storage'] == 'file'
        assert cmd['content']['spec']['uri'] == 'dl/foo.html'
        assert cmd['command'] == 'etl:test_spider --storage=file --uri=dl/foo.html'
        assert 'messageId' in cmd

    def test_storage_from_spider(self):
        spider = self.make_spider('oss')
        cmd = json.loads(get_command('etl:torrent', 'q', 'path', spider))
        assert cmd['content']['spec']['storage'] == 'oss'
        assert cmd['command'] == 'etl:torrent --storage=oss --uri=path'


# ---------------------------------------------------------------------------
# Item serialization
# ---------------------------------------------------------------------------

class TestRawJsonItem:
    @pytest.fixture
    def item(self):
        return RawJsonItem(
            url='https://example.com/data.json',
            version='1.0',
            task='test_task',
            timestamp=1234567890,
            content='{"key": "value"}',
        )

    def test_to_string(self, item):
        parsed = json.loads(item.to_string())
        assert parsed['url'] == 'https://example.com/data.json'
        assert parsed['version'] == '1.0'
        assert parsed['task'] == 'test_task'
        assert parsed['timestamp'] == 1234567890
        assert parsed['content'] == '{"key": "value"}'

    def test_repr(self, item):
        assert 'RawJsonItem' in repr(item)
        assert 'example.com' in repr(item)

    def test_to_string_keeps_unicode_readable(self):
        item = RawJsonItem(
            url='https://example.com/data.json',
            version='1.0',
            task='test_task',
            timestamp=1234567890,
            content={'title': 'いい作品'},
        )

        serialized = item.to_string()

        assert 'いい作品' in serialized
        assert '\\u3044' not in serialized


class TestRawHtmlItem:
    @pytest.fixture
    def item(self):
        return RawHtmlItem(
            url='https://example.com/page.html',
            version='2.0',
            task='test_task',
            timestamp=1234567890,
            html='<html><body>Hello</body></html>',
        )

    def test_to_string(self, item):
        s = item.to_string()
        assert 'url:https://example.com/page.html' in s
        assert 'version:2.0' in s
        assert 'task:test_task' in s
        assert 'timestamp:1234567890' in s
        assert '<html><body>Hello</body></html>' in s

    def test_repr(self, item):
        assert 'RawHtmlItem' in repr(item)


class TestBinaryFileItem:
    @pytest.fixture
    def item(self):
        return BinaryFileItem(
            url=b'http://example.com/file.bin',
            version='1.0',
            task='test_task',
            timestamp=1234567890,
            from_url='http://example.com/file.bin',
            body=b'\x00\x01\x02\x03',
        )

    def test_to_bytes(self, item):
        assert item.to_bytes() == b'\x00\x01\x02\x03'

    def test_repr(self, item):
        assert 'BinaryFileItem' in repr(item)


class TestTorrentFileItem:
    @pytest.fixture
    def valid_torrent_body(self):
        """A minimal valid bencoded torrent with an info dict."""
        info = {'name': b'test', 'piece length': 16384, 'pieces': b'a' * 20}
        return bencode.bencode({'info': info, 'announce': b'http://tracker'})

    @pytest.fixture
    def item(self, valid_torrent_body):
        return TorrentFileItem(
            url=b'http://example.com/test.torrent',
            version='1.0',
            task='test_task',
            timestamp=1234567890,
            from_url='http://example.com/test.torrent',
            body=valid_torrent_body,
        )

    def test_get_info_hash(self, item, valid_torrent_body):
        info = bencode.bdecode(valid_torrent_body)['info']
        expected_hash = hashlib.sha1(bencode.bencode(info)).hexdigest()
        assert item.get_info_hash() == expected_hash

    def test_get_info_hash_cached(self, item):
        first = item.get_info_hash()
        second = item.get_info_hash()
        assert first == second

    def test_get_meta(self, item):
        meta = item.get_meta()
        assert meta['url'] == b'http://example.com/test.torrent'
        assert meta['from_url'] == 'http://example.com/test.torrent'

    def test_repr(self, item):
        assert 'TorrentFileItem' in repr(item)
        assert item.get_info_hash() in repr(item)


# ---------------------------------------------------------------------------
# Pipeline logic
# ---------------------------------------------------------------------------

class TestLocalFilePipeline:
    @pytest.fixture
    def pipeline(self):
        return LocalFilePipeline()

    @pytest.fixture
    def spider(self):
        spider = MagicMock()
        spider.name = 'test_spider'
        spider.settings = {
            'APP_STORAGE_ROOT_PATH': 'dl',
            'APP_STORAGE_DEPTH': 0,
            'APP_TASK': 'test_task',
        }
        return spider

    def test_passthrough_non_queue_item(self, pipeline):
        """Non-QueueBasedItem should be returned as-is."""
        result = pipeline.process_item("not_an_item")
        assert result == "not_an_item"

    def test_writes_file(self, pipeline, spider):
        """RawJsonItem should be written to disk."""
        item = RawJsonItem(
            url='https://example.com/test.json',
            version='1.0',
            task='test_task',
            timestamp=1234567890,
            content='{"a": 1}',
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.object(pathlib.Path, 'mkdir'):
                with patch('evascrapy.pipelines.open', create=True) as mock_open:
                    mock_file = MagicMock()
                    mock_open.return_value.__enter__.return_value = mock_file
                    pipeline.crawler = MagicMock()
                    pipeline.crawler.spider = spider

                    result = pipeline.process_item(item)
                    assert result is item
                    mock_file.write.assert_called_once()


class TestElasticDupePipeline:
    @pytest.fixture
    def pipeline(self):
        return ElasticDupePipeline()

    @pytest.fixture
    def spider(self):
        spider = MagicMock()
        spider.settings = {
            'TORRENT_FILE_ELASTIC_DUPE_URL': 'https://user:pass@localhost:9200',
            'TORRENT_FILE_ELASTIC_DUPE_INDICE': 'torrents',
        }
        return spider

    def test_passthrough_non_torrent_item(self, pipeline, spider):
        """Non-TorrentFileItem should pass through."""
        item = RawJsonItem(url='http://x.com', version='1', task='t', timestamp=1, content='{}')
        result = pipeline.process_item(item, spider)
        assert result is item

    def test_passthrough_non_queue_item(self, pipeline, spider):
        result = pipeline.process_item("string_item", spider)
        assert result == "string_item"

    def test_returns_none_when_duplicate(self, pipeline, spider):
        item = MagicMock(spec=TorrentFileItem)
        item.get_info_hash.return_value = 'abc123'

        mock_es = MagicMock()
        mock_es.exists.return_value = True
        pipeline._es = mock_es

        result = pipeline.process_item(item, spider)
        assert result is None

    def test_returns_item_when_not_duplicate(self, pipeline, spider):
        item = MagicMock(spec=TorrentFileItem)
        item.get_info_hash.return_value = 'abc123'

        mock_es = MagicMock()
        mock_es.exists.return_value = False
        pipeline._es = mock_es

        result = pipeline.process_item(item, spider)
        assert result is item


class TestNatsPipeline:
    @pytest.fixture
    def pipeline(self):
        return NatsPipeline()

    @pytest.fixture
    def spider(self):
        spider = MagicMock()
        spider.settings = {
            'NATS_SERVER_LIST': ['nats://localhost:4222'],
            'NATS_SUBJECT': 'test.subject',
            'NATS_CONNECT_TIMEOUT': 5,
            'NATS_ALLOW_RECONNECT': True,
            'NATS_MAX_RECONNECT_ATTEMPTS': 60,
        }
        return spider

    def test_passthrough_non_queue_item(self, pipeline, spider):
        result = pipeline.process_item("not_an_item", spider)
        assert result == "not_an_item"

    @patch('evascrapy.pipelines.NATS')
    def test_publishes_message(self, mock_nats_class, pipeline, spider):
        item = MagicMock(spec=QueueBasedItem)
        item.to_nats_message.return_value = '{"command": "test"}'

        mock_nats = MagicMock()
        mock_nats_class.return_value = mock_nats

        mock_loop = MagicMock()
        with patch('evascrapy.pipelines.asyncio.new_event_loop', return_value=mock_loop):
            result = pipeline.process_item(item, spider)

        assert result is item
        # connect was called
        mock_loop.run_until_complete.assert_called()
        # publish was called with subject and encoded message
        publish_call = [c for c in mock_loop.method_calls if 'publish' in str(c)]
        assert len(publish_call) > 0


class TestKafkaPipeline:
    @pytest.fixture
    def pipeline(self):
        return KafkaPipeline()

    @pytest.fixture
    def spider(self):
        spider = MagicMock()
        spider.settings = {
            'KAFKA_SSL_ENABLE': False,
            'KAFKA_SERVER_STRING': 'localhost:9092',
            'KAFKA_TOPIC': 'test_topic',
        }
        return spider

    def test_passthrough_non_queue_item(self, pipeline, spider):
        result = pipeline.process_item("not_an_item", spider)
        assert result == "not_an_item"

    @patch('evascrapy.pipelines.KafkaProducer')
    def test_sends_message(self, mock_producer_class, pipeline, spider):
        item = MagicMock(spec=QueueBasedItem)
        item.to_kafka_message.return_value = b'test_message'

        mock_future = MagicMock()
        mock_producer = MagicMock()
        mock_producer.send.return_value = mock_future
        mock_producer_class.return_value = mock_producer

        result = pipeline.process_item(item, spider)

        assert result is item
        mock_producer.send.assert_called_once_with('test_topic', b'test_message')
        mock_future.get.assert_called_once()


class TestAliyunMnsPipeline:
    @pytest.fixture
    def pipeline(self):
        return AliyunMnsPipeline()

    @pytest.fixture
    def spider(self):
        spider = MagicMock()
        spider.settings = {
            'MNS_ACCOUNT_ENDPOINT': 'https://12345.mns.cn-hangzhou.aliyuncs.com',
            'MNS_ACCESSKEY_ID': 'test_id',
            'MNS_ACCESSKEY_SECRET': 'test_secret',
            'MNS_QUEUE_NAME': 'test_queue',
        }
        return spider

    def test_passthrough_non_queue_item(self, pipeline, spider):
        result = pipeline.process_item("not_an_item", spider)
        assert result == "not_an_item"

    @patch('evascrapy.pipelines.Account')
    def test_sends_message(self, mock_account_class, pipeline, spider):
        item = MagicMock(spec=QueueBasedItem)
        item.to_mns_message.return_value = '{"command": "test"}'

        mock_queue = MagicMock()
        mock_account = MagicMock()
        mock_account.get_queue.return_value = mock_queue
        mock_account_class.return_value = mock_account

        result = pipeline.process_item(item, spider)

        assert result is item
        mock_queue.send_message.assert_called_once()


class TestAwsS3Pipeline:
    @pytest.fixture
    def pipeline(self):
        return AwsS3Pipeline()

    @pytest.fixture
    def spider(self):
        spider = MagicMock()
        spider.name = 'test_spider'
        spider.settings = {
            'AWS_S3_ENDPOINT': 'play.min.io',
            'AWS_S3_ACCESS_KEY': 'test_key',
            'AWS_S3_ACCESS_SECRET': 'test_secret',
            'AWS_S3_REGION': 'us-east-1',
            'AWS_S3_ACCESS_SECURE': False,
            'AWS_S3_DEFAULT_BUCKET': 'test-bucket',
            'APP_STORAGE_ROOT_PATH': 'dl',
            'APP_STORAGE_DEPTH': 3,
            'APP_TASK': 'test_task',
        }
        return spider

    def test_passthrough_non_queue_item(self, pipeline, spider):
        result = pipeline.process_item("not_an_item", spider)
        assert result == "not_an_item"

    @patch('evascrapy.pipelines.Minio')
    def test_puts_object(self, mock_minio_class, pipeline, spider):
        item = RawJsonItem(
            url='https://example.com/data.json',
            version='1.0',
            task='test_task',
            timestamp=1234567890,
            content='{"a": 1}',
        )

        mock_client = MagicMock()
        mock_minio_class.return_value = mock_client

        result = pipeline.process_item(item, spider)

        assert result is item
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs['bucket_name'] == 'test-bucket'
        assert call_kwargs['object_name'].endswith('.json')
        assert call_kwargs['metadata'] is None


class TestAliyunOssPipeline:
    @pytest.fixture
    def pipeline(self):
        return AliyunOssPipeline()

    @pytest.fixture
    def spider(self):
        spider = MagicMock()
        spider.name = 'test_spider'
        spider.settings = {
            'OSS_ACCESS_KEY_ID': 'test_id',
            'OSS_ACCESS_KEY_SECRET': 'test_secret',
            'OSS_ENDPOINT': 'http://oss-cn-hangzhou.aliyuncs.com',
            'OSS_BUCKET': 'test-bucket',
            'APP_STORAGE_ROOT_PATH': 'dl',
            'APP_STORAGE_DEPTH': 3,
            'APP_TASK': 'test_task',
        }
        return spider

    def test_passthrough_non_queue_item(self, pipeline, spider):
        result = pipeline.process_item("not_an_item", spider)
        assert result == "not_an_item"

    @patch('evascrapy.pipelines.oss2.Auth')
    @patch('evascrapy.pipelines.oss2.Bucket')
    def test_puts_object(self, mock_bucket_class, mock_auth_class, pipeline, spider):
        item = RawJsonItem(
            url='https://example.com/data.json',
            version='1.0',
            task='test_task',
            timestamp=1234567890,
            content='{"a": 1}',
        )

        mock_bucket = MagicMock()
        mock_bucket_class.return_value = mock_bucket

        result = pipeline.process_item(item, spider)

        assert result is item
        mock_bucket.put_object.assert_called_once()
        call_kwargs = mock_bucket.put_object.call_args[1]
        assert call_kwargs['key'].endswith('.json')
