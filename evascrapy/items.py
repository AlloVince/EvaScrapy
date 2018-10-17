# -*- coding: utf-8 -*-
import hashlib
import json
import random
import string
import bencode
from scrapy import Item, Field


def url_to_filepath(url, root_path: str, depth=0, extension='html'):
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
    chunk_size = 2
    hash_chunks = [url_hash[i:i + chunk_size] for i in range(0, len(url_hash), chunk_size)]
    return [
        '/'.join([root_path] + [i for i in hash_chunks[0:int(depth)]]),
        '%s.%s' % (''.join([i for i in hash_chunks[int(depth):]]), extension)
    ]


def hash_to_filepath(hash: str, root_path: str, depth: int = 0, extension: str = 'torrent'):
    chunk_size = 2
    hash_chunks = [hash[i:i + chunk_size] for i in range(0, len(hash), chunk_size)]
    return [
        '/'.join([root_path] + [i for i in hash_chunks[0:int(depth)]]),
        '%s.%s' % (''.join([i for i in hash_chunks[int(depth):]]), extension)
    ]


def random_id() -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))


def get_command(command_name: str, queue_name: str, filepath: str, spider) -> str:
    return json.dumps({
        'messageId': random_id(),
        'queueName': queue_name,
        'content': {
            'name': command_name,
            'spec': {
                'storage': spider.settings['APP_STORAGE'],
                'uri': filepath
            }
        },
        'command': '%s --storage=%s --uri=%s' % (
            command_name, spider.settings['APP_STORAGE'], filepath
        )
    })


class QueueBasedItem(Item):
    def to_filepath(self, spider):
        pass

    def to_mns_message(self, spider):
        pass

    def to_kafka_message(self, spider):
        pass


class RawTextItem(QueueBasedItem):
    url: bytes = Field()
    version: str = Field()
    task: str = Field()
    timestamp: int = Field()
    extension: str = 'html'

    def to_filepath(self, spider):
        [filepath, filename] = url_to_filepath(
            self['url'],
            '/'.join([spider.settings['APP_STORAGE_ROOT_PATH'], spider.name, spider.settings['APP_TASK']]),
            spider.settings['APP_STORAGE_DEPTH'],
            self['extension']
        )
        return '/'.join([filepath, filename])

    def to_mns_message(self, spider):
        command = get_command(
            command_name='etl:%s' % spider.name,
            filepath=self.to_filepath(spider),
            queue_name=spider.settings['MNS_QUEUE_NAME'],
            spider=spider,
        )
        return command

    def to_kafka_message(self, spider):
        command = get_command(
            command_name='etl:%s' % spider.name,
            filepath=self.to_filepath(spider),
            queue_name=spider.settings['KAFKA_TOPIC'],
            spider=spider,
        )
        return bytes(command, encoding='utf8')

    def to_string(self):
        pass


class RawJsonItem(RawTextItem):
    extension: str = 'json'
    content: str = Field()

    def __repr__(self):
        return "<RawJsonItem %s>" % (self['url'])

    def to_string(self):
        return json.dumps({
            'url': self['url'],
            'version': self['version'],
            'task': self['task'],
            'timestamp': self['timestamp'],
            'content': self['content'],
        })


class RawHtmlItem(RawTextItem):
    extension: str = 'html'
    html: str = Field()

    def __repr__(self):
        return "<RawHtmlItem %s>" % (self['url'])

    def to_string(self):
        return "<!--url:%s-->\n<!--version:%s-->\n<!--task:%s-->\n<!--timestamp:%s-->\n%s" % (
            self['url'], self['version'], self['task'], self['timestamp'], self['html'])


class BinaryFileItem(QueueBasedItem):
    url: bytes = Field()
    version: str = Field()
    task: str = Field()
    timestamp: int = Field()
    from_url: str = Field()
    body: bytes = Field()

    def __repr__(self):
        return "<BinaryFileItem %s from %s>" % (self['url'], len(self['from_url']))

    def to_bytes(self) -> bytes:
        return self['body']

    def to_filepath(self, spider):
        [filepath, filename] = hash_to_filepath(
            self.get_info_hash(),
            '/'.join([spider.settings['TORRENT_FILE_PIPELINE_ROOT_PATH']]),
            spider.settings['TORRENT_FILE_PIPELINE_DEPTH']
        )
        return '/'.join([filepath, filename])

    def to_mns_message(self, spider):
        command = get_command(
            command_name='etl:torrent',
            filepath=self.to_filepath(spider),
            queue_name=spider.settings['MNS_QUEUE_NAME'],
            spider=spider,
        )
        return command

    def to_kafka_message(self, spider):
        command = get_command(
            command_name='etl:torrent',
            filepath=self.to_filepath(spider),
            queue_name=spider.settings['KAFKA_TOPIC'],
            spider=spider,
        )
        return bytes(command, encoding='utf8')


class TorrentFileItem(BinaryFileItem):
    info_hash: str = Field()

    def get_info_hash(self):
        if self.get('info_hash'):
            return self.get('info_hash')

        torrent = bencode.bdecode(self['body']).get('info')
        self['info_hash'] = hashlib.sha1(bencode.bencode(torrent)).hexdigest()
        return self['info_hash']

    def __repr__(self):
        return "<TorrentFileItem hash %s from %s>" % (self['url'], self['from_url'])


class FilesItem(Item):
    file_urls = Field()
    files = Field()
    source_url: str = Field()

    def __repr__(self):
        return "<FilesItem source_from %s with %s urls>" % (self['source_url'], len(self['file_urls']))
