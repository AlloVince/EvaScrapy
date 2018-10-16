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


class RawTextItem(Item):
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
        filepath = self.to_filepath(spider)
        command = json.dumps({
            'messageId': ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
            'queueName': spider.settings['MNS_QUEUE_NAME'],
            'content': {
                'name': 'etl:%s' % spider.name,
                'spec': {
                    'storage': spider.settings['APP_STORAGE'],
                    'uri': filepath
                }
            },
            'command': 'etl:%s --storage=%s --uri=%s' % (
                spider.name, spider.settings['APP_STORAGE'], filepath
            )
        })
        return command

    def to_kafka_message(self, spider):
        filepath = self.to_filepath(spider)
        command = json.dumps({
            'messageId': ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
            'queueName': spider.settings['KAFKA_TOPIC'],
            'content': {
                'name': 'etl:%s' % spider.name,
                'spec': {
                    'storage': spider.settings['APP_STORAGE'],
                    'uri': filepath
                }
            },
            'command': 'etl:%s --storage=%s --uri=%s' % (
                spider.name, spider.settings['APP_STORAGE'], filepath
            )
        })
        return bytes(command, encoding='utf8')


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


class BinaryFileItem(Item):
    url: bytes = Field()
    version: str = Field()
    task: str = Field()
    timestamp: int = Field()
    from_url: str = Field()

    def __repr__(self):
        return "<BinaryFileItem %s from %s>" % (self['url'], len(self['from_url']))


class TorrentFileItem(BinaryFileItem):
    # info_hash: str = Field()
    body: bytes = Field()

    # info_hash = None

    def get_info_hash(self):
        # if self['info_hash']:
        #     return self['info_hash']

        torrent = bencode.bdecode(self['body']).get('info')
        return hashlib.sha1(bencode.bencode(torrent)).hexdigest()
        # self['info_hash'] = hashlib.sha1(bencode.bencode(torrent)).hexdigest()
        # return self['info_hash']

    def __repr__(self):
        return "<TorrentFileItem hash %s from %s>" % (self['url'], self['from_url'])


class FilesItem(Item):
    file_urls = Field()
    files = Field()
    source_url: str = Field()

    def __repr__(self):
        return "<FilesItem source_from %s with %s urls>" % (self['source_url'], len(self['file_urls']))
