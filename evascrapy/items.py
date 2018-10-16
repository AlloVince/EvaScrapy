# -*- coding: utf-8 -*-
from scrapy import Item, Field


class RawHtmlItem(Item):
    url: bytes = Field()
    html: str = Field()
    version: str = Field()
    task: str = Field()
    timestamp: int = Field()

    def __repr__(self):
        return "<RawHtmlItem %s>" % (self['url'])

    def to_string(self):
        return "<!--url:%s-->\n<!--version:%s-->\n<!--task:%s-->\n<!--timestamp:%s-->\n%s" % (
            self['url'], self['version'], self['task'], self['timestamp'], self['html'])


class FilesItem(Item):
    file_urls = Field()
    files = Field()
    source_url: str = Field()

    def __repr__(self):
        return "<FileItem source_from %s with %s urls>" % (self['source_url'], len(self['file_urls']))
