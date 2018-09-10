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
