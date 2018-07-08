from evascrapy.pipelines import HtmlFilePipeline
import pytest


def test_url_to_filepath():
    assert HtmlFilePipeline.url_to_filepath('https://avnpc.com', 'dl', 0) \
           == ['dl',
               'e64a7949178724d29183923ec58179fb.html']

    assert HtmlFilePipeline.url_to_filepath('https://avnpc.com', 'dl', 1) \
           == ['dl/e6',
               '4a7949178724d29183923ec58179fb.html']
