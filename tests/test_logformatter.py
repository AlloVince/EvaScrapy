import logging

from scrapy.settings import default_settings

from evascrapy.logformatter import LogFormatter
from evascrapy.settings import _normalize_log_format


def test_json_log_format_falls_back_to_scrapy_default():
    assert _normalize_log_format('json') == default_settings.LOG_FORMAT
    assert _normalize_log_format('%(message)s') == '%(message)s'


def test_item_error_preserves_pipeline_exception():
    exception = RuntimeError('storage unavailable')
    result = LogFormatter().item_error('item', exception, None, None)

    assert result['level'] == logging.ERROR
    assert result['args'] == {'item': 'item', 'exception': exception}
