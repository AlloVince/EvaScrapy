import os
from dotenv import load_dotenv
import logging
from scrapy.utils.reactor import install_reactor


load_dotenv(dotenv_path=os.path.dirname(os.path.realpath(__file__)) + '/.env')
# runner.py imports Twisted's reactor, so install the reactor requested by
# Scrapy before importing the scheduler.
install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')

from evascrapy.runner import ScheduleCrawlerRunner  # noqa: E402
levels = {
    'CRITICAL': logging.CRITICAL,
    'FATAL': logging.FATAL,
    'ERROR': logging.ERROR,
    'WARING': logging.WARNING,
    'WARN': logging.WARN,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET,
}
level = os.getenv('LOG_LEVEL', 'DEBUG').upper()

logging.basicConfig(
    level=levels[level] if level in levels else levels['INFO'],
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S%z'
)
runner = ScheduleCrawlerRunner(os.getenv('APP_SPIDER', 'demo'))
runner.run_crawler()
runner.schedule()
runner.start()
