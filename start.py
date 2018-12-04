import os
from evascrapy.runner import ScheduleCrawlerRunner
from dotenv import load_dotenv
import logging
from scrapy.utils.log import configure_logging


load_dotenv(dotenv_path=os.path.dirname(os.path.realpath(__file__)) + '/.env')
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
