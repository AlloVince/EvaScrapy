import os
from evascrapy.runner import ScheduleCrawlerRunner
from scrapy.utils.log import configure_logging
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.dirname(os.path.realpath(__file__)) + '/.env')

configure_logging({'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s'})
runner = ScheduleCrawlerRunner(os.getenv('APP_SPIDER', 'demo'))
runner.run_crawler()
runner.schedule()
runner.start()
