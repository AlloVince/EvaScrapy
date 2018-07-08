import os
from evascrapy.runner import ScheduleCrawlerRunner
from scrapy.utils.log import configure_logging

configure_logging({'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s'})
runner = ScheduleCrawlerRunner(os.getenv('APP_SPIDER', 'demo'))
runner.run_crawler()
runner.schedule()
runner.start()
