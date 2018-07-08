from kafka import KafkaProducer
import ssl
from evascrapy import settings as s
from scrapy.utils.log import configure_logging

settings = vars(s)
configure_logging({'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s'})

class KafkaPipeline(object):
    _kafka_producer = None

    def get_producer(self):
        if self._kafka_producer:
            return self._kafka_producer

        # context = ssl.create_default_context()
        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(settings['KAFKA_SASL_CA_CERT_LOCATION'])
        self._kafka_producer = KafkaProducer(
            bootstrap_servers=settings['KAFKA_SERVER_STRING'].split(','),
            # sasl_mechanism=settings['KAFKA_SASL_MECHANISM'],
            # ssl_context=context,
            # security_protocol=settings['KAFKA_SECURITY_PROTOCOL'],
            api_version=(0, 10),
            retries=5,
            # sasl_plain_username=settings['KAFKA_SASL_PLAIN_USERNAME'],
            # sasl_plain_password=settings['KAFKA_SASL_PLAIN_PASSWORD']
        )
        return self._kafka_producer


p = KafkaPipeline().get_producer()
future = p.send(settings['KAFKA_TOPIC'], b'foo')
print(future.get())