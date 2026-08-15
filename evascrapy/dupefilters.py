import logging

from minio import Minio
from minio.error import S3Error
from scrapy.dupefilters import RFPDupeFilter

from evascrapy.items import url_to_filepath

logger = logging.getLogger(__name__)


class S3DupeFilter(RFPDupeFilter):
    """Optional S3-backed filter layered on top of Scrapy fingerprints."""

    _not_found_codes = {'NoSuchKey', 'NoSuchObject', 'NotFound'}

    @classmethod
    def from_crawler(cls, crawler):
        dupefilter = super().from_crawler(crawler)
        dupefilter.settings = crawler.settings
        dupefilter._s3_client = None
        dupefilter._existing_keys = set()
        return dupefilter

    def _get_s3_client(self):
        if self._s3_client:
            return self._s3_client

        settings = self.settings
        self._s3_client = Minio(
            settings['AWS_S3_ENDPOINT'],
            access_key=settings['AWS_S3_ACCESS_KEY'],
            secret_key=settings['AWS_S3_ACCESS_SECRET'],
            region=settings['AWS_S3_REGION'],
            secure=settings['AWS_S3_ACCESS_SECURE'],
        )
        return self._s3_client

    def _get_object_key(self, request):
        detail_url = request.meta.get('detail_url')
        if not detail_url:
            return None

        root_path = self.settings.get('S3_DUPEFILTER_ROOT_PATH')
        if not root_path:
            raise RuntimeError(
                'S3_DUPEFILTER_ROOT_PATH is required when S3DupeFilter is enabled'
            )

        depth = self.settings.get('S3_DUPEFILTER_DEPTH')
        if depth is None:
            depth = self.settings.getint('APP_STORAGE_DEPTH')

        filepath, filename = url_to_filepath(
            detail_url,
            root_path,
            depth=depth,
            extension='json',
        )
        return '/'.join([filepath, filename])

    def _object_exists(self, object_key):
        if object_key in self._existing_keys:
            return True

        self._get_s3_client().stat_object(
            bucket_name=self.settings['AWS_S3_DEFAULT_BUCKET'],
            object_name=object_key,
        )
        self._existing_keys.add(object_key)
        return True

    def request_seen(self, request):
        # Preserve Scrapy's official in-run fingerprint filtering first.
        if super().request_seen(request):
            return True

        if not self.settings.getbool('S3_DUPEFILTER_ENABLED'):
            return False

        object_key = self._get_object_key(request)
        if not object_key:
            return False

        try:
            exists = self._object_exists(object_key)
        except S3Error as error:
            if error.code in self._not_found_codes:
                return False
            raise

        if exists:
            logger.info(
                'Filtered existing S3 object: %s',
                request.meta.get('content_id') or object_key,
            )
        return exists
