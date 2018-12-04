from scrapy.downloadermiddlewares.cookies import CookiesMiddleware
from http.cookies import SimpleCookie
from cfscrape import get_tokens
import logging


class GlobalCookiesMiddleware(CookiesMiddleware):
    cookies_global = None

    def get_cookies_global(self, spider):
        if self.cookies_global:
            return self.cookies_global

        cookies_custom = spider.settings['COOKIES_GLOBAL']
        if not cookies_custom:
            return self.cookies_global

        parser = SimpleCookie()
        parser.load(cookies_custom)

        cookies = {}
        for key, morsel in parser.items():
            cookies[key] = morsel.value
        self.cookies_global = cookies
        return self.cookies_global

    def process_request(self, request, spider):
        if request.meta.get('dont_merge_cookies', False):
            return

        cookiejarkey = request.meta.get("cookiejar")
        jar = self.jars[cookiejarkey]

        cookies_global = self.get_cookies_global(spider)
        if cookies_global:
            request.cookies = cookies_global

        cookies = self._get_request_cookies(jar, request)
        for cookie in cookies:
            jar.set_cookie_if_ok(cookie, request)

        request.headers.pop('Cookie', None)
        jar.add_cookie_header(request)
        self._debug_cookie(request, spider)


# NOTE: source code is from https://github.com/clemfromspace/scrapy-cloudflare-middleware
class CloudFlareMiddleware:
    """Scrapy middleware to bypass the CloudFlare's anti-bot protection"""

    @staticmethod
    def is_cloudflare_challenge(response):
        """Test if the given response contains the cloudflare's anti-bot protection"""

        return (
                response.status == 503
                and response.headers.get('Server', '').startswith(b'cloudflare')
                and 'jschl_vc' in response.text
                and 'jschl_answer' in response.text
        )

    def process_response(self, request, response, spider):
        """Handle the a Scrapy response"""

        if not self.is_cloudflare_challenge(response):
            return response

        logger = logging.getLogger('cloudflaremiddleware')

        logger.debug(
            'Cloudflare protection detected on %s, trying to bypass...',
            response.url
        )

        cloudflare_tokens, __ = get_tokens(
            request.url,
            user_agent=spider.settings.get('USER_AGENT')
        )

        logger.debug(
            'Successfully bypassed the protection for %s, re-scheduling the request',
            response.url
        )

        request.cookies.update(cloudflare_tokens)
        request.priority = 99999

        return request
