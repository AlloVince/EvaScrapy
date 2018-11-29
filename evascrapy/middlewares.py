from scrapy.downloadermiddlewares.cookies import CookiesMiddleware
from http.cookies import SimpleCookie


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
