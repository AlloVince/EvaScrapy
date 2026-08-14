from scrapy.http import Request
from evascrapy.middlewares import GlobalCookiesMiddleware


def test_global_cookies_middleware_adds_configured_cookie():
    spider = type('Spider', (), {'settings': {'COOKIES_GLOBAL': 'session=enabled'}})()
    middleware = GlobalCookiesMiddleware(debug=False)
    request = Request('https://example.com/')

    middleware.process_request(request, spider)

    assert request.headers['Cookie'] == b'session=enabled'
