import re
import scrapy
from scrapy import log
from scraper.items import AppItem, AppItemLoader
from scraper.selector import Selector


class PlayStoreSpider(scrapy.Spider):
    name = 'playstorescrapy'
    allowed_domains = ["play.google.com"]
    start_urls = []

    CRAWL_URL = 'https://play.google.com/store/search?q={0}&c=apps'
    APP_URLS = "//div[@class='details']/a[@class='card-click-target' and @tabindex='-1' and @aria-hidden='true']/@href"

    # count how many items have been scraped
    item_count = 0

    def __init__(self, keywords=None, max_item=0, download_delay=0, output='item.csv', *args, **kwargs):
        """
        :param keywords: search keywords separated by comma (required)
        :param max_item: maximum items to be scraped
        :param download_delay: download delay (in seconds)
        :param output: output filepath
        :param args: additional list of arguments
        :param kwargs: additional dictionary of arguments
        """

        super(PlayStoreSpider, self).__init__(*args, **kwargs)

        if keywords:
            arr_keywords = keywords.split(",")
            for keyword in arr_keywords:
                self.start_urls.append(self.CRAWL_URL.format(keyword.strip()))
        else:
            raise ValueError('"keywords" parameter is required')

        try:
            self.max_item = int(max_item)
        except ValueError:
            raise ValueError('"max_item" parameter is invalid: ' + str(max_item))

        try:
            self.download_delay = int(download_delay)
        except ValueError:
            raise ValueError('"download_delay" parameter is invalid: ' + str(download_delay))

        if output:
            self.output_file = output.strip()
        else:
            raise ValueError('"output" parameter is invalid: ' + str(output))

    # override
    def start_requests(self):
        requests = []

        for url in self.start_urls:
            requests.append(scrapy.FormRequest(
                url,
                formdata={
                    'ipf': '1',
                    'xhr': '1',
                },
                meta={
                    'url_set': set(),
                },
                callback=self.parse))

        return requests

    def parse(self, response):
        """
        Parse search page.
        """

        if self.is_max_item_reached():
            return

        log.msg("==== Scraping: " + response.url, level=log.INFO)

        url_set = response.meta.get('url_set')

        app_urls = Selector(xpath=self.APP_URLS).get_value_list(response)
        for url in app_urls:
            if url not in url_set:
                url_set.add(url)
                yield scrapy.Request(AppItem.APP_URL_PREFIX + url + "&hl=en", callback=self.parse_app_url)
        else:
            page_token = self.get_page_token(response.body)
            if page_token is not None:
                yield scrapy.FormRequest(
                    response.url,
                    formdata={
                        'ipf': '1',
                        'xhr': '1',
                        'pagTok': page_token,
                    },
                    meta={
                        'url_set': url_set,
                    },
                    callback=self.parse)

    def is_max_item_reached(self):
        """
        Check if max_item has been reached.
        :return: True if max_item has been reached else False
        """

        return self.max_item > 0 and self.item_count >= self.max_item

    @staticmethod
    def get_page_token(content):
        """
        Get "pagTok" value from play store's search page.
        The token value will be used to fetch next stream data (see parse function).

        :param content: response string.
        :return: pagTok value or None if not found.
        """

        match = re.search(r"'\[.*\\42((?:.(?!\\42))*:S:.*?)\\42.*\]\\n'", content)
        if match:
            return match.group(1).replace('\\\\', '\\').decode('unicode-escape')
        else:
            return None

    def parse_app_url(self, response):
        """
        Parse App detail page
        """

        if self.is_max_item_reached():
            log.msg("==== Max Item reached", level=log.INFO)
            # Stop the crawling if max item reached
            self.crawler.engine.close_spider(self, "Max Item reached")
            return

        loader = AppItemLoader(item=AppItem(), response=response)
        yield loader.load_item()

        self.item_count += 1
        log.msg("==== Item Count: " + str(self.item_count), level=log.INFO)

    # override
    def closed(self, reason):
        log.msg("==== Spider has stopped. Reason: " + reason +
                ". Total Scraped Items: " + str(self.item_count), level=log.INFO)