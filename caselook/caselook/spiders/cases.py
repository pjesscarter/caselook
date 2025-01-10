import traceback

import scrapy
from scrapy_playwright.page import PageMethod
import json
from caselook.items import CaselookItem
from scrapy.loader import ItemLoader
from datetime import datetime
import pandas as pd
import asyncio
import time

class CasesSpider(scrapy.Spider):
    name = "cases"
    allowed_domains = ["caselook.ru"]

    def __init__(self, *args, **kwargs):
        super(CasesSpider, self).__init__(*args, **kwargs)
        self.codes = pd.read_csv('../courts_codes.csv')
        self.start_urls = ["https://caselook.ru/#/search"]
        self.seen_urls = set()
        self.lock = asyncio.Lock()

    def find_earliest_date(self, documents):
        # Extract dates and parse them to datetime objects
        try:
            dates = [datetime.strptime(doc["date"], "%d.%m.%Y") for doc in documents]
        except:
            dates = [datetime.strptime(doc["date"], "%Y%m%d") for doc in documents]
        # Find the earliest date
        earliest_date = min(dates)
        # Return formatted date as YYYYMMDD
        return earliest_date.strftime("%Y%m%d")

    def start_requests(self):
        self.logger.info("Provided email: %s", self.email)
        self.logger.info("Provided password: %s", self.password)
        # counter = 0
        yield scrapy.Request(
            url='https://caselook.ru/#/search',
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod('fill', selector='input[name="email"]', value=self.email),
                    PageMethod('fill', selector='input[name="password"]', value=self.password),
                    PageMethod('click', selector='button'),  # Assuming this logs in
                    PageMethod('wait_for_timeout', 100),  # Wait for a second to ensure the login is processed
                ],
                # 'courtid': courtid,
                # 'date_upper': date_upper,
                # 'courttitle': courttitle
            },
            dont_filter=True,
            callback=self.start_regions,
            errback=self.errback,
        )

    async def start_regions(self, response):
        page = response.meta["playwright_page"]
        cookies = await page.context.cookies()
        cookie_header = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Referer": "https://caselook.ru/",
            "Cookie": cookie_header,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept-Language": "en-NZ,en;q=0.9,ka-GE;q=0.8,ka;q=0.7,ru-RU;q=0.6,ru;q=0.5,hy-AM;q=0.4,hy;q=0.3,en-US;q=0.2,en-GB;q=0.1"
        }
        for cid, n, incl in zip(self.codes.Code, self.codes.Title, self.codes.Include):
            if incl:
                courtid = cid
                courttitle = n
                date_upper = datetime.today().strftime("%Y%m%d")
                self.logger.info("Starting crawl for court %s with id %s from date %s", courttitle, str(courtid),
                                 date_upper)
                yield scrapy.Request(
                    url='https://caselook.ru/#/search',
                    headers=headers,
                    meta={
                        "playwright": True,
                        "playwright_include_page": True,
                        'courtid': courtid,
                        'date_upper': date_upper,
                        'courttitle': courttitle
                    },
                    dont_filter=True,  # Prevent Scrapy from filtering this request as a duplicate
                    callback=self.parse_search,
                    errback=self.errback
                )
            else:
                continue
        await page.close()
    async def parse_search(self, response):
        # Extract cookies from Playwright session

        page = response.meta["playwright_page"]
        cookies = await page.context.cookies()
        # Construct payload for POST request
        courtid = response.meta["courtid"]
        courttitle = response.meta["courttitle"]
        date_upper = response.meta["date_upper"]
        self.logger.info("Making search for court %s with id %s from date %s",courttitle,str(courtid),date_upper)
        payload = {
            "conditions": [
                {"id": "6749aae1-2863-4741-8ccf-1f82149be53e", "contains": True, "type": "document_type",
                 "data": {"id": 150}},
                {"id": "d81b48d4-ba5d-4de7-84fd-94140af0bc80", "contains": True, "type": "court",
                 "data": {"id": courtid, "title": courttitle}},
                {"id": "358bd74c-5c24-40bb-b936-9f82bcecf5c2", "type": "date",
                 "data": {"scope": "range", "value": ["20050101", date_upper]}},
                # {"id":"bf6a1c48-7ce2-49e4-ba73-fc1768a8a4b9","contains":True,"type":"case_type",
                #  "data":{"id":1}},
                {"id": "bf6a1c48-7ce2-49e4-ba73-fc1768a8a4b9", "contains": True, "type": "case_type",
                 "data": {"id": 3}}
            ]
        }

        # Add cookies to headers
        cookie_header = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Origin": "https://caselook.ru",
            "Referer": "https://caselook.ru/",
            "Cookie": cookie_header,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept-Language": "en-NZ,en;q=0.9,ka-GE;q=0.8,ka;q=0.7,ru-RU;q=0.6,ru;q=0.5,hy-AM;q=0.4,hy;q=0.3,en-US;q=0.2,en-GB;q=0.1"
        }
        # self.logger.info("Passing cookies: %s", cookie_header)

        pwresponse = await page.request.post(
            url="https://caselook.ru/search",
            data=json.dumps(payload),
            headers=headers
        )
        self.logger.info("Response status: %s", pwresponse.status)
        # self.logger.info("SearchID: %s", await pwresponse.text())
        searchid = json.loads(await pwresponse.text())['search_id']
        url = "https://caselook.ru/#/search/" + str(searchid) + "/documents/"
        tick_selector = "div.sc-cexmgL.sc-hySdjw.bIWcKG.dGkXVY > div.sc-cexmgL.bIWcKG > div.sc-fAjPcg.cwBrMu > div.sc-cWAxUP.fUJNhd > i.sc-gsnTZi.jwKNJP > svg"
        # Make POST request
        yield scrapy.Request(
            url=url,
            headers=headers,
            callback=self.parse_Page,
            dont_filter=True,  # Prevent Scrapy from filtering this request as a duplicate
            errback=self.errback,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod('wait_for_selector', selector=tick_selector, state='visible'),
                    PageMethod('click', selector=tick_selector)
                ],
                'searchid': str(searchid),
                'courtid': courtid,
                'courttitle': courttitle,
                'date_upper': date_upper
            }
        )
        await page.close()

    async def parse_Page(self, response):
        page = response.meta["playwright_page"]
        searchid = response.meta["searchid"]
        cookies = await page.context.cookies()
        # self.logger.info("Response from download request: %s", response.body)
        cookie_header = "; ".join([f"{cookie['name']}={cookie['value']}" for cookie in cookies])
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Referer": "https://caselook.ru/",
            "Cookie": cookie_header,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept-Language": "en-NZ,en;q=0.9,ka-GE;q=0.8,ka;q=0.7,ru-RU;q=0.6,ru;q=0.5,hy-AM;q=0.4,hy;q=0.3,en-US;q=0.2,en-GB;q=0.1"
        }
        pwresponse = await page.request.get(
            url=f"https://caselook.ru/download/search/{searchid}/xlsx",
            headers=headers
        )
        self.logger.info("Response status: %s", pwresponse.status)
        # self.logger.info("DLStatus: %s", await pwresponse.text())
        dlbutton_selector = "a.sc-ftvSup.fPXEyR"
        seen = True
        while seen:
            # Wait for a button to appear
            await page.wait_for_selector(dlbutton_selector, state="visible")

            # Extract all buttons
            buttons = await page.query_selector_all(dlbutton_selector)
            for button in buttons:
                href = await button.get_attribute("href")
                if href:
                    # Use the lock to safely modify self.seen_urls
                    async with self.lock:
                        if href not in self.seen_urls:
                            self.seen_urls.add(href)

                            # Process the unseen download link
                            self.logger.info("New download link: %s", href)
                            l = ItemLoader(item=CaselookItem(), response=response)
                            l.add_value("file_urls", response.urljoin(href))
                            l.add_value("court", response.meta["courtid"])
                            l.add_value("date_upper", response.meta["date_upper"])
                            yield l.load_item()
                            seen = False  # Exit the loop after finding one unseen href

            # If all buttons are seen, wait briefly before checking again
            await page.wait_for_timeout(500)
        pagenum = 40
        try:
            lastdate = await page.request.get(
                url=f"https://caselook.ru/search/{searchid}/list/{pagenum}",
                headers=headers
            )
            self.logger.info("Response status: %s", lastdate.status)
            final_cases = json.loads(await lastdate.text())
            cases = final_cases["documents"]
            newdate = self.find_earliest_date(cases)
            self.logger.info("Earliest date: %s", newdate)
            if datetime.strptime(newdate, "%Y%m%d") > datetime.strptime("01.01.2008", "%d.%m.%Y"):
                yield scrapy.Request(
                    url='https://caselook.ru/#/search',
                    headers=headers,
                    meta={
                        "playwright": True,
                        "playwright_include_page": True,
                        'courtid': response.meta["courtid"],
                        'date_upper': newdate,
                        'courttitle': response.meta["courttitle"]
                    },
                    dont_filter=True,  # Prevent Scrapy from filtering this request as a duplicate
                    callback=self.parse_search,
                    errback=self.errback
                )
            else:
                self.logger.warning("Reached cutoff date: terminating crawl for court %s", response.meta["courttitle"])
                await page.screenshot(path=(str(response.meta["courtid"]) + ".png"), full_page=True)
        except Exception as e:
            self.logger.warning(e)
            self.logger.warning("Could not get page 40: terminating crawl for court %s", response.meta["courttitle"])
            await page.screenshot(path=(str(response.meta["courtid"]) + ".png"), full_page=True)
        await page.close()

    def errback(self, failure):
        self.logger.error(repr(failure))

