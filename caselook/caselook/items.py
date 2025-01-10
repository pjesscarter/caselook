# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CaselookItem(scrapy.Item):
    # define the fields for your item here like:
    court = scrapy.Field()
    date_upper = scrapy.Field()
    file_urls = scrapy.Field()
    files = scrapy.Field()
