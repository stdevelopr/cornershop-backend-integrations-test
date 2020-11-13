import scrapy
from scrapers.items import ProductItem
import json


class CaWalmartSpider(scrapy.Spider):
    name = "ca_walmart"
    allowed_domains = ["walmart.ca"]
    headers = {
        "Accept": "application/json",
        "Content-type": "application/json",
        "X-Requested-With": "XMLHttpRequest"
    }


    def start_requests(self):
        start_urls = ["https://www.walmart.ca/en/grocery/fruits-vegetables/fruits/N-3852"]
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)


    def parse(self, response):
        items_list= []
        stores = ["3124", "3106"]
        product_query_info = {
            "products":{},
            "origin":"browse"
        }
        for item in response.css('article[data-rollup-id]'):
            items_list.append(self.parse_item(item))
            productQuery = item.xpath(".//input[@class='productQueryData']/@value").get()
            for key, value in (eval(productQuery)).items():
                product_query_info['products'][key] = value

        next_page = response.css("a#loadmore::attr(href)").get()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse, headers=self.headers)

        for branch in stores:
            product_query_info["stores"] = [branch]
            url = "https://www.walmart.ca/ws/en/products/availability"
            yield scrapy.Request(url, method='POST', callback=self.parse_info, meta={"products":items_list, "branch":branch}, headers=self.headers, body=json.dumps(product_query_info))


    def parse_item(self, item):
        product = {
            "sku":item.xpath(".//input[@class='productID']/@value").get(),
            "name": item.xpath(".//div[@class='title']/@aria-label").get(),
            "description":"",
            "url": item.xpath(".//a[@class='product-link']/@href").get(),
            "category": "Grocery | Fruits & Vegetables | Fruits",
            "brand": "",
            "package": item.xpath(".//div[@class='description']/text()").get(),
            "barcodes": item.xpath("@data-rollup-id").get(),
            "image_url": item.xpath(".//img[@class='image lazy-img']/@src").get()
        }
        return product


    def parse_info(self, response):
        data = json.loads(response.body)
        for item in response.meta.get("products"):
            item['branch'] = response.meta.get("branch")
            item['price'] = data[item['barcodes']]['online'][0]['maxCurrentPrice']
            item['store'] = data[item['barcodes']]['online'][0]['sellerName']
            item['stock'] = data[item['barcodes']]['online'][0]['sellerId']

            yield item
