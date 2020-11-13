import scrapy
from scrapers.items import ProductItem
import json
import requests


class CaWalmartSpider(scrapy.Spider):
    name = "ca_walmart"
    allowed_domains = ["walmart.ca"]
    headers = {
        "Accept": "application/json",
        "Content-type": "application/json",
        "X-Requested-With": "XMLHttpRequest"
    }

    api_headers = {
        "Content-Type": "application/json;charset=utf-8",
        "Server": "nginx",
        "X-Bazaarvoice-Api-Version": "5.5"
    }

    bazaarvoice_key = "e6wzzmz844l2kk3v6v7igfl6i"

    def start_requests(self):
        start_urls = ["https://www.walmart.ca/en/grocery/fruits-vegetables/fruits/N-3852"]
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse, headers=self.headers)


    def parse(self, response):
        items_list= []
        # stores to check
        stores = ["3124", "3106"]
        # basic payload structure
        product_query_info = {
            "products":{},
            "origin":"browse"
        }

        # loop all products of the page
        for item in response.css('article[data-rollup-id]'):
            # parse each item basic information
            parsed_item = self.parse_item(item)
            # construct a list with the products to extract the info from a json
            items_list.append(parsed_item)
            # construct a query info structure to send as payload for the branches availability check
            productQuery = item.xpath(".//input[@class='productQueryData']/@value").get()
            for key, value in (eval(productQuery)).items():
                product_query_info['products'][key] = value

        # run the pagination
        next_page = response.css("a#loadmore::attr(href)").get()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse, headers=self.headers)


        # get products for each branch
        for branch in stores:
            product_query_info["stores"] = [branch]
            url = "https://www.walmart.ca/ws/en/products/availability"
            yield scrapy.Request(url, method='POST', callback=self.parse_info,
                            meta={"products":items_list, "branch":branch},
                            headers=self.headers, body=json.dumps(product_query_info))


    def parse_item(self, item):
        """Run the selectors to get the basic products info"""

        product = {
            "sku":item.xpath(".//input[@class='productID']/@value").get(),
            "name": item.xpath(".//div[@class='title']/@aria-label").get(),
            "url": item.xpath(".//a[@class='product-link']/@href").get(),
            "category": "Grocery | Fruits & Vegetables | Fruits",
            "package": item.xpath(".//div[@class='description']/text()").get(),
            "barcodes": item.xpath("@data-rollup-id").get(),
        }
        return product


    def parse_info(self, response):
        """ Receive the response from the availability check api and parse info from json """
        data = json.loads(response.body)
        for item in response.meta.get("products"):
            item['branch'] = response.meta.get("branch")
            item['price'] = data[item['barcodes']]['online'][0]['maxCurrentPrice']
            item['store'] = data[item['barcodes']]['online'][0]['sellerName']
            item['stock'] = data[item['barcodes']]['online'][0]['sellerId']
            yield item


    def get_product_details(self, item):
        """ Make a request to the products detail api and parse the info """
         # get item details
        api = "https://api.bazaarvoice.com/data/products.json?passkey="+self.bazaarvoice_key+"&apiversion=5.5&filter=id%3Aeq%3A"+item['barcodes']
        r = requests.get(api)
        data = json.loads(r.text)
        item['description'] = data['Results'][0]['Description']
        item["image_url"] = data['Results'][0]['ImageUrl']
        item["brand"] = data['Results'][0]['Brand']['Name']
        return item
