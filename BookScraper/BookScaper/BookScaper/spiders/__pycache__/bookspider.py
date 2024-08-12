import scrapy


class BookspiderSpider(scrapy.Spider):
    name = "bookspider"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com"]

    def parse(self, response):
        books = response.css("article.product_pod")

        for book in books:
            relative_url = response.css("h3 a ::attr(href)")

            if "catalogue/" in relative_url:
                book_url = "https://books.toscrape.com" + relative_url
            else:
                book_url = "https://books.toscrape.com/catalogue/" + relative_url
            yield response.follow(book_url, callback=self.parse_book_page)

        next_page = response.css("li.next a ::attr(href)")

        if next_page is not None:
            if "catalogue/" in next_page:
                next_page_url = "https://books.toscrape.com" + next_page
            else:
                next_page_url = (
                    "https://books.toscrape.com" + "catalogue/" + next_page_url
                )

            yield response.follow(next_page_url, callback=self.parse)

    def parse_book_page(self, response):
        table_rows = response.css("table tr")

        yield {
            "url": response.url,
            "title": response.css(".product_main h1::text").get(),
            "product_type": table_row[1].css("td ::text").get(),
            "price_excl. tax": table_row[2].css("td ::text").get(),
            "price_incl. tax": table_row[3].css("td ::text").get(),
            "tax": table_row[4].css("td ::text").get(),
        }
        pass
