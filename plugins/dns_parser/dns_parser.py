from bs4 import BeautifulSoup, PageElement
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumbase import Driver
from selenium.common.exceptions import TimeoutException
import re
import time


class DNSProduct:

    def __init__(self,
                 dns_product_link: str,
                 price: int,
                 image_link: str,
                 rating: float,
                 service_rating: str,
                 name: str,
                 stats: [str],
                 category: str):
        self.dns_product_link = dns_product_link
        self.price = price
        self.image_link = image_link
        self.rating = rating
        self.service_rating = service_rating
        self.name = name
        self.stats = stats
        self.category = category

    def __str__(self):
        return (f"Product name: {self.name}\n"
                f"Price: {self.price}\n"
                f"DNS link: {self.dns_product_link}\n"
                f"Image link: {self.image_link}\n"
                f"Rating: {self.rating}\n"
                f"Service rating: {self.service_rating}\n"
                f"Stats: {' '.join(i for i in self.stats)}\n"
                f"Category: {self.category}\n")

    @staticmethod
    def parse_dns_product(product_string: PageElement):
        title_element = product_string.find("a", class_="catalog-product__name")
        title = title_element.text if title_element else ""
        dns_product_link = "https://www.dns-shop.ru" + title_element['href'] if title_element else ""

        price_element = product_string.find("div", class_="product-buy__price")
        price = 0
        if price_element:
            price_text = price_element.text.strip()
            price = int(re.search(r'(\d{1,3}(?: \d{3})*)\s*₽', price_text).group(1).replace(' ', ''))

        image_element = product_string.find("img")
        if image_element:
            image_link = image_element.get('src') or image_element.get('data-src')
        else:
            image_link = ""

        rating_element = product_string.find("a", class_="catalog-product__rating")
        rating = rating_element['data-rating'] if rating_element else ""

        service_rating_element = product_string.find("a", class_="catalog-product__service-rating")
        service_rating = service_rating_element.text if service_rating_element else ""

        parsed_title = title.split('[')
        name = parsed_title[0] if parsed_title else ""
        stats = parsed_title[1].rstrip(']').split(', ') if len(parsed_title) > 1 else []

        return dns_product_link, price, image_link, rating, service_rating, name, stats


class DNSParser:
    def __init__(self):
        self.base_dns_catalog_url = "https://www.dns-shop.ru/catalog/"
        self.driver = Driver(uc=True, headless=True)

    def parse_catalog_category(self, category_id: str) -> [DNSProduct]:
        category_url = self.base_dns_catalog_url + category_id
        all_products = []

        self.driver.get(category_url)
        print(f"<- parsing {category_url} started...->")

        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)

            WebDriverWait(self.driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "img.loaded"))
            )

            soup = BeautifulSoup(self.driver.page_source, features="html.parser")
            products_selector = soup.find_all("div", class_='catalog-product')
            products_category = soup.find("h1", class_="title").get_text()
            print(products_category)

            for product in products_selector:
                product_object = DNSProduct(*DNSProduct.parse_dns_product(product), products_category)
                all_products.append(product_object)
                print(product_object)

            try:
                show_more_button = WebDriverWait(self.driver, 30).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "pagination-widget__show-more-btn"))
                )
                show_more_button.click()
                WebDriverWait(self.driver, 30).until(
                    EC.staleness_of(show_more_button)
                )
            except TimeoutException:
                print("No more pages to load")
                break

        self.driver.quit()
        # Странный фикс ошибки OSError которая вылетает при закрытии браузера
        try:
            time.sleep(0.1)
        except OSError:
            pass

        return all_products


def main():
    parser = DNSParser()
    category_id = "17a892f816404e77/noutbuki/"
    parser.parse_catalog_category(category_id)


if __name__ == "__main__":
    main()
