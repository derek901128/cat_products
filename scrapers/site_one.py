from dataclasses import dataclass, asdict
from loggers.logger import set_logger
from playwright.sync_api import sync_playwright, TimeoutError
from selectolax.parser import HTMLParser
from typing import Optional
import httpx
import polars as pl
import time

@dataclass 
class Search:
    page: int
    sort_by: Optional[str] = ''
    order_by: Optional[str] = ''
    limit: int = 72

def get_html(client, base_url, search, headers):
    return client.get(
        base_url,
        params=asdict(search), 
        headers=headers
    ).text
    
def get_last_page_no(client, base_url, headers):
    current_page = 1
    
    while True:
        search = Search(page=current_page)
        html = get_html(client, base_url, search, headers)
        last_page = [int(node.text(strip=True)) 
                     for node in HTMLParser(html).css(".ProductList-paginator.pagination a")
                     if node.text(strip=True).isnumeric()][-1]
        
        if current_page == last_page:
            break
        current_page = last_page
    
    return current_page

def get_names(html):
    return [node.text(strip=True) 
            for node in HTMLParser(html).css("product-item .info-box div.title.text-primary-color")]

def get_prod_links(html):
    return [node.attributes["href"] 
            for node in HTMLParser(html).css("product-item a")]

def get_weight_flavor_price(url):
    price_selectors = [
        '.product-detail-actions .price:not(.price-crossed)',
        '.product-detail-actions .price-label:not(.price-crossed)',
        '.product-detail-actions .js-price .price:not(.price-crossed)',
        '.product-detail-actions .js-price:not(.price-crossed)'
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch()
        print("*****browser launched*****")
        page = browser.new_page()
        page.goto(url)
        print(f"*****{url} landed*****")

        price_selector = ''
        price = ''

        for selector in price_selectors:
            price = page.locator(selector, has_text='HK$').first
            if price.count() > 0:
                price = price.inner_text()
                price_selector = selector
                break

        if price == '':
            return [['NA', 'NA', 'NA']]

        print(f"*****Base price: {price}*****")
        print(f"*****price selector: {price_selector}*****")

        weight_flavor_price = []

        select_boxes = page.locator("div.select-box")

        print(f"****option types found: {select_boxes.count()}*****")

        match select_boxes.count():
            case 0:
                return [['NA', 'NA', price]]
            case 1:
                options = select_boxes.locator("div.variation-label.ng-binding.ng-scope").all()
                for option in options:
                    option.click()
                    print("*****option clicked!*****")
                    if option.inner_text()[0].isnumeric():
                        weight_flavor_price.append([
                            option.inner_text(),
                            'NA',
                            page.locator(price_selector, has_text='HK$').first.inner_text()
                        ])
                    else:
                        weight_flavor_price.append([
                            'NA',
                            option.inner_text(),
                            page.locator(price_selector, has_text='HK$').first.inner_text()
                        ])
            case _:
                weights = select_boxes.all()[0].locator("div.variation-label.ng-binding.ng-scope").all()
                print(f"***** Number of weights found: {len(weights)}*****")
                for weight in weights:
                    weight.click()
                    print("*****Weight clicked*****")
                    for flavor in select_boxes.all()[1].locator("div.variation-label.ng-binding.ng-scope:not(div.variation-label--out-of-stock)").all():
                            print(f'*****Number of flavors found: {len(select_boxes.all()[1].locator("div.variation-label.ng-binding.ng-scope:not(div.variation-label--out-of-stock)").all())}*****')
                            flavor.click()
                            print("*****Flavor clicked*****")
                            weight_flavor_price.append([
                                    weight.inner_text(),
                                    flavor.inner_text(),
                                    page.locator(price_selector, has_text='HK$').first.inner_text()
                            ])

        browser.close()

    return weight_flavor_price

def scrape_site_one() -> pl.DataFrame:
    start = time.perf_counter()

    logger = set_logger(logger_name='site_one_log')

    base_url = "https://www.teruhomu.com/categories/cat-product"

    headers = {
        "content-type" : "text/html; charset=utf-8",
        "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }

    products = []

    with httpx.Client() as client:
        logger.info("\n****************************************************************************************************************************\n")
        logger.info(f"Fetching from {base_url} starting")
        logger.info("\n****************************************************************************************************************************\n")
        
        last_page = get_last_page_no(client, base_url, headers)

        logger.info(f"Number of pages to be fetched: {last_page}")
        logger.info("")
        
        for page in range(1, last_page + 1):
            logger.info(f"Page no. {page} fetching..")
            
            search = Search(page=page)
                
            html = get_html(client, base_url, search, headers)

            names = get_names(html)
            logger.info("Names fetched !")
            
            links = get_prod_links(html)
            logger.info("Links fetched !")

            logger.info(f"Number of items to be fetched: {len(names)}")

            for name, link in zip(names, links):
                logger.info(f"fetching: {name} :: {link} ")
                for weight, flavor, price in get_weight_flavor_price(url=link):
                    products.append({
                        "sitenames": "卷卷巿集",
                        "names": name,
                        "prices": price,
                        "links": link,
                        "weights": weight,
                        "flavors": flavor
                    })

                    logger.info(f"----> Name: {name}, weight: {weight}, flavor: {flavor}, price: {price}, link: {link}")

            logger.info("")

    end = time.perf_counter()

    all_products = pl.DataFrame(products)

    logger.info("\n****************************************************************************************************************************\n")
    logger.info(f"Fetching from {base_url} finshed ! {len(products)} rows fetched. Time used: {end - start}")
    logger.info("\n****************************************************************************************************************************\n")

    return all_products

def main():
    scrape_site_one()

if __name__ == "__main__":
    main()

