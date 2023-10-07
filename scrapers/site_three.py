from loggers.logger import set_logger
from selectolax.parser import HTMLParser
import httpx
import polars as pl
import time

def get_html(client, base_url, headers):
    return client.get(base_url, headers=headers).text

def get_manufacturers_links(html):
    return [node.attributes["href"]
            for node in HTMLParser(html).css(".manufacturer-content li a")]

def get_manufactuers_page_count(html):
    manu_page_count = HTMLParser(html).css_first(".pagination .results", default=0)
    
    if manu_page_count != 0:
        manu_page_raw_string = manu_page_count.text(strip=True)
        first_letter = manu_page_raw_string.find("計")
        sec_letter = manu_page_raw_string.find("頁")
        manu_page_count = int(manu_page_raw_string[first_letter+1:sec_letter].strip())
        
    return manu_page_count

def get_products_names(html):
    return [node.text(strip=True)
            for node in HTMLParser(html).css(".name a")] 
    
def get_products_links(html):
    return [node.attributes['href']
            for node in HTMLParser(html).css(".name a")]
    
def get_products_prices(html):
    prices = []
    
    for node in HTMLParser(html).css('.price'):
        if not node.any_css_matches(('.price-new',)):
            prices.append(node.text(strip=True))
        else:
            if not node.any_css_matches(('.discount', )):
                prices.append(node.css_first('.price-new').text(strip=True))
            else:
                prices.append(node.css_first('.price-new').text(strip=True) + node.css_first('.discount').text(strip=True))
                
    return prices

def scrape_site_three() -> pl.DataFrame:
    start = time.perf_counter()

    logger = set_logger(logger_name='site_three_log')

    base_url = "https://www.catiscat.com.hk/index.php?route=product/manufacturer"

    headers = {
        "content-type" : "text/html; charset=utf-8",
        "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    
    dfs = []
    
    with httpx.Client() as client:
        logger.info("\n****************************************************************************************************************************\n")
        logger.info(f"Fetching from {base_url} starting")
        logger.info("\n****************************************************************************************************************************\n")
        
        html = httpx.get(base_url, headers=headers).text
        
        manufacturers_links = get_manufacturers_links(html)
        
        for manu_link in manufacturers_links:
            logger.info(f"Fetching from {manu_link}")
            manu_html = get_html(client, f"{manu_link}&limit=100", headers)
            logger.info("Manufacturer html fetched")
            manu_name = HTMLParser(manu_html).css_first(".heading-title").text(strip=True)
            logger.info("Manufacturer name fetched: {manu_name}")
            manu_page_count = get_manufactuers_page_count(manu_html)
            logger.info(f"Number of pages for be fetched for {manu_name}: {manu_page_count}")
            
            match manu_page_count:
                case 0:
                    logger.info(f"----> {manu_name} got no products....")
                case 1:
                    manu_product_names = get_products_names(manu_html)
                    logger.info(f"----> {manu_name}: Names fetched !")
                    manu_product_prices = get_products_prices(manu_html)
                    logger.info(f"----> {manu_name}: Prices fetched !")
                    manu_product_links = get_products_links(manu_html) 
                    logger.info(f"----> {manu_name}: Link fetched !")

                case _:
                    logger.info(f"----> {manu_name} got more !!!!!")
                    
            df = pl.DataFrame({
                "sitenames": "Cat is Cat",
                "names": manu_product_names,
                "prices": manu_product_prices,
                "links": manu_product_links,
                "manufacturer_names": manu_name
            })

            dfs.append(df)

    all_products = pl.concat(
        dfs, 
        how="vertical"
    )
    
    end = time.perf_counter()
    
    logger.info("\n****************************************************************************************************************************\n")
    logger.info(f"Fetching from {base_url} finshed ! {all_products.height} rows fetched. Time used: {end - start}")
    logger.info("\n****************************************************************************************************************************\n")

    return all_products

def main():
    scrape_site_three()

if __name__ == "__main__":
    main()