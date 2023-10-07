from loggers.logger import set_logger
from selectolax.parser import HTMLParser
import httpx
import polars as pl
import time

def get_html(client, base_url, headers):
    return client.get(base_url, headers=headers).text
    
def get_last_page_no(client, headers):
    current_page = 2
    while True:
        html = get_html(client, f"https://superpetmo.com/product-category/cats/page/{current_page}/", headers)
        last_page = [int(node.text(strip=True)) 
                     for node in HTMLParser(html).css("ul.page-numbers li") 
                     if node.text(strip=True).isnumeric()][-1]
        
        if current_page == last_page:
            break
    
        current_page = last_page
    
    return current_page

def get_names(html):
    return [node.text(strip=True) 
            for node in HTMLParser(html).css(".woocommerce-loop-product__title")]

def get_prices(html):
    return [node.text(strip=True).split("$")[-1]
            if node.text(strip=True).find("$", 1) > 0
            else node.text(strip=True).split("$")[1]
            for node in HTMLParser(html).css(".price")]

def get_links(html):
    return [node.attributes["href"] 
            for node in HTMLParser(html).css(".products.columns-5 li div:nth-child(1) a:nth-child(1)")]

def scrape_site_two():
    start = time.perf_counter()

    logger = set_logger(logger_name='site_two_log')
    
    base_url = "https://superpetmo.com/product-category/cats/"
    
    headers = {
        "content-type" : "text/html; charset=utf-8",
        "User-Agent" : "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    
    dfs = []
    
    with httpx.Client() as client:
        logger.info("\n****************************************************************************************************************************\n")
        logger.info(f"Fetching from {base_url} starting")
        logger.info("\n****************************************************************************************************************************\n")
        
        last_page = get_last_page_no(client, headers) 
        
        logger.info(f"Number of pages to be fetched: {last_page - 1}")
        logger.info("")
        
        for i in range(1, last_page):
            logger.info(f"Page no. {i} fetching..")
            
            match i:
                case 1:
                    url = "https://superpetmo.com/product-category/cats/"
                case _:
                    url = f"https://superpetmo.com/product-category/cats/page/{i}/"
                    
            html = get_html(client, url, headers)
        
            names = get_names(html)
            logger.info("----> Names fetched !")
            
            prices = get_prices(html)
            logger.info("----> Prices fetched !")
            
            links = get_links(html)
            logger.info("----> Links fetched !")
            logger.info("")
            
            df = pl.DataFrame(
                {
                    "sitenames": "Super Pet",
                    "names": names,
                    "prices": prices,
                    "links": links
                }
            ).with_columns(
                pl.col("names")
                .str.extract_groups(r'([0-9]+\.?[0-9]*?[[kg]?g?[ml]?l?L?ML?]+)')
                .struct["1"]
                .alias("weights")
            )

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
    scrape_site_two().write_csv("site_two.csv")

if __name__ == "__main__":
    main()
