from dataclasses import dataclass, asdict
from enum import Enum
from loggers.logger import set_logger
from typing import Optional
import httpx
import polars as pl
import time

@dataclass
class Data:
    cUids: Optional[int]
    key: Optional[str] = ''
    checkCatSaleTime: bool = False
    pageIdx: int = 0
    tags: str = ''
    categories: str = ''
    size: int = 100
    ignoreCart: bool = True
    comboCategoryUid: int = 0
    isSeries: bool = True

class CID(Enum):
    貓砂 = 1561012722827715845
    貓乾糧 = 1561012722827728954
    貓濕糧 = 1561012722827797248
    貓零食 = 1566793316267736535
    貓保健小食 = 1618821872897261690
    貓保健食品 = 1566793346048691155
    貓用品 = 1566803538869630398
    清潔用品 = 1566803572817377679
    貓護理用品 = 1566803601014481165
    貓玩具 = 1566803562503321675
    義賣物 = 1566803528340631029
    民宿服務 = 1566803489089399246
    訂貨物品 = 1566803500351457596
    脫水原肉_凍乾零食 = 1691742581589971051
    日本Aixia = 1692418546358774880
    台灣喵探長 = 1695013555144202837
    肉塊_肉條零食 = 1695182584078522688
    幫助排毛 = 1695182584078629504
    啫喱_果凍零食 = 1695290629734512504

base_url = "http://zds86-17.pospal.cn/wxapi/product/ListMulti"

headers = {
    'Host': 'zds86-17.pospal.cn',
    'Accept': '*/*',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Origin': 'http://zds86-17.pospal.cn',
    'Referer': 'http://zds86-17.pospal.cn/d?fbclid=IwAR1t71PcaYBsZ6Hx0sznHhKj4lknxjMBdmDO965Q_Oykx6fk7JkPqJ_ioec',
    'Accept-Language':'en-GB,en-US;q=0.9,en;q=0.8',
    'Cookie': 'uuvid=UwACWwE6VjcANgJtW2hdPAI8AWIJYAhpBWELOgRsV2ICZFY2X2wFMFdmAz5aYAszVTcCZg5hWjIBYVFgAWxSNFNjAjE=; isLogin=false; Hm_lvt_7d46a3151782b7a795ffeba367b5387d=1695455740; Hm_lpvt_7d46a3151782b7a795ffeba367b5387d=1695455777'
}

names = []
prices = []
categories = []
links = []

def scrape_site_four() -> pl.DataFrame:
    start = time.perf_counter()

    logger = set_logger(logger_name='site_four_log')

    logger.info("\n****************************************************************************************************************************\n")
    logger.info(f"Fetching from {base_url} starting")
    logger.info("\n****************************************************************************************************************************\n")

    with httpx.Client() as client:

        cur_pg = 0

        for cid in CID:
            while True:
                logger.info(f"Fetching from {cid}")

                data = asdict(Data(
                    cUids=cid.value,
                    pageIdx=cur_pg
                ))

                res = client.post(
                    url=base_url,
                    data=data,
                    headers=headers
                )

                if res.json()['count'] == 0:
                    cur_pg = 0
                    break

                for product in res.json()['data']:
                    names.append(product['name'])
                    prices.append(product['sellPrice'])
                    categories.append(product['category']['name'])
                    links.append(f"http://zds86-17.pospal.cn/m#/details/{product['id']}")

                    logger.info(f"-----> Category: {product['category']['name']}")
                    logger.info(f"-----> Name: {product['name']}")
                    logger.info(f"-----> Price: {product['sellPrice']}")
                    logger.info(f"-----> link: http://zds86-17.pospal.cn/m#/details/{product['id']}")

                cur_pg += 1

    all_products = pl.DataFrame({
        "sitenames": "皮皮",
        "names": names,
        "prices": prices,
        "links": links,
        "categories": categories
    }).with_columns(
        pl.col("prices").cast(pl.Utf8),
        pl.col("names").str.extract(r"([0-9]+\.?[0-9]*?[[kg]?[KG]?g?G?[ml]?[ML]?l?L?磅?CC?oz])").alias("weights")
    )

    end = time.perf_counter()

    logger.info("\n****************************************************************************************************************************\n")
    logger.info(f"Fetching from {base_url} finshed ! {all_products.height} rows fetched. Time used: {end - start}")
    logger.info("\n****************************************************************************************************************************\n")

    return all_products

def main():
    scrape_site_four()

if __name__ == "__main__":
    main()