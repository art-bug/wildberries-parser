import asyncio
from asyncio.timeouts import timeout
import random
import json
import functools
from dataclasses import asdict, dataclass
from string import Template as t
from typing import Any, Dict, List, Optional

import httpx
import pandas as pd
from fake_useragent import UserAgent

# API Constants
SEARCH_API = "https://search.wb.ru/exactmatch/ru/common/v4/search"
DEFAULT_DEST = -1257786

# String Templates
GEO_API = t("https://search.wb.ru/geo/get?city=$city")
BASKET_BASE = t("https://basket-$basket.wb.ru/vol$vol/part$part/$nm_id")
ITEM_LINK = t("https://www.wildberries.ru/catalog/$nm_id/detail.aspx")
SELLER_LINK = t("https://www.wildberries.ru/seller/$brand_id")
INFO_JSON = t("$base_url/info/ru/card.json")
IMAGE_LINK = t("$base_url/images/big/$num.jpg")

# Mapping for Excel output
EXCEL_COLUMNS = {
    "link": "Ссылка на товар",
    "id": "Артикул",
    "name": "Название",
    "price": "Цена",
    "description": "Описание",
    "images": "Ссылки на изображения через запятую",
    "characteristics": "Все характеристики с сохранением их структуры",
    "seller_name": "Название селлера",
    "seller_link": "Ссылка на селлера",
    "sizes": "Размеры товара через запятую",
    "stock": "Остатки по товару (число)",
    "rating": "Рейтинг",
    "feedbacks": "Количество отзывов"
}

@dataclass(slots=True, frozen=True)
class Product:
    """Product data model"""

    link: str
    id: int
    name: str
    price: float
    description: str
    images: str
    characteristics: str
    seller_name: str
    seller_link: str
    sizes: str
    stock: int
    rating: float
    feedbacks: int
    country: str  # Internal field for filtering

def with_client(func=None, **client_kwargs):
    """
    Decorator that injects an AsyncClient.
    Accepts any arguments supported by httpx.AsyncClient.
    """

    if func is None:
        return lambda f: with_client(f, **client_kwargs)

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # If client is passed explicitly (DI), use it
        if kwargs.get('client'):
            return await func(*args, **kwargs)

        # Otherwise, create a new one with given params
        async with httpx.AsyncClient(**client_kwargs) as client:
            kwargs['client'] = client
            return await func(*args, **kwargs)

    return wrapper

class WBParser:
    def __init__(self):
        self.ua = UserAgent()
        self.limit_sem = asyncio.Semaphore(5) # Throttling for stability

    def _get_basket_url(self, nm_id: int) -> str:
        """Calculates basket URL based on WB storage distribution algorithm."""

        vol = nm_id // 100_000
        part = nm_id // 1_000

        # Current WB server intervals
        if vol <= 143: basket = "01"
        elif vol <= 287: basket = "02"
        elif vol <= 431: basket = "03"
        elif vol <= 719: basket = "04"
        elif vol <= 1007: basket = "05"
        elif vol <= 1061: basket = "06"
        elif vol <= 1115: basket = "07"
        elif vol <= 1169: basket = "08"
        elif vol <= 1313: basket = "09"
        elif vol <= 1601: basket = "10"
        elif vol <= 1655: basket = "11"
        elif vol <= 1919: basket = "12"
        elif vol <= 2045: basket = "13"
        elif vol <= 2189: basket = "14"
        else: basket = "15"

        return BASKET_BASE.substitute(basket=basket, vol=vol, part=part, nm_id=nm_id)

    def _middleware_response_handler(self, response, *, parse_json: bool = True):
        """Centralized check for all responses."""

        # Check for HTTP status (4xx, 5xx)
        response.raise_for_status()

        # Handle empty response (e.g. 204 No Content)
        if response.status_code == 204 or not response.content:
            return None

        if not parse_json:
            return response

        # Check for header Content-Type
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' not in content_type:
            msg = f"Expected JSON, got {content_type}\n{response.content}"
            raise httpx.DecodingError(msg)

        # Try to parse
        try:
            return response.json()
        except ValueError:
            raise httpx.DecodingError("Response content is not valid JSON")

    @with_client
    async def _request(self, url, method: str = "GET", parse_json: bool = True, client: httpx.AsyncClient = None, **kwargs):
        """Unified entry point for all requests with exponential backoff for 429 errors."""

        max_retries = 5
        for attempt in range(max_retries):
            try:
                response = await client.request(method, url, **kwargs)

                if response.status_code == 429:
                    wait_time = (2 ** attempt) + random.random()
                    msg_on_429 = (
                        f"[429] Limit reached. Waiting {wait_time:.2f}s..."
                        f" (Attempt {attempt+1}/{max_retries})"
                    )
                    print(msg_on_429)

                    await asyncio.sleep(wait_time)
                    continue

                return self._middleware_response_handler(response, parse_json=parse_json)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    wait_time = (2 ** attempt) + random.random()
                    await asyncio.sleep(wait_time)
                    continue
                raise

        raise Exception("Max retries exceeded")

    @with_client
    async def _get_dest_id(self, city: str, client: httpx.AsyncClient = None) -> int:
        """Dynamic determination of destination ID for a region."""

        try:
            res = await self._request(GEO_API.substitute(city=city), 
                # params={"area": city}, 
                client=client
            )
            return res.get("dest", DEFAULT_DEST)
        except Exception:
            return DEFAULT_DEST

    @with_client(timeout=10)
    async def _fetch_details(self, item: Dict[str, Any], client: httpx.AsyncClient = None) -> Product:
        """Enriches product data with description and characteristics."""

        async with self.limit_sem:
            nm_id = item["id"]
            base_url = self._get_basket_url(nm_id)

            try:
                url = INFO_JSON.substitute(base_url=base_url)
                details = await self._request(url, client=client)
            except Exception:
                details = {}

            opts = {o["name"]: o["value"] for o in details.get("options", [])}

            return Product(
                link=ITEM_LINK.substitute(item_id=nm_id),
                id=nm_id,
                name=item.get("name", ''),
                price=item.get("salePriceU", 0) / 100,
                description=details.get("description", ''),
                images=', '.join(
                    IMAGE_LINK.substitute(base_url=base_url, num=i)
                    for i in range(1, 4)
                ),
                characteristics=json.dumps(opts, ensure_ascii=False),
                seller_name=item.get("brand", ''),
                seller_link=SELLER_LINK.substitute(brand_id=item.get("brandId")),
                sizes=', '.join(
                    s.get("name", '')
                    for s in item.get("sizes", [])
                ),
                stock=sum(
                    sum(st.get("qty", 0) for st in s.get("stocks", []))
                    for s in item.get("sizes", [])
                ),
                rating=item.get("rating", 0),
                feedbacks=item.get("feedbacks", 0),
                country=opts.get("Страна производства", "Не указана")
            )

    async def run(self, query: str, city: str = "Москва", pages: int = 1) -> List[Product]:
        """Starts the parser."""

        headers = {
            "User-Agent": self.ua.random,
            "Accept": "*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Origin": "https://www.wildberries.ru",
            "Referer": "https://www.wildberries.ru/",
            "Connection": "keep-alive"
        }

        params = {
            "query": query,
            "resultset": "catalog",
            "sort": "popular",
            "appType": 1,
            "curr": "rub",
            "locale": "ru"
        }

        client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=20)

        async with client:
            dest = await self._get_dest_id(city, client=client)
            params.update(dest=dest)

            raw_items = []
            for page in range(1, pages + 1):
                params.update(page=page)

                res = await self._request(SEARCH_API, params=params, client=client)
                items = res.get("data", {}).get("products", [])
                if not items: break

                raw_items.extend(items)

            tasks = [self._fetch_details(item, client=client) for item in raw_items]
            return list(await asyncio.gather(*tasks))

async def main():
    query = "пальто из натуральной шерсти"
    parser = WBParser()
    products = await parser.run(query, pages=1)

    if not products:
        print("Данных нет.")
        return

    # Products to DataFrame
    df = pd.DataFrame([asdict(p) for p in products])

    # 1. Full catalog
    df_full = df.drop(columns=["country"]).rename(columns=EXCEL_COLUMNS)
    df_full.to_excel("wb_full.xlsx", index=False)

    # 2. Filtering
    # (Рейтинг >= 4.5, Цена <= 10000, Страна == Россия)
    mask = (df["rating"].astype(float) >= 4.5) & (df["price"].astype(float) <= 10000) & (df["country"] == "Россия")
    df_filtered = df[mask].drop(columns=["country"]).rename(columns=EXCEL_COLUMNS)
    df_filtered.to_excel("wb_filtered.xlsx", index=False)

    print(f"Готово. Всего: {len(df_full)}. Отфильтровано: {len(df_filtered)}")

if __name__ == "__main__":
    import sys
    # If we are in Jupyter/IPython
    if 'ipykernel' in sys.modules:
        await main()
    else:
        asyncio.run(main())
