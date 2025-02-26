import json
import random
import asyncio
import aiohttp

from itertools import cycle

from core import settings

import redis.asyncio as redis

redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=2,
    decode_responses=True,
)


# Список FastAPI-инстансов
FASTAPI_INSTANCES = [
    # "http://127.0.0.1:7777/v1/kaspi/etl/get_seller_position/",
    "http://127.0.0.1:7778/v1/kaspi/etl/get_seller_position/",
    "https://cupiditate.serveo.net/v1/kaspi/etl/get_seller_position/",
    # "http://127.0.0.1:7779/v1/kaspi/etl/get_seller_position/",
    # "http://127.0.0.1:7780/v1/kaspi/etl/get_seller_position/",
]

# Балансировка (Round Robin)
instance_cycle = cycle(FASTAPI_INSTANCES)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]


async def fetch_all(headers: dict, master_sku: str, city_ids: list):
    """Функция парсинга с использованием aiohttp и балансировкой при 403"""

    kaspi_url = settings.etl_sellers_pos.url.format(master_sku=master_sku)
    headers.setdefault("referer", kaspi_url)

    # Ограничиваем число одновременных соединений
    connector = aiohttp.TCPConnector(limit_per_host=10)
    timeout = aiohttp.ClientTimeout(total=30)
    semaphore = asyncio.Semaphore(5)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        offers_list = {}

        async def fetch(city_id):
            """Функция отправки запроса и балансировки при 403."""
            payload = {
                "cityId": city_id,
                "id": master_sku,
                "merchantUID": "",
                "limit": 50,
                "page": 0,
                "sortOption": "PRICE",
                "highRating": None,
                "searchText": None,
                "zoneId": ["Magnum_ZONE5"],
                "installationId": "-1",
            }

            async with semaphore:  # Ограничиваем число одновременных запросов
                try:
                    headers["User-Agent"] = random.choice(
                        USER_AGENTS
                    )  # Меняем User-Agent
                    async with session.post(
                        kaspi_url, json=payload, headers=headers
                    ) as response:
                        if response.status == 200:
                            try:
                                data = await response.json()
                                offers_list[city_id] = data
                                print(
                                    f"✅ {master_sku} Успешный ответ для города {city_id}"
                                )
                                return
                            except aiohttp.ContentTypeError:
                                print(
                                    f"❌ Ошибка JSON-декодирования для города {city_id}"
                                )

                        elif response.status == 403:
                            print(
                                f"🚫 403 Forbidden для {city_id}, ждем 1.5 сек и перенаправляем..."
                            )

                            # await asyncio.sleep(1.5)  # Ждем перед повторной попыткой
                            new_url = next(instance_cycle)  # Новый FastAPI-инстанс

                            try:
                                async with session.post(
                                    new_url,
                                    json={
                                        "city_ids": [city_id],
                                        "master_sku": master_sku,
                                    },
                                    headers=headers,
                                ) as new_response:

                                    if new_response.status == 200:
                                        new_data = await new_response.json()
                                        offers_list[city_id] = new_data
                                        print(
                                            f"🔀 {city_id} успешно обработан на {new_url}"
                                        )
                                    else:
                                        print(
                                            f"⚠️ {city_id} не обработан, ошибка {new_response.status} на {new_url}"
                                        )

                            except aiohttp.ClientError as exc:
                                print(
                                    f"❌ Ошибка запроса {city_id} на {new_url}: {exc}"
                                )

                        else:
                            print(f"⚠️ Ошибка {response.status} для города {city_id}")

                except aiohttp.ClientError as exc:
                    print(f"❌ Ошибка запроса для города {city_id}: {exc}")

        # Запускаем все запросы параллельно
        await asyncio.gather(*[fetch(city_id) for city_id in city_ids])

    # Сохраняем результат в Redis
    print("_____________FINAL_____________")
    # data_redis = json.dumps(offers_list)
    # await redis_client.set(master_sku, data_redis)
    return master_sku


# ++++++++++++++++++++++++++++++++++


async def check_redis(unic_key):
    data = json.loads(await redis_client.get(unic_key))
    return data
