import json
import random
import asyncio
import aiohttp

from itertools import cycle

from core import settings

import redis.asyncio as redis

redis_tasks = redis.Redis(
    host="185.100.67.246",
    port=6379,
    db=1,
    decode_responses=False,
)

redis_403 = redis.Redis(
    host="185.100.67.246",
    port=6379,
    db=2,
    decode_responses=False,
)

redis_result_db = redis.Redis(
    host="185.100.67.246",
    port=6379,
    db=3,
    decode_responses=False,
)


# Список FastAPI-инстансов
FASTAPI_INSTANCES = [
    "http://f5cbc5d9cf3b5a9d24f7b0632bb0067f.serveo.net/v1/kaspi/etl/get_seller_position/",
    "http://a313006f8846cfdef9f59d094082db9a.serveo.net/v1/kaspi/etl/get_seller_position/",
    # "http://cupiditate.serveo.net/v1/kaspi/etl/get_seller_position/",
    # "https://fideiussorem.serveo.net/v1/kaspi/etl/get_seller_position/",
    # "http://127.0.0.1:7777/v1/kaspi/etl/get_seller_position/",
    # "http://127.0.0.1:7777/v1/kaspi/etl/get_seller_position/",
    # "http://127.0.0.1:7778/v1/kaspi/etl/get_seller_position/",
    # "https://cupiditate.serveo.net/v1/kaspi/etl/get_seller_position/",
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
import pickle


# Настройка FastAPI
INSTANCE_NAME = "FA1"  # Для второго инстанса поменять на "FA2"
OTHER_INSTANCES = ["FA1", "FA2"]
OTHER_INSTANCES.remove(INSTANCE_NAME)
FASTAPI_403 = f"fastapi:403"

# Константы
BASE_URL = "https://kaspi.kz/yml/offer-view/offers/{master_sku}"  # URL-шаблон
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "referer": "https://kaspi.kz/",
    "Content-Type": "application/json",
}


# Лок для предотвращения одновременной обработки
processing_lock = asyncio.Lock()


async def fetch_data(session, city_id, master_sku):
    """Отправляет асинхронный HTTP-запрос к ресурсу."""
    url = BASE_URL.format(master_sku=master_sku)
    payload = {
        "cityId": city_id,
        "id": master_sku,
        "merchantUID": "",
        "limit": 50,
        "page": 0,
        "sortOption": "PRICE",
        "highRating": None,
        "searchText": None,
        "installationId": "-1",
    }

    try:
        async with session.post(url, json=payload, headers=HEADERS) as response:
            data = await response.text()
            status = response.status  # Код ответа

            if status == 200:
                print(f"✅ Успешный запрос ({city_id}): {data[:200]}")
                await redis_result_db.set(
                    f"{master_sku}:{city_id}",
                    data,  # Теперь сохраняем как строку
                    ex=3600,
                )

            elif status == 403:
                try:
                    print(f"❌ 403 Forbidden.")
                    instance = random.choice(
                        OTHER_INSTANCES
                    )  # Выбираю случайный инстанс fastapi
                    failed_task = {"city_ids": [city_id], "master_sku": master_sku}
                    existing_403_tasks_bytearray = await redis_403.get(instance)
                    existing_403_tasks = pickle.loads(existing_403_tasks_bytearray)

                    existing_403_tasks.append(
                        failed_task
                    )  # добавим в список провальную задачу
                    await redis_403.set(
                        instance, pickle.dumps(existing_403_tasks), ex=3600
                    )
                    print(f"🔄 Задача {master_sku} ({city_id}) сохранена в {instance}")
                except Exception as e:
                    print(f"- e -> {e}")
            else:
                print(f"⚠️ Ошибка {status} ({city_id}): {data[:200]}")

            return data
    except Exception as e:
        print(f"[ERROR] Ошибка при запросе {url} ({city_id}): {e}")
        return None


async def process_tasks(tasks):
    """Обрабатывает задачи: делает HTTP-запросы по каждому city_id"""

    if isinstance(tasks, str):
        tasks = json.loads(tasks)

    print(f"- tasks -> {tasks} -> {type(tasks)}")

    async with aiohttp.ClientSession() as session:
        requests = []
        for task in tasks:
            master_sku = task.get("master_sku")
            city_ids = task.get("city_ids", [])

            for city_id in city_ids:
                requests.append(fetch_data(session, city_id, master_sku))

        results = await asyncio.gather(*requests)
        print(f"[{INSTANCE_NAME}] Завершена обработка {len(results)} запросов.")


async def check_redis():
    """Периодически проверяет Redis и запускает обработку задач"""

    redis_key = f":1:fastapi:{INSTANCE_NAME}"  # так сохраняет Django

    while True:
        async with processing_lock:
            tasks_bytearray = await redis_tasks.get(redis_key)
            if tasks_bytearray:
                tasks = pickle.loads(tasks_bytearray)  # Десериализуем pickle
                print(f"- tasks > {tasks} -> {type(tasks)}")
                await process_tasks(tasks)
                await redis_tasks.set(redis_key, pickle.dumps([]))
            else:  # иначе проверим накопленные ошибки
                tasks_403_bytearra = await redis_403.get(INSTANCE_NAME)
                if tasks_403_bytearra:
                    tasks_403 = pickle.loads(tasks_403_bytearra)
                    print(f"- tasks_403 > {tasks_403} -> {type(tasks_403)}")
                    await process_tasks(tasks_403)
                    await redis_403.set(INSTANCE_NAME, pickle.dumps([]))
        # print("- DEBIUG PRINT check_redis END CYCLE -")


# async def check_redis(unic_key):
#     data = json.loads(await redis_client.get(unic_key))
#     return data
