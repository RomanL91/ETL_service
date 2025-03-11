import json
import uvicorn
import asyncio
import aiohttp
import aio_pika
import redis.asyncio as redis
from fastapi import FastAPI
from contextlib import asynccontextmanager

BASE_URL = "https://kaspi.kz/yml/offer-view/offers/{master_sku}"
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "referer": "https://kaspi.kz/",
    "Content-Type": "application/json",
}

RABBITMQ_HOST = "amqp://guest:guest@185.100.67.246/"

app = FastAPI()


redis_result_db = redis.Redis(
    host="185.100.67.246",
    port=6379,
    db=5,
    decode_responses=False,
)


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
                print(f"✅ 200 OK ({city_id}): {data[:200]}")
                await redis_result_db.set(
                    f"{master_sku}:{city_id}",
                    data,  # Теперь сохраняем как строку
                    ex=9600,
                )
            elif status == 403:
                print(f"❌ 403 Forbidden ({city_id}). Повторная попытка...")

            return status
    except Exception as e:
        print(f"[ERROR] Ошибка при запросе {url} ({city_id}): {e}")
        return None


async def process_message(message: aio_pika.IncomingMessage):
    """Обрабатывает сообщение из RabbitMQ"""
    try:
        data = json.loads(message.body)
        city_id = data["city_id"]
        master_sku = data["master_sku"]

        async with aiohttp.ClientSession() as session:
            status = await fetch_data(session, city_id, master_sku)

        if status == 200:
            print(f"✅ Успешная обработка, сообщение удалено: {data}")
            await message.ack()  # Подтверждаем обработку (удаляем из очереди)

        elif status == 403:
            print(f"🔄 403 Forbidden, возвращаем в очередь: {data}")
            await message.nack(requeue=True)  # Повторная обработка

        else:
            print(f"⚠️ Ошибка обработки {data}, отбрасываем сообщение.")
            await message.nack(requeue=False)  # Сообщение удаляется

    except Exception as e:
        print(f"❌ Ошибка обработки сообщения: {e}")
        await message.nack(requeue=False)  # Сообщение удаляется


async def consume():
    """Асинхронное получение сообщений из RabbitMQ"""
    connection = await aio_pika.connect_robust(RABBITMQ_HOST)
    channel = await connection.channel()

    # Ограничиваем количество сообщений, которые инстанс берет за раз
    await channel.set_qos(prefetch_count=1)

    queue = await channel.declare_queue("parsing_tasks", durable=True)

    print("🔄 [*] Ожидание сообщений. Для выхода нажмите CTRL+C")

    await queue.consume(process_message)  # Подписка на очередь


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Асинхронный обработчик жизненного цикла приложения"""
    task = asyncio.create_task(consume())  # Асинхронный запуск RabbitMQ
    yield
    task.cancel()
    print("🔴 Завершаем обработчик RabbitMQ")


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"message": "FastAPI RabbitMQ worker запущен!"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8004)
