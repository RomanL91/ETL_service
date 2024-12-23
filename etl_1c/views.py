import json
import httpx
import pandas as pd

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from core import settings
from donload import download


router = APIRouter(tags=["1C"])


async def fetch_vendor_codes():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                settings.elt_1c.url_get_vendor_code,
                headers={"Content-Type": "application/json", "Host": "localhost"},
            )
            response.raise_for_status()
            return response.json()
        except httpx.RequestError as e:
            print(f"An error occurred while requesting {e.request.url!r}.")
            raise HTTPException(status_code=400, detail="Error fetching vendor codes")
        except httpx.HTTPStatusError as e:
            print(
                f"Error response {e.response.status_code} while requesting {e.request.url!r}."
            )
            raise HTTPException(
                status_code=e.response.status_code,
                detail="Error with vendor code service",
            )


async def process_csv_data(filter_out=True, chunk_count=100, hourly=False):
    # Скачивание CSV файла с FTP-сервера
    download(hourly)

    read_file = (
        settings.elt_1c.path_to_hourly_CSV if hourly else settings.elt_1c.path_to_CSV
    )
    # Чтение данных из CSV файла
    data = pd.read_csv(
        read_file,
        delimiter=";",
        encoding="cp1251",
    )
    data.columns = [
        "product_name",
        "product_code",
        "price",
        "_1",
        "stock",
        "warehouse_code",
        "_2",
    ]
    # Удаление строк с NaN в product_code и столбцов
    data = data.drop(columns=["_1", "_2"])
    data = data.dropna(subset=["product_code"])

    # Преобразование данных в нужный формат
    # data["product_name"] = data["product_name"].str.replace(",", ".")
    # data["product_name"] = data["product_name"].str.replace('"', "*")
    # data["product_name"] = data["product_name"].str.replace("/", "**")
    # data["product_name"] = data["product_name"].str.replace("\n", "***")
    # data["product_name"] = data["product_name"].str.replace("\t", "****")
    # data["product_name"] = data["product_name"].str.replace("\r", "*****")
    # data["product_name"] = data["product_name"].str.replace("\r\n", "******")
    data["product_code"] = data["product_code"].astype(str).str.rstrip(".0")
    data["price"] = data["price"].fillna(0).astype(int)
    data["stock"] = data["stock"].fillna(0).astype(int)
    data["warehouse_code"] = data["warehouse_code"].fillna(0).astype(int)

    # Фильтрация данных по кодам поставщиков
    vendor_codes = await fetch_vendor_codes()
    if filter_out:
        filtered_data = data[~data["product_code"].isin(vendor_codes)]
    else:
        filtered_data = data[data["product_code"].isin(vendor_codes)]

    # Генерация чанков по количеству объектов
    for i in range(0, len(filtered_data), chunk_count):
        yield filtered_data.iloc[i : i + chunk_count].to_dict(orient="records")


@router.get("/get_for_create/")
async def go_tasks_chunked():
    # Генератор данных
    data_generator = process_csv_data(True, 150, False)

    # Функция стриминга
    async def stream_data():
        async for chunk in data_generator:
            yield json.dumps(chunk)

    # Возвращаем поток данных
    return StreamingResponse(
        stream_data(),
        media_type="application/json",
    )


@router.get("/get_for_update/")
async def go_update_tasks():
    data_generator = process_csv_data(False, 250, True)

    # Функция стриминга
    async def stream_data():
        async for chunk in data_generator:
            yield json.dumps(chunk)

    # Возвращаем поток данных
    return StreamingResponse(
        stream_data(),
        media_type="application/json",
    )
