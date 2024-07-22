import httpx
import pandas as pd

from fastapi import APIRouter, HTTPException

from core import settings


router = APIRouter(tags=["1C"])


async def fetch_vendor_codes():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(settings.elt_1c.url_get_vendor_code)
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


async def process_csv_data(filter_out=True):
    # Чтение данных из CSV файла
    data = pd.read_csv(
        settings.elt_1c.path_to_CSV,
        delimiter=";",
        encoding="cp1251",
    )
    data.rename(
        columns={
            "Товар наименование": "product_name",
            "код товара": "product_code",
            "цена": "price",
            "Остаток": "stock",
            "код склада": "warehouse_code",
        },
        inplace=True,
    )
    data["product_code"] = data["product_code"].astype(str)

    vendor_codes = await fetch_vendor_codes()
    if filter_out:
        filtered_data = data[~data["product_code"].isin(vendor_codes)]
    else:
        filtered_data = data[data["product_code"].isin(vendor_codes)]
    return filtered_data.to_dict(orient="records")


@router.get("/get_for_create/")
async def go_tasks():
    filtered_data = await process_csv_data(True)
    return filtered_data


@router.get("/get_for_update/")
async def go_update_tasks():
    filtered_data = await process_csv_data(False)
    return filtered_data
