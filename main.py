import httpx
import uvicorn
import pandas as pd
from fastapi import FastAPI, HTTPException


app = FastAPI()


URL_VENDOR_CODES = 'http://127.0.0.1:8000/api/v1/products/all/vendor_cods/'


async def fetch_vendor_codes():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(URL_VENDOR_CODES)
            response.raise_for_status()
            return response.json()
        except httpx.RequestError as e:
            print(f"An error occurred while requesting {e.request.url!r}.")
            raise HTTPException(status_code=400, detail="Error fetching vendor codes")
        except httpx.HTTPStatusError as e:
            print(f"Error response {e.response.status_code} while requesting {e.request.url!r}.")
            raise HTTPException(status_code=e.response.status_code, detail="Error with vendor code service")

async def process_csv_data(filter_out=True):
    # Чтение данных из CSV файла
    data = pd.read_csv('D:\DEV\etl\CSV2_ежечасная выгрузка на сайт измененных карточек.csv', delimiter=';', encoding='cp1251')
    data.rename(columns={
        'Товар наименование': 'product_name',
        'код товара': 'product_code',
        'цена': 'price',
        'Остаток': 'stock',
        'код склада': 'warehouse_code'
    }, inplace=True)
    data['product_code'] = data['product_code'].astype(str)

    vendor_codes = await fetch_vendor_codes()
    if filter_out:
        filtered_data = data[~data['product_code'].isin(vendor_codes)]
    else:
        filtered_data = data[data['product_code'].isin(vendor_codes)]
    return filtered_data.to_dict(orient='records')

@app.get("/go/")
async def go_tasks():
    print("Running from CELERY task")
    filtered_data = await process_csv_data(True)
    print(f"Filtered Data: {filtered_data}")
    return filtered_data

@app.get("/go_update/")
async def go_update_tasks():
    print("Running from CELERY task")
    filtered_data = await process_csv_data(False)
    print(f"Filtered Data: {filtered_data}")
    return filtered_data

if __name__ == "__main__":
    uvicorn.run("main:app", port=7777, reload=True)
