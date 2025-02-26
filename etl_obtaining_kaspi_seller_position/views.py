import random

from fastapi import APIRouter

from etl_obtaining_kaspi_seller_position.utilites import fetch_all, check_redis

from etl_obtaining_kaspi_seller_position.schemas import KaspiRequestConfig

router = APIRouter(tags=["KASPI_seller_position"])


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]


@router.post("/get_seller_position/")
async def get_seller_position_from_kaspi(data: KaspiRequestConfig):
    print("--- RUN FUNK GET SELLER POS ---")
    data.headers["User-Agent"] = random.choice(USER_AGENTS)
    data_to_request = data.model_dump()
    result = await fetch_all(**data_to_request)
    return result


@router.get("/check/")
async def check(unic_key):
    return await check_redis(unic_key)
