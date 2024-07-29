from fastapi import APIRouter

from etl_kaspi.models import DataPost
from etl_kaspi.utilites import fetch_archive_orders_from_kaspi


router = APIRouter(tags=["KASPI"])


@router.post("/get_archive_orders/")
async def get_archive_orders_from_kaspi(data: DataPost):
    return await fetch_archive_orders_from_kaspi(data)
