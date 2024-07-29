from pydantic import BaseModel


class DataPost(BaseModel):
    kaspi_token: str
    params: dict
    orders_api: str
