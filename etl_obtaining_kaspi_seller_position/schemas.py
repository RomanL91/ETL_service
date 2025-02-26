from typing import Dict, List
from pydantic import BaseModel, ConfigDict, Field

# for examples
MASTER_SKU = "120323778"
CITY_IDS = [
    "352210000",
    "591010000",
    "196220100",
    "151010000",
    "710000000",
    "352410000",
    "750000000",
    "351010000",
    "195020100",
    "352810000",
    "353220100",
    "116651100",
    "195220100",
]
# for value default
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/132.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;"
        "q=0.8,application/signed-exchange;v=b3;q=0.7"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Content-Type": "application/json",
}


class KaspiRequestConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    headers: None | Dict[str, str] = Field(
        HEADERS,
        description="HTTP-заголовки запроса",
        examples=[HEADERS],
    )
    city_ids: List[str] = Field(
        ...,
        description="Список идентификаторов городов",
        examples=[CITY_IDS],
    )
    master_sku: str = Field(
        ...,
        description="Master SKU товара",
        examples=[MASTER_SKU],
    )
