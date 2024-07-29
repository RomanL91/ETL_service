from pathlib import Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings


BASE_DIR = Path(__file__).parent.parent


class SettingsETLproc1C(BaseModel):
    path_to_CSV: str = (
        "D:\DEV\etl\CSV2_ежечасная выгрузка на сайт измененных карточек.csv"
    )
    url_get_vendor_code: str = "http://127.0.0.1:8000/api/v1/products/all/vendor_cods/"
    api_v1_prefix: str = "/v1/1c/etl"


class SettingsETLprocKaspi(BaseModel):
    url_api_get_customers_id: str = "http://127.0.0.1:8000/api/v1/customers/"
    url_api_get_products_kaspi_id: str = "http://127.0.0.1:8000/api/v1/kaspi_products/"
    url_api_get_orders_id: str = "http://127.0.0.1:8000/api/v1/orders/"
    headers: dict = {
        "Content-Type": "application/vnd.api+json",
        "X-Auth-Token": None,
        "User-Agent": "MyApp/1.0 (http://example.com)",
    }
    api_v1_prefix: str = "/v1/kaspi/etl"


class Settings(BaseSettings):
    elt_1c: SettingsETLproc1C = SettingsETLproc1C()
    etl_kaspi: SettingsETLprocKaspi = SettingsETLprocKaspi()


settings = Settings()
