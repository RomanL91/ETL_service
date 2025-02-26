import os
from pathlib import Path
from pydantic import BaseModel
from pydantic_settings import BaseSettings


BASE_DIR = Path(__file__).parent.parent


class SettingsETLSellerPositions(BaseModel):
    url: str = "https://kaspi.kz/yml/offer-view/offers/{master_sku}"


class SettingsETLproc1C(BaseModel):
    path_to_CSV: str = BASE_DIR.joinpath("PCSV2.csv")
    path_to_hourly_CSV: str = BASE_DIR.joinpath("CSV2.csv")
    url_get_vendor_code: str = (
        "http://shop_service:8888/api/v1/products/all/vendor_cods/"
    )
    api_v1_prefix: str = "/v1/1c/etl"


class SettingsFTPServCred(BaseModel):
    # Параметры подключения
    ftp_server: str = "185.98.5.149"
    ftp_user: str = "user1c"
    ftp_password: str = "wz912%n1Z"  # Замените на ваш пароль
    remote_file_path: str = (
        "/httpdocs/shops/import/PCSV2.csv"  # Путь к файлу на сервере выгрузка вся
    )
    remote_file_hourly_path: str = (
        "/httpdocs/shops/import/CSV2.csv"  # Путь к файлу на сервере выгрузка изменений
    )
    local_file_path: str = "PCSV2.csv"  # Имя файла для сохранения локально
    local_file_hourly_path: str = "CSV2.csv"  # Имя файла для сохранения локально
    current_directory: Path = os.getcwd()  # Текущая рабочая директория


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
    FTP_serv_cred: SettingsFTPServCred = SettingsFTPServCred()
    etl_sellers_pos: SettingsETLSellerPositions = SettingsETLSellerPositions()


settings = Settings()
