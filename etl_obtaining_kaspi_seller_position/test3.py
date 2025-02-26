import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import ssl
from urllib3.util.ssl_ import create_urllib3_context


class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.options |= ssl.OP_NO_TLSv1_3  # Отключаем TLS 1.3
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


class NoVerifyTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.check_hostname = False  # Отключаем проверку домена
        ctx.verify_mode = ssl.CERT_NONE  # Отключаем проверку сертификатов
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


# Используем SOCKS5-прокси
proxies = {
    "http": "socks5h://roman_lebedev91:QrekSwYVwA@91.147.126.129:50101",
    "https": "socks5h://roman_lebedev91:QrekSwYVwA@91.147.126.129:50101",
}

url = "https://kaspi.kz/yml/offer-view/offers/114968947"

payload = {
    "cityId": "710000000",
    "id": "114968947",
    "merchantUID": "",
    "limit": 50,
    "page": 0,
    "sortOption": "PRICE",
    "highRating": None,
    "searchText": None,
    "installationId": "-1",
}

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "referer": "https://kaspi.kz/shop/p/brw-brest-domenika-00042337-180x200-sm-bez-pod-jomnogo-mehanizma-seryi-svetlo-korichnevyi-114968947/",
    "Referer": "https://kaspi.kz/shop/p/brw-brest-domenika-00042337-180x200-sm-bez-pod-jomnogo-mehanizma-seryi-svetlo-korichnevyi-114968947/",
    "Content-Type": "application/json",
    "Origin": "https://kaspi.kz",
    "X-Requested-With": "XMLHttpRequest",
}

# Отправка POST-запроса через SOCKS5
try:

    session = requests.Session()
    session.mount(
        "https://",
        NoVerifyTLSAdapter(),
    )

    response = session.post(
        url,
        headers=headers,
        json=payload,
        verify=False,
        proxies=proxies,
    )
    # response = requests.patch(
    #     url, headers=headers, json=payload, proxies=proxies, timeout=15
    # )
    print("✅ Статус:", response.status_code)
    print("✅ Статус:", response.text)
except requests.RequestException as e:
    print("❌ Ошибка запроса через прокси:", e)
