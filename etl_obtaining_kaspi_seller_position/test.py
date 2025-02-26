import requests
import json


PROXIES = [
    "http://roman_lebedev91:QrekSwYVwA@91.147.126.129:50100",
    "http://roman_lebedev91:QrekSwYVwA@109.248.199.136:50100",
    "http://roman_lebedev91:QrekSwYVwA@46.8.31.64:50100",
    "http://roman_lebedev91:QrekSwYVwA@188.130.160.106:50100",
    "http://roman_lebedev91:QrekSwYVwA@185.120.79.107:50100",
]

# proxy = "http://roman_lebedev91:QrekSwYVwA@91.147.126.129:50100"
# proxy = "socks5://roman_lebedev910S3Ds:UKseutgP8Q@91.147.119.244:50101"
proxy = "socks5://roman_lebedev910S3Ds:UKseutgP8Q@91.147.119.244:50101"

proxies = {
    "http": proxy,
    "https": proxy,
}

url = "https://kaspi.kz/yml/offer-view/offers/114968947"

payload = json.dumps(
    {
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
)
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "referer": "https://kaspi.kz/shop/p/brw-brest-domenika-00042337-180x200-sm-bez-pod-jomnogo-mehanizma-seryi-svetlo-korichnevyi-114968947/",
    "Content-Type": "application/json",
    # "Origin": "https://kaspi.kz",
}

# # headers["X-Requested-With"] = "XMLHttpRequest"

response = requests.request(
    "POST",
    url,
    headers=headers,
    data=payload,
    proxies=proxies,
    # verify=False,
)

print(response.text)
