import curl_cffi.requests as requests


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
    # "Referer": "https://kaspi.kz/shop/p/brw-brest-domenika-00042337-180x200-sm-bez-pod-jomnogo-mehanizma-seryi-svetlo-korichnevyi-114968947/",
    "Content-Type": "application/json",
    # "Origin": "https://kaspi.kz",
    # "X-Requested-With": "XMLHttpRequest",
}

response = requests.post(
    url, headers=headers, json=payload, proxies=proxies, impersonate="chrome110"
)
print(response.status_code)
print(response.text)
