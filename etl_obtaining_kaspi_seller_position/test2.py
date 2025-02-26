import requests


# proxy = "socks5://roman_lebedev910S3Ds:UKseutgP8Q@91.147.119.244:50101"
proxy = "socks5://roman_lebedev91:QrekSwYVwA@91.147.126.129:50101"

proxies = {
    "http": proxy,
    "https": proxy,
}

url = "http://httpbin.org/ip"  # HTTP-запрос, а не HTTPS

try:
    response = requests.get(url, proxies=proxies, timeout=10)
    print("✅ Прокси работает! IP:", response.json())
except requests.RequestException as e:
    print("❌ Ошибка соединения с прокси:", e)
