import requests

proxies = {
    "http": "socks5h://roman_lebedev91:QrekSwYVwA@91.147.126.129:50101",
    "https": "socks5h://roman_lebedev91:QrekSwYVwA@91.147.126.129:50101",
}

url = "https://httpbin.org/post"

payload = {"test": "hello"}
headers = {"User-Agent": "Mozilla/5.0"}

try:
    response = requests.post(
        url, headers=headers, json=payload, proxies=proxies, timeout=10
    )
    print("✅ Прокси поддерживает `POST`! Ответ:", response.json())
except requests.RequestException as e:
    print("❌ Ошибка запроса через прокси:", e)
