import undetected_chromedriver as uc


options = uc.ChromeOptions()
options.add_argument(
    "--proxy-server=socks5h://roman_lebedev91:QrekSwYVwA@91.147.126.129:50101"
)  # Прокси

driver = uc.Chrome(options=options)
driver.get(
    "https://kaspi.kz/shop/p/brw-brest-domenika-00042337-180x200-sm-bez-pod-jomnogo-mehanizma-seryi-svetlo-korichnevyi-114968947/"
)
result = driver.execute_script(
    """
    fetch("https://kaspi.kz/yml/offer-view/offers/114968947", {
        method: "POST",
        headers: {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "referer": "https://kaspi.kz/shop/p/brw-brest-domenika-00042337-180x200-sm-bez-pod-jomnogo-mehanizma-seryi-svetlo-korichnevyi-114968947/",
            "Content-Type": "application/json",
        },
        body: JSON.stringify({
            "cityId": "710000000",
            "id": "114968947",
            "merchantUID": "",
            "limit": 50,
            "page": 0,
            "sortOption": "PRICE",
            "highRating": null,
            "searchText": null,
            "installationId": "-1"
        })
    }).then(res => res.json()).then(data => console.log(data));
"""
)
logs = driver.get_log("browser")
print("📜 Логи браузера:", logs)
print("✅ Результат:", result)  # 👀 Выведет ответ API
driver.quit()
