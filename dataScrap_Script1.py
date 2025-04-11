import requests
import csv
import random
import time
import uuid
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_browser_session():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://blinkit.com")
    time.sleep(5)  

    cookies = driver.get_cookies()
    user_agent = driver.execute_script("return navigator.userAgent;")
    driver.quit()

    session = requests.Session()
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'])

    return session, user_agent

session, user_agent = get_browser_session()

coordinates = [
    (28.678051, 77.314262),
    (28.5045, 77.012),
    (22.59643333, 88.39996667),
    (23.1090018, 72.57299832),
    (18.95833333, 72.83333333),
    (28.7045, 77.15366667),
    (12.88326667, 77.5594),
    (28.7295, 77.12866667),
    (28.67571622, 77.36149677),
    (28.3501, 77.31673333),
    (28.59086667, 77.3054),
    (28.49553604, 77.51297417),
    (28.44176667, 77.3084),
    (28.48783333, 77.09533333),
    (12.93326667, 77.61773333),
    (13.00826667, 77.64273333),
    (28.4751, 77.4334),
    (26.85653333, 75.71283333),
    (26.8982, 75.8295),
    (18.54316667, 73.914),
]

categories = [
    ("munchies", 1237, "bhujia-mixtures", 1178),
    ("munchies", 1237, "munchies-gift-packs", 1694),
    ("munchies", 1237, "namkeen-snacks", 29),
    ("munchies", 1237, "papad-fryums", 80),
    ("munchies", 1237, "chips-crisps", 940),
    ("sweet-tooth", 9, "indian-sweets", 943)
]

output_file = "blinkit_category_scraping_stream.csv"
fieldnames = [
    "date", "l1_category", "l1_category_id", "l2_category", "l2_category_id",
    "store_id", "variant_id", "variant_name", "group_id",
    "selling_price", "mrp", "in_stock", "inventory", "is_sponsored",
    "image_url", "brand_id", "brand"
]

def fetch_products(lat, lng, l1_cat_id, l2_cat_id,l1_cat, l2_cat, attempt=1):
    session_uuid = str(uuid.uuid4())
    device_id = str(uuid.uuid4())

    headers = {
        "User-Agent": user_agent,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "Origin": "https://blinkit.com",
        "Referer": f"https://blinkit.com/cn/{l1_cat}/{l2_cat}/cid/{l1_cat_id}/{l2_cat_id}",
        "app_client": "consumer_web",
        "app_version": "1010101010",
        "device_id": device_id,
        "lat": str(lat),
        "lon": str(lng),
        "platform": "desktop_web",
        "web_app_version": "1008010016",
        "x-age-consent-granted": "true",
        "session_uuid": session_uuid
    }

    url = f"https://blinkit.com/v1/layout/listing_widgets?l0_cat={l1_cat_id}&l1_cat={l2_cat_id}"

    try:
        response = session.post(url, headers=headers, json={})
        if response.status_code == 403:
            raise requests.exceptions.RequestException("403 Forbidden")

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        if attempt <= 3:
            wait_time = 2 ** attempt
            print(f"[Retry {attempt}] Sleeping for {wait_time} sec due to error: {e}")
            time.sleep(wait_time)
            return fetch_products(lat, lng, l1_cat_id, l2_cat_id,l1_cat, l2_cat, attempt + 1)
        else:
            print(f"[FAILED] Too many retries for {lat}, {lng}, {l1_cat_id}-{l2_cat_id}")
            return None

with open(output_file, "w", newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for lat, lng in coordinates:
        for l1_cat, l1_id, l2_cat, l2_id in categories:
            data = fetch_products(lat, lng, l1_id, l2_id,l1_cat, l2_cat)
            if not data:
                continue

            widgets = data.get('widgets', [])
            for widget in widgets:
                products = widget.get('data', {}).get('products', [])
                for item in products:
                    writer.writerow({
                        "date": datetime.today().strftime('%Y-%m-%d'),
                        "l1_category": l1_cat,
                        "l1_category_id": l1_id,
                        "l2_category": l2_cat,
                        "l2_category_id": l2_id,
                        "store_id": item.get('store_id'),
                        "variant_id": item.get('variant_id'),
                        "variant_name": item.get('variant_name'),
                        "group_id": item.get('group_id'),
                        "selling_price": item.get('price', {}).get('value'),
                        "mrp": item.get('price', {}).get('mrp'),
                        "in_stock": item.get('in_stock'),
                        "inventory": item.get('inventory'),
                        "is_sponsored": item.get('is_sponsored'),
                        "image_url": item.get('image_url'),
                        "brand_id": item.get('brand_id'),
                        "brand": item.get('brand')
                    })

            time.sleep(random.uniform(4, 8))  
