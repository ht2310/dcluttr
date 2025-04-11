import csv
import asyncio
import random
from datetime import datetime
from playwright.async_api import async_playwright

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
    ("munchies", 1237, "nachos", 316),
    ("munchies", 1237, "bhujia-mixtures", 1178),
    ("munchies", 1237, "munchies-gift-packs", 1694),
    ("munchies", 1237, "namkeen-snacks", 29),
    ("munchies", 1237, "papad-fryums", 80),
    ("munchies", 1237, "chips-crisps", 940),
    ("sweet-tooth", 9, "indian-sweets", 943)
]


output_file = "blinkit_category_scraping_playwright.csv"
fieldnames = [
    "date", "l1_category", "l1_category_id", "l2_category", "l2_category_id",
    "variant_id", "variant_name", "group_id", "selling_price", "mrp",
    "in_stock", "inventory", "is_sponsored", "image_url", "brand_id", "brand"
]

async def fetch_products(playwright, lat, lng, l1_cat, l1_id, l2_cat, l2_id):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(
        locale='en-US',
        geolocation={"latitude": lat, "longitude": lng},
        permissions=["geolocation"]
    )
    page = await context.new_page()

    try:
        url = f"https://blinkit.com/v1/layout/listing_widgets?l0_cat={l1_id}&l1_cat={l2_id}"
        #url = f"https://blinkit.com/cn/{l1_cat}/{l2_cat}/cid/{l1_id}/{l2_id}"
        print(f"Fetching: {url} at ({lat}, {lng})")
        
        await page.goto(url, timeout=20000)
        await page.wait_for_timeout(3000)

        response = await page.evaluate(
            """async ({ l1_id, l2_id, lat, lng }) => {
                const res = await fetch(`https://blinkit.com/v1/layout/listing_widgets?l0_cat=${l1_id}&l1_cat=${l2_id}`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'auth_key': 'c761ec3633c22afad934fb17a66385c1c06c5472b4898b866b7306186d0bb477',
                        'app_client': 'consumer_web',
                        'app_version': '1010101010',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
                        'origin': 'https://blinkit.com',
                        'referer': 'https://blinkit.com/',
                        'platform': 'desktop_web',
                        'x-age-consent-granted': 'true',
                        'lat': lat.toString(),
                        'lon': lng.toString(),
                        'device_id': '05d1376c-7bdf-48b9-92fd-47892f62375a'
                    },
                    body: JSON.stringify({})
                });
                return res.ok ? await res.json() : { error: true, status: res.status };
            }""",
            { "l1_id": l1_id, "l2_id": l2_id, "lat": lat, "lng": lng }
        )

        print(f"Fetched {len(response.get('widgets', []))} widgets for {l2_cat}")
        return response

    except Exception as e:
        print(f"❌ Error for ({lat},{lng}) {l2_cat}: {e}")
        return None

    finally:
        await context.close()
        await browser.close()
async def main():
    async with async_playwright() as playwright:
        with open(output_file, "w", newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for lat, lng in coordinates:
                for l1_cat, l1_id, l2_cat, l2_id in categories:
                    data = await fetch_products(playwright, lat, lng, l1_cat, l1_id, l2_cat, l2_id)
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

                    await asyncio.sleep(random.uniform(4, 8))

if __name__ == "__main__":
    asyncio.run(main())
