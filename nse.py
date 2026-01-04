import requests
import zipfile
import io
import os
from datetime import datetime, timedelta

BASE_DIR = "data"
os.makedirs(BASE_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/"
}

session = requests.Session()
session.headers.update(HEADERS)
session.get("https://www.nseindia.com", timeout=10)

def try_download(target_date):
    dd = target_date.strftime("%d")
    mm = target_date.strftime("%m")
    MMM = target_date.strftime("%b").upper()
    yyyy = target_date.strftime("%Y")

    bhav_zip = f"cm{dd}{MMM}{yyyy}bhav.csv.zip"
    bhav_url = f"https://archives.nseindia.com/content/historical/EQUITIES/{yyyy}/{MMM}/{bhav_zip}"

    delivery_file = f"MTO_{dd}{mm}{yyyy}.csv"
    delivery_url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{dd}{mm}{yyyy}.csv"

    print(f"Trying date: {target_date.date()}")

    try:
        bhav = session.get(bhav_url, timeout=15)
        bhav.raise_for_status()

        with zipfile.ZipFile(io.BytesIO(bhav.content)) as z:
            z.extractall(BASE_DIR)

        delivery = session.get(delivery_url, timeout=15)
        delivery.raise_for_status()

        with open(os.path.join(BASE_DIR, delivery_file), "wb") as f:
            f.write(delivery.content)

        print("✅ SUCCESS:", target_date.date())
        return True

    except Exception as e:
        print("❌ Failed:", e)
        return False


# 🔁 Try today and fallback up to last 7 days
today = datetime.now()

for i in range(7):
    date_to_try = today - timedelta(days=i)

    # Skip weekends early
    if date_to_try.weekday() >= 5:
        continue

    if try_download(date_to_try):
        break
else:
    raise Exception("❌ No NSE data available in last 7 days")
