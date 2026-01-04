import requests
import zipfile
import io
import os
from datetime import datetime

BASE_DIR = "data"
os.makedirs(BASE_DIR, exist_ok=True)

today = datetime.now()
dd = today.strftime("%d")
mm = today.strftime("%m")
MMM = today.strftime("%b").upper()
yyyy = today.strftime("%Y")

BHAV_ZIP = f"cm{dd}{MMM}{yyyy}bhav.csv.zip"
BHAV_URL = f"https://archives.nseindia.com/content/historical/EQUITIES/{yyyy}/{MMM}/{BHAV_ZIP}"

DELIVERY_FILE = f"MTO_{dd}{mm}{yyyy}.csv"
DELIVERY_URL = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{dd}{mm}{yyyy}.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/"
}

session = requests.Session()
session.headers.update(HEADERS)

# Mandatory NSE warm-up request
session.get("https://www.nseindia.com", timeout=10)

# -------- Bhavcopy --------
print("Downloading Bhavcopy...")
bhav = session.get(BHAV_URL, timeout=20)
bhav.raise_for_status()

with zipfile.ZipFile(io.BytesIO(bhav.content)) as z:
    z.extractall(BASE_DIR)

print("Bhavcopy saved")

# -------- Deliverable Data --------
print("Downloading Deliverable Data...")
delivery = session.get(DELIVERY_URL, timeout=20)
delivery.raise_for_status()

with open(os.path.join(BASE_DIR, DELIVERY_FILE), "wb") as f:
    f.write(delivery.content)

print("Deliverable data saved")
print("✅ DONE")
