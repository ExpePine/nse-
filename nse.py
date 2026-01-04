import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

import gspread
from google.oauth2.service_account import Credentials

# ================= CONFIG =================
START_DATE = "2025-09-26"
END_DATE = datetime.today().strftime("%Y-%m-%d")

TEMP_DOWNLOAD_DIR = os.path.abspath("download")

SPREADSHEET_NAME = "Tradingview Data Reel Experimental May"
WORKSHEET_NAME = "Sheet10"

REQUIRED_COLUMNS = ["NO_OF_TRADES", "DELIV_QTY"]
# =========================================

os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

# ========== GOOGLE AUTH (GITHUB SECRET) ==========
service_account_info = json.loads(
    os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
)

credentials = Credentials.from_service_account_info(
    service_account_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ],
)

gc = gspread.authorize(credentials)

# ✅ OPEN BY NAME + SHEET NAME (SAFE)
worksheet = gc.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)

# ========== CHROME SETUP ==========
chrome_options = Options()
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--start-maximized")

chrome_options.add_experimental_option(
    "prefs",
    {
        "download.default_directory": TEMP_DOWNLOAD_DIR,
        "download.prompt_for_download": False
    }
)

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

wait = WebDriverWait(driver, 30)

# ========== HELPERS ==========
def wait_for_download():
    while True:
        files = os.listdir(TEMP_DOWNLOAD_DIR)
        if files and not any(f.endswith(".crdownload") for f in files):
            return os.path.join(TEMP_DOWNLOAD_DIR, files[0])
        time.sleep(1)

def clear_temp():
    for f in os.listdir(TEMP_DOWNLOAD_DIR):
        try:
            os.remove(os.path.join(TEMP_DOWNLOAD_DIR, f))
        except:
            pass

def daterange(start, end):
    for n in range((end - start).days + 1):
        yield start + timedelta(days=n)

# ========== START ==========
driver.get("https://www.nseindia.com/all-reports#cr_equity_archives")
time.sleep(5)

start = datetime.strptime(START_DATE, "%Y-%m-%d")
end = datetime.strptime(END_DATE, "%Y-%m-%d")

for day in daterange(start, end):
    date_str = day.strftime("%d-%m-%Y")
    print(f"📅 Processing {date_str}")

    clear_temp()
    downloaded_file = None

    try:
        date_input = wait.until(EC.presence_of_element_located((By.ID, "crDate")))
        driver.execute_script("arguments[0].value = '';", date_input)
        date_input.send_keys(date_str)

        driver.find_element(By.ID, "getData").click()
        time.sleep(3)

        driver.find_element(By.XPATH, "//a[contains(@href,'xls')]").click()

        downloaded_file = wait_for_download()

        df = pd.read_excel(downloaded_file)
        df = df[REQUIRED_COLUMNS]

        worksheet.append_rows(df.values.tolist(), value_input_option="RAW")
        print("✅ Uploaded")

    except Exception as e:
        print(f"❌ Skipped {date_str}: {e}")

    finally:
        if downloaded_file and os.path.exists(downloaded_file):
            os.remove(downloaded_file)
            print("🗑️ Deleted")

driver.quit()
print("🎯 COMPLETED — GITHUB READY")
