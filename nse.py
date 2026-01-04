import os
import json
import time
import pandas as pd
from datetime import datetime

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
# Since 20-12-2025 is a Saturday, we target the nearest trading day (Friday)
TARGET_DATE = "19-12-2025" 
TEMP_DOWNLOAD_DIR = os.path.abspath("download")

SPREADSHEET_NAME = "Tradingview Data Reel Experimental May"
WORKSHEET_NAME = "Sheet10"

# Updated UDiFF Column Names (Common in late 2025/2026 reports)
# NO_OF_TRADES is often 'Trads' and DELIV_QTY is often 'DlvryQty' or 'DELIV_QTY'
# We will use a flexible cleaning function below.
REQUIRED_COLUMNS = ["SYMBOL", "NO_OF_TRADES", "DELIV_QTY"] 
# =========================================

os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)

# ========== GOOGLE AUTH ==========
# Ensure your Github Secret or local Environment variable is set
service_account_info = json.loads(os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "{}"))
if not service_account_info:
    raise ValueError("Google Service Account JSON not found in environment variables.")

credentials = Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"],
)

gc = gspread.authorize(credentials)
worksheet = gc.open(SPREADSHEET_NAME).worksheet(WORKSHEET_NAME)

# ========== CHROME SETUP ==========
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

chrome_options.add_experimental_option("prefs", {
    "download.default_directory": TEMP_DOWNLOAD_DIR,
    "download.prompt_for_download": False,
})

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
wait = WebDriverWait(driver, 30)

def wait_for_download():
    timeout = 60
    start_time = time.time()
    while time.time() - start_time < timeout:
        files = [f for f in os.listdir(TEMP_DOWNLOAD_DIR) if not f.endswith('.crdownload')]
        if files:
            return os.path.join(TEMP_DOWNLOAD_DIR, files[0])
        time.sleep(1)
    return None

# ========== EXECUTION ==========
try:
    print(f"🚀 Targeting Date: {TARGET_DATE}")
    driver.get("https://www.nseindia.com/all-reports#cr_equity_archives")
    time.sleep(5)

    # Input Date using JavaScript for reliability
    date_input = wait.until(EC.presence_of_element_located((By.ID, "crDate")))
    driver.execute_script(f"arguments[0].value = '{TARGET_DATE}';", date_input)
    driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", date_input)

    # Click Filter
    driver.find_element(By.ID, "getData").click()
    time.sleep(5)

    # Find the Full Bhavcopy CSV link
    # XPath updated to look for text containing 'Bhavcopy' and 'csv'
    download_link = wait.until(EC.element_to_be_clickable(
        (By.XPATH, "//a[contains(@href, 'csv') and (contains(text(), 'Bhavcopy') or contains(text(), 'bhavcopy'))]")
    ))
    download_link.click()

    file_path = wait_for_download()
    if file_path:
        print(f"📦 File downloaded: {file_path}")
        
        # Read data - NSE files can be CSV or zipped CSV
        df = pd.read_csv(file_path)
        
        # CLEANING: Strip spaces from headers
        df.columns = [str(c).strip().upper() for c in df.columns]
        
        # FLEXIBLE MAPPING: Handle variations in NSE column naming
        mapping = {
            "TRADES": "NO_OF_TRADES",
            "TOTTRD": "NO_OF_TRADES",
            "DELIV_QTY": "DELIV_QTY",
            "DLVRY_QTY": "DELIV_QTY"
        }
        df = df.rename(columns=mapping)

        # Filter for required data
        available_cols = [c for c in REQUIRED_COLUMNS if c in df.columns]
        df_final = df[available_cols].copy()
        df_final['DATE'] = TARGET_DATE

        # Upload to Google Sheets
        data_list = df_final.fillna(0).astype(str).values.tolist()
        worksheet.append_rows(data_list, value_input_option="RAW")
        
        print(f"✅ Successfully uploaded {len(data_list)} rows.")
    else:
        print("❌ Download timed out. Is it a trading holiday?")

finally:
    driver.quit()
    if os.path.exists(TEMP_DOWNLOAD_DIR):
        for f in os.listdir(TEMP_DOWNLOAD_DIR):
            os.remove(os.path.join(TEMP_DOWNLOAD_DIR, f))
    print("🎯 Process Finished.")
