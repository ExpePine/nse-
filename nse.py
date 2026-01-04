import os
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
import json

# ================= CONFIGURATION =================
TARGET_DATE = "19-12-2025"  # Format: DD-MM-YYYY
SHEET_NAME = "Tradingview Data Reel Experimental May"
WORKSHEET_NAME = "Sheet10"
# Based on your image headers:
COLS_TO_EXTRACT = ["NO_OF_TRADES", "DELIV_QTY"] 
# =================================================

TEMP_DIR = os.path.join(os.getcwd(), "temp_nse_data")
os.makedirs(TEMP_DIR, exist_ok=True)

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    # Masking automation to bypass NSE blocks
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("prefs", {
        "download.default_directory": TEMP_DIR,
        "download.prompt_for_download": False,
    })
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def get_google_sheet():
    # Looks for secret in GitHub environment
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return gspread.authorize(creds).open(SHEET_NAME).worksheet(WORKSHEET_NAME)

def main():
    driver = setup_driver()
    wait = WebDriverWait(driver, 20)
    
    try:
        print(f"🌐 Navigating to NSE Archives for {TARGET_DATE}...")
        driver.get("https://www.nseindia.com/all-reports#cr_equity_archives")
        time.sleep(5)

        # 1. Select the Date
        date_input = wait.until(EC.presence_of_element_located((By.ID, "crDate")))
        driver.execute_script(f"arguments[0].value = '{TARGET_DATE}';", date_input)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", date_input)
        
        # 2. Click Filter
        driver.find_element(By.ID, "getData").click()
        time.sleep(4)

        # 3. Download CSV (targeting Bhavcopy)
        csv_link = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//a[contains(@href, 'csv') and contains(text(), 'Bhavcopy')]")
        ))
        csv_link.click()
        
        # 4. Wait for file to land in temp folder
        time.sleep(5)
        files = os.listdir(TEMP_DIR)
        if not files:
            print("❌ No file downloaded.")
            return

        file_path = os.path.join(TEMP_DIR, files[0])
        print(f"📦 Processing: {files[0]}")

        # 5. Filter Data
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip() # Clean headers
        
        # We only keep the specific columns you asked for
        df_filtered = df[COLS_TO_EXTRACT].fillna(0)
        
        # 6. Upload to Sheets
        sheet = get_google_sheet()
        sheet.append_rows(df_filtered.values.tolist(), value_input_option="USER_ENTERED")
        print(f"✅ Uploaded {len(df_filtered)} rows to {WORKSHEET_NAME}")

    except Exception as e:
        print(f"⚠️ Error: {e}")
    finally:
        driver.quit()
        # 7. Temporary cleanup: Delete files after upload
        for f in os.listdir(TEMP_DIR):
            os.remove(os.path.join(TEMP_DIR, f))
        print("🗑️ Temp files cleared.")

if __name__ == "__main__":
    main()
