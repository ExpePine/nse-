import os
import time
import pandas as pd
import json
import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIG ---
TARGET_DATE = "19-12-2025"
SHEET_NAME = "Tradingview Data Reel Experimental May"
WORKSHEET_NAME = "Sheet10"
TEMP_DIR = os.path.abspath("temp_nse_data")
os.makedirs(TEMP_DIR, exist_ok=True)

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    # Crucial: Use a real-looking user agent
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option("prefs", {
        "download.default_directory": TEMP_DIR,
        "download.prompt_for_download": False,
        "directory_upgrade": True
    })
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def main():
    driver = setup_driver()
    wait = WebDriverWait(driver, 30) # Increased timeout
    
    try:
        # STEP 1: Prime cookies by visiting the homepage first
        print("🌐 Priming cookies at NSE homepage...")
        driver.get("https://www.nseindia.com/")
        time.sleep(5) 

        # STEP 2: Navigate to Archives
        print(f"📂 Navigating to Archives for {TARGET_DATE}...")
        driver.get("https://www.nseindia.com/all-reports#cr_equity_archives")
        
        # STEP 3: Handle Date Picker
        date_input = wait.until(EC.element_to_be_clickable((By.ID, "crDate")))
        driver.execute_script(f"arguments[0].value = '{TARGET_DATE}';", date_input)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", date_input)
        
        driver.find_element(By.ID, "getData").click()
        print("⏳ Waiting for data table to refresh...")
        time.sleep(8) # Mandatory sleep for NSE's slow AJAX

        # STEP 4: Download the Bhavcopy CSV
        # Updated XPath to be more specific to the 'Full Bhavcopy' link
        download_xpath = "//a[contains(@href, 'csv') and contains(translate(text(), 'BHAVCOPY', 'bhavcopy'), 'bhavcopy')]"
        download_btn = wait.until(EC.element_to_be_clickable((By.XPATH, download_xpath)))
        download_btn.click()
        
        print("📥 Download started...")
        time.sleep(10) # Wait for file to land

        # STEP 5: Process and Upload
        files = os.listdir(TEMP_DIR)
        if files:
            file_path = os.path.join(TEMP_DIR, files[0])
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.strip().upper()
            
            # Map columns and upload (ensure SYMBOL is included to identify data)
            final_df = df[['SYMBOL', 'NO_OF_TRADES', 'DELIV_QTY']].copy()
            final_df['DATE'] = TARGET_DATE
            
            # Auth and Upload
            info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
            creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
            ws = gspread.authorize(creds).open(SHEET_NAME).worksheet(WORKSHEET_NAME)
            ws.append_rows(final_df.fillna(0).astype(str).values.tolist())
            print(f"✅ Success! Uploaded {len(final_df)} rows.")
        else:
            print("❌ File not found in temp directory.")

    except Exception as e:
        print(f"❌ Automation failed: {e}")
    finally:
        driver.quit()
        for f in os.listdir(TEMP_DIR):
            os.remove(os.path.join(TEMP_DIR, f))

if __name__ == "__main__":
    main()
