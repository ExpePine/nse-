import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- SETTINGS ---
TARGET_DATE = "19-12-2025"  # Date for the Friday before your requested Saturday
DOWNLOAD_DIR = os.path.abspath("temp_nse_data")

# Ensure directory exists and is empty
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)
for f in os.listdir(DOWNLOAD_DIR):
    os.remove(os.path.join(DOWNLOAD_DIR, f))

def setup_driver():
    options = Options()
    options.add_argument("--headless=new") # Required for GitHub Actions
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    # Bypasses simple bot detection
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    # Tell Chrome where to put the downloaded file
    options.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "directory_upgrade": True
    })
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def download_bhavcopy():
    driver = setup_driver()
    wait = WebDriverWait(driver, 30)
    
    try:
        # Step 1: Prime cookies by visiting homepage
        print("🌐 Visiting NSE Homepage...")
        driver.get("https://www.nseindia.com/")
        time.sleep(3)

        # Step 2: Navigate to Archive page
        print(f"📂 Navigating to Archives for {TARGET_DATE}...")
        driver.get("https://www.nseindia.com/all-reports#cr_equity_archives")
        
        # Step 3: Input Date using JavaScript for reliability
        date_field = wait.until(EC.element_to_be_clickable((By.ID, "crDate")))
        driver.execute_script(f"arguments[0].value = '{TARGET_DATE}';", date_field)
        driver.execute_script("arguments[0].dispatchEvent(new Event('change'))", date_field)
        
        # Step 4: Click Filter/Get Data
        driver.find_element(By.ID, "getData").click()
        print("⏳ Waiting for report list to refresh...")
        time.sleep(7) # NSE needs time to load the results table

        # Step 5: Click the "Full Bhavcopy" CSV link
        # This XPath finds the CSV link specifically for the Bhavcopy
        download_xpath = "//a[contains(@href, 'csv') and contains(translate(text(), 'BHAVCOPY', 'bhavcopy'), 'bhavcopy')]"
        download_btn = wait.until(EC.element_to_be_clickable((By.XPATH, download_xpath)))
        download_btn.click()
        
        print("📥 Download triggered. Waiting for file...")
        
        # Step 6: Verify download completion
        timeout = 20
        start_time = time.time()
        while time.time() - start_time < timeout:
            files = os.listdir(DOWNLOAD_DIR)
            if files and not any(f.endswith(".crdownload") for f in files):
                print(f"✅ Download Successful! File: {files[0]}")
                return True
            time.sleep(1)
            
        print("❌ Download timed out.")
        return False

    except Exception as e:
        print(f"❌ Error during download: {e}")
        return False
    finally:
        driver.quit()

if __name__ == "__main__":
    download_bhavcopy()
