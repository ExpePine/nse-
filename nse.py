import requests
import os
import time
from datetime import datetime, timedelta

def download_range(start_date_str, end_date_str):
    base_url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    save_dir = "data"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
    end_date = datetime.strptime(end_date_str, "%d-%m-%Y")

    current_date = start_date
    while current_date <= end_date:
        date_formatted = current_date.strftime("%d%m%Y")
        url = base_url.format(date_formatted)
        
        print(f"Downloading for: {current_date.strftime('%Y-%m-%d')}...")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                with open(f"{save_dir}/bhavcopy_{date_formatted}.csv", "wb") as f:
                    f.write(response.content)
                print("--- Saved successfully.")
            else:
                print(f"--- Skipped (Status {response.status_code})")
        except Exception as e:
            print(f"--- Error: {e}")
        
        current_date += timedelta(days=1)
        time.sleep(1)

if __name__ == "__main__":
    # Range requested: 10 Dec 2025 to 12 Dec 2025
    download_range("10-12-2025", "12-12-2025")
