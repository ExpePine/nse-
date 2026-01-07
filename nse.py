import requests
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

def download_and_merge():
    base_url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate"
    }

    save_dir = "data"
    os.makedirs(save_dir, exist_ok=True)

    # ===== DATE RANGE: JANUARY 1 → TODAY =====
    end_date = datetime.now()
    start_date = datetime(end_date.year, 1, 1)

    print(f"Fetching NSE data from {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}\n")

    all_dataframes = []
    current_date = start_date

    while current_date <= end_date:
        # Skip weekends (Sat=5, Sun=6)
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%d%m%Y")
            url = base_url.format(date_str)

            print(f"📥 {current_date.strftime('%d-%b-%Y')}", end=" → ")

            try:
                response = requests.get(url, headers=headers, timeout=15)

                if response.status_code == 200 and len(response.text) > 100:
                    df = pd.read_csv(StringIO(response.text))
                    all_dataframes.append(df)
                    print("✅")
                else:
                    print("❌ Holiday / No file")

            except Exception as e:
                print(f"⚠️ Error: {e}")

            time.sleep(1)  # Avoid blocking

        current_date += timedelta(days=1)

    # ===== MERGE & SAVE =====
    if all_dataframes:
        master_df = pd.concat(all_dataframes, ignore_index=True)
        master_df.columns = master_df.columns.str.strip()

        output_file = os.path.join(save_dir, "combined_bhavcopy_jan_to_today.csv")
        master_df.to_csv(output_file, index=False)

        print(f"\n🎉 DONE! File saved at:\n{output_file}")
        print(f"📊 Total rows: {len(master_df)}")

    else:
        print("\n❌ No data downloaded.")

if __name__ == "__main__":
    download_and_merge()
