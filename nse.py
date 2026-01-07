import requests
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

def download_and_merge():
    BASE_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"

    SAVE_DIR = "data"
    OUTPUT_FILE = "combined_bhavcopy_2025_to_today.csv"

    os.makedirs(SAVE_DIR, exist_ok=True)

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/csv,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive"
    }

    # ---------- DATE RANGE ----------
    start_date = datetime(2025, 1, 1)
    end_date = datetime.now()

    print(
        f"\n📊 NSE Bhavcopy Download\n"
        f"From: {start_date.strftime('%d-%b-%Y')}\n"
        f"To  : {end_date.strftime('%d-%b-%Y')}\n"
    )

    # ---------- SESSION (MANDATORY FOR NSE) ----------
    session = requests.Session()
    session.headers.update(HEADERS)

    # Get cookies
    session.get("https://www.nseindia.com", timeout=15)
    time.sleep(2)

    all_dfs = []
    current_date = start_date

    while current_date <= end_date:
        # Skip weekends
        if current_date.weekday() < 5:
            date_str = current_date.strftime("%d%m%Y")
            url = BASE_URL.format(date_str)

            print(f"📥 {current_date.strftime('%d-%b-%Y')}", end=" → ")

            try:
                response = session.get(url, timeout=20)

                content_type = response.headers.get("Content-Type", "").lower()

                # ✅ STRICT VALIDATION (prevents HTML/503 saving)
                if (
                    response.status_code == 200
                    and "text/csv" in content_type
                    and len(response.text) > 1000
                ):
                    df = pd.read_csv(StringIO(response.text))
                    all_dfs.append(df)
                    print("✅")
                else:
                    print("❌ Holiday / Blocked / No file")

            except Exception as e:
                print(f"⚠️ Error: {e}")

            time.sleep(2)  # REQUIRED to avoid 503

        current_date += timedelta(days=1)

    # ---------- MERGE & SAVE ----------
    if all_dfs:
        master_df = pd.concat(all_dfs, ignore_index=True)
        master_df.columns = master_df.columns.str.strip()

        output_path = os.path.join(SAVE_DIR, OUTPUT_FILE)
        master_df.to_csv(output_path, index=False)

        print("\n🎉 SUCCESS")
        print(f"📁 Saved at: {output_path}")
        print(f"📈 Rows: {len(master_df)}")

    else:
        print("\n❌ No valid data downloaded.")

if __name__ == "__main__":
    download_and_merge()
