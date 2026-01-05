import requests
import os
import time
import pandas as pd
from datetime import datetime, timedelta

def download_and_merge(start_date_str, end_date_str):
    base_url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    save_dir = "data"
    os.makedirs(save_dir, exist_ok=True)

    start_date = datetime.strptime(start_date_str, "%d-%m-%Y")
    end_date = datetime.strptime(end_date_str, "%d-%m-%Y")

    all_dataframes = []
    current_date = start_date

    while current_date <= end_date:
        date_formatted = current_date.strftime("%d%m%Y")
        url = base_url.format(date_formatted)
        
        print(f"Fetching: {current_date.strftime('%Y-%m-%d')}...", end=" ")
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                # Use StringIO to read the CSV content directly into Pandas
                from io import StringIO
                df = pd.read_csv(StringIO(response.text))
                all_dataframes.append(df)
                print("✅")
            else:
                print("❌ (Holiday)")
        except Exception as e:
            print(f"Error: {e}")
        
        current_date += timedelta(days=1)
        time.sleep(1)

    # Combine all data if we found any
    if all_dataframes:
        master_df = pd.concat(all_dataframes, ignore_index=True)
        # Remove leading/trailing spaces from column names (NSE data often has them)
        master_df.columns = master_df.columns.str.strip()
        
        output_file = f"{save_dir}/combined_bhavcopy.csv"
        master_df.to_csv(output_file, index=False)
        print(f"\nSuccess! Combined file saved at: {output_file}")
    else:
        print("\nNo data was downloaded to combine.")

if __name__ == "__main__":
    # You can change these dates as needed
    download_and_merge("10-12-2025", "12-12-2025")
