import requests
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

def download_and_find_max_volume():
    base_url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    save_dir = "data"
    os.makedirs(save_dir, exist_ok=True)

    # DYNAMIC DATE CALCULATION
    # Yesterday's date
    end_date = datetime.now() - timedelta(days=1)
    # Approx 4 months ago (120 days)
    start_date = end_date - timedelta(days=120)

    print(f"Running report from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    all_dataframes = []
    current_date = start_date

    while current_date <= end_date:
        # NSE skips weekends (Saturday=5, Sunday=6)
        if current_date.weekday() < 5:
            date_formatted = current_date.strftime("%d%m%Y")
            url = base_url.format(date_formatted)
            
            try:
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    df = pd.read_csv(StringIO(response.text))
                    df.columns = df.columns.str.strip()
                    
                    # Filter for Equity (EQ) series only
                    if 'SERIES' in df.columns:
                        df = df[df['SERIES'].str.strip() == 'EQ']
                    
                    all_dataframes.append(df)
                    print(f"✅ {current_date.strftime('%Y-%m-%d')}: Downloaded")
                else:
                    print(f"❌ {current_date.strftime('%Y-%m-%d')}: No data (Holiday)")
            except Exception as e:
                print(f"⚠️ {current_date.strftime('%Y-%m-%d')}: Error {e}")
        
        current_date += timedelta(days=1)
        time.sleep(0.5) # Gentle delay to avoid blocking

    if all_dataframes:
        master_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Standardize Column Names
        date_col = 'DATE1' if 'DATE1' in master_df.columns else 'DATE'
        master_df[date_col] = pd.to_datetime(master_df[date_col])
        master_df['TOTTRDQTY'] = pd.to_numeric(master_df['TOTTRDQTY'], errors='coerce')

        # Logic to find the record with the ABSOLUTE MAXIMUM volume for each stock
        # We sort by Quantity (High to Low) and then drop all but the first (highest) entry for each SYMBOL
        max_volume_report = master_df.sort_values('TOTTRDQTY', ascending=False).drop_duplicates('SYMBOL')
        
        # Select and rename columns for the final file
        final_report = max_volume_report[['SYMBOL', 'TOTTRDQTY', date_col]].copy()
        final_report.rename(columns={
            'TOTTRDQTY': 'MAX_TRADED_QTY_4_MONTHS', 
            date_col: 'DATE_OF_PEAK_VOLUME'
        }, inplace=True)
        
        output_file = f"{save_dir}/max_volume_4months.csv"
        final_report.to_csv(output_file, index=False)
        print(f"\nSUCCESS! Created {output_file} with {len(final_report)} stocks.")
    else:
        print("\nNo data found for the selected period.")

if __name__ == "__main__":
    download_and_find_max_volume()
