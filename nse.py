import requests
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

def update_nse_report():
    # 1. Folder check aur create karna
    save_dir = "data"
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        print(f"✅ Folder '{save_dir}' created.")

    base_url = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=120)

    all_data = []
    current_date = start_date

    while current_date <= end_date:
        if current_date.weekday() < 5: 
            date_str = current_date.strftime("%d%m%Y")
            url = base_url.format(date_str)
            try:
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    df = pd.read_csv(StringIO(response.text))
                    df.columns = df.columns.str.strip()
                    if 'SERIES' in df.columns:
                        df = df[df['SERIES'].str.strip() == 'EQ']
                    all_data.append(df)
            except Exception as e:
                print(f"Error on {current_date.date()}: {e}")
        current_date += timedelta(days=1)
        time.sleep(0.5)

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        date_col = next((c for c in ['DATE1', 'DATE', 'TradDt'] if c in combined_df.columns), None)
        vol_col = next((c for c in ['TOTTRDQTY', 'TtlTradgVol'] if c in combined_df.columns), None)

        if vol_col and date_col:
            combined_df[date_col] = pd.to_datetime(combined_df[date_col])
            combined_df[vol_col] = pd.to_numeric(combined_df[vol_col], errors='coerce')
            
            max_report = combined_df.sort_values(vol_col, ascending=False).drop_duplicates('SYMBOL')
            final_report = max_report[['SYMBOL', vol_col, date_col, 'CLOSE']].copy()
            final_report.rename(columns={vol_col: 'MAX_VOLUME', date_col: 'DATE_OF_MAX'}, inplace=True)

            # Sahi filename jo aapke YAML mein hai
            output_path = os.path.join(save_dir, "max_volume_4months.csv")
            final_report.to_csv(output_path, index=False)
            print(f"🚀 File saved successfully at: {output_path}")
    else:
        print("⚠️ No data found. File was not created.")

if __name__ == "__main__":
    update_nse_report()
