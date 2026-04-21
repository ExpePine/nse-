import requests
import pandas as pd
import gspread
from io import StringIO
from datetime import datetime, timedelta
import os

# --- CONFIGURATION ---
SHEET_NAME = "MV2 for SQL"
WORKSHEET_NAME = "Sheet28"
NSE_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"
CACHE_FILE = "last_run_sheet28.txt"

# Cleaned Headers: Symbol, Top 3 DQs, Top 3 Dates, and Current Data
HEADERS = [
    "SYMBOL", 
    "DQ1", "DQ2", "DQ3", 
    "DATE1", "DATE2", "DATE3", 
    "CURR_DQ", "CURR_DATE"
]

def cleanup_old_files():
    for f in os.listdir():
        if f.endswith(".csv"):
            try: os.remove(f)
            except: pass

def get_best_available_data():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.nseindia.com/"
    }
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=10)

    now_utc = datetime.utcnow()
    # Check today if after 13:00 UTC (6:30 PM IST), otherwise fallback to yesterday
    target_dt = now_utc if now_utc.hour >= 13 else (now_utc - timedelta(days=1))
    
    date_str = target_dt.strftime("%d%m%Y")
    resp = session.get(NSE_URL.format(date_str), headers=headers)
    
    if resp.status_code != 200:
        date_str = (target_dt - timedelta(days=1)).strftime("%d%m%Y")
        resp = session.get(NSE_URL.format(date_str), headers=headers)

    if resp.status_code == 200:
        df = pd.read_csv(StringIO(resp.text))
        df.columns = df.columns.str.strip()
        df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce').fillna(0).astype(int)
        df['DATE1'] = pd.to_datetime(df['DATE1'], errors='coerce').dt.strftime('%m/%d/%Y')
        return df
    return None

def update_process():
    today_run = datetime.utcnow().strftime("%Y-%m-%d")
    
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r") as f:
            if f.read().strip() == today_run:
                print(f"⏭️ Skipping: Already processed for {today_run}.")
                return

    cleanup_old_files()

    try:
        gc = gspread.service_account(filename="service_account.json")
        sh = gc.open(SHEET_NAME)
        worksheet = sh.worksheet(WORKSHEET_NAME)
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return

    records = worksheet.get_all_records()
    df_sheet = pd.DataFrame(records)
    
    if df_sheet.empty:
        print("ℹ️ Sheet is empty. Please add Symbols in Column A.")
        return

    bhav_df = get_best_available_data()
    if bhav_df is None:
        print("❌ No NSE data available at this time.")
        return

    final_output = [HEADERS] 

    for _, row in df_sheet.iterrows():
        symbol = str(row.get('SYMBOL', '')).strip()
        if not symbol: continue
        
        stock_data = bhav_df[bhav_df['SYMBOL'] == symbol]
        
        # 1. Existing DQ Top 3
        top_list = [
            (int(float(row.get('DQ1', 0) or 0)), str(row.get('DATE1', ''))),
            (int(float(row.get('DQ2', 0) or 0)), str(row.get('DATE2', ''))),
            (int(float(row.get('DQ3', 0) or 0)), str(row.get('DATE3', '')))
        ]

        curr_dq = 0
        curr_date = "No Trade"

        if not stock_data.empty:
            curr_dq = int(stock_data.iloc[0]['DELIV_QTY'])
            curr_date = str(stock_data.iloc[0]['DATE1'])

            # Ranking Logic: Add today and sort if it's a new date
            existing_dates = [item[1] for item in top_list]
            if curr_date not in existing_dates:
                top_list.append((curr_dq, curr_date))
                top_list.sort(key=lambda x: x[0], reverse=True)
                top_list = top_list[:3]

        # 2. Build the Final Row
        new_row = [
            symbol,
            top_list[0][0], top_list[1][0], top_list[2][0], # DQ1, DQ2, DQ3
            top_list[0][1], top_list[1][1], top_list[2][1], # DATE1, DATE2, DATE3
            curr_dq, curr_date                             # Today's Data
        ]
        final_output.append(new_row)

    # Overwrite Sheet starting from A1
    worksheet.update(range_name='A1', values=final_output)
    
    with open(CACHE_FILE, "w") as f:
        f.write(today_run)
        
    print(f"🎉 Success! Sheet28 updated with Top 3 DQ rankings for {today_run}.")
    cleanup_old_files()

if __name__ == "__main__":
    update_process()
