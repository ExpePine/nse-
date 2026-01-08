import requests
import time
import pandas as pd
import gspread
from io import StringIO
from datetime import datetime, timedelta
import os

# --- CONFIG ---
SHEET_NAME = "Tradingview Data Reel Experimental May"
WORKSHEET_NAME = "Sheet13"
NSE_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"

def get_best_available_data():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.nseindia.com/"
    }
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=10)

    now_utc = datetime.utcnow()
    # IST is UTC + 5:30. 6:30 PM IST is 1:00 PM UTC.
    yesterday_str = (now_utc - timedelta(days=1)).strftime("%d%m%Y")
    today_str = now_utc.strftime("%d%m%Y")

    best_df = None
    final_date = yesterday_str

    # 1. Start with Yesterday (as requested)
    print(f"🔍 Checking Yesterday's data ({yesterday_str})...")
    resp_yest = session.get(NSE_URL.format(yesterday_str), headers=headers)
    
    if resp_yest.status_code == 200:
        print(f"✅ Yesterday's data found.")
        best_df = pd.read_csv(StringIO(resp_yest.text))
    else:
        print(f"⚠️ Yesterday not available. Checking Today directly...")

    # 2. Check Today only if it's after 6:30 PM IST (13:00 UTC)
    if now_utc.hour >= 13:
        print(f"🕒 Past 6:30 PM IST. Checking Today's data ({today_str})...")
        resp_today = session.get(NSE_URL.format(today_str), headers=headers)
        if resp_today.status_code == 200:
            print(f"⭐ Today's data ({today_str}) is ready! Using it.")
            best_df = pd.read_csv(StringIO(resp_today.text))
            final_date = today_str

    if best_df is not None:
        best_df.columns = best_df.columns.str.strip()
        # FIX FOR THE DASH '-' ERROR:
        # Convert columns to numeric, forcing errors like '-' to NaN, then fill with 0
        best_df['NO_OF_TRADES'] = pd.to_numeric(best_df['NO_OF_TRADES'], errors='coerce').fillna(0).astype(int)
        best_df['DELIV_QTY'] = pd.to_numeric(best_df['DELIV_QTY'], errors='coerce').fillna(0).astype(int)
        
    return best_df, final_date

def update_process():
    try:
        gc = gspread.service_account(filename="service_account.json")
        sh = gc.open(SHEET_NAME)
        worksheet = sh.worksheet(WORKSHEET_NAME)
    except Exception as e:
        print(f"❌ Auth Error: {e}")
        return

    records = worksheet.get_all_records()
    df_sheet = pd.DataFrame(records)
    bhav_df, final_date = get_best_available_data()
    
    if bhav_df is None:
        print("❌ No data found.")
        return

    final_rows = []
    for _, row in df_sheet.iterrows():
        symbol = str(row['SYMBOL']).strip()
        stock_data = bhav_df[bhav_df['SYMBOL'] == symbol]
        
        m_trd = row.get('Max_NO_OF_TRADES', 0)
        m_del = row.get('Max_DELIV_QTY', 0)
        dt_trd = row.get('DATE_MAX_TRADES', '')
        dt_del = row.get('DATE_MAX_DELIV', '')

        if not stock_data.empty:
            c_trd = int(stock_data.iloc[0]['NO_OF_TRADES'])
            c_del = int(stock_data.iloc[0]['DELIV_QTY'])
            c_dt = stock_data.iloc[0]['DATE1']

            if c_trd > m_trd: m_trd, dt_trd = c_trd, c_dt
            if c_del > m_del: m_del, dt_del = c_del, c_dt
            curr_vals = [c_trd, c_del, c_dt]
        else:
            curr_vals = [0, 0, "No Trade"]

        final_rows.append([symbol, m_trd, m_del, dt_trd, dt_del] + curr_vals)

    worksheet.update('A2', final_rows)
    print(f"🎉 Updated Sheet using {final_date} data.")
    
    # Cleanup: Delete any CSV files
    for f in os.listdir():
        if f.endswith(".csv"): os.remove(f)

if __name__ == "__main__":
    update_process()
