import requests
import time
import pandas as pd
import gspread
from io import StringIO
from datetime import datetime, timedelta
import os

# --- CONFIGURATION ---
SHEET_NAME = "MV2 for SQL"
WORKSHEET_NAME = "Sheet8"
NSE_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"

# Required Headers
REQUIRED_HEADERS = [
    "SYMBOL", "Max_NO_OF_TRADES", "Max_DELIV_QTY", 
    "DATE_MAX_TRADES", "DATE_MAX_DELIV", 
    "CURR_TRADES", "CURR_DELIV", "CURR_DATE"
]

def cleanup_old_files():
    for f in os.listdir():
        if f.endswith(".csv"):
            try:
                os.remove(f)
            except:
                pass

def get_best_available_data():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.nseindia.com/"
    }
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=headers, timeout=10)

    now_utc = datetime.utcnow()
    yesterday_str = (now_utc - timedelta(days=1)).strftime("%d%m%Y")
    today_str = now_utc.strftime("%d%m%Y")

    best_df = None
    
    # Check Yesterday
    resp_yest = session.get(NSE_URL.format(yesterday_str), headers=headers)
    if resp_yest.status_code == 200:
        best_df = pd.read_csv(StringIO(resp_yest.text))
    
    # Check Today
    if now_utc.hour >= 13:
        resp_today = session.get(NSE_URL.format(today_str), headers=headers)
        if resp_today.status_code == 200:
            best_df = pd.read_csv(StringIO(resp_today.text))

    if best_df is not None:
        best_df.columns = best_df.columns.str.strip()
        best_df['NO_OF_TRADES'] = pd.to_numeric(best_df['NO_OF_TRADES'], errors='coerce').fillna(0).astype(int)
        best_df['DELIV_QTY'] = pd.to_numeric(best_df['DELIV_QTY'], errors='coerce').fillna(0).astype(int)
        # Format the fresh data to American
        best_df['DATE1'] = pd.to_datetime(best_df['DATE1'], errors='coerce').dt.strftime('%m/%d/%Y') 
        
    return best_df

def update_process():
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
        print("ℹ️ Sheet is empty.")
        return

    bhav_df = get_best_available_data()
    if bhav_df is None:
        print("❌ No NSE data available.")
        return

    final_rows = []
    for _, row in df_sheet.iterrows():
        symbol = str(row['SYMBOL']).strip()
        stock_data = bhav_df[bhav_df['SYMBOL'] == symbol]
        
        # 1. READ AND CONVERT EXISTING MAX DATES TO AMERICAN
        # This fixes the existing DD-MM-YYYY dates in your screenshot
        dt_trd = row.get('DATE_MAX_TRADES', '')
        dt_del = row.get('DATE_MAX_DELIV', '')

        def ensure_american(date_str):
            if not date_str or date_str == "": return ""
            try:
                # Try to parse whatever is there and force it to MM/DD/YYYY
                return pd.to_datetime(date_str, dayfirst=True).strftime('%m/%d/%Y')
            except:
                return str(date_str)

        dt_trd = ensure_american(dt_trd)
        dt_del = ensure_american(dt_del)

        # Get numeric Max values
        try:
            m_trd = int(float(row.get('Max_NO_OF_TRADES', 0) or 0))
            m_del = int(float(row.get('Max_DELIV_QTY', 0) or 0))
        except:
            m_trd, m_del = 0, 0

        if not stock_data.empty:
            c_trd = int(stock_data.iloc[0]['NO_OF_TRADES'])
            c_del = int(stock_data.iloc[0]['DELIV_QTY'])
            c_dt = str(stock_data.iloc[0]['DATE1']) 

            # Update Max if current is higher
            if c_trd > m_trd: 
                m_trd = c_trd
                dt_trd = c_dt
            if c_del > m_del: 
                m_del = c_del
                dt_del = c_dt
                
            curr_vals = [c_trd, c_del, c_dt]
        else:
            curr_vals = [0, 0, "No Trade"]

        final_rows.append([symbol, m_trd, m_del, dt_trd, dt_del] + curr_vals)

    # Overwrite sheet with corrected formats
    worksheet.update('A2', final_rows)
    print("🎉 Success! All dates in Columns D, E, and H are now MM/DD/YYYY.")
    cleanup_old_files()

if __name__ == "__main__":
    update_process()
