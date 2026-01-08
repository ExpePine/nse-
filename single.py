import requests
import time
import pandas as pd
import gspread
from io import StringIO
from datetime import datetime
import os

# --- CONFIGURATION ---
# It is better to use Environment Variables for security in GitHub
SHEET_NAME = "Tradingview Data Reel Experimental May"
WORKSHEET_NAME = "Sheet13"
JSON_KEYFILE = "service_account.json" 
NSE_URL = "https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{}.csv"

def get_nse_data():
    """Fetches today's Bhavcopy from NSE."""
    date_str = datetime.now().strftime("%d%m%Y") 
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.nseindia.com/"
    }
    
    session = requests.Session()
    # NSE requires visiting the main site first to set cookies
    session.get("https://www.nseindia.com", headers=headers, timeout=15)
    time.sleep(2)
    
    print(f"📥 Downloading NSE Bhavcopy for {date_str}...")
    response = session.get(NSE_URL.format(date_str), headers=headers)

    if response.status_code == 200 and "text/csv" in response.headers.get("Content-Type", ""):
        df = pd.read_csv(StringIO(response.text))
        df.columns = df.columns.str.strip()
        return df
    else:
        print(f"❌ Data not available. Status: {response.status_code}")
        return None

def update_google_sheets():
    # 1. Authorize Gspread
    try:
        # Looks for service_account.json in the root directory
        gc = gspread.service_account(filename=JSON_KEYFILE)
        sh = gc.open(SHEET_NAME)
        worksheet = sh.worksheet(WORKSHEET_NAME)
    except Exception as e:
        print(f"❌ Google Sheet Auth Error: {e}")
        return

    # 2. Get Sheet Data
    records = worksheet.get_all_records()
    if not records:
        print("❌ Sheet is empty or headers are missing.")
        return
    
    df_sheet = pd.DataFrame(records)
    
    # 3. Get NSE Data
    bhav_df = get_nse_data()
    if bhav_df is None: return

    # 4. Processing Logic
    updated_data = []
    
    for _, row in df_sheet.iterrows():
        symbol = str(row['SYMBOL']).strip()
        stock_today = bhav_df[bhav_df['SYMBOL'] == symbol]
        
        # Initialize variables from existing sheet data
        max_trades = row.get('Max_NO_OF_TRADES', 0)
        max_deliv = row.get('Max_DELIV_QTY', 0)
        date_max_trades = row.get('DATE_MAX_TRADES', '')
        date_max_deliv = row.get('DATE_MAX_DELIV', '')

        if not stock_today.empty:
            # Current values from NSE
            curr_trades = int(stock_today.iloc[0]['NO_OF_TRADES'])
            curr_deliv = int(stock_today.iloc[0]['DELIV_QTY'])
            curr_date = stock_today.iloc[0]['DATE1']

            # Compare and update Max values
            if curr_trades > max_trades:
                max_trades = curr_trades
                date_max_trades = curr_date
            
            if curr_deliv > max_deliv:
                max_deliv = curr_deliv
                date_max_deliv = curr_date

            # Current Values for columns 6, 7, 8, 9
            cur_val_trades = curr_trades
            cur_val_deliv = curr_deliv
            cur_val_date = curr_date
        else:
            # If stock didn't trade today, current values are 0 or N/A
            cur_val_trades = 0
            cur_val_deliv = 0
            cur_val_date = "No Trade"

        # Create row based on your requested header order
        new_row = [
            symbol,             # Column A
            max_trades,         # Column B
            max_deliv,          # Column C
            date_max_trades,    # Column D
            date_max_deliv,     # Column E
            cur_val_trades,     # Column F (Current Trades)
            cur_val_deliv,      # Column G (Current Deliv)
            cur_val_date        # Column H (Current Date)
        ]
        updated_data.append(new_row)

    # 5. Overwrite the sheet data (Starting from A2)
    # We use a range update for speed
    cell_range = f'A2:H{len(updated_data) + 1}'
    worksheet.update(cell_range, updated_data)
    print(f"✅ Successfully updated {len(updated_data)} symbols.")

if __name__ == "__main__":
    update_google_sheets()
