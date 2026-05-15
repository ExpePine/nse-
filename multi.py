```python
import requests
import pandas as pd
import gspread
from io import StringIO
from datetime import datetime, timedelta
import os

# ================= CONFIGURATION =================

SHEET_NAME = "MV2 for SQL"
WORKSHEET_NAME = "Sheet28"

NSE_URL = (
    "https://nsearchives.nseindia.com/products/content/"
    "sec_bhavdata_full_{}.csv"
)

CACHE_FILE = "last_run_sheet28.txt"

HEADERS = [
    "SYMBOL",
    "DQ1", "DQ2", "DQ3",
    "DATE1", "DATE2", "DATE3",
    "CURR_DQ", "CURR_DATE"
]

# ================= CLEANUP =================

def cleanup_old_files():
    for file in os.listdir():
        if file.endswith(".csv"):
            try:
                os.remove(file)
            except Exception:
                pass

# ================= DATE FORMATTER =================

def format_to_american(date_val):
    """
    Converts Indian-style dates safely into MM/DD/YYYY.
    Prevents ambiguity like 08/04/2026.
    """

    if (
        not date_val
        or str(date_val).strip() == ""
        or "No Trade" in str(date_val)
        or str(date_val).lower() == "nan"
    ):
        return ""

    try:
        return pd.to_datetime(
            date_val,
            dayfirst=True,
            errors="coerce"
        ).strftime("%m/%d/%Y")
    except Exception:
        return ""

# ================= NSE DATA FETCH =================

def get_best_available_data():

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36"
        ),
        "Referer": "https://www.nseindia.com/"
    }

    session = requests.Session()

    try:
        session.get(
            "https://www.nseindia.com",
            headers=headers,
            timeout=10
        )
    except Exception:
        pass

    now_utc = datetime.utcnow()

    # NSE bhavcopy generally available after market hours
    target_dt = (
        now_utc
        if now_utc.hour >= 13
        else (now_utc - timedelta(days=1))
    )

    for i in range(2):

        try_date = target_dt - timedelta(days=i)
        date_str = try_date.strftime("%d%m%Y")

        try:

            response = session.get(
                NSE_URL.format(date_str),
                headers=headers,
                timeout=20
            )

            if response.status_code == 200:

                df = pd.read_csv(StringIO(response.text))

                df.columns = df.columns.str.strip()

                # Clean Delivery Quantity
                df["DELIV_QTY"] = pd.to_numeric(
                    df["DELIV_QTY"],
                    errors="coerce"
                ).fillna(0).astype(int)

                # Convert to American format safely
                df["DATE1"] = pd.to_datetime(
                    df["DATE1"],
                    errors="coerce",
                    dayfirst=True
                ).dt.strftime("%m/%d/%Y")

                return df

        except Exception as e:
            print(f"⚠️ Error fetching NSE data: {e}")

    return None

# ================= MAIN PROCESS =================

def update_process():

    today_run = datetime.utcnow().strftime("%Y-%m-%d")

    # Prevent multiple runs same day
    if os.path.exists(CACHE_FILE):

        with open(CACHE_FILE, "r") as f:

            if f.read().strip() == today_run:
                print(f"⏭️ Already processed for {today_run}")
                return

    cleanup_old_files()

    # ================= GOOGLE SHEET =================

    try:

        gc = gspread.service_account(
            filename="service_account.json"
        )

        sh = gc.open(SHEET_NAME)

        worksheet = sh.worksheet(WORKSHEET_NAME)

    except Exception as e:

        print(f"❌ Google Sheet Connection Error: {e}")
        return

    # ================= READ SHEET =================

    try:

        records = worksheet.get_all_records()
        df_sheet = pd.DataFrame(records)

    except Exception as e:

        print(f"❌ Sheet Read Error: {e}")
        return

    if df_sheet.empty:
        print("ℹ️ Sheet is empty.")
        return

    # ================= FETCH NSE DATA =================

    bhav_df = get_best_available_data()

    if bhav_df is None:
        print("❌ NSE data unavailable.")
        return

    final_output = [HEADERS]

    # ================= PROCESS EACH SYMBOL =================

    for _, row in df_sheet.iterrows():

        symbol = str(row.get("SYMBOL", "")).strip()

        if not symbol:
            continue

        stock_data = bhav_df[
            bhav_df["SYMBOL"] == symbol
        ]

        # Existing Top Values
        top_list = [

            (
                int(float(row.get("DQ1", 0) or 0)),
                format_to_american(row.get("DATE1", ""))
            ),

            (
                int(float(row.get("DQ2", 0) or 0)),
                format_to_american(row.get("DATE2", ""))
            ),

            (
                int(float(row.get("DQ3", 0) or 0)),
                format_to_american(row.get("DATE3", ""))
            )
        ]

        curr_dq = 0
        curr_date = "No Trade"

        # ================= CURRENT DAY DATA =================

        if not stock_data.empty:

            try:

                curr_dq = int(stock_data.iloc[0]["DELIV_QTY"])

            except Exception:
                curr_dq = 0

            curr_date = stock_data.iloc[0]["DATE1"]

            existing_dates = [
                item[1]
                for item in top_list
                if item[1]
            ]

            # Avoid duplicate dates
            if curr_date not in existing_dates:

                top_list.append((curr_dq, curr_date))

                # Sort descending by DQ
                top_list.sort(
                    key=lambda x: x[0],
                    reverse=True
                )

                # Keep top 3 only
                top_list = top_list[:3]

        # Ensure exactly 3 rows
        while len(top_list) < 3:
            top_list.append((0, ""))

        # ================= FINAL ROW =================

        new_row = [

            symbol,

            top_list[0][0],
            top_list[1][0],
            top_list[2][0],

            top_list[0][1],
            top_list[1][1],
            top_list[2][1],

            curr_dq,
            curr_date
        ]

        final_output.append(new_row)

    # ================= UPDATE SHEET =================

    try:

        worksheet.update(
            range_name="A1",
            values=final_output
        )

    except Exception as e:

        print(f"❌ Sheet Update Error: {e}")
        return

    # ================= CACHE SAVE =================

    with open(CACHE_FILE, "w") as f:
        f.write(today_run)

    print(
        f"🎉 SUCCESS: "
        f"{WORKSHEET_NAME} updated successfully "
        f"for {today_run}"
    )

    cleanup_old_files()

# ================= RUN =================

if __name__ == "__main__":
    update_process()
```
