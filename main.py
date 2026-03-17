import requests
import pandas as pd
import gspread
import json
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from oauth2client.service_account import ServiceAccountCredentials

# ===== LOAD GOOGLE CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# ===== OPEN SHEET =====
sheet = client.open("DATA_STOCK").sheet1

# ===== SYMBOL LIST =====
symbols = """AAA AAM AAT ABR ABS ABT ACB ACC""".split()  # test ít trước

data_all = []

# ===== HÀM TRỪ =====
def safe_calc(a, b):
    return (a or 0) - (b or 0)

headers = {"User-Agent": "Mozilla/5.0"}

# ===== FETCH =====
def fetch_symbol(symbol):
    url = f"https://api-finance-t19.24hmoney.vn/v1/ios/stock/statistic-investor-history?symbol={symbol}"
    result = []

    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()

        for item in data.get("data", []):
            timestamp = item.get("trading_date") or item.get("date")

            if not timestamp:
                continue

            dt = datetime.utcfromtimestamp(
                int(timestamp) / 1000 if int(timestamp) > 1e12 else int(timestamp)
            ) + timedelta(hours=7)

            # 🔥 convert sang string NGAY TỪ ĐÂY
            date_str = dt.strftime("%d/%m/%Y")

            result.append({
                "Date": date_str,
                "Ma CP": symbol,
                "nuoc ngoai": safe_calc(item.get("foreign_buy"), item.get("foreign_sell")),
                "tu doanh": safe_calc(item.get("proprietary_buy"), item.get("proprietary_sell")),
                "to chuc trong nuoc": safe_calc(item.get("local_institutional_buy"), item.get("local_institutional_sell")),
                "ca nhan trong nuoc": safe_calc(item.get("local_individual_buy"), item.get("local_individual_sell")),
                "to chuc nuoc ngoai": safe_calc(item.get("foreign_institutional_buy"), item.get("foreign_institutional_sell")),
                "ca nhan nuoc ngoai": safe_calc(item.get("foreign_individual_buy"), item.get("foreign_individual_sell"))
            })

        time.sleep(0.1)

    except Exception as e:
        print(f"Lỗi {symbol}: {e}")

    return result

# ===== MULTI THREAD =====
with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(fetch_symbol, symbols)

for r in results:
    data_all.extend(r)

# ===== DATAFRAME =====
df = pd.DataFrame(data_all)

if df.empty:
    print("Không có dữ liệu")
    exit()

# ===== SORT =====
df = df.sort_values(by=["Ma CP", "Date"])

# ===== PUSH =====
sheet.clear()
sheet.update(
    [df.columns.tolist()] + df.values.tolist(),
    value_input_option="USER_ENTERED"
)

print("DONE")
