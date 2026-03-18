import requests
import gspread
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

# ===== LOAD CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

# ===== CONFIG =====
SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "Price"

symbols = """AAA AAM AAT ABR""".split()  # 👈 TEST NGẮN TRƯỚC

# ===== FETCH =====
def get_price(symbol):
    url = f"https://api-finfo.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{symbol}&size=120&page=1"

    for i in range(2):  # retry 2 lần
        try:
            res = requests.get(url, timeout=10)
            data = res.json().get("data", [])

            print(f"{symbol}: {len(data)} rows")

            if len(data) > 0:
                return [[symbol, r["date"], r["adClose"]] for r in data]

        except Exception as e:
            print(f"{symbol} ERROR:", e)

        time.sleep(0.5)

    return []

# ===== RUN =====
all_data = []

with ThreadPoolExecutor(max_workers=5) as executor:  # 👈 giảm thread cho ổn định
    results = executor.map(get_price, symbols)

for r in results:
    all_data.extend(r)

print("TOTAL ROWS:", len(all_data))

# ===== SORT =====
all_data.sort(key=lambda x: (x[0], x[1]))

# ===== SHEET =====
sh = client.open_by_key(SHEET_ID)

try:
    ws = sh.worksheet(SHEET_NAME)
except:
    ws = sh.add_worksheet(title=SHEET_NAME, rows="1000", cols="10")

ws.clear()

if len(all_data) > 0:
    ws.update("A1", [["symbol", "date", "close"]] + all_data)
    print("WRITE SUCCESS")
else:
    ws.update("A1", [["NO DATA"]])
    print("NO DATA - CHECK API / SYMBOL")

print("DONE")
