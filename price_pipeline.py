import requests
import gspread
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

# ===== CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

# ===== CONFIG =====
SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "Price"

symbols = ["HPG"]  # test trước

def get_price(symbol):
    url = f"https://api-finfo.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{symbol}&size=120&page=1"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    try:
        res = requests.get(url, headers=headers, timeout=10)

        if res.status_code != 200:
            print(symbol, "STATUS:", res.status_code)
            return []

        if not res.text.strip():
            print(symbol, "EMPTY")
            return []

        try:
            json_data = res.json()
        except:
            print(symbol, "NOT JSON:", res.text[:100])
            return []

        data = json_data.get("data", [])

        print(symbol, "rows:", len(data))

        return [
            [symbol, row["date"], row["adClose"]]
            for row in data
        ]

    except Exception as e:
        print(symbol, "ERROR:", e)
        return []

# ===== RUN =====
all_data = []

with ThreadPoolExecutor(max_workers=3) as executor:
    results = executor.map(get_price, symbols)

for r in results:
    all_data.extend(r)

print("TOTAL:", len(all_data))

# ===== SHEET =====
sh = client.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

ws.clear()

if all_data:
    ws.update("A1", [["symbol", "date", "close"]] + all_data)
    print("WRITE OK")
else:
    ws.update("A1", [["NO DATA"]])
    print("NO DATA")

print("DONE")
