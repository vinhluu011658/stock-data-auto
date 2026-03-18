import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor

# ===== CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

# ===== CONFIG =====
SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "Price"

symbols = ["HPG"]  # test 1 mã trước

def get_price(symbol):
    url = f"https://api-finfo.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{symbol}&size=120&page=1"

    res = requests.get(url)
    data = res.json()["data"]

    print(symbol, "rows:", len(data))

    return [
        [symbol, row["date"], row["adClose"]]
        for row in data
    ]

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

print("👉 Writing to sheet:", ws.title)

ws.clear()

if all_data:
    ws.update("A1", [["symbol", "date", "close"]] + all_data)
    print("✅ WRITE OK")
else:
    print("❌ NO DATA")

print("DONE")
