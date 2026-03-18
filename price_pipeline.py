import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor

# ===== LOAD CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

# ===== CONFIG =====
SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "Price"

symbols = ["HPG", "VCB", "SSI"]  # thay 400 mã của bạn

# ===== FETCH =====
def get_price(symbol):
    url = f"https://api-finfo.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{symbol}&size=120&page=1"
    
    try:
        res = requests.get(url, timeout=10)
        data = res.json()["data"]
        
        return [
            [symbol, row["date"], row["adClose"]]
            for row in data
        ]
    except:
        return []

# ===== RUN PARALLEL =====
all_data = []

with ThreadPoolExecutor(max_workers=20) as executor:
    results = executor.map(get_price, symbols)

for r in results:
    all_data.extend(r)

# ===== SORT =====
all_data.sort(key=lambda x: (x[0], x[1]))

# ===== WRITE SHEET =====
sh = client.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

ws.clear()
ws.update("A1", [["symbol", "date", "close"]] + all_data)

print("DONE PRICE PIPELINE")
