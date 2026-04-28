import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from requests.adapters import HTTPAdapter

# ===== CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

# ===== CONFIG =====
SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "Foreign"

# ===== SYMBOL LIST =====
symbols = """AAA HPG ACB MBB""".split()

# ===== SESSION OPTIMIZED =====
session = requests.Session()
adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
session.mount("https://", adapter)

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# ===== FETCH 1 SYMBOL =====
def get_price(symbol):
    url = f"https://api-finfo.vndirect.com.vn/v4/foreigns?sort=tradingDate&q=code:{symbol}&size=550&page=1"

    try:
        res = session.get(url, headers=headers, timeout=10)

        if res.status_code != 200:
            return None

        json_data = res.json()
        data = json_data.get("data", [])

        if not data:
            return None

        # tránh spam API
        time.sleep(0.05)

        return [
            [row.get("code"), row.get("tradingDate"), row.get("netVol", 0)]
            for row in data
        ]

    except:
        return None


# ===== MAIN LOOP =====
all_data = []
remaining = set(symbols)
round_num = 0
MAX_ROUNDS = 5

while remaining and round_num < MAX_ROUNDS:
    round_num += 1
    print(f"\n🔁 ROUND {round_num} | còn {len(remaining)} mã")

    success = set()
    temp_data = []

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {executor.submit(get_price, s): s for s in remaining}

        for future in as_completed(futures):
            symbol = futures[future]
            result = future.result()

            if result:
                temp_data.extend(result)
                success.add(symbol)

    remaining -= success
    all_data.extend(temp_data)

    print(f"✅ LẤY ĐƯỢC: {len(success)} | ❌ CÒN: {len(remaining)}")

    if remaining:
        time.sleep(2)

# log mã fail nếu có
if remaining:
    print("❌ FAIL SAU MAX RETRY:", remaining)

print("\n🎯 DONE FETCH DATA")

# ===== SORT DATA =====
all_data.sort(key=lambda x: (x[0], x[1]))

# ===== PUSH TO GOOGLE SHEET =====
sh = client.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

ws.batch_clear(["A:C"])

if all_data:
    ws.update("A1", [["code", "tradingDate", "netVol"]] + all_data)
    print("✅ WRITE OK")
else:
    ws.update("A1", [["NO DATA"]])
    print("⚠️ NO DATA")

print("🚀 DONE")
