import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== GOOGLE =====

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "BCTC"

# ===== SYMBOLS =====

symbols = [
    "BSR"      # test trước
]

# ===== SESSION =====

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://cafef.vn/"
}

# ===== CDKT =====

def get_cdkt(symbol):

    url = (
        "https://apiweb.cafef.vn/api/v2/BCTC/GetReportCDKT"
        f"?symbol={symbol}"
        "&pageIndex=1"
        "&pageSize=10"
        "&reportType=ALL"
        "&TypeTime=NAM"
    )

    try:

        r = session.get(
            url,
            headers=headers,
            timeout=30
        )

        if r.status_code != 200:
            print(symbol, r.status_code)
            return []

        js = r.json()

        if not js.get("isSuccess"):
            print(symbol, "FAIL")
            return []

        value = js["value"]

        # ===== MAP CODE -> TÊN =====

        account_map = {}

        for group in value["templace"]:

            for item in group["data"]:

                account_map[item["code"]] = item["name"]

        rows = []

        # ===== DỮ LIỆU =====

        for section in value["data"]:

            for year_block in section["data"]:

                year = year_block["year"]

                for item in year_block["data"]:

                    code = item["code"]

                    rows.append([
                        symbol,
                        "CDKT",
                        code,
                        account_map.get(code, ""),
                        year,
                        item["value"]
                    ])

        print(symbol, len(rows))

        return rows

    except Exception as e:

        print(symbol, "ERROR:", e)

        return []

# ===== CHẠY =====

all_data = []

with ThreadPoolExecutor(max_workers=5) as executor:

    futures = {
        executor.submit(get_cdkt, symbol): symbol
        for symbol in symbols
    }

    for future in as_completed(futures):

        result = future.result()

        if result:
            all_data.extend(result)

# ===== APPEND SHEET =====

sh = client.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

existing_rows = len(ws.col_values(1))

print("Current rows:", existing_rows)

ws.update(
    f"A{existing_rows + 1}",
    all_data
)

print(f"APPEND DONE: {len(all_data)} rows")
