import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== GOOGLE SHEET =====

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "BCTC"

# ===== DANH SÁCH MÃ =====

symbols = """
AAA AAM
""".split()

# ===== SESSION =====

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://cafef.vn/"
}

# ===== LẤY KQKD =====

def get_kqkd(symbol):

    url = (
        "https://apiweb.cafef.vn/api/v1/BCTC/GetReportDetail"
        f"?symbol={symbol}"
        "&pageIndex=1"
        "&pageSize=10"
        "&reportType=KQKD"
        "&TypeTime=NAM"
    )

    try:

        r = session.get(
            url,
            headers=headers,
            timeout=30
        )

        if r.status_code != 200:
            print(symbol, "STATUS", r.status_code)
            return []

        js = r.json()

        if not js.get("isSuccess"):
            print(symbol, "isSuccess=False")
            return []

        value = js["value"]

        # map code -> tên tài khoản
        account_map = {
            item["code"]: item["name"]
            for item in value["templace"]
        }

        rows = []

        for year_block in value["data"]:

            year = year_block["year"]

            for item in year_block["data"]:

                rows.append([
                    symbol,
                    "KQKD",
                    account_map.get(item["code"], ""),
                    year,
                    item["value"]
                ])

        print(symbol, len(rows))

        return rows

    except Exception as e:

        print(symbol, "ERROR:", e)

        return []

# ===== CHẠY SONG SONG =====

all_data = []

with ThreadPoolExecutor(max_workers=20) as executor:

    futures = {
        executor.submit(get_kqkd, symbol): symbol
        for symbol in symbols
    }

    for future in as_completed(futures):

        result = future.result()

        if result:
            all_data.extend(result)

# ===== GHI SHEET =====

sh = client.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

ws.clear()

sheet_data = [
    [
        "ma_cp",
        "loai_bc",
        "tai_khoan",
        "nam",
        "gia_tri"
    ]
]

sheet_data.extend(all_data)

ws.update("A1", sheet_data)

print(f"DONE: {len(all_data):,} rows")
