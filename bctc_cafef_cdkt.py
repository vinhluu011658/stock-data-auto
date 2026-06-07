import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# GOOGLE SHEETS
# =========================

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "BCTC"

# =========================
# TEST SYMBOLS
# =========================

symbols = """
BSR
HPG
VNM
""".split()

# =========================
# SESSION
# =========================

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://cafef.vn/"
}

# =========================
# HÀM CHUNG CDKT + LCTT
# =========================

def get_report(symbol, loai_bc, base_url):

    url = (
        f"{base_url}"
        f"?symbol={symbol}"
        "&pageIndex=1"
        "&pageSize=5"
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
            print(symbol, loai_bc, "STATUS", r.status_code)
            return []

        js = r.json()

        if not js.get("isSuccess"):
            print(symbol, loai_bc, "isSuccess=False")
            return []

        value = js["value"]

        # =====================
        # MAP CODE -> TÀI KHOẢN
        # =====================

        account_map = {}

        for group in value["templace"]:

            for item in group["data"]:

                account_map[item["code"]] = item["name"]

        rows = []

        # =====================
        # ĐỌC DỮ LIỆU
        # =====================

        for section in value["data"]:

            for year_block in section["data"]:

                year = year_block["year"]

                for item in year_block["data"]:

                    code = item["code"]

                    rows.append([
                        symbol,
                        loai_bc,
                        code,
                        account_map.get(code, ""),
                        year,
                        item["value"]
                    ])

        print(symbol, loai_bc, len(rows))

        return rows

    except Exception as e:

        print(symbol, loai_bc, "ERROR:", e)

        return []

# =========================
# LẤY CDKT + LCTT CHO 1 MÃ
# =========================

def get_symbol_data(symbol):

    rows = []

    # CDKT

    rows.extend(
        get_report(
            symbol,
            "CDKT",
            "https://apiweb.cafef.vn/api/v2/BCTC/GetReportCDKT"
        )
    )

    # LCTT

    rows.extend(
        get_report(
            symbol,
            "LCTT",
            "https://apiweb.cafef.vn/api/v2/BCTC/GetReportLCTT"
        )
    )

    return rows

# =========================
# CHẠY SONG SONG
# =========================

all_data = []

with ThreadPoolExecutor(max_workers=30) as executor:

    futures = {
        executor.submit(get_symbol_data, symbol): symbol
        for symbol in symbols
    }

    for future in as_completed(futures):

        result = future.result()

        if result:
            all_data.extend(result)

print("TOTAL ROWS:", len(all_data))

# =========================
# APPEND GOOGLE SHEET
# =========================

sh = client.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

# TÌM DÒNG CUỐI CÙNG CÓ DỮ LIỆU Ở CỘT A

last_row = len(ws.col_values(1))

# Nếu sheet hoàn toàn trống

if last_row == 0:

    ws.update(
        "A1",
        [[
            "ma_cp",
            "loai_bc",
            "code",
            "tai_khoan",
            "nam",
            "gia_tri"
        ]]
    )

    last_row = 1

# GHI TIẾP DỮ LIỆU

ws.update(
    f"A{last_row + 1}",
    all_data
)

print(f"APPEND DONE: {len(all_data):,} rows")
