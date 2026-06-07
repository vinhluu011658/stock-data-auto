import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== GOOGLE =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

# ===== SHEET =====
SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "Laisuat"

# ===== SYMBOL LIST =====
symbols = """
AAA AAM AAT ABR ABS ABT ACB ACC ACG ACL ADG ADP ADS
BAF BCE BCG BCM BFC BHN BIC BID
CTD CTG CTR CTS
FPT FRT
GAS GEX GMD GVR
HCM HDB HPG
MBB MSN MWG
PVD PVT POW PNJ
REE
SSI STB
TCB TPB
VCB VCI VHM VIC VNM VPB VRE
""".split()

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://cafef.vn/"
}

# ===== LẤY 1 MÃ =====
def get_financial(symbol):

    url = f"https://cafef.vn/du-lieu/Ajax/PageNew/ChiSoTaiChinh.ashx?Symbol={symbol}"

    try:
        r = session.get(url, headers=headers, timeout=20)

        if r.status_code != 200:
            print(symbol, "status", r.status_code)
            return None

        js = r.json()

        if not js.get("Success"):
            return None

        row = {
            "ma_cp": symbol
        }

        for item in js["Data"]:
            code = item["Code"]
            value = item["Value"]

            row[code] = value

        return row

    except Exception as e:
        print(symbol, e)
        return None

# ===== CHẠY SONG SONG =====

results = []

with ThreadPoolExecutor(max_workers=20) as executor:

    futures = {
        executor.submit(get_financial, s): s
        for s in symbols
    }

    for future in as_completed(futures):

        data = future.result()

        if data:
            results.append(data)

# ===== HEADER =====

headers_out = [
    "ma_cp",
    "EPScoBan",
    "EPSphaLoang",
    "P/E",
    "GiaTriSoSach",
    "Beta",
    "VonHoaThiTruong",
    "KhopLenh10Phien",
    "KlcpNY",
    "KlcpLuuHanh"
]

sheet_data = [headers_out]

for row in results:

    sheet_data.append([
        row.get("ma_cp", ""),
        row.get("EPScoBan", ""),
        row.get("EPSphaLoang", ""),
        row.get("P/E", ""),
        row.get("GiaTriSoSach", ""),
        row.get("Beta", ""),
        row.get("VonHoaThiTruong", ""),
        row.get("KhopLenh10Phien", ""),
        row.get("KlcpNY", ""),
        row.get("KlcpLuuHanh", "")
    ])

# ===== GHI SHEET =====

sh = client.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

# bắt đầu từ cột F
ws.batch_clear(["F:o"])

ws.update("F1", sheet_data)

print("DONE")
