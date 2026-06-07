import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== GOOGLE SHEETS =====

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

# ===== CONFIG =====

SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "BCTC"

# ===== DANH SÁCH MÃ =====

symbols = """
AAA AAM AAT ABR ABS ABT ACB ACC ACG ACL ADG ADP ADS
BAF BCE BCG BCM BFC BHN BIC BID BKG BMI BMP BSI BSR
BVH BWE
CII CMG CTD CTG CTR CTS
DBC DCM DGC DGW DIG DPM DPR DRC DXG DXS
EIB
FCN FPT FRT FTS
GAS GEX GMD GVR
HAG HAH HCM HDB HDC HDG HPG HSG
IDC
KBC KDC KDH KSB
LPB
MBB MIG MSN MWG
NAB NLG NVL
OCB ORS
PAN PC1 PDR PET PLX POW PTB PVD PVT
REE
SAB SHB SIP SSI STB SZC
TCB TCH TCM TPB
VCB VCG VCI VHC VHM VIC VIX VJC VND VNM VPB VRE
""".split()

# ===== SESSION =====

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://cafef.vn/"
}

# ===== API =====

REPORTS = {
    "KQKD": (
        "https://apiweb.cafef.vn/api/v1/BCTC/GetReportDetail"
        "?symbol={symbol}"
        "&pageIndex=1"
        "&pageSize=10"
        "&reportType=KQKD"
        "&TypeTime=NAM"
    ),

    "CDKT": (
        "https://apiweb.cafef.vn/api/v2/BCTC/GetReportCDKT"
        "?symbol={symbol}"
        "&pageIndex=1"
        "&pageSize=10"
        "&reportType=ALL"
        "&TypeTime=NAM"
    ),

    "LCTT": (
        "https://apiweb.cafef.vn/api/v1/BCTC/GetReportLCTT"
        "?symbol={symbol}"
        "&pageIndex=1"
        "&pageSize=10"
        "&reportType=ALL"
        "&TypeTime=NAM"
    )
}

# ===== LẤY BCTC 1 MÃ =====

def get_bctc(symbol):

    rows = []

    try:

        for loai_bc, url_template in REPORTS.items():

            url = url_template.format(symbol=symbol)

            r = session.get(
                url,
                headers=headers,
                timeout=30
            )

            if r.status_code != 200:
                print(symbol, loai_bc, r.status_code)
                continue

            js = r.json()

            if not js.get("isSuccess"):
                print(symbol, loai_bc, "isSuccess=False")
                continue

            value = js["value"]

            # map code -> tên tài khoản
            account_map = {
                item["code"]: item["name"]
                for item in value["templace"]
            }

            # dữ liệu theo năm
            for year_block in value["data"]:

                year = year_block["year"]

                for item in year_block["data"]:

                    rows.append([
                        symbol,
                        loai_bc,
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
        executor.submit(get_bctc, symbol): symbol
        for symbol in symbols
    }

    for future in as_completed(futures):

        rows = future.result()

        if rows:
            all_data.extend(rows)

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
