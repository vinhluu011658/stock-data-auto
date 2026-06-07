import requests
import pandas as pd
import gspread
import json
import os
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from oauth2client.service_account import ServiceAccountCredentials
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================================================
# GOOGLE AUTH
# =========================================================

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(
    creds_dict,
    scope
)

client = gspread.authorize(creds)

# =========================================================
# OPEN SHEET
# =========================================================

sheet = client.open("DATA_STOCK").worksheet("BCTC")

# =========================================================
# SYMBOL LIST
# =========================================================

symbols = """
AAA AAM AAT ABR ABS ABT ACB ACC ACG ACL ADG ADP ADS AFX AGG AGR ANT ANV APG APH ASG ASM ASP AST
BAF BCE BCG BCM BFC BHN BIC BID BKG BMC BMI BMP BRC BSI BSR BTP BTT BVH BWE
C32 C47 CCC CCI CCL CDC CHP CIG CII CKG CLC CLL CLW CMG CMV CMX CNG COM CRC CRE CRV CSM CSV CTD CTF CTG CTI CTR CTS CVT
D2D DAH DAT DBC DBD DBT DC4 DCL DCM DGC DGW DHA DHC DHG DHM DIG DLG DMC DPG DPM DPR DQC DRC DRH DRL DSC DSE DSN DTA DTL DTT DVP DXG DXS DXV
EIB ELC EVE EVF EVG
FCM FCN FDC FIR FIT FMC FPT FRT FTS
GAS GDT GEE GEG GEL GEX GHC GIL GMD GMH GSP GTA GVR
HAG HAH HAP HAR HAS HAX HCD HCM HDB HDC HDG HHP HHS HHV HID HII HMC HNA HPA HPG HPX HQC HRC HSG HSL HT1 HTG HTI HTL HTN HTV HU1 HUB HVH HVN
ICT IDI IJC ILB IMP ITC ITD
JVC
KBC KDC KDH KHG KHP KLB KMR KOS KSB
L10 LAF LBM LCG LDG LGC LGL LHG LIX LM8 LPB LSS
MBB MCH MCM MCP MDG MHC MIG MSB MSH MSN MWG
NAB NAF NAV NBB NCT NHA NHH NHT NKG NLG NNC NO1 NSC NT2 NTC NTL NVL NVT
OCB OGC OPC ORS
PAC PAN PC1 PDN PDR PDV PET PGC PGD PGI PGV PHC PHR PIT PJT PLP PLX PMG PNC PNJ POW PPC PTB PTC PTL PVD PVP PVT
QCG QNP
RAL REE RYG
S4A SAB SAM SAV SBA SBG SBT SBV SC5 SCR SCS SFC SFG SFI SGN SGR SGT SHA SHB SHI SHP SIP SJD SJS SKG SMA SMB SMC SPM SRC SRF SSB SSC SSI ST8 STB STG STK SVC SVD SVT SZC SZL
TAL TBC TCB TCD TCH TCI TCL TCM TCO TCR TCT TCX TDC TDG TDH TDM TDP TDW TEG THG TIP TIX TLD TLG TLH TMP TMS TMT TN1 TNC TNH TNI TNT TPB TPC TRA TRC TSA TSC TTA TTE TTF TV2 TVB TVS TVT TYA
UIC
VAB VAF VCA VCB VCF VCG VCI VCK VDP VDS VFG VGC VHC VHM VIB VIC VID VIP VIX VJC VMD VND VNE VNG VNL VNM VNS VOS VPB VPD VPG VPH VPI VPL VPS VPX VRC VRE VSC VSH VSI VTB VTO VTP VVS
YBM YEG
""".split()

# =========================================================
# REPORT TYPE
# =========================================================

view_name_map = {
    1: "CDKT",
    2: "KQKD",
    3: "LCTT"
}

# =========================================================
# API
# =========================================================

BASE_URL = "https://api-finance-t19.24hmoney.vn/v1/ios/company/financial-report"

headers = {
    "User-Agent": "Mozilla/5.0"
}

# =========================================================
# SESSION + RETRY
# =========================================================

session = requests.Session()

retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504]
)

adapter = HTTPAdapter(
    max_retries=retry_strategy,
    pool_connections=100,
    pool_maxsize=100
)

session.mount("https://", adapter)
session.mount("http://", adapter)

# =========================================================
# FETCH FUNCTION
# =========================================================

def fetch_symbol_view(symbol, view):

    try:

        params = {
            "symbol": symbol,
            "period": 1,
            "view": view,
            "page": 1,
            "expanded": "true"
        }

        response = session.get(
            BASE_URL,
            params=params,
            headers=headers,
            timeout=20
        )

        if response.status_code != 200:
            print(f"FAILED {symbol} VIEW {view}")
            return []

        json_data = response.json()

        if "data" not in json_data:
            return []

        headers_data = json_data["data"]["headers"]
        rows = json_data["data"]["rows"]

        # =================================================
        # GET PERIOD COLUMNS
        # =================================================

        period_columns = []

        for idx, h in enumerate(headers_data):

            if h.get("type") == "normal":

                year = h.get("year")
                quarter = h.get("quarter")

                if quarter != 0:
                    period_name = f"Q{quarter}_{year}"
                else:
                    period_name = int(year)

                period_columns.append({
                    "index": idx,
                    "period": period_name
                })

        # =================================================
        # PARSE LONG FORMAT
        # =================================================

        result = []

        for row in rows:

            values = row.get("values", [])

            for col in period_columns:

                value_idx = col["index"]

                if value_idx < len(values):

                    value = values[value_idx]

                    result.append({
                        "ma_cp": symbol,
                        "loai_bc": view_name_map[view],
                        "tai_khoan": row.get("name"),
                        "nam": col["period"],
                        "gia_tri": value
                    })

        print(f"DONE {symbol} VIEW {view}")

        return result

    except Exception as e:

        print(f"ERROR {symbol} VIEW {view}: {e}")

        return []

# =========================================================
# MAIN
# =========================================================

start_time = time.time()

data_all = []

MAX_WORKERS = 30

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

    futures = []

    for symbol in symbols:

        for view in [1, 2, 3]:

            futures.append(
                executor.submit(fetch_symbol_view, symbol, view)
            )

    for future in as_completed(futures):

        result = future.result()

        if result:
            data_all.extend(result)

# =========================================================
# DATAFRAME
# =========================================================

df = pd.DataFrame(data_all)

# =========================================================
# CLEAN DATA
# =========================================================

df = df.fillna("")

# =========================================================
# REMOVE DUPLICATES
# ƯU TIÊN GIỮ DÒNG CÓ GIÁ TRỊ
# =========================================================

df["is_empty"] = (
    df["gia_tri"].astype(str).str.strip() == ""
)

df = (
    df.sort_values(
        by=["is_empty"]
    )
    .drop_duplicates(
        subset=[
            "ma_cp",
            "loai_bc",
            "tai_khoan",
            "nam"
        ],
        keep="first"
    )
    .drop(columns=["is_empty"])
)

# =========================================================
# SORT
# =========================================================

df = df.sort_values(
    by=[
        "ma_cp",
        "loai_bc",
        "tai_khoan",
        "nam"
    ]
).reset_index(drop=True)

# =========================================================
# UPDATE SHEET
# =========================================================

print("CLEAR SHEET...")

sheet.batch_clear(["A:E"])

print("UPLOAD DATA...")

all_values = [
    df.columns.values.tolist()
] + df.values.tolist()

BATCH_SIZE = 5000

for start_row in range(
    0,
    len(all_values),
    BATCH_SIZE
):

    end_row = start_row + BATCH_SIZE

    batch = all_values[start_row:end_row]

    sheet.update(
        f"A{start_row + 1}",
        batch,
        value_input_option="RAW"
    )

    print(
        f"UPLOADED ROWS {start_row} -> {end_row}"
    )

    time.sleep(1)

# =========================================================
# DONE
# =========================================================

elapsed = round(
    time.time() - start_time,
    2
)

print(f"DONE IN {elapsed} SECONDS")
print(f"TOTAL ROWS: {len(df)}")
