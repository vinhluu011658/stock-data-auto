import requests
import pandas as pd
import gspread
import json
import os
import time
from oauth2client.service_account import ServiceAccountCredentials

# ===== LOAD GOOGLE CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# ===== OPEN SHEET =====
sheet = client.open("DATA_STOCK").worksheet("BCTC_kqkd")

# ===== SYMBOL LIST =====
#symbols = ["BSR", "HPG", "VNM"]
# ===== SYMBOL LIST =====
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

# ===== API URL =====
BASE_URL = "https://api-finance-t19.24hmoney.vn/v1/ios/company/financial-report"

headers = {
    "User-Agent": "Mozilla/5.0"
}

data_all = []

for symbol in symbols:

    print(f"Fetching {symbol}...")

    params = {
        "symbol": symbol,
        "period": 1,
        "view": 2,
        "page": 1,
        "expanded": "true"
    }

    try:
        response = requests.get(
            BASE_URL,
            params=params,
            headers=headers,
            timeout=30
        )

        json_data = response.json()

        headers_data = json_data["data"]["headers"]
        rows = json_data["data"]["rows"]

        # ===== CHỈ LẤY CỘT NORMAL =====
        normal_columns = []
        normal_indexes = []

        for idx, h in enumerate(headers_data):

            if h["type"] == "normal":

                year = h["year"]
                quarter = h["quarter"]

                # nếu có quarter
                if quarter != 0:
                    col_name = f"Q{quarter}_{year}"
                else:
                    col_name = str(year)

                normal_columns.append(col_name)
                normal_indexes.append(idx)

        # ===== PARSE DATA =====
        for row in rows:

            item = {
                "symbol": symbol,
                "name": row.get("name")
            }

            values = row.get("values", [])

            for col_idx, value_idx in enumerate(normal_indexes):

                if value_idx < len(values):
                    item[normal_columns[col_idx]] = values[value_idx]

            data_all.append(item)

        time.sleep(1)

    except Exception as e:
        print(f"Error {symbol}: {e}")

# ===== DATAFRAME =====
df = pd.DataFrame(data_all)

# ===== REPLACE NaN =====
df = df.fillna("")

# ===== CLEAR SHEET =====
sheet.clear()

# ===== UPDATE SHEET =====
sheet.update(
    [df.columns.values.tolist()] + df.values.tolist()
)

print("DONE!")
