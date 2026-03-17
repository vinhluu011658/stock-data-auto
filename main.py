import requests
import pandas as pd
import gspread
import json
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
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
sheet = client.open("DATA_STOCK").sheet1

# ===== SYMBOL LIST =====
symbols = """AAA AAM AAT ABR ABS ABT ACB ACC ACG ACL ADG ADP ADS AFX AGG AGR ANT ANV APG APH ASG ASM ASP AST
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
YBM YEG""".split()  # test ít trước

data_all = []

# ===== HÀM TRỪ =====
def safe_calc(a, b):
    return (a or 0) - (b or 0)

headers = {"User-Agent": "Mozilla/5.0"}

# ===== FETCH =====
def fetch_symbol(symbol):
    url = f"https://api-finance-t19.24hmoney.vn/v1/ios/stock/statistic-investor-history?symbol={symbol}"
    result = []

    try:
        res = requests.get(url, headers=headers, timeout=10)
        data = res.json()

        for item in data.get("data", []):
            timestamp = item.get("trading_date") or item.get("date")

            if not timestamp:
                continue

            dt = datetime.utcfromtimestamp(
                int(timestamp) / 1000 if int(timestamp) > 1e12 else int(timestamp)
            ) + timedelta(hours=7)

            # 🔥 convert sang string NGAY TỪ ĐÂY
            date_str = dt.strftime("%d/%m/%Y")

            result.append({
                "Date": date_str,
                "Ma CP": symbol,
                "nuoc ngoai": safe_calc(item.get("foreign_buy"), item.get("foreign_sell")),
                "tu doanh": safe_calc(item.get("proprietary_buy"), item.get("proprietary_sell")),
                "to chuc trong nuoc": safe_calc(item.get("local_institutional_buy"), item.get("local_institutional_sell")),
                "ca nhan trong nuoc": safe_calc(item.get("local_individual_buy"), item.get("local_individual_sell")),
                "to chuc nuoc ngoai": safe_calc(item.get("foreign_institutional_buy"), item.get("foreign_institutional_sell")),
                "ca nhan nuoc ngoai": safe_calc(item.get("foreign_individual_buy"), item.get("foreign_individual_sell"))
            })

        time.sleep(0.1)

    except Exception as e:
        print(f"Lỗi {symbol}: {e}")

    return result

# ===== MULTI THREAD =====
with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(fetch_symbol, symbols)

for r in results:
    data_all.extend(r)

# ===== DATAFRAME =====
df = pd.DataFrame(data_all)

if df.empty:
    print("Không có dữ liệu")
    exit()

# ===== SORT =====
df = df.sort_values(by=["Ma CP", "Date"])

# ===== PUSH =====
sheet.clear()
sheet.update(
    [df.columns.tolist()] + df.values.tolist(),
    value_input_option="USER_ENTERED"
)

print("DONE")
