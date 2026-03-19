import requests
import gspread
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# ===== CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

# ===== CONFIG =====
SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "Price"

# ===== FULL SYMBOL LIST 400 mã =====
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
YBM YEG""".split()

# ===== SESSION =====
session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# ===== FETCH 1 MÃ =====
def get_price(symbol):
    url = f"https://api-finfo.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{symbol}&size=120&page=1"

    try:
        res = session.get(url, headers=headers, timeout=10)

        if res.status_code != 200:
            print(symbol, "STATUS:", res.status_code)
            return None

        if not res.text.strip():
            print(symbol, "EMPTY")
            return None

        try:
            json_data = res.json()
        except:
            print(symbol, "NOT JSON:", res.text[:100])
            return None

        data = json_data.get("data", [])

        if not data:
            return None

        print(symbol, "rows:", len(data))

        return [[symbol, row["date"], row["adClose"]] for row in data]

    except Exception as e:
        print(symbol, "ERROR:", e)
        return None

# ===== MAIN LOOP SONG SONG + RETRY =====
all_data = []
remaining = set(symbols)
round_num = 0

while remaining:
    round_num += 1
    print(f"\n🔁 ROUND {round_num} | còn {len(remaining)} mã")

    success = set()
    temp_data = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(get_price, s): s for s in remaining}

        for future in as_completed(futures):
            symbol = futures[future]
            result = future.result()

            if result:
                temp_data.extend(result)
                success.add(symbol)

    # update remaining mã fail
    remaining = remaining - success
    all_data.extend(temp_data)

    print(f"✅ LẤY ĐƯỢC: {len(success)} | ❌ CÒN: {len(remaining)}")

    if remaining:
        print("⏳ Chờ 2s trước khi retry các mã fail...")
        time.sleep(2)  # tránh spam API quá nhiều

print("\n🎯 DONE FULL 100% 400 mã")

# ===== PUSH LÊN SHEET =====
sh = client.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)

ws.batch_clear(["A:C"])

if all_data:
    ws.update("A1", [["symbol", "date", "close"]] + all_data)
    print("WRITE OK")
else:
    ws.update("A1", [["NO DATA"]])
    print("NO DATA")

print("DONE 🚀")
