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
