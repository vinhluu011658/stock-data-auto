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
SHEET_NAME = "BCTC_Quy"

# =========================
# TEST SYMBOLS
# =========================

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
        "&pageSize=4"
        "&reportType=ALL"
        "&TypeTime=QUY"
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

                year = year_block["time"]

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
