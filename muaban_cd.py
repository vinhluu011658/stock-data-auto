import os
import time
import json
import requests
import gspread

from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# 1. GOOGLE SHEETS
# ============================================================

GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]

SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "MB_CD"

gc = gspread.service_account_from_dict(
    json.loads(GOOGLE_CREDENTIALS)
)

sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)


# ============================================================
# 2. NGÀY GIAO DỊCH
# ============================================================

TRADING_HISTORY_URL = (
    "https://api-finance-t19.24hmoney.vn/"
    "v2/ios/stock/trading-history"
)


def get_trading_date(symbol):
    """
    Lấy trading_date mới nhất theo từng mã cổ phiếu
    từ API 24HMoney và chuyển sang yyyy-mm-dd.
    """

    try:

        url = (
            f"{TRADING_HISTORY_URL}"
            f"?symbol={symbol}&floor_code=10"
        )

        response = session.get(
            url,
            headers=headers,
            timeout=30
        )

        response.raise_for_status()

        source = response.json()

        # --------------------------------------------------------
        # Tìm tất cả trading_date trong JSON
        # --------------------------------------------------------

        trading_dates = []

        def collect_trading_dates(obj):

            if isinstance(obj, dict):

                if "trading_date" in obj:

                    value = obj.get("trading_date")

                    try:

                        trading_dates.append(
                            float(value)
                        )

                    except (TypeError, ValueError):

                        pass

                for value in obj.values():

                    collect_trading_dates(value)

            elif isinstance(obj, list):

                for item in obj:

                    collect_trading_dates(item)

        collect_trading_dates(source)

        if not trading_dates:

            print(
                f"{symbol}: Không tìm thấy trading_date"
            )

            return None

        # --------------------------------------------------------
        # Lấy trading_date mới nhất
        # --------------------------------------------------------

        latest_timestamp = max(trading_dates)

        # --------------------------------------------------------
        # Unix timestamp -> yyyy-mm-dd
        # API dùng UTC
        # --------------------------------------------------------

        vn_timezone = timezone(
            timedelta(hours=7)
        )

        ngay_gd = datetime.fromtimestamp(
            latest_timestamp,
            tz=timezone.utc
        ).astimezone(
            vn_timezone
        ).strftime("%Y-%m-%d")

        return ngay_gd

    except Exception as e:

        print(
            f"{symbol}: Lỗi lấy trading_date - {e}"
        )

        return None


# ============================================================
# 3. DANH SÁCH MÃ CỔ PHIẾU
# ============================================================

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
YBM YEG
""".split()


# ============================================================
# 4. SESSION + HEADER
# ============================================================

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}


# ============================================================
# 5. HÀM LẤY DỮ LIỆU GIAO DỊCH
# ============================================================

def get_transaction(symbol):

    url = (
        "https://api-finance-t19.24hmoney.vn/"
        "v1/web/stock/transaction-list-ssi"
        f"?&&symbol={symbol}&page=1&per_page=10000"
    )

    try:

        response = session.get(
            url,
            headers=headers,
            timeout=30
        )

        response.raise_for_status()

        source = response.json()

        # ----------------------------------------------------
        # Giữ nguyên concept từ Power Query
        # Record.ToTable(Source)
        # #"Converted to Table"{2}[Value]
        # ----------------------------------------------------

        values = list(source.values())

        if len(values) < 3:

            print(
                f"{symbol}: Không tìm thấy dữ liệu giao dịch"
            )

            return None

        data = values[2]

        if not isinstance(data, list):

            print(
                f"{symbol}: Dữ liệu không phải dạng list"
            )

            return None

        return symbol, data

    except Exception as e:

        print(
            f"{symbol}: Lỗi API - {e}"
        )

        return None


# ============================================================
# 6. TÍNH TOÁN THEO TỪNG MỨC GIÁ
# ============================================================

def calculate_symbol(symbol, data):

    # ========================================================
    # GOM DỮ LIỆU THEO GIÁ
    #
    # Mỗi giá chỉ xuất hiện 1 lần
    #
    # price_data[price] =
    # {
    #     "gia_tri_mua": ...,
    #     "gia_tri_ban": ...
    # }
    # ========================================================

    price_data = {}

    # --------------------------------------------------------
    # DUYỆT TOÀN BỘ GIAO DỊCH
    # --------------------------------------------------------

    for row in data:

        try:

            # ------------------------------------------------
            # GIÁ
            #
            # API price bị chia 1.000
            # ------------------------------------------------

            price = float(
                row.get("price", 0) or 0
            ) * 1000

            # ------------------------------------------------
            # KHỐI LƯỢNG KHỚP
            # ------------------------------------------------

            match_qtty = float(
                row.get("match_qtty", 0) or 0
            )

            if price <= 0:
                continue

            if match_qtty <= 0:
                continue

            # ------------------------------------------------
            # SIDE
            #
            # bu = mua chủ động
            # sd = bán chủ động
            # ------------------------------------------------

            side = str(
                row.get("side", "")
            ).lower().strip()

            if side not in ("bu", "sd"):
                continue

            # ------------------------------------------------
            # GIÁ TRỊ KHỚP LỆNH
            #
            # Giá × Khối lượng
            # ------------------------------------------------

            gia_tri_lenh = (
                price * match_qtty
            )

            if gia_tri_lenh <= 0:
                continue

            # ------------------------------------------------
            # NẾU GIÁ CHƯA CÓ -> TẠO MỚI
            # ------------------------------------------------

            if price not in price_data:

                price_data[price] = {

                    "gia_tri_mua": 0.0,

                    "gia_tri_ban": 0.0

                }

            # ------------------------------------------------
            # CỘNG DỒN THEO GIÁ
            # ------------------------------------------------

            if side == "bu":

                price_data[price][
                    "gia_tri_mua"
                ] += gia_tri_lenh

            elif side == "sd":

                price_data[price][
                    "gia_tri_ban"
                ] += gia_tri_lenh

        except Exception:

            # Giao dịch lỗi thì bỏ qua
            continue


    # ========================================================
    # TẠO KẾT QUẢ
    # ========================================================

    results = []

    for price, values in price_data.items():

        gia_tri_mua = values[
            "gia_tri_mua"
        ]

        gia_tri_ban = values[
            "gia_tri_ban"
        ]

        # ----------------------------------------------------
        # GIÁ TRỊ CHỦ ĐỘNG
        #
        # Mua chủ động - Bán chủ động
        # ----------------------------------------------------

        gia_tri_chu_dong = (
            gia_tri_mua
            - gia_tri_ban
        )

        results.append([

            symbol,

            round(price, 2),

            round(
                gia_tri_mua,
                2
            ),

            round(
                gia_tri_ban,
                2
            ),

            round(
                gia_tri_chu_dong,
                2
            )

        ])

    # --------------------------------------------------------
    # SẮP XẾP THEO GIÁ TĂNG DẦN
    # --------------------------------------------------------

    results.sort(
        key=lambda x: x[1]
    )

    return results


# ============================================================
# 7. LẤY DATA TOÀN BỘ THỊ TRƯỜNG
# ============================================================

all_data = {}

remaining = list(symbols)

round_num = 1


while remaining:

    print(
        f"\n===== LẦN CHẠY {round_num} "
        f"===== {len(remaining)} mã ====="
    )

    temp_data = []

    with ThreadPoolExecutor(
        max_workers=20
    ) as executor:

        futures = {

            executor.submit(
                get_transaction,
                symbol
            ): symbol

            for symbol in remaining

        }

        for future in as_completed(futures):

            symbol = futures[future]

            try:

                result = future.result()

                if result is not None:

                    all_data[
                        symbol
                    ] = result[1]

                    temp_data.append(
                        symbol
                    )

                    print(
                        f"{symbol}: OK "
                        f"({len(result[1])} giao dịch)"
                    )

            except Exception as e:

                print(
                    f"{symbol}: "
                    f"lỗi xử lý - {e}"
                )


    # ========================================================
    # CẬP NHẬT DANH SÁCH MÃ CHƯA THÀNH CÔNG
    # ========================================================

    remaining = [

        symbol

        for symbol in remaining

        if symbol not in temp_data

    ]

    print(
        f"Thành công: {len(temp_data)}"
    )

    print(
        f"Còn lại: {len(remaining)}"
    )


    # ========================================================
    # RETRY
    # ========================================================

    if remaining:

        round_num += 1

        if round_num <= 3:

            print(
                "Chờ 3 giây trước khi thử lại..."
            )

            time.sleep(3)

        else:

            print(
                "Đã thử lại tối đa. "
                "Dừng các mã còn lỗi."
            )

            break


# ============================================================
# 8. TÍNH TOÁN KẾT QUẢ
# ============================================================

results = []


for symbol in symbols:

    if symbol not in all_data:

        print(
            f"{symbol}: không có dữ liệu"
        )

        continue

    try:

        symbol_results = calculate_symbol(

            symbol,

            all_data[symbol]

        )

        results.extend(
            symbol_results
        )

    except Exception as e:

        print(
            f"{symbol}: "
            f"lỗi tính toán - {e}"
        )


# ============================================================
# 9. HEADER GOOGLE SHEETS
# ============================================================

output = [

    [

        "ngay_gd",

        "ma_cp",

        "gia_khop_lenh",

        "gia_tri_mua",

        "gia_tri_ban",

        "gia_tri_chu_dong"

    ]

]


# ============================================================
# 10. CACHE NGÀY GIAO DỊCH
# ============================================================

trading_dates = {}


# ============================================================
# 11. THÊM KẾT QUẢ VÀO OUTPUT
# ============================================================

for result in results:

    symbol = result[0]

    # --------------------------------------------------------
    # Chỉ gọi API trading-date 1 lần / mã
    # --------------------------------------------------------

    if symbol not in trading_dates:

        trading_dates[symbol] = (
            get_trading_date(symbol)
        )

    ngay_gd = trading_dates[symbol]

    if ngay_gd is None:

        print(
            f"{symbol}: "
            f"Không lấy được ngày giao dịch, bỏ qua"
        )

        continue

    output.append([

        ngay_gd,

        *result

    ])


# ============================================================
# 12. XÓA DATA CŨ
# ============================================================

ws.clear()


# ============================================================
# 13. GHI DATA MỚI
# ============================================================

ws.update(
    "A1",
    output,
    value_input_option="USER_ENTERED"
)


# ============================================================
# 14. HOÀN TẤT
# ============================================================

print(
    "\n========================================"
)

print(
    "HOÀN TẤT!"
)

print(
    "Concept: GOM GIAO DỊCH THEO GIÁ"
)

print(
    "Mua chủ động = bu"
)

print(
    "Bán chủ động = sd"
)

print(
    "Giá trị chủ động = "
    "Giá trị mua - Giá trị bán"
)

print(
    f"Đã ghi {len(results)} dòng giá "
    "vào Google Sheets."
)

print(
    f"Tổng số mã: {len(symbols)}"
)

print(
    f"Số mã thành công: {len(all_data)}"
)

print(
    f"Số mã lỗi: "
    f"{len(symbols) - len(all_data)}"
)

print(
    "========================================"
)
