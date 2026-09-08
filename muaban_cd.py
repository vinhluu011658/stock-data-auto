import os
import time
import requests
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================
# 1. GOOGLE SHEETS
# ============================================================

GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]

SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "MB_CD"

gc = gspread.service_account_from_dict(
    eval(GOOGLE_CREDENTIALS)
)

sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet(SHEET_NAME)


# ============================================================
# 2. DANH SÁCH MÃ CỔ PHIẾU
# ============================================================

symbols = [
    # GIỮ NGUYÊN TOÀN BỘ DANH SÁCH symbols CỦA BẠN Ở ĐÂY
    # Ví dụ:
    "AAA",
    "AAM",
    "AAT",
    "ACB",
    "ACG",
    "ACL",
    "ADG",
    "AGG",
    "ANV",
    "APG",
    "APH",
    "ASM",
    "BCM",
    "BIC",
    "BID",
    "BMP",
    "BSI",
    "BSR",
    "BVH",
    "CII",
    "CMG",
    "CTD",
    "CTG",
    "DBC",
    "DCM",
    "DGC",
    "DGW",
    "DIG",
    "DPM",
    "DXG",
    "EIB",
    "FPT",
    "GAS",
    "GEX",
    "GMD",
    "HAG",
    "HCM",
    "HDB",
    "HDC",
    "HDG",
    "HPG",
    "HSG",
    "HT1",
    "HVN",
    "IDI",
    "IMP",
    "KBC",
    "KDH",
    "KHG",
    "LPB",
    "MBB",
    "MSB",
    "MSN",
    "MWG",
    "NKG",
    "NLG",
    "NTL",
    "NVL",
    "OCB",
    "PAN",
    "PC1",
    "PDR",
    "PHR",
    "PLX",
    "PNJ",
    "POW",
    "PPC",
    "PVD",
    "PVS",
    "REE",
    "SAB",
    "SBT",
    "SHB",
    "SIP",
    "SJS",
    "SSB",
    "SSI",
    "STB",
    "TCB",
    "TCH",
    "TPB",
    "VCB",
    "VCG",
    "VCI",
    "VHM",
    "VIB",
    "VIC",
    "VIX",
    "VJC",
    "VND",
    "VNM",
    "VPB",
    "VRE",
    "VSC",
    "VSH",
    "VTP",
]


# ============================================================
# 3. SESSION + HEADER
# ============================================================

session = requests.Session()

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}


# ============================================================
# 4. HÀM LẤY DỮ LIỆU GIAO DỊCH
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

        # Tương đương:
        # Record.ToTable(Source){2}[Value]
        values = list(source.values())

        if len(values) < 3:
            print(f"{symbol}: Không tìm thấy dữ liệu giao dịch")
            return None

        data = values[2]

        if not isinstance(data, list):
            print(f"{symbol}: Dữ liệu không phải dạng list")
            return None

        return symbol, data

    except Exception as e:

        print(f"{symbol}: Lỗi API - {e}")

        return None


# ============================================================
# 5. TÍNH TOÁN CHO TỪNG MÃ
# ============================================================

def calculate_symbol(symbol, data):

    # --------------------------------------------------------
    # Giá trị giao dịch theo từng vùng
    # --------------------------------------------------------

    v1_buy = 0.0
    v1_sell = 0.0

    v2_buy = 0.0
    v2_sell = 0.0

    v3_buy = 0.0
    v3_sell = 0.0

    v4_buy = 0.0
    v4_sell = 0.0

    # --------------------------------------------------------
    # Khối lượng mua / bán
    # --------------------------------------------------------

    khoi_luong_mua = 0.0
    khoi_luong_ban = 0.0


    # ========================================================
    # DUYỆT TOÀN BỘ GIAO DỊCH
    # ========================================================

    for row in data:

        try:

            price = float(row.get("price", 0) or 0)

            match_qtty = float(
                row.get("match_qtty", 0) or 0
            )

            side = str(
                row.get("side", "")
            ).lower().strip()

            # Giá trị lệnh
            gia_tri_lenh = price * match_qtty

            if gia_tri_lenh <= 0:
                continue


            # =================================================
            # KHỐI LƯỢNG MUA / BÁN
            # =================================================

            if side == "bu":

                khoi_luong_mua += match_qtty

            elif side == "sd":

                khoi_luong_ban += match_qtty


            # =================================================
            # PHÂN VÙNG GIÁ TRỊ LỆNH
            # =================================================

            if gia_tri_lenh < 250_000_000:

                if side == "bu":
                    v1_buy += gia_tri_lenh

                elif side == "sd":
                    v1_sell += gia_tri_lenh


            elif gia_tri_lenh < 500_000_000:

                if side == "bu":
                    v2_buy += gia_tri_lenh

                elif side == "sd":
                    v2_sell += gia_tri_lenh


            elif gia_tri_lenh <= 1_000_000_000:

                if side == "bu":
                    v3_buy += gia_tri_lenh

                elif side == "sd":
                    v3_sell += gia_tri_lenh


            else:

                if side == "bu":
                    v4_buy += gia_tri_lenh

                elif side == "sd":
                    v4_sell += gia_tri_lenh


        except Exception:

            continue


    # ========================================================
    # CHÊNH LỆCH TỪNG VÙNG
    # ========================================================

    chenh_v1 = v1_buy - v1_sell
    chenh_v2 = v2_buy - v2_sell
    chenh_v3 = v3_buy - v3_sell
    chenh_v4 = v4_buy - v4_sell


    # ========================================================
    # TỔNG CHÊNH LỆCH
    # ========================================================

    chenh_tong = (
        chenh_v1
        + chenh_v2
        + chenh_v3
        + chenh_v4
    )


    # ========================================================
    # TỶ TRỌNG TỪNG VÙNG
    # ========================================================

    if chenh_tong != 0:

        v1_percent = (
            chenh_v1 / chenh_tong
        ) * 100

        v2_percent = (
            chenh_v2 / chenh_tong
        ) * 100

        v3_percent = (
            chenh_v3 / chenh_tong
        ) * 100

        v4_percent = (
            chenh_v4 / chenh_tong
        ) * 100

    else:

        v1_percent = 0
        v2_percent = 0
        v3_percent = 0
        v4_percent = 0


    # ========================================================
    # TỶ LỆ CHÊNH KHỐI LƯỢNG
    # ========================================================

    tong_khoi_luong = (
        khoi_luong_mua
        + khoi_luong_ban
    )

    if tong_khoi_luong != 0:

        ty_le_chenh_khoi_luong = (
            (khoi_luong_mua - khoi_luong_ban)
            / tong_khoi_luong
        ) * 100

    else:

        ty_le_chenh_khoi_luong = 0


    # ========================================================
    # KẾT QUẢ CUỐI CÙNG
    # ========================================================

    return [
        symbol,
        round(v1_percent, 4),
        round(v2_percent, 4),
        round(v3_percent, 4),
        round(v4_percent, 4),
        round(khoi_luong_mua, 2),
        round(khoi_luong_ban, 2),
        round(ty_le_chenh_khoi_luong, 4),
    ]


# ============================================================
# 6. LẤY DỮ LIỆU CHO TOÀN BỘ THỊ TRƯỜNG
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

    with ThreadPoolExecutor(max_workers=20) as executor:

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

                    all_data[symbol] = result[1]

                    temp_data.append(symbol)

                    print(
                        f"{symbol}: OK "
                        f"({len(result[1])} giao dịch)"
                    )

            except Exception as e:

                print(
                    f"{symbol}: lỗi xử lý - {e}"
                )


    # --------------------------------------------------------
    # Các mã chưa thành công
    # --------------------------------------------------------

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
# 7. TÍNH TOÁN KẾT QUẢ
# ============================================================

results = []


for symbol in symbols:

    if symbol not in all_data:

        print(
            f"{symbol}: không có dữ liệu"
        )

        continue

    try:

        result = calculate_symbol(
            symbol,
            all_data[symbol]
        )

        results.append(result)

    except Exception as e:

        print(
            f"{symbol}: lỗi tính toán - {e}"
        )


# ============================================================
# 8. HEADER GOOGLE SHEETS
# ============================================================

output = [

    [
        "ma_cp",
        "V1_%",
        "V2_%",
        "V3_%",
        "V4_%",
        "khoi_luong_mua",
        "khoi_luong_ban",
        "ty_le_chenh_khoi_luong",
    ]

]


# ============================================================
# 9. THÊM KẾT QUẢ
# ============================================================

output.extend(results)


# ============================================================
# 10. XÓA DỮ LIỆU CŨ
# ============================================================

ws.clear()


# ============================================================
# 11. GHI DỮ LIỆU MỚI
# ============================================================

ws.update(
    "A1",
    output,
    value_input_option="USER_ENTERED"
)


# ============================================================
# 12. HOÀN TẤT
# ============================================================

print(
    f"\nHoàn tất! "
    f"Đã ghi {len(results)} mã vào Google Sheets."
)

print(
    f"Tổng số mã: {len(symbols)}"
)

print(
    f"Số mã thành công: {len(results)}"
)

print(
    f"Số mã lỗi: {len(symbols) - len(results)}"
)
