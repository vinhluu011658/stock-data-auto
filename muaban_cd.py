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
    # ========================================================
    # DÁN TOÀN BỘ DANH SÁCH ~400 MÃ CỦA BẠN VÀO ĐÂY
    # ========================================================

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

        # ----------------------------------------------------
        # Tương đương Power Query:
        #
        # Record.ToTable(Source)
        # Value = #"Converted to Table"{2}[Value]
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
# 5. TÍNH TOÁN CHO TỪNG MÃ
# ============================================================

def calculate_symbol(symbol, data):

    # ========================================================
    # GIÁ TRỊ MUA / BÁN THEO 4 VÙNG
    # ========================================================

    v1_buy = 0.0
    v1_sell = 0.0

    v2_buy = 0.0
    v2_sell = 0.0

    v3_buy = 0.0
    v3_sell = 0.0

    v4_buy = 0.0
    v4_sell = 0.0


    # ========================================================
    # KHỐI LƯỢNG MUA / BÁN
    # ========================================================

    khoi_luong_mua = 0.0
    khoi_luong_ban = 0.0


    # ========================================================
    # DUYỆT TOÀN BỘ GIAO DỊCH
    # ========================================================

    for row in data:

        try:

            # ------------------------------------------------
            # API 24HMoney đang trả giá bị chia 1.000
            #
            # Ví dụ:
            # Giá thực tế = 65.000
            # API trả       = 65
            #
            # Vì vậy nhân lại 1.000
            # ------------------------------------------------

            price = float(
                row.get("price", 0) or 0
            ) * 1000


            # ------------------------------------------------
            # Khối lượng khớp
            # ------------------------------------------------

            match_qtty = float(
                row.get("match_qtty", 0) or 0
            )


            # ------------------------------------------------
            # Loại giao dịch
            #
            # bu = mua chủ động
            # sd = bán chủ động
            # ------------------------------------------------

            side = str(
                row.get("side", "")
            ).lower().strip()


            # ------------------------------------------------
            # Giá trị lệnh
            # ------------------------------------------------

            gia_tri_lenh = (
                price * match_qtty
            )


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
            # PHÂN LOẠI 4 VÙNG GIÁ TRỊ LỆNH
            # =================================================

            # -------------------------------------------------
            # V1: < 250 triệu
            # -------------------------------------------------

            if gia_tri_lenh < 250_000_000:

                if side == "bu":

                    v1_buy += gia_tri_lenh

                elif side == "sd":

                    v1_sell += gia_tri_lenh


            # -------------------------------------------------
            # V2: 250 triệu đến < 500 triệu
            # -------------------------------------------------

            elif gia_tri_lenh < 500_000_000:

                if side == "bu":

                    v2_buy += gia_tri_lenh

                elif side == "sd":

                    v2_sell += gia_tri_lenh


            # -------------------------------------------------
            # V3: 500 triệu đến 1 tỷ
            # -------------------------------------------------

            elif gia_tri_lenh <= 1_000_000_000:

                if side == "bu":

                    v3_buy += gia_tri_lenh

                elif side == "sd":

                    v3_sell += gia_tri_lenh


            # -------------------------------------------------
            # V4: > 1 tỷ
            # -------------------------------------------------

            else:

                if side == "bu":

                    v4_buy += gia_tri_lenh

                elif side == "sd":

                    v4_sell += gia_tri_lenh


        except Exception:

            # Nếu một dòng lỗi thì bỏ qua dòng đó
            continue


    # ========================================================
    # CHÊNH LỆCH MỖI VÙNG
    # ========================================================

    chenh_v1 = (
        v1_buy - v1_sell
    )

    chenh_v2 = (
        v2_buy - v2_sell
    )

    chenh_v3 = (
        v3_buy - v3_sell
    )

    chenh_v4 = (
        v4_buy - v4_sell
    )


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
    # TÍNH TỶ TRỌNG V1 - V4
    # ========================================================

    if chenh_tong != 0:

        v1 = (
            chenh_v1
            / chenh_tong
            * 100
        )

        v2 = (
            chenh_v2
            / chenh_tong
            * 100
        )

        v3 = (
            chenh_v3
            / chenh_tong
            * 100
        )

        v4 = (
            chenh_v4
            / chenh_tong
            * 100
        )

    else:

        v1 = 0
        v2 = 0
        v3 = 0
        v4 = 0


    # ========================================================
    # TỶ LỆ CHÊNH KHỐI LƯỢNG
    # ========================================================

    tong_khoi_luong = (
        khoi_luong_mua
        + khoi_luong_ban
    )


    if tong_khoi_luong != 0:

        ty_le_chenh_khoi_luong = (

            (
                khoi_luong_mua
                - khoi_luong_ban
            )
            / tong_khoi_luong

        ) * 100

    else:

        ty_le_chenh_khoi_luong = 0


    # ========================================================
    # KẾT QUẢ CUỐI CÙNG
    # ========================================================

    return [

        symbol,

        round(v1, 4),

        round(v2, 4),

        round(v3, 4),

        round(v4, 4),

        round(khoi_luong_mua, 2),

        round(khoi_luong_ban, 2),

        round(
            ty_le_chenh_khoi_luong,
            4
        ),

    ]


# ============================================================
# 6. LẤY DỮ LIỆU TOÀN BỘ THỊ TRƯỜNG
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
# 7. TÍNH TOÁN KẾT QUẢ CHO TOÀN BỘ MÃ
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


        results.append(
            result
        )


    except Exception as e:

        print(
            f"{symbol}: "
            f"lỗi tính toán - {e}"
        )


# ============================================================
# 8. HEADER GOOGLE SHEETS
# ============================================================

output = [

    [

        "ma_cp",

        "V1",

        "V2",

        "V3",

        "V4",

        "khoi_luong_mua",

        "khoi_luong_ban",

        "ty_le_chenh_khoi_luong",

    ]

]


# ============================================================
# 9. THÊM KẾT QUẢ
# ============================================================

output.extend(
    results
)


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
    "\n========================================"
)

print(
    "HOÀN TẤT!"
)

print(
    f"Đã ghi {len(results)} mã "
    f"vào Google Sheets."
)

print(
    f"Tổng số mã: {len(symbols)}"
)

print(
    f"Số mã thành công: {len(results)}"
)

print(
    f"Số mã lỗi: "
    f"{len(symbols) - len(results)}"
)

print(
    "========================================"
)
