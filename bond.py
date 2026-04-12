import requests
import pandas as pd
import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials


# ================= FETCH HNX (API) =================
def fetch_hnx_bonds():
    url = "https://cbonds.hnx.vn/Handler/SearchHandler.ashx"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest"
    }

    all_data = []
    page = 1

    while True:
        print(f"🔄 Page {page}")

        payload = []

        # 👉 page hiện tại (QUAN TRỌNG)
        payload.append(("arrCurrentPage[]", str(page)))

        # 👉 fill giống request thật
        for _ in range(11):
            payload.append(("arrCurrentPage[]", "1"))

        for _ in range(12):
            payload.append(("arrNumberRecord[]", "10"))

        try:
            response = requests.post(url, data=payload, headers=headers, timeout=30)

            if response.status_code != 200:
                print("❌ Request lỗi:", response.status_code)
                break

            data = response.json()
            rows = data.get("d", [])

            if not rows:
                print("👉 Hết data")
                break

            for r in rows:
                all_data.append([
                    r.get("IssueDate", ""),
                    r.get("IssuerName", ""),
                    r.get("BondCode", ""),
                    str(r.get("IssueVolume", "")).replace(",", ""),
                    str(r.get("ParValue", "")).replace(",", ""),
                    r.get("InterestRate", "")
                ])

            print(f"✅ Lấy {len(rows)} dòng")

            page += 1

        except Exception as e:
            print("❌ Lỗi:", e)
            break

    df = pd.DataFrame(all_data, columns=[
        "Ngày đăng tin",
        "Tên DN",
        "Mã TP",
        "Khối lượng",
        "Mệnh giá",
        "Lãi suất (%)"
    ])

    return df


# ================= GOOGLE SHEET =================
def connect_gsheet():
    creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(
        "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
    ).worksheet("Laisuat")

    return sheet


# ================= UPDATE =================
def update_sheet(sheet, df):
    if df.empty:
        print("❌ Không có dữ liệu")
        return

    df = df.fillna("")

    # 👉 clear vùng cũ (tránh data dư)
    sheet.batch_clear(["G1:Z10000"])

    sheet.update(
        "G1",
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ Đã ghi Google Sheet")


# ================= MAIN =================
def main():
    print("===== HNX BONDS (API MODE) =====")

    df = fetch_hnx_bonds()

    print("📊 Tổng dòng:", len(df))
    print(df.head())

    sheet = connect_gsheet()
    update_sheet(sheet, df)

    print("🚀 DONE")


if __name__ == "__main__":
    main()
