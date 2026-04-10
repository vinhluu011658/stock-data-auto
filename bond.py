import requests
from bs4 import BeautifulSoup
import pandas as pd

import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials

import urllib3

# 🔥 Tắt cảnh báo SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ================= HNX SCRAPER =================
def scrape_hnx_bonds():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-phat-hanh/tim-kiem"
    home = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-phat-hanh"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": home
    }

    session = requests.Session()

    # 🔥 BẮT BUỘC: lấy cookie trước
    print("🔄 Init session...")
    session.get(home, headers=headers, verify=False)

    all_data = []

    for page in range(1, 30):
        payload = {
            "searchKeys[]": "",
            "arrCurrentPage[]": str(page),
            "arrNumberRecord[]": "50"
        }

        try:
            res = session.post(
                url,
                data=payload,
                headers=headers,
                verify=False,
                timeout=30
            )
        except Exception as e:
            print(f"❌ Lỗi page {page}: {e}")
            break

        # DEBUG
        print(f"Page {page} - response length:", len(res.text))

        soup = BeautifulSoup(res.text, "html.parser")
        rows = soup.select("#tbReleaseResult tbody tr")

        if not rows:
            print(f"👉 Hết dữ liệu tại page {page}")
            break

        for row in rows:
            cols = [td.get_text(strip=True) for td in row.find_all("td")]

            if len(cols) < 17:
                continue

            all_data.append([
                cols[1],  # Ngày đăng tin
                cols[2],  # Tên DN
                cols[3],  # Mã TP
                cols[9].replace(",", ""),   # Khối lượng
                cols[10].replace(",", ""),  # Mệnh giá
                cols[16]  # Lãi suất (%)
            ])

        print(f"✅ Page {page}: {len(rows)} records")

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
        print("❌ Không có dữ liệu HNX")
        return

    df = df.fillna("")

    sheet.update(
        "G1",
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ Đã ghi dữ liệu vào cột G")


# ================= MAIN =================
def main():
    print("===== HNX BONDS =====")

    print("🔄 Đang lấy dữ liệu...")
    df = scrape_hnx_bonds()

    print(df.head())

    print("🔄 Kết nối Google Sheet...")
    sheet = connect_gsheet()

    print("🔄 Ghi dữ liệu...")
    update_sheet(sheet, df)

    print("🚀 DONE")


if __name__ == "__main__":
    main()
