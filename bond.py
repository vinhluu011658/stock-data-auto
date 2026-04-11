import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials


# ================= SELENIUM SETUP =================
def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=options)
    return driver


# ================= SCRAPE HNX =================
def scrape_hnx_bonds():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-phat-hanh"

    driver = init_driver()
    driver.get(url)

    time.sleep(5)  # chờ load JS

    all_data = []
    page = 1

    while True:
        print(f"🔄 Page {page}")

        time.sleep(3)

        rows = driver.find_elements(By.CSS_SELECTOR, "#tbReleaseResult tbody tr")

        if not rows:
            print("❌ Không có data")
            break

        for row in rows:
            cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]

            if len(cols) < 17:
                continue

            all_data.append([
                cols[1],  # Ngày đăng
                cols[2],  # Tên DN
                cols[3],  # Mã TP
                cols[9].replace(",", ""),   # Khối lượng
                cols[10].replace(",", ""),  # Mệnh giá
                cols[16]  # Lãi suất
            ])

        print(f"✅ Lấy {len(rows)} dòng")

        # 👉 tìm nút next
        try:
            next_btn = driver.find_element(By.LINK_TEXT, str(page + 1))
            next_btn.click()
            page += 1
        except:
            print("👉 Hết trang")
            break

    driver.quit()

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

    sheet.update(
        "G1",
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ Đã ghi Google Sheet")


# ================= MAIN =================
def main():
    print("===== HNX BONDS =====")

    print("🔄 Đang scrape...")
    df = scrape_hnx_bonds()

    print(df.head())

    print("🔄 Kết nối Google Sheet...")
    sheet = connect_gsheet()

    print("🔄 Ghi dữ liệu...")
    update_sheet(sheet, df)

    print("🚀 DONE")


if __name__ == "__main__":
    main()
