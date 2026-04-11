import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials


# ================= DRIVER =================
def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")

    driver = webdriver.Chrome(options=options)
    return driver


# ================= SCRAPER =================
def scrape_hnx_bonds():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-phat-hanh"

    driver = init_driver()
    wait = WebDriverWait(driver, 20)

    driver.get(url)

    print("🔄 Đang chờ load bảng...")

    # 🔥 đợi có bảng + có dữ liệu
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
    wait.until(lambda d: len(d.find_elements(By.XPATH, "//table//tbody//tr")) > 0)

    all_data = []
    page = 1

    while True:
        print(f"\n📄 Page {page}")

        # 🔥 đảm bảo data đã render
        rows = wait.until(
            lambda d: d.find_elements(By.XPATH, "//table//tbody//tr")
        )

        print(f"✅ Rows tìm được: {len(rows)}")

        for row in rows:
            cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]

            if len(cols) < 17:
                continue

            all_data.append([
                cols[1],
                cols[2],
                cols[3],
                cols[9].replace(",", ""),
                cols[10].replace(",", ""),
                cols[16]
            ])

        # 🔥 thử click trang tiếp
        try:
            next_page = page + 1

            next_btn = driver.find_element(
                By.XPATH, f"//a[normalize-space()='{next_page}']"
            )

            driver.execute_script("arguments[0].click();", next_btn)

            # 🔥 đợi page load khác đi
            time.sleep(2)

            wait.until(
                lambda d: len(d.find_elements(By.XPATH, "//table//tbody//tr")) > 0
            )

            page += 1

        except Exception as e:
            print("👉 Hết trang hoặc không click được:", e)
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
    print("===== HNX BONDS (STABLE VERSION) =====")

    df = scrape_hnx_bonds()

    print("\n📊 Preview:")
    print(df.head())

    sheet = connect_gsheet()
    update_sheet(sheet, df)

    print("\n🚀 DONE")


if __name__ == "__main__":
    main()
