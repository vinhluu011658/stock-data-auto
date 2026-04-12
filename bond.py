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


# ================= SELENIUM SETUP =================
def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    return driver


# ================= HANDLE POPUP =================
def handle_popup(driver):
    wait = WebDriverWait(driver, 10)

    try:
        print("🔐 Xử lý popup...")

        # đợi checkbox xuất hiện
        checkbox = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='checkbox']"))
        )
        driver.execute_script("arguments[0].click();", checkbox)

        # đợi nút đồng ý
        agree_btn = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Đồng ý')]"))
        )
        driver.execute_script("arguments[0].click();", agree_btn)

        # đợi popup biến mất
        wait.until(EC.invisibility_of_element(checkbox))

        print("✅ Đã accept điều khoản")

    except Exception as e:
        print("⚠️ Không thấy popup hoặc đã xử lý:", e)


# ================= SCRAPE HNX =================
def scrape_hnx_bonds():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-phat-hanh"

    driver = init_driver()
    wait = WebDriverWait(driver, 20)

    driver.get(url)

    # xử lý popup
    handle_popup(driver)

    all_data = []
    page = 1

    while True:
        print(f"🔄 Page {page}")

        try:
            # đợi table load
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#tbReleaseResult tbody tr"))
            )

            rows = driver.find_elements(By.CSS_SELECTOR, "#tbReleaseResult tbody tr")

            if not rows:
                print("❌ Không có data")
                break

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

            print(f"✅ Lấy {len(rows)} dòng")

            # next page
            try:
                next_btn = wait.until(
                    EC.element_to_be_clickable((By.LINK_TEXT, str(page + 1)))
                )
                driver.execute_script("arguments[0].click();", next_btn)

                # đợi reload bảng
                time.sleep(2)

                page += 1

            except:
                print("👉 Hết trang")
                break

        except Exception as e:
            print("❌ Lỗi khi scrape:", e)
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

    df = scrape_hnx_bonds()
    print(df.head())

    sheet = connect_gsheet()
    update_sheet(sheet, df)

    print("🚀 DONE")


if __name__ == "__main__":
    main()
