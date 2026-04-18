import time
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials


# ================= SELENIUM =================
def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(options=options)


# ================= POPUP =================
def handle_popup(driver):
    try:
        for cb in driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']"):
            driver.execute_script("arguments[0].click();", cb)

        for btn in driver.find_elements(By.XPATH, "//button[contains(text(),'Đồng ý')]"):
            driver.execute_script("arguments[0].click();", btn)

        driver.execute_script("document.body.classList.remove('modal-open');")
    except:
        pass


# ================= CLEAN =================
def clean_number(x):
    return x.replace(",", "").strip()


# ================= SCRAPE (1 PAGE) =================
def scrape_hnx_bonds_one_page():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-phat-hanh"

    driver = init_driver()
    wait = WebDriverWait(driver, 15)

    driver.get(url)
    time.sleep(3)
    handle_popup(driver)

    all_data = []

    try:
        rows = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "#tbReleaseResult tbody tr")
            )
        )

        print(f"✅ Lấy {len(rows)} dòng")

        for row in rows:
            cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]

            if len(cols) < 18:
                continue

            # ===== FIELD =====
            ky_han = cols[5]
            issue_date = cols[6]
            maturity_date = cols[7]
            status = cols[17]   # ⭐ TÌNH TRẠNG

            # ===== CALC REMAINING DAYS =====
            remaining_days = ""
            try:
                maturity_dt = datetime.strptime(maturity_date, "%d/%m/%Y")
                remaining_days = (maturity_dt - datetime.today()).days
            except:
                pass

            all_data.append([
                cols[1],  # Ngày đăng
                cols[2],  # Tên DN
                cols[3],  # Mã TP
                ky_han,
                issue_date,
                maturity_date,
                remaining_days,
                clean_number(cols[9]),
                clean_number(cols[10]),
                cols[16],
                status
            ])

    except Exception as e:
        print("❌ Lỗi:", e)

    driver.quit()

    df = pd.DataFrame(all_data, columns=[
        "Ngày đăng tin",
        "Tên DN",
        "Mã TP",
        "Kỳ hạn",
        "Ngày phát hành",
        "Ngày đáo hạn",
        "Kỳ hạn còn lại (ngày)",
        "Khối lượng",
        "Mệnh giá",
        "Lãi suất phát hành (%/năm)",
        "Tình trạng"
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
    
    # ⭐ ĐỔI . → , ở cột lãi suất
    df["Lãi suất phát hành (%/năm)"] = df["Lãi suất phát hành (%/năm)"].astype(str).str.replace(".", ",", regex=False)
    
    sheet.batch_clear(["G:Q"])
    
    sheet.update(
        "G1",
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ Đã ghi Google Sheet")


# ================= MAIN =================
def main():
    print("===== HNX BONDS (1 PAGE) =====")

    df = scrape_hnx_bonds_one_page()
    print("📊 Tổng dòng:", len(df))
    print(df.head())

    sheet = connect_gsheet()
    update_sheet(sheet, df)

    print("🚀 DONE")


if __name__ == "__main__":
    main()
