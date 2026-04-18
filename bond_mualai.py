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


# ================= GET TEXT FIX =================
def get_text(driver, element):
    return driver.execute_script("return arguments[0].innerText;", element).strip()


# ================= SCRAPE =================
def scrape_hnx_repurchase():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-mua-lai"  # ⭐ nên dùng đúng URL này

    driver = init_driver()
    wait = WebDriverWait(driver, 20)

    driver.get(url)
    handle_popup(driver)

    all_data = []

    try:
        # ✅ chờ table load thật sự
        wait.until(EC.presence_of_element_located((By.ID, "tbRepurchaseResult")))
        wait.until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "#tbRepurchaseResult tbody tr")
        ))

        rows = driver.find_elements(By.CSS_SELECTOR, "#tbRepurchaseResult tbody tr")

        print(f"✅ Lấy {len(rows)} dòng")

        for row in rows:
            tds = row.find_elements(By.TAG_NAME, "td")
            cols = [get_text(driver, td) for td in tds]

            # debug nếu cần
            # print(cols)

            if len(cols) < 16:
                continue

            # bỏ dòng rỗng
            if all(c == "" for c in cols):
                continue

            all_data.append([
                cols[1],
                cols[2],
                cols[3],
                clean_number(cols[4]),
                cols[5],
                cols[6],
                cols[7],
                clean_number(cols[8]),
                clean_number(cols[9]),
                clean_number(cols[10]),
                cols[11],
                clean_number(cols[12]),
                cols[13],
                cols[14],
                cols[15],
                cols[16] if len(cols) > 16 else ""
            ])

    except Exception as e:
        print("❌ Lỗi:", e)

    driver.quit()

    df = pd.DataFrame(all_data, columns=[
        "Ngày đăng tin",
        "Tên DN",
        "Mã TP",
        "Mệnh giá",
        "Kỳ hạn",
        "Ngày phát hành",
        "Ngày đáo hạn",
        "Giá trị phát hành",
        "Giá trị lưu hành",
        "Giá trị mua lại",
        "Số lượng mua lại",
        "Giá trị còn lại",
        "Số lượng còn lại",
        "Ngày mua lại",
        "Tình trạng",
        "Ghi chú"
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

    # chỉ clear từ G15 trở xuống
    sheet.batch_clear(["G15:Q1000"])

    # ghi từ G15
    sheet.update(
        range_name="G15",
        values=[df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ Đã ghi Google Sheet")


# ================= MAIN =================
def main():
    print("===== HNX REPURCHASE =====")

    df = scrape_hnx_repurchase()
    print("📊 Tổng dòng:", len(df))
    print(df.head())

    sheet = connect_gsheet()
    update_sheet(sheet, df)

    print("🚀 DONE")


if __name__ == "__main__":
    main()
