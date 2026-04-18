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


# ================= SCRAPE =================
def scrape_hnx_repurchase():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-phat-hanh"

    driver = init_driver()
    wait = WebDriverWait(driver, 15)

    driver.get(url)
    time.sleep(3)
    handle_popup(driver)

    all_data = []

    try:
        # ✅ chọn 100 dòng (giữ style của bạn)
        try:
            select_box = driver.find_element(By.ID, "slChangeNumberRecord_1")
            driver.execute_script(
                "arguments[0].value='100'; arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                select_box
            )
            time.sleep(3)
        except:
            pass

        # scroll để load JS
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        rows = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "#tbRepurchaseResult tbody tr")
            )
        )

        print(f"✅ Lấy {len(rows)} dòng")

        for row in rows:
            cols = [
                td.get_attribute("innerText").strip()
                for td in row.find_elements(By.TAG_NAME, "td")
            ]

            # bỏ dòng rỗng
            if not any(cols):
                continue

            # vì bạn dùng tới cols[14] → cần ít nhất 15 cột
            if len(cols) < 15:
                continue

            all_data.append([
                cols[1],   # Ngày đăng tin
                cols[2],   # Tên DN
                cols[3],   # Mã TP
                cols[6],   # Ngày phát hành
                cols[7],   # Ngày đáo hạn
                clean_number(cols[10]),  # Giá trị mua lại
                clean_number(cols[12]),  # Giá trị còn lại
                cols[14],  # Ngày mua lại
            ])

    except Exception as e:
        print("❌ Lỗi:", e)

    driver.quit()

    df = pd.DataFrame(all_data, columns=[
        "Ngày đăng tin",
        "Tên DN",
        "Mã TP",
        "Ngày phát hành",
        "Ngày đáo hạn",
        "Giá trị mua lại",
        "Giá trị còn lại",
        "Ngày mua lại"
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

    # chỉ xóa từ G105 trở xuống
    sheet.batch_clear(["G105:n205"])

    # ghi từ G105
    sheet.update(
        "G105",
        [df.columns.tolist()] + df.values.tolist(),
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
