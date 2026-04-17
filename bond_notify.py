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


# ================= SCRAPE =================
def scrape_hnx_inconstant():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/tin-cong-bo"

    driver = init_driver()
    wait = WebDriverWait(driver, 20)

    driver.get(url)
    time.sleep(5)
    handle_popup(driver)

    all_data = []

    try:
        # chọn 100 dòng
        try:
            select_box = driver.find_element(By.ID, "slChangeNumberRecord_1")
            driver.execute_script(
                "arguments[0].value='100'; arguments[0].dispatchEvent(new Event('change'));",
                select_box
            )
            time.sleep(3)
        except:
            pass

        # scroll để load JS
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        rows = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "#tbInconstant tbody tr")
            )
        )

        print(f"✅ Lấy {len(rows)} dòng")

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")

            if len(cols) < 7:
                continue

            all_data.append([
                cols[1].text.strip(),
                cols[2].text.strip(),
                cols[3].text.strip(),
                cols[4].text.strip(),
                cols[6].text.strip()
            ])

    except Exception as e:
        print("❌ Lỗi:", e)

    driver.quit()

    df = pd.DataFrame(all_data, columns=[
        "Ngày đăng tin",
        "Tên DN",
        "Mã TP",
        "Tiêu đề",
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

    # clear vùng T1 → K (dữ liệu cũ)
    sheet.batch_clear(["t1:x120"])

    # ghi từ t1
    sheet.update(
        "t1",
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ Đã ghi Google Sheet (từ G15)")


# ================= MAIN =================
def main():
    print("===== HNX - TIN BẤT THƯỜNG =====")

    df = scrape_hnx_inconstant()

    print("📊 Tổng dòng:", len(df))
    print(df.head())

    sheet = connect_gsheet()
    update_sheet(sheet, df)

    print("🚀 DONE")


if __name__ == "__main__":
    main()
