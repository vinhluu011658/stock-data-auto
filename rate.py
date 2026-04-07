from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

import gspread
import json
import os
import re
from oauth2client.service_account import ServiceAccountCredentials


# ===== CLEAN RATE (GIỮ DẤU PHẨY, BỎ (*)) =====
def clean_rate(text):
    if not text:
        return ""
    return re.sub(r"[^\d,]", "", text)


# ===== CLEAN VOLUME (BỎ (*), BỎ ,) =====
def clean_volume(text):
    if not text:
        return ""
    return re.sub(r"[^\d]", "", text)


# ===== SCRAPE SBV =====
def scrape_sbv():
    url = "https://sbv.gov.vn/vi/l%C3%A3i-su%E1%BA%A5t1"

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get(url)

    wait = WebDriverWait(driver, 15)

    # ===== LẤY NGÀY =====
    try:
        date_element = wait.until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'Ngày áp dụng')]"))
        )
        date_text = date_element.text
        match = re.search(r"\d{2}/\d{2}/\d{4}", date_text)
        apply_date = match.group(0) if match else ""
    except:
        apply_date = ""

    # ===== TABLE =====
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.bi01-table"))
    )

    rows = driver.find_elements(By.CSS_SELECTOR, "table.bi01-table tr")

    data = []
    for row in rows[1:-1]:
        cols = row.find_elements(By.TAG_NAME, "td")

        if len(cols) >= 3:
            name = cols[0].text.strip()

            rate_raw = cols[1].text.strip()
            volume_raw = cols[2].text.strip()

            # 🔥 CLEAN
            rate_clean = clean_rate(rate_raw)
            volume_clean = clean_volume(volume_raw)

            # convert volume và chia 10
            try:
                volume_value = float(volume_clean) / 10 if volume_clean else ""
            except:
                volume_value = ""

            data.append([apply_date, name, rate_clean, volume_value])

    driver.quit()

    df = pd.DataFrame(data, columns=["Ngày áp dụng", "Thời hạn", "Lãi suất liên ngân hàng", "Doanh số"])

    return df


# ===== CONNECT GOOGLE SHEET =====
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


# ===== MAIN =====
def main():
    print("Đang lấy dữ liệu SBV...")
    df = scrape_sbv()

    print(df)

    if df.empty:
        print("❌ Không có dữ liệu")
        return

    df = df.fillna("")

    print("Đang cập nhật Google Sheet...")
    sheet = connect_gsheet()

    sheet.batch_clear(["A:D"])
    sheet.update(
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ DONE")


if __name__ == "__main__":
    main()
