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


# ===== HÀM CLEAN SỐ (QUAN TRỌNG NHẤT) =====
def clean_number(text):
    if not text:
        return ""
    # giữ lại số và dấu chấm
    return re.sub(r"[^\d.]", "", text)


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

    WebDriverWait(driver, 15).until(
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

            # 🔥 CLEAN DATA
            rate_clean = clean_number(rate_raw)
            volume_clean = clean_number(volume_raw)

            data.append([name, rate_clean, volume_clean])

    driver.quit()

    df = pd.DataFrame(data, columns=["Ten lai suat", "Rate", "Volume"])

    # convert sang số
    df["Rate"] = pd.to_numeric(df["Rate"], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

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

    # 🔥 FIX JSON ERROR (NaN → rỗng)
    df = df.fillna("")

    print("Đang cập nhật Google Sheet...")
    sheet = connect_gsheet()

    sheet.clear()
    sheet.update(
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ DONE - Đã cập nhật Google Sheet")


if __name__ == "__main__":
    main()
