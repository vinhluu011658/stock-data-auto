from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

import gspread
from google.oauth2.service_account import Credentials


def scrape_sbv():
    url = "https://sbv.gov.vn/vi/l%C3%A3i-su%E1%BA%A5t1"

    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--headless")  # chạy ngầm (optional)

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
            data.append([
                cols[0].text,
                cols[1].text.replace(",", "."),
                cols[2].text
            ])

    driver.quit()

    df = pd.DataFrame(data, columns=["date", "rate", "volume"])
    df["rate"] = pd.to_numeric(df["rate"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce") / 10

    return df


def upload_to_gsheet(df):
    # scope quyền
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(
        "credentials.json", scopes=scope
    )

    client = gspread.authorize(creds)

    # mở file theo ID
    spreadsheet = client.open_by_key("1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o")

    # chọn sheet
    worksheet = spreadsheet.worksheet("Laisuat")

    # clear dữ liệu cũ
    worksheet.clear()

    # chuẩn bị data (có header)
    data = [df.columns.values.tolist()] + df.values.tolist()

    # ghi vào sheet
    worksheet.update("A1", data)


if __name__ == "__main__":
    df = scrape_sbv()
    print(df)

    if df.empty:
        print("NO DATA - không có dữ liệu để ghi")
    else:
        upload_to_gsheet(df)
        print("Đã cập nhật Google Sheet thành công")
