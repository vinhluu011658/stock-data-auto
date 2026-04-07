import os
import json
import time
import pandas as pd
import gspread

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ===== GOOGLE SHEETS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
client = gspread.service_account_from_dict(creds_dict)

SHEET_ID = "1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o"
SHEET_NAME = "Laisuat"


# ===== SCRAPE SBV =====
def scrape_sbv():
    url = "https://sbv.gov.vn/vi/l%C3%A3i-su%E1%BA%A5t1"

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0")

    driver = webdriver.Chrome(options=options)

    try:
        print("🌐 Opening SBV...")
        driver.get(url)

        # ===== WAIT TABLE LOAD =====
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "table.bi01-table"))
        )

        time.sleep(2)  # thêm buffer cho chắc

        rows = driver.find_elements(By.CSS_SELECTOR, "table.bi01-table tr")
        print("📊 Rows found:", len(rows))

        data = []
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 3:
                data.append([
                    cols[0].text.strip(),
                    cols[1].text.strip(),
                    cols[2].text.strip()
                ])

        if not data:
            print("❌ SBV: NO DATA")
            return None

        # ===== DATAFRAME =====
        df = pd.DataFrame(data, columns=["date", "rate", "volume"])

        # ===== CLEAN =====
        df = df[:-1]  # bỏ dòng cuối

        df["rate"] = (
            df["rate"]
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

        df["volume"] = (
            df["volume"]
            .str.replace(",", "", regex=False)
            .astype(float) / 10
        )

        print("✅ Data cleaned:", df.shape)

        return df

    except Exception as e:
        print("❌ SBV ERROR:", e)
        return None

    finally:
        driver.quit()


# ===== PUSH TO SHEET =====
def push_to_sheet(df):
    sh = client.open_by_key(SHEET_ID)
    ws = sh.worksheet(SHEET_NAME)

    ws.batch_clear(["A:C"])

    if df is not None and not df.empty:
        ws.update("A1", [df.columns.values.tolist()] + df.values.tolist())
        print("✅ WRITE TO SHEET OK")
    else:
        ws.update("A1", [["NO DATA"]])
        print("❌ SHEET EMPTY")


# ===== MAIN =====
if __name__ == "__main__":
    print("===== SBV RATE JOB =====")

    df = scrape_sbv()
    push_to_sheet(df)

    print("🚀 DONE")
