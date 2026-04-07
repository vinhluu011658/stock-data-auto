from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time


def update_sbv_sheet(client):
    url = "https://sbv.gov.vn/vi/l%C3%A3i-su%E1%BA%A5t1"

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    try:
        driver.get(url)
        time.sleep(5)

        rows = driver.find_elements(By.CSS_SELECTOR, "table.bi01-table tr")

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
            return

        df = pd.DataFrame(data, columns=["date", "rate", "volume"])

        # clean giống Excel của bạn
        df = df[:-1]
        df["rate"] = df["rate"].str.replace(",", ".", regex=False).astype(float)
        df["volume"] = df["volume"].str.replace(",", "", regex=False).astype(float) / 10

        # ===== PUSH GOOGLE SHEET =====
        sh = client.open_by_key("1VX-dTuwjyQpG_kIke8D2ID1KOMrfTy1Ksu75YJT_C-o")
        ws = sh.worksheet("Laisuat")

        ws.batch_clear(["A:C"])
        ws.update("A1", [df.columns.values.tolist()] + df.values.tolist())

        print("✅ SBV WRITE OK")

    except Exception as e:
        print("❌ SBV ERROR:", e)

    finally:
        driver.quit()
