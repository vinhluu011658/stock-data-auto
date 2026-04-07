from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

import gspread
import json
import os
from oauth2client.service_account import ServiceAccountCredentials


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
            rate = cols[1].text.replace(",", ".").strip()
            volume = cols[2].text.replace(",", "").strip()

            data.append([name, rate, volume])

    driver.quit()

    df = pd.DataFrame(data, columns=["Ten lai suat", "Rate", "Volume"])

    # convert number
    df["Rate"] = pd.to_numeric(df["Rate"], errors="coerce")
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    return df


# ===== GOOGLE CREDS =====
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


# ===== MAIN =====
df = scrape_sbv()

print(df)

if df.empty:
    print("NO DATA")
else:
    sheet.clear()
    sheet.update(
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )
    print("DONE - Updated Google Sheet")
