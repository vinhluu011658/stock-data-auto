from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

def scrape_sbv():
    url = "https://sbv.gov.vn/vi/l%C3%A3i-su%E1%BA%A5t1"

    options = Options()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=options)

    driver.get(url)

    # nếu có iframe → bật dòng này
    # driver.switch_to.frame(0)

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
