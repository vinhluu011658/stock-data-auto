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


# ================= SELENIUM SETUP =================
def init_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    return driver


# ================= HANDLE POPUP =================
def handle_popup(driver):
    try:
        # click checkbox nếu có
        checkboxes = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']")
        for cb in checkboxes:
            driver.execute_script("arguments[0].click();", cb)

        # click nút đồng ý nếu có
        buttons = driver.find_elements(By.XPATH, "//button[contains(text(),'Đồng ý')]")
        for btn in buttons:
            driver.execute_script("arguments[0].click();", btn)

        # remove overlay nếu còn
        driver.execute_script("document.body.classList.remove('modal-open');")

    except:
        pass


# ================= SCRAPE HNX =================
def scrape_hnx_bonds():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/thong-tin-phat-hanh"

    driver = init_driver()
    wait = WebDriverWait(driver, 20)

    driver.get(url)
    time.sleep(3)

    handle_popup(driver)

    all_data = []
    page = 1

    while True:
        print(f"🔄 Page {page}")

        try:
            handle_popup(driver)

            rows = wait.until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "#tbReleaseResult tbody tr")
                )
            )

            if not rows:
                print("❌ Không có data")
                break

            old_first_row = rows[0].text

            for row in rows:
                cols = [td.text.strip() for td in row.find_elements(By.TAG_NAME, "td")]

                if len(cols) < 17:
                    continue

                all_data.append([
                    cols[1],
                    cols[2],
                    cols[3],
                    cols[9].replace(",", ""),
                    cols[10].replace(",", ""),
                    cols[16]
                ])

            print(f"✅ Lấy {len(rows)} dòng")

            # ================= PAGINATION =================
            try:
                next_page = page + 1

                # tìm nút số trang
                next_btn = wait.until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        f"//ul[contains(@class,'pagination')]//a[normalize-space()='{next_page}']"
                    ))
                )

                # scroll tới nút
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", next_btn
                )
                time.sleep(1)

                # click bằng JS
                driver.execute_script("arguments[0].click();", next_btn)

                # xử lý popup lại nếu có
                handle_popup(driver)

                # đợi dữ liệu đổi
                wait.until(lambda d:
                    len(d.find_elements(By.CSS_SELECTOR, "#tbReleaseResult tbody tr")) > 0 and
                    d.find_elements(By.CSS_SELECTOR, "#tbReleaseResult tbody tr")[0].text != old_first_row
                )

                time.sleep(1.5)

                page += 1

            except Exception as e:
                print("👉 Hết trang hoặc lỗi pagination:", e)
                break

        except Exception as e:
            print("❌ Lỗi scrape:", e)
            break

    driver.quit()

    df = pd.DataFrame(all_data, columns=[
        "Ngày đăng tin",
        "Tên DN",
        "Mã TP",
        "Khối lượng",
        "Mệnh giá",
        "Lãi suất (%)"
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

    sheet.update(
        "G1",
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("✅ Đã ghi Google Sheet")


# ================= MAIN =================
def main():
    print("===== HNX BONDS =====")

    df = scrape_hnx_bonds()
    print("📊 Tổng dòng:", len(df))
    print(df.head())

    sheet = connect_gsheet()
    update_sheet(sheet, df)

    print("🚀 DONE")


if __name__ == "__main__":
    main()
