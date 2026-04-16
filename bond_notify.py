import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


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
def scrape_inconstant_one_page():
    url = "https://cbonds.hnx.vn/to-chuc-phat-hanh/tin-cong-bo-x"

    driver = init_driver()
    wait = WebDriverWait(driver, 15)

    driver.get(url)
    time.sleep(3)
    handle_popup(driver)

    all_data = []

    try:
        rows = wait.until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "#tbInconstant tbody tr")
            )
        )

        print(f"✅ Lấy {len(rows)} dòng")

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")

            if len(cols) < 8:
                continue

            # ===== EXTRACT =====
            ngay_dang = cols[1].text.strip()
            ten_dn = cols[2].text.strip()
            ma_tp = cols[3].text.strip()

            # tiêu đề có link
            try:
                tieu_de = cols[4].find_element(By.TAG_NAME, "a").text.strip()
            except:
                tieu_de = cols[4].text.strip()

            ghi_chu = cols[5].text.strip()
            tinh_trang = cols[6].text.strip()

            # ===== FILE =====
            file_id = ""
            try:
                icon = cols[7].find_element(By.TAG_NAME, "i")
                onclick = icon.get_attribute("onclick")

                # parse ViewFile('31949.0', '3', '')
                file_id = onclick
            except:
                pass

            all_data.append([
                ngay_dang,
                ten_dn,
                ma_tp,
                tieu_de,
                ghi_chu,
                tinh_trang,
                file_id
            ])

    except Exception as e:
        print("❌ Lỗi:", e)

    driver.quit()

    df = pd.DataFrame(all_data, columns=[
        "Ngày đăng tin",
        "Tên doanh nghiệp",
        "Mã TP liên quan",
        "Tiêu đề",
        "Ghi chú",
        "Tình trạng",
        "File (raw onclick)"
    ])

    return df


# ================= MAIN =================
if __name__ == "__main__":
    df = scrape_inconstant_one_page()
    print("📊 Tổng dòng:", len(df))
    print(df.head())
