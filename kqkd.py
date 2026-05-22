import requests
import pandas as pd
import gspread
import json
import os
import time
from oauth2client.service_account import ServiceAccountCredentials

# ===== LOAD GOOGLE CREDS =====
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# ===== OPEN SHEET =====
sheet = client.open("DATA_STOCK").worksheet("BCTC")

# ===== SYMBOL LIST =====
symbols = ["BSR", "HPG", "VNM"]

# ===== API URL =====
BASE_URL = "https://api-finance-t19.24hmoney.vn/v1/ios/company/financial-report"

headers = {
    "User-Agent": "Mozilla/5.0"
}

data_all = []

for symbol in symbols:

    print(f"Fetching {symbol}...")

    params = {
        "symbol": symbol,
        "period": 1,
        "view": 2,
        "page": 1,
        "expanded": "true"
    }

    try:
        response = requests.get(
            BASE_URL,
            params=params,
            headers=headers,
            timeout=30
        )

        json_data = response.json()

        headers_data = json_data["data"]["headers"]
        rows = json_data["data"]["rows"]

        # ===== TẠO TÊN CỘT =====
        columns = []

        for h in headers_data:
            year = h["year"]
            q = h["quarter"]
            t = h["type"]

            if q == 0:
                period = f"{year}"
            else:
                period = f"Q{q}_{year}"

            col_name = f"{period}_{t}"
            columns.append(col_name)

        # ===== PARSE DATA =====
        for row in rows:

            item = {
                "symbol": symbol,
                "key": row.get("key"),
                "name": row.get("name"),
                "level": row.get("level")
            }

            values = row.get("values", [])

            for i, val in enumerate(values):
                if i < len(columns):
                    item[columns[i]] = val

            data_all.append(item)

        time.sleep(1)

    except Exception as e:
        print(f"Error {symbol}: {e}")

# ===== DATAFRAME =====
df = pd.DataFrame(data_all)

# ===== REPLACE NaN =====
df = df.fillna("")

# ===== CLEAR SHEET =====
sheet.clear()

# ===== UPDATE SHEET =====
sheet.update(
    [df.columns.values.tolist()] + df.values.tolist()
)

print("DONE!")
