import requests
import pandas as pd
import gspread
import json
import os
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
sheet = client.open("DATA_STOCK").sheet1
# hoặc dùng ID (khuyến nghị)
# sheet = client.open_by_key("YOUR_SHEET_ID").sheet1

# ===== SYMBOL LIST =====
symbols = ["VCB", "BID", "CTG"]  # sau này thay 400 mã

data_all = []

# ===== HÀM TRỪ AN TOÀN =====
def safe_calc(a, b):
    return (a or 0) - (b or 0)

# ===== CALL API =====
for symbol in symbols:
    url = f"https://api-finance-t19.24hmoney.vn/v1/ios/stock/statistic-investor-history?symbol={symbol}"
    
    try:
        res = requests.get(url)
        data = res.json()
        
        for item in data.get("data", []):
            data_all.append({
                "symbol": symbol,

                "foreign_individual": safe_calc(item.get("foreign_individual_buy"), item.get("foreign_individual_sell")),
                "proprietary": safe_calc(item.get("proprietary_buy"), item.get("proprietary_sell")),
                "local_individual": safe_calc(item.get("local_individual_buy"), item.get("local_individual_sell")),
                "local_institutional": safe_calc(item.get("local_institutional_buy"), item.get("local_institutional_sell")),
                "foreign_institutional": safe_calc(item.get("foreign_institutional_buy"), item.get("foreign_institutional_sell")),
                "foreign_total": safe_calc(item.get("foreign_buy"), item.get("foreign_sell")),

                "trading_date": item.get("trading_date") or item.get("date")
            })
            
    except Exception as e:
        print(f"Lỗi {symbol}: {e}")

# ===== DATAFRAME =====
df = pd.DataFrame(data_all)

# ===== SORT (khuyến nghị) =====
df = df.sort_values(by=["symbol", "trading_date"])

# ===== PUSH TO GOOGLE SHEETS =====
sheet.clear()
sheet.update([df.columns.values.tolist()] + df.values.tolist())

print("DONE")
