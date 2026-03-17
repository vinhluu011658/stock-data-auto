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

# ===== SYMBOL LIST =====
symbols = ["VCB", "BID", "CTG"]

data_all = []

# ===== CALL API =====
for symbol in symbols:
    url = f"https://api-finance-t19.24hmoney.vn/v1/ios/stock/statistic-investor-history?symbol={symbol}"
    
    try:
        res = requests.get(url)
        data = res.json()
        
        for item in data.get("data", []):
            data_all.append({
                "symbol": symbol,
                "date": item.get("date"),
                "value": item.get("value")
            })
            
    except Exception as e:
        print(f"Lỗi {symbol}: {e}")

# ===== SAVE TO SHEET =====
df = pd.DataFrame(data_all)

sheet.clear()
sheet.update([df.columns.values.tolist()] + df.values.tolist())

print("DONE")
