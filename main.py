import requests
import pandas as pd

symbols = ["VCB", "BID", "CTG"]

data_all = []

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

df = pd.DataFrame(data_all)
df.to_csv("output.csv", index=False)

print("DONE")
