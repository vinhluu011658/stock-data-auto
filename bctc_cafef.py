import requests
import json

url = (
    "https://apiweb.cafef.vn/api/v1/BCTC/GetReportDetail"
    "?symbol=BSR"
    "&pageIndex=1"
    "&pageSize=10"
    "&reportType=KQKD"
    "&TypeTime=NAM"
)

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://cafef.vn/"
}

r = requests.get(url, headers=headers)

print("STATUS:", r.status_code)

js = r.json()

print("SUCCESS:", js["isSuccess"])

value = js["value"]

print("\n=== TEMPLATE ===")
print("Số tài khoản:", len(value["templace"]))

print("\n=== DATA ===")
print("Số năm:", len(value["data"]))

first_year = value["data"][0]

print(json.dumps(first_year, indent=2, ensure_ascii=False))
