import requests
import pandas as pd


def scrape_sbv():
    url = "https://sbv.gov.vn/Services/StatisticsService.svc/GetInterbankRates"

    try:
        res = requests.get(url, timeout=10)

        if res.status_code != 200:
            print("❌ API STATUS:", res.status_code)
            return None

        data = res.json()

        if not data:
            print("❌ NO DATA FROM API")
            return None

        rows = []
        for item in data:
            rows.append([
                item.get("date"),
                item.get("rate"),
                item.get("volume")
            ])

        df = pd.DataFrame(rows, columns=["date", "rate", "volume"])

        print("✅ API rows:", len(df))

        return df

    except Exception as e:
        print("❌ API ERROR:", e)
        return None
