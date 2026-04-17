import requests
import pandas as pd


def fetch_hnx_inconstant():
    url = "https://cbonds.hnx.vn/Handler/Search/SearchTinCongBo"

    payload = {
        "keysSearch[]": [""] * 7,
        "currentPages[]": [1, 1, 1, 1],
        "numberRecord[]": [20, 20, 20, 20]  # lấy 20 dòng
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest"
    }

    response = requests.post(url, data=payload, headers=headers)

    html = response.text

    # parse HTML table
    df_list = pd.read_html(html)

    if not df_list:
        print("❌ Không có bảng")
        return pd.DataFrame()

    df = df_list[0]

    # rename cột
    df.columns = [
        "STT",
        "Ngày đăng tin",
        "Tên DN",
        "Mã TP",
        "Tiêu đề",
        "Ghi chú",
        "Tình trạng",
        "File"
    ]

    # chọn cột cần
    df = df[[
        "Ngày đăng tin",
        "Tên DN",
        "Mã TP",
        "Tiêu đề",
        "Tình trạng"
    ]]

    return df


def main():
    print("===== API HNX =====")

    df = fetch_hnx_inconstant()

    print("📊 Tổng dòng:", len(df))
    print(df.head())


if __name__ == "__main__":
    main()
