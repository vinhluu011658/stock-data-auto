# stock-data-auto
Tự động thu thập dữ liệu chứng khoán, xử lý và đẩy vào Google Sheets phục vụ phân tích & dashboard (Looker Studio).
🚀 Tính năng chính

📥 Lấy dữ liệu từ API chứng khoán

🔄 Tự động chạy mỗi ngày bằng GitHub Actions

📊 Đẩy dữ liệu vào Google Sheets

⚡ Xử lý hàng trăm mã cổ phiếu (400+ symbols)

🧹 Tự động làm sạch & chuẩn hóa dữ liệu

📅 Lưu lịch sử theo ngày (120 ngày gần nhất)
🏗️ Kiến trúc hệ thống
API → Python Script → Google Sheets → Looker Studio Dashboard
.
├── main.py              # Script chính xử lý data
├── requirements.txt     # Thư viện Python
├── .github/workflows/   # GitHub Actions (cron job)
└── README.md
⚙️ Cài đặt
1. Clone repo
git clone https://github.com/your-username/your-repo.git
cd your-repo
2. Cài thư viện
pip install -r requirements.txt
🔐 Cấu hình Google Sheets API

Tạo Service Account trên Google Cloud

Tải file JSON credentials

Share Google Sheet cho email trong credentials
🔑 Setup biến môi trường

Trên GitHub:

Vào Settings → Secrets → Actions

Tạo secret: GOOGLE_CREDENTIALS = {JSON credentials}
