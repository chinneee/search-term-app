import streamlit as st
import pandas as pd
import re
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

# ---- SETUP GOOGLE SHEETS ----
def get_gsheet_client(json_keyfile):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(json_keyfile, scopes=scope)
    return gspread.authorize(credentials)

# ---- PREPROCESSING ----
def preprocess_file(uploaded_file):
    try:
        df = pd.read_csv(uploaded_file, skiprows=1)
    except Exception as e:
        st.error(f"Lỗi đọc file: {e}")
        return None

    expected_columns = ['Search Term', 'Search Frequency Rank']
    if not all(col in df.columns for col in expected_columns):
        st.warning(f"Thiếu cột. Các cột hiện tại: {list(df.columns)}")
        return None

    df = df[['Search Term', 'Search Frequency Rank']]

    def is_meaningful_english(text):
        if isinstance(text, str):
            pattern = r'^[a-zA-Z0-9\s]+$'
            has_letter = re.search(r'[a-zA-Z]', text)
            return bool(re.match(pattern, text) and has_letter)
        return False

    df = df[df['Search Term'].apply(is_meaningful_english)]
    df = df[df['Search Frequency Rank'] <= 300000]

    keywords = ["gifts", "gift", "card", "cards", "ornaments", "ornament"]
    df = df[df['Search Term'].str.contains('|'.join(keywords), case=False, na=False)]

    return df

# ---- APP ----
st.title("📈 Weekly Search Term Uploader to Google Sheet")

uploaded_file = st.file_uploader("📂 Tải lên file CSV tuần mới", type=["csv"])

gsheet_json = st.file_uploader("🔐 Tải lên Google Service Account JSON", type="json")

sheet_url = "11JBd0uoCe7Gf9jzb9PHYy_AsIU6eCi4kStegRHrxuJw"

sheet_name = "Top Search Term"

if uploaded_file and gsheet_json and sheet_url and sheet_name:
    with st.spinner("Đang xử lý dữ liệu..."):
        df = preprocess_file(uploaded_file)
        if df is not None:
            # Lấy ngày từ tên file
            filename = uploaded_file.name
            match = re.search(r'(\d{4})_(\d{2})_(\d{2})', filename)
            if match:
                week_str = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                try:
                    df['Week'] = pd.to_datetime(week_str)
                except:
                    st.warning("Không thể chuyển đổi tuần sang datetime.")
                    df['Week'] = week_str
            else:
                df['Week'] = "Unknown"

            # Kết nối Google Sheet
            client = get_gsheet_client(json_keyfile=pd.read_json(gsheet_json))

            try:
                sheet = client.open_by_url(sheet_url).worksheet(sheet_name)
            except Exception as e:
                st.error(f"Không thể mở Google Sheet: {e}")
                st.stop()

            # Lấy số dòng hiện tại + 1 (để không ghi đè header)
            current_rows = len(sheet.get_all_values())
            start_row = current_rows + 1

            # Ghi dữ liệu
            records = df[['Search Term', 'Search Frequency Rank', 'Week']].astype(str).values.tolist()

            try:
                sheet.insert_rows(records, row=start_row)
                st.success(f"✅ Đã ghi {len(records)} dòng vào Google Sheet tại dòng {start_row}.")
            except Exception as e:
                st.error(f"❌ Lỗi ghi dữ liệu vào sheet: {e}")
        else:
            st.warning("Không có dữ liệu hợp lệ để ghi.")
