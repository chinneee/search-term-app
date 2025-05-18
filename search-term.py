import streamlit as st
import pandas as pd
import re
import gspread
import json
from datetime import datetime
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# ---- SETUP GOOGLE SHEETS ----
def get_gsheet_client(json_keyfile):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(json_keyfile, scopes=scope)
    return gspread.authorize(credentials)

def get_gsheet_df(json_keyfile, sheet_url, sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(json_keyfile, scopes=scope)
    client = gspread.authorize(credentials)
    sheet = client.open_by_url(sheet_url).worksheet(sheet_name)
    data = sheet.get_all_records()
    return pd.DataFrame(data)

# ---- PREPROCESSING CSV ----
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

# ---- CATEGORY DETECTION ----
def detect_category(term):
    term = term.lower()
    if any(kw in term for kw in ['gift', 'gifts']):
        return 'Gift'
    elif any(kw in term for kw in ['card', 'cards']):
        return 'Card'
    elif any(kw in term for kw in ['ornament', 'ornaments']):
        return 'Ornament'
    return 'Other'

# ---- POST-PROCESSING ----
def analyze_search_terms(df):
    try:
        df['Week'] = pd.to_datetime(df['Week'], errors='coerce')
    except:
        st.warning("⚠️ Cột 'Week' có giá trị không hợp lệ.")
        return df

    df = df.dropna(subset=['Week'])
    df['Week_Number'] = df['Week'].dt.isocalendar().week
    df.sort_values(by=['Search Term', 'Week'], inplace=True)

    df['Category'] = df['Search Term'].apply(detect_category)
    df['Rank Change vs Last Week'] = None
    df['Is New Term'] = False

    for term in df['Search Term'].unique():
        term_df = df[df['Search Term'] == term].sort_values(by='Week')
        term_df = term_df.dropna(subset=['Search Frequency Rank'])

        term_ranks = term_df['Search Frequency Rank'].values
        changes = [None] + list(term_ranks[1:] - term_ranks[:-1])
        df.loc[term_df.index, 'Rank Change vs Last Week'] = changes

        if not term_df.empty:
            df.loc[term_df.index[0], 'Is New Term'] = True

    # Format data types
    df['Search Frequency Rank'] = df['Search Frequency Rank'].astype(str)
    df['Week_Number'] = df['Week_Number'].astype(str)
    df['Rank Change vs Last Week'] = df['Rank Change vs Last Week'].astype(str)
    df['Is New Term'] = df['Is New Term'].astype(bool)

    return df

# ---- STREAMLIT APP ----
st.title("📈 Weekly Search Term Uploader & Analyzer")

uploaded_file = st.file_uploader("📂 Tải lên file CSV tuần mới", type=["csv"])
gsheet_json = st.file_uploader("🔐 Tải lên Google Service Account JSON", type="json")
sheet_url_input = st.text_input("🔗 Google Sheet URL", "https://docs.google.com/spreadsheets/d/11JBd0uoCe7Gf9jzb9PHYy_AsIU6eCi4kStegRHrxuJw")
sheet_name_input = st.text_input("📄 Tên Sheet", "Top Search Term 2025")

if uploaded_file and gsheet_json and sheet_url_input and sheet_name_input:
    with st.spinner("🔄 Đang xử lý dữ liệu..."):
        df = preprocess_file(uploaded_file)
        if df is not None:
            filename = uploaded_file.name
            match = re.search(r'(\d{4})_(\d{2})_(\d{2})', filename)
            if match:
                week_str = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
                try:
                    df['Week'] = pd.to_datetime(week_str)
                except:
                    df['Week'] = week_str
            else:
                df['Week'] = "Unknown"

            # Kết nối & ghi dữ liệu mới
            json_keyfile = json.load(gsheet_json)
            client = get_gsheet_client(json_keyfile)
            try:
                sheet = client.open_by_url(sheet_url_input).worksheet(sheet_name_input)
            except Exception as e:
                st.error(f"❌ Không thể mở Google Sheet: {e}")
                st.stop()

            current_rows = len(sheet.get_all_values())
            start_row = current_rows + 1
            records = df[['Search Term', 'Search Frequency Rank', 'Week']].astype(str).values.tolist()

            try:
                sheet.insert_rows(records, row=start_row)
                st.success(f"✅ Đã ghi {len(records)} dòng vào Google Sheet tại dòng {start_row}.")
            except Exception as e:
                st.error(f"❌ Lỗi ghi dữ liệu vào sheet: {e}")
                st.stop()

            # Phân tích dữ liệu toàn bộ
            df_full = get_gsheet_df(json_keyfile, sheet_url_input, sheet_name_input)
            df_analyzed = analyze_search_terms(df_full)

            if df_analyzed is not None:
                try:
                    set_with_dataframe(sheet, df_analyzed)
                    st.success("📊 Đã cập nhật bảng dữ liệu phân tích vào Google Sheet.")
                    st.dataframe(df_analyzed.tail(10))
                except Exception as e:
                    st.error(f"❌ Không thể ghi dữ liệu phân tích: {e}")
