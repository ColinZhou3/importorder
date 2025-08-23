import streamlit as st
import pdfplumber
import pandas as pd
import re
import io
from datetime import datetime

# -------------------------------
# 供应商识别
# -------------------------------
def detect_supplier(text):
    if "Fresh Auckland Distribution" in text or "Foodstuffs" in text:
        return "foodstuffs"
    elif "Christchurch FDC Produce" in text or "Woolworths" in text:
        return "wwnz"
    elif "My Food Bag" in text:
        return "mfb"
    else:
        return None

# -------------------------------
# Foodstuffs 解析
# -------------------------------
def parse_foodstuffs(pdf):
    items = []
    with pdfplumber.open(pdf) as pdf_obj:
        for page in pdf_obj.pages:
            text = page.extract_text()
            lines = text.split("\n")
            for line in lines:
                match = re.search(r"(\d{7})\s+([A-Za-z0-9\-\s]+)\s+(\d+)\s+\$([\d\.]+)", line)
                if match:
                    items.append({
                        "item_id": match.group(1),
                        "quantity": int(match.group(3)),
                        "price": float(match.group(4)),
                    })
    return items

# -------------------------------
# Woolworths 解析
# -------------------------------
def parse_wwnz(pdf):
    items = []
    with pdfplumber.open(pdf) as pdf_obj:
        for page in pdf_obj.pages:
            text = page.extract_text()
            lines = text.split("\n")
            for line in lines:
                match = re.search(r"(\d{6,7})\s+(.+?)\s+(\d+)\s+\$([\d\.]+)", line)
                if match:
                    items.append({
                        "item_id": match.group(1),
                        "quantity": int(match.group(3)),
                        "price": float(match.group(4)),
                    })
    return items

# -------------------------------
# MFB 解析
# -------------------------------
def parse_mfb(pdf):
    items = []
    with pdfplumber.open(pdf) as pdf_obj:
        for page in pdf_obj.pages:
            text = page.extract_text()
            lines = text.split("\n")
            for line in lines:
                match = re.search(r"(\d{6,7})\s+(.+?)\s+(\d+)\s+\$([\d\.]+)", line)
                if match:
                    items.append({
                        "item_id": match.group(1),
                        "quantity": int(match.group(3)),
                        "price": float(match.group(4)),
                    })
    return items

# -------------------------------
# 主界面
# -------------------------------
st.title("📦 PDF 订单解析 & 导出 CSV")
st.write("上传 Foodstuffs / Woolworths / MFB 的 PO PDF 文件，自动生成订单 CSV。")

uploaded_files = st.file_uploader("上传 PDF 文件", type=["pdf"], accept_multiple_files=True)

store_map = st.file_uploader("上传 store_map.csv（包含 store_id, name, sales_id）", type=["csv"])

if uploaded_files and store_map:
    store_df = pd.read_csv(store_map)
    final_data = []

    for uploaded_file in uploaded_files:
        with pdfplumber.open(uploaded_file) as pdf_obj:
            first_page_text = pdf_obj.pages[0].extract_text()
            supplier = detect_supplier(first_page_text)

        if not supplier:
            st.error(f"❌ 无法识别供应商: {uploaded_file.name}")
            continue

        if supplier == "foodstuffs":
            items = parse_foodstuffs(uploaded_file)
            store_id = store_df.loc[store_df["name"].str.contains("Fresh Auckland", case=False), "store_id"].values[0]
        elif supplier == "wwnz":
            items = parse_wwnz(uploaded_file)
            store_id = store_df.loc[store_df["name"].str.contains("Christchurch FDC", case=False), "store_id"].values[0]
        elif supplier == "mfb":
            items = parse_mfb(uploaded_file)
            store_id = store_df.loc[store_df["name"].str.contains("My Food Bag", case=False), "store_id"].values[0]

        for item in items:
            final_data.append({
                "store_id": store_id,
                "name": supplier.upper(),
                "sales_id": "N/A",
                "order_date": datetime.today().strftime("%Y-%m-%d"),
                "item_id": item["item_id"],
                "quantity": item["quantity"],
                "price": item["price"],
            })

    df = pd.DataFrame(final_data)
    st.dataframe(df)

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    st.download_button(
        label="📥 下载 CSV",
        data=csv_buffer.getvalue(),
        file_name="orders.csv",
        mime="text/csv"
    )
