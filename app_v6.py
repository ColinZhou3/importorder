
import re
import tempfile
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import pdfplumber

st.set_page_config(page_title="PO PDF → CSV (v6)", layout="wide")
st.title("PO PDF → CSV（v6 修复版）")

VENDOR_PROFILES = {
    "Foodstuffs_NI": {
        "detect_keywords": ["Foodstuffs North Island Limited", "Order Forecast", "O/F"],
        "store_regex": r"(?:Delivery\s+To|Delivery\s+Address)[:：]?\s*([^\n]+)",
        "header_extract": {
            "PO_Number": r"Order\s+Forecast\s+Number[:：]?\s*([0-9]+)",
            "Order_Date": r"Date\s+of\s+Order[:：]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
            "Delivery_Date": r"Delivery\s+Date[:：]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})"
        }
    },
    "WWNZ": {
        "detect_keywords": ["WOOLWORTHS NZ", "WOOLWORTHS NEW ZEALAND", "PRODUCE ORDER NUMBER", "VENDOR COPY"],
        "store_regex": r"Deliver\s+To:\s*([0-9]{3,6})\s*\n\s*([^\n]+)",
        "header_extract": {
            "PO_Number": r"PRODUCE\s+ORDER\s+NUMBER\s*:\s*([0-9]+)",
            "Order_Date": r"Order\s+Date\s*:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
            "Delivery_Date": r"Delivery\s+Date\s*:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})"
        }
    },
    "MyFoodBag": {
        "detect_keywords": ["My Food Bag", "My Food Bag Limited", "GST Reg. No:"],
        "store_regex": r"(My\s*Food\s*Bag[\s\S]{0,200}?Christchurch\s*8042)",
        "header_extract": {
            "PO_Number": r"Purchase\s+(?:Order\s+)?No[:：]?\s*([0-9]+)",
            "Order_Date": r"Order\s*Date[:：]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})"
        }
    }
}

def parse_date_safe(s: str):
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date().isoformat()
        except Exception:
            continue
    return s

def extract(pattern, text, flags=re.I|re.S):
    if not pattern or not text:
        return None
    m = re.search(pattern, text, flags=flags)
    if not m:
        return None
    return m.group(1).strip() if m.lastindex else m.group(0).strip()

def detect_vendor(text_concat: str) -> str|None:
    t = (text_concat or "").upper()
    if any(kw in t for kw in [k.upper() for k in VENDOR_PROFILES["WWNZ"]["detect_keywords"]]):
        return "WWNZ"
    for v in ("Foodstuffs_NI","MyFoodBag"):
        if any(kw.upper() in t for kw in VENDOR_PROFILES[v]["detect_keywords"]):
            return v
    return None

def normalize_numeric(series):
    if series is None or len(series) == 0:
        return pd.Series(dtype=float)
    s = pd.Series(series)
    s = s.astype(str).str.replace(r"[\s,\$]", "", regex=True)
    s = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(s, errors="coerce")

def extract_text_from_pdf(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() + "\n"
    return text

def parse_foodstuffs(text):
    pat = re.compile(
        r"^\s*\d+\s+(?P<article>\d{6,})\s+[A-Z0-9$]+\s+.+?\s+(?P<qty>\d+)\s+[A-Z]{2,4}\s+\d+\s+\$?(?P<price>[\d,]+\.\d{2}).*?\$?[\d,]+\.\d{2}\s*$",
        re.I
    )
    rows = []
    for ln in text.splitlines():
        m = pat.match(ln)
        if m:
            rows.append({
                "item_id": m.group("article"),
                "quantity": m.group("qty"),
                "price": m.group("price").replace(',', ''),
            })
    return pd.DataFrame(rows)

def parse_wwnz(text):
    rows = []
    lines = text.splitlines()
    data_started = False
    for ln in lines:
        if re.search(r'LINE.*ITEM NO.*ORD QTY.*PRICE', ln, re.I):
            data_started = True
            continue
        if not data_started:
            continue
        if re.search(r'Order Totals|Total Value', ln, re.I):
            break
        match = re.match(r'^\s*(\d+)\s+(\d{8,14})\s+(.*?)\s+(\d{5,})\s+([\d.]+)\s+(\S+)\s+(\d+)\s+(\d+)\s+([\d.]+)', ln.strip())
        if match:
            _, _, _, item_no, _, _, _, qty, price = match.groups()
            rows.append({
                "item_id": item_no,
                "quantity": qty,
                "price": price,
            })
    return pd.DataFrame(rows)

def parse_mfb(text):
    rows = []
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    for i, ln in enumerate(lines):
        if re.search(r'item\s*no.*qty.*description', ln, re.I):
            for data_line in lines[i+1:]:
                if re.search(r'\btotal\b|balance\s+due|page\s+\d+', data_line, re.I):
                    break
                if re.match(r'^\s*10\d{6,}', data_line):
                    parts = re.split(r'\s{2,}', data_line.strip())
                    if len(parts) >= 5:
                        item_no = parts[0].strip()
                        qty = re.sub(r'[^\d.]', '', parts[1].strip())
                        price = re.sub(r'[^\d.]', '', parts[4].strip())
                        if qty and price and item_no:
                            rows.append({
                                "item_id": item_no,
                                "quantity": qty,
                                "price": price
                            })
            break
    return pd.DataFrame(rows)

def validate_dataframe(df):
    if df.empty:
        return df
    df = df.dropna(how='all')
    df = df[df['item_id'].notna() & (df['item_id'] != '')]
    if 'quantity' in df.columns:
        df['quantity'] = normalize_numeric(df['quantity'])
    if 'price' in df.columns:
        df['price'] = normalize_numeric(df['price'])
    return df[(df.get('quantity', 0) > 0) & (df.get('price', 0) > 0)].reset_index(drop=True)

with st.sidebar:
    st.header("选项")
    vendor_choice = st.selectbox("供应商", ["Auto","Foodstuffs_NI","WWNZ","MyFoodBag"], index=0)
    store_map_file = st.file_uploader("上传 store_map.csv（列：name,store_id）", type=["csv"])
    store_map_df = None
    if store_map_file:
        store_map_df = pd.read_csv(store_map_file)

uploaded = st.file_uploader("上传一个或多个 PO PDF", type=["pdf"], accept_multiple_files=True)

results = []
if uploaded:
    progress_bar = st.progress(0)
    for idx, f in enumerate(uploaded):
        progress_bar.progress((idx + 1) / len(uploaded))
        with tempfile.TemporaryDirectory() as td:
            fp = Path(td) / f.name
            fp.write_bytes(f.read())
            text = extract_text_from_pdf(str(fp))
        active_vendor = detect_vendor(text) if vendor_choice == "Auto" else vendor_choice
        if active_vendor == "Foodstuffs_NI":
            body = parse_foodstuffs(text)
        elif active_vendor == "WWNZ":
            body = parse_wwnz(text)
        else:
            body = parse_mfb(text)
        body = validate_dataframe(body)
        prof = VENDOR_PROFILES[active_vendor]
        sales_id = extract(prof["header_extract"].get("PO_Number"), text)
        order_date = extract(prof["header_extract"].get("Delivery_Date") or prof["header_extract"].get("Order_Date"), text)
        order_date = parse_date_safe(order_date)
        raw_name = extract(prof["store_regex"], text)
        out = pd.DataFrame({
            "store_id": None,
            "name": raw_name,
            "sales_id": sales_id,
            "order_date": order_date,
            "item_id": body.get("item_id"),
            "quantity": body.get("quantity"),
            "price": body.get("price")
        })
        results.append(out)
        csv_data = out.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label=f"⬇️ 下载 {Path(f.name).stem}.csv",
            data=csv_data,
            file_name=f"{Path(f.name).stem}.csv",
            mime="text/csv"
        )
    if results:
        merged = pd.concat(results, ignore_index=True)
        st.download_button(
            "⬇️ 下载合并 CSV（orders.csv）",
            merged.to_csv(index=False).encode("utf-8-sig"),
            file_name="orders.csv",
            mime="text/csv"
        )
    progress_bar.empty()
else:
    st.info("上传 PDF 开始解析。")
