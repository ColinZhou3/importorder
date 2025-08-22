# app.py (v5.1 Minimal) — CSV columns: store_id,name,sales_id,order_date,item_id,quantity,price
# Mapping rules fixed per vendor:
# - Foodstuffs: item_id=Article Number (first 6+ digit token), quantity=Order Qty, price=Price Per Ord. Unit; store name from "Delivery To"
# - WWNZ:       item_id=ITEM NO,       quantity=ORD QTY,     price=PRICE EXCL;               store name from "Deliver To"
# - MyFoodBag:  item_id=Item No,       quantity=QTY,         price=PRICE;                    store name from "My Food Bag ... Christchurch 8042"
#
# store_id is looked up from store_map.csv (columns: name,store_id) using fuzzy match on name.
# If not matched, store_id left blank. You can edit value in your CSV or update the mapping file.

import re
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

st.set_page_config(page_title="PO PDF → CSV (Minimal)", layout="wide")
st.title("PO PDF → CSV（最小化导出：store_id,name,sales_id,order_date,item_id,quantity,price）")

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

# ---------- helpers ----------
def pdftotext_layout(pdf_path: str) -> str:
    try:
        out = subprocess.check_output(["pdftotext", "-layout", pdf_path, "-"], stderr=subprocess.STDOUT)
        return out.decode("utf-8", errors="ignore")
    except Exception:
        return ""

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
    # prefer WWNZ first
    if any(kw in t for kw in [k.upper() for k in VENDOR_PROFILES["WWNZ"]["detect_keywords"]]):
        return "WWNZ"
    for v in ("Foodstuffs_NI","MyFoodBag"):
        if any(kw.upper() in t for kw in VENDOR_PROFILES[v]["detect_keywords"]):
            return v
    return None

def normalize_numeric(series):
    s = pd.Series(series)
    s = s.apply(lambda x: (x[0] if isinstance(x, (list, tuple)) and len(x)>0 else x))
    s = s.astype(str).str.replace(r"[\s,\$]", "", regex=True)
    s = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(s, errors="coerce")

def store_lookup(name_text, store_map_df):
    if store_map_df is None or not name_text:
        return None, None
    df = store_map_df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    key = str(name_text).strip().lower()
    # forward contains
    m = df[df.get("name","").astype(str).str.lower().str.contains(re.escape(key[:40]))]
    if not m.empty:
        return m.iloc[0].get("store_id"), m.iloc[0].get("name")
    # reverse contains
    m = df[df.get("name","").astype(str).str.lower().apply(lambda x: key in x)]
    if not m.empty:
        return m.iloc[0].get("store_id"), m.iloc[0].get("name")
    return None, None

# ---------- vendor parsers (line-based) ----------
def parse_foodstuffs(text):
    # Example: "130 5251600 CARTON GINGER 5KG ... 9 EA 5 26.69 ... 229.32"
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
                "price": m.group("price"),
            })
    return pd.DataFrame(rows)

def parse_wwnz(text):
    # Expect columns: GTIN  ITEM DESCRIPTION  ITEM NO  OM  TIxHI  ORD QTY  PRICE EXCL ...
    pat = re.compile(
        r"^\s*\d+\s+\d{8,14}\s+.+?\s+(?P<itemno>\d{5,})\s+\S+\s+\S+\s+(?P<qty>[\d,]+)\s+\$?(?P<price>[\d,]+\.\d{2})\s*$",
        re.I
    )
    rows = []
    for ln in text.splitlines():
        m = pat.match(ln)
        if m:
            rows.append({
                "item_id": m.group("itemno"),
                "quantity": m.group("qty"),
                "price": m.group("price"),
            })
    return pd.DataFrame(rows)

def parse_mfb(text):
    # Find header then split rows by 2+ spaces: Item No | QTY | DESCRIPTION | Delivery Date | PRICE | TOTAL
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    hdr = -1
    for i, ln in enumerate(lines):
        if re.search(r"item\s*no", ln, re.I) and re.search(r"desc", ln, re.I):
            hdr = i
            break
    if hdr == -1:
        return pd.DataFrame()
    rows = []
    for ln in lines[hdr+1:]:
        if re.search(r"\btotal\b", ln, re.I):
            break
        parts = re.split(r"\s{2,}", ln.strip())
        if len(parts) < 5:
            continue
        itemno = parts[0]; qty = parts[1]
        price = parts[-2] if re.search(r"\d", parts[-2]) else parts[-1]
        rows.append({"item_id": itemno, "quantity": qty, "price": price})
    return pd.DataFrame(rows)

# ---------- Sidebar ----------
with st.sidebar:
    st.header("选项")
    vendor_choice = st.selectbox("供应商", ["Auto","Foodstuffs_NI","WWNZ","MyFoodBag"], index=0)
    store_map_file = st.file_uploader("上传 store_map.csv（列：name,store_id）", type=["csv"])
    store_map_df = None
    if store_map_file:
        try:
            store_map_df = pd.read_csv(store_map_file)
            st.caption("已加载门店映射，用 name 模糊匹配到 store_id。")
        except Exception as e:
            st.error(f"store_map 读取失败：{e}")

uploaded = st.file_uploader("上传一个或多个 PO PDF", type=["pdf"], accept_multiple_files=True)

# ---------- Main ----------
results = []
if uploaded:
    for f in uploaded:
        # Always save to temp file for pdftotext
        with tempfile.TemporaryDirectory() as td:
            fp = Path(td)/f.name
            fp.write_bytes(f.read())
            text = pdftotext_layout(str(fp))

        active_vendor = detect_vendor(text) if vendor_choice=="Auto" else vendor_choice
        if not active_vendor:
            st.error(f"{f.name}: 未识别供应商")
            continue
        prof = VENDOR_PROFILES[active_vendor]

        sales_id = extract(prof["header_extract"].get("PO_Number"), text)
        # order_date 优先 Delivery_Date，其次 Order_Date
        order_date = extract(prof["header_extract"].get("Delivery_Date") or prof["header_extract"].get("Order_Date"), text)
        order_date = parse_date_safe(order_date)
        name_txt = extract(prof["store_regex"], text)

        if active_vendor == "Foodstuffs_NI":
            body = parse_foodstuffs(text)
        elif active_vendor == "WWNZ":
            body = parse_wwnz(text)
        else:
            body = parse_mfb(text)

        body["quantity"] = normalize_numeric(body["quantity"])
        body["price"] = normalize_numeric(body["price"])

        store_id, mapped_name = store_lookup(name_txt, store_map_df)
        final_name = mapped_name or name_txt

        out = pd.DataFrame({
            "store_id": store_id,
            "name": final_name,
            "sales_id": sales_id,
            "order_date": order_date,
            "item_id": body.get("item_id"),
            "quantity": body.get("quantity"),
            "price": body.get("price")
        })

        st.markdown(f"**📄 {f.name}** · 供应商：**{active_vendor}**")
        st.dataframe(out, use_container_width=True)
        # Per-file download
        st.download_button(
            label=f"⬇️ 下载 {Path(f.name).stem}.csv",
            data=out.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"{Path(f.name).stem}.csv",
            mime="text/csv",
            key=f"dl_{f.name}"
        )

        results.append(out)

    if results:
        merged = pd.concat(results, ignore_index=True)
        st.download_button("⬇️ 下载合并 CSV（orders.csv）",
                           merged.to_csv(index=False).encode("utf-8-sig"),
                           file_name="orders.csv",
                           mime="text/csv")
else:
    st.info("上传 PDF 开始解析。")
