# app.py (v5.3 Fixed) – CSV columns: store_id,name,sales_id,order_date,item_id,quantity,price
# Fixed issues:
# - WWNZ price extraction (was missing price column)
# - MyFoodBag fractional quantity parsing
# - Empty row pollution in CSV output
# - Store name cleaning for WWNZ
# - Better error handling and validation

import re
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

st.set_page_config(page_title="PO PDF → CSV (Fixed)", layout="wide")
st.title("PO PDF → CSV（修复版：store_id,name,sales_id,order_date,item_id,quantity,price）")

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
    except Exception as e:
        st.error(f"pdftotext failed: {e}")
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

def keyword_hits(text: str):
    text_u = (text or '').upper()
    def count(keys):
        return sum(text_u.count(k.upper()) for k in keys)
    return {
        'Foodstuffs_NI': count(VENDOR_PROFILES['Foodstuffs_NI']['detect_keywords']),
        'WWNZ': count(VENDOR_PROFILES['WWNZ']['detect_keywords']),
        'MyFoodBag': count(VENDOR_PROFILES['MyFoodBag']['detect_keywords'])
    }

def normalize_numeric(series):
    if series is None or len(series) == 0:
        return pd.Series(dtype=float)
    
    s = pd.Series(series)
    s = s.apply(lambda x: (x[0] if isinstance(x, (list, tuple)) and len(x)>0 else x))
    s = s.astype(str).str.replace(r"[\s,\$]", "", regex=True)
    s = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(s, errors="coerce")

def clean_store_name(vendor, raw_name):
    """Clean up store names, especially for WWNZ"""
    if not raw_name:
        return raw_name
        
    if vendor == "WWNZ":
        # Remove vendor numbers and extra text
        # "9793\nChristchurch FDC Produce" -> "Christchurch FDC Produce"
        cleaned = re.sub(r'\d{4}-?\s*-?\s*Vendor\s*Number:?\s*\d+', '', raw_name)
        cleaned = re.sub(r'^\d{3,5}\s*\n?\s*', '', cleaned)
        return cleaned.strip()
    
    return raw_name.strip() if raw_name else raw_name

def store_lookup(name_text, store_map_df):
    if store_map_df is None or not name_text:
        return None, None
    
    try:
        df = store_map_df.copy()
        df.columns = [c.strip().lower() for c in df.columns]
        key = str(name_text).strip().lower()
        
        # forward contains
        m = df[df.get("name","").astype(str).str.lower().str.contains(re.escape(key[:40]), na=False)]
        if not m.empty:
            return m.iloc[0].get("store_id"), m.iloc[0].get("name")
        
        # reverse contains
        m = df[df.get("name","").astype(str).str.lower().apply(lambda x: key in x if x else False)]
        if not m.empty:
            return m.iloc[0].get("store_id"), m.iloc[0].get("name")
        
        return None, None
    except Exception as e:
        st.warning(f"Store lookup error: {e}")
        return None, None

# ---------- vendor parsers (FIXED) ----------
def parse_foodstuffs(text):
    """Parse Foodstuffs PDF - this was working correctly"""
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
    """FIXED: Parse WWNZ PDF with proper price extraction"""
    # Expected format from PDF:
    # LINE ST GTIN/TUN ITEM DESCRIPTION ITEM NO TU SFX TI x HI OM ORD QTY PRICE EXCL TAXES PLU
    # 1    9334892005555 Ginger        734951  10.00 6x8 10     12   54.00
    
    rows = []
    lines = text.splitlines()
    
    # Find the data section
    data_started = False
    for ln in lines:
        # Look for the header line
        if re.search(r'LINE.*ITEM NO.*ORD QTY.*PRICE', ln, re.I):
            data_started = True
            continue
            
        if not data_started:
            continue
            
        # Stop at totals
        if re.search(r'Order Totals|Total Value', ln, re.I):
            break
            
        # Parse data lines - more flexible pattern
        # Match: LINE_NO GTIN DESCRIPTION ITEM_NO ... ORD_QTY PRICE
        match = re.match(r'^\s*(\d+)\s+(\d{8,14})\s+(.*?)\s+(\d{5,})\s+([\d.]+)\s+(\S+)\s+(\d+)\s+(\d+)\s+([\d.]+)', ln.strip())
        
        if match:
            line_no, gtin, desc, item_no, tu, sfx, om, qty, price = match.groups()
            rows.append({
                "item_id": item_no,
                "quantity": qty,
                "price": price,
            })
    
    return pd.DataFrame(rows)

def parse_mfb(text):
    """FIXED: Parse MyFoodBag PDF with correct quantity extraction"""
    # Expected format from PDF:
    # Item No. QTY DESCRIPTION                    Delivery Date PRICE  TOTAL
    # 10014642  20 Oranges, 1kg (imported)       20/08/25      3.80   76.00
    
    rows = []
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    
    # Find the header with Item No, QTY, etc.
    header_found = False
    for i, ln in enumerate(lines):
        if re.search(r'item\s*no.*qty.*description', ln, re.I):
            header_found = True
            # Start processing from next line
            for data_line in lines[i+1:]:
                # Stop at totals or end markers
                if re.search(r'\btotal\b|balance\s+due|page\s+\d+', data_line, re.I):
                    break
                
                # Look for lines starting with item numbers (10xxxxxxx for MFB)
                if re.match(r'^\s*10\d{6,}', data_line):
                    # Split the line carefully
                    parts = re.split(r'\s{2,}', data_line.strip())
                    
                    if len(parts) >= 5:  # Item No, QTY, Description, Date, Price, [Total]
                        item_no = parts[0].strip()
                        qty = parts[1].strip()
                        price = parts[4].strip() if len(parts) > 4 else parts[-2].strip()
                        
                        # Clean numeric values
                        qty = re.sub(r'[^\d.]', '', qty)
                        price = re.sub(r'[^\d.]', '', price)
                        
                        if qty and price and item_no:
                            rows.append({
                                "item_id": item_no,
                                "quantity": qty,
                                "price": price
                            })
            break
    
    return pd.DataFrame(rows)

def validate_dataframe(df, vendor_name):
    """Validate and clean the parsed dataframe"""
    if df.empty:
        return df
    
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Remove rows without item_id
    df = df[df['item_id'].notna() & (df['item_id'] != '')]
    
    # Ensure numeric columns
    if 'quantity' in df.columns:
        df['quantity'] = normalize_numeric(df['quantity'])
    if 'price' in df.columns:
        df['price'] = normalize_numeric(df['price'])
    
    # Remove rows with invalid data
    df = df[(df.get('quantity', 0) > 0) & (df.get('price', 0) > 0)]
    
    return df.reset_index(drop=True)

# ---------- Sidebar ----------
with st.sidebar:
    st.header("选项")
    vendor_choice = st.selectbox("供应商", ["Auto","Foodstuffs_NI","WWNZ","MyFoodBag"], index=0)
    store_map_file = st.file_uploader("上传 store_map.csv（列：name,store_id）", type=["csv"])
    
    store_map_df = None
    if store_map_file:
        try:
            store_map_df = pd.read_csv(store_map_file)
            # Validate required columns
            required_cols = ['name', 'store_id']
            missing_cols = [col for col in required_cols if col not in store_map_df.columns]
            if missing_cols:
                st.error(f"store_map.csv 缺少必需列: {missing_cols}")
                store_map_df = None
            else:
                st.caption(f"✅ 已加载门店映射，共 {len(store_map_df)} 条记录")
        except Exception as e:
            st.error(f"store_map 读取失败：{e}")

uploaded = st.file_uploader("上传一个或多个 PO PDF", type=["pdf"], accept_multiple_files=True)

# ---------- Main Processing ----------
results = []
if uploaded:
    progress_bar = st.progress(0)
    
    for idx, f in enumerate(uploaded):
        progress_bar.progress((idx + 1) / len(uploaded))
        
        # Save to temp file for pdftotext
        with tempfile.TemporaryDirectory() as td:
            fp = Path(td) / f.name
            fp.write_bytes(f.read())
            text = pdftotext_layout(str(fp))

        if not text:
            st.error(f"{f.name}: PDF 文本提取失败")
            continue

        hits = keyword_hits(text)
        active_vendor = detect_vendor(text) if vendor_choice == "Auto" else vendor_choice

        # Auto fallback: try all parsers and pick the one with most rows
        body = None
        chosen_by_rows = None
        if active_vendor is None and vendor_choice == "Auto":
            fs_df = validate_dataframe(parse_foodstuffs(text), "Foodstuffs_NI")
            ww_df = validate_dataframe(parse_wwnz(text), "WWNZ")
            mfb_df = validate_dataframe(parse_mfb(text), "MyFoodBag")
            
            sizes = {'Foodstuffs_NI': len(fs_df), 'WWNZ': len(ww_df), 'MyFoodBag': len(mfb_df)}
            if any(sizes.values()):
                chosen_by_rows = max(sizes, key=lambda k: sizes[k])
                active_vendor = chosen_by_rows
                body = {'Foodstuffs_NI': fs_df, 'WWNZ': ww_df, 'MyFoodBag': mfb_df}[chosen_by_rows]

        if active_vendor is None:
            st.error(f"{f.name}: 未识别供应商（关键词命中：{hits}）。可在左侧手动选择供应商后再试。")
            continue

        # Extract header information
        prof = VENDOR_PROFILES[active_vendor]
        sales_id = extract(prof["header_extract"].get("PO_Number"), text)
        order_date = extract(prof["header_extract"].get("Delivery_Date") or prof["header_extract"].get("Order_Date"), text)
        order_date = parse_date_safe(order_date)
        
        # Extract and clean store name
        raw_name = extract(prof["store_regex"], text)
        name_txt = clean_store_name(active_vendor, raw_name)

        # Parse vendor data if not already done
        if body is None:
            try:
                if active_vendor == "Foodstuffs_NI":
                    body = parse_foodstuffs(text)
                elif active_vendor == "WWNZ":
                    body = parse_wwnz(text)
                else:
                    body = parse_mfb(text)
                
                body = validate_dataframe(body, active_vendor)
            except Exception as e:
                st.error(f"{f.name}: 解析失败 - {e}")
                continue

        if body.empty:
            st.warning(f"{f.name}: 未解析到任何产品数据")
            continue

        # Store lookup
        store_id, mapped_name = store_lookup(name_txt, store_map_df)
        final_name = mapped_name or name_txt

        # Create output dataframe
        out = pd.DataFrame({
            "store_id": store_id,
            "name": final_name,
            "sales_id": sales_id,
            "order_date": order_date,
            "item_id": body.get("item_id"),
            "quantity": body.get("quantity"),
            "price": body.get("price")
        })

        # Final cleanup - remove any remaining empty rows
        out = out.dropna(subset=['item_id'])
        out = out[out['item_id'] != '']

        # Display results
        info = f"**📄 {f.name}** · 供应商：**{active_vendor}**"
        if chosen_by_rows:
            info += "（自动回退：按解析到的行数判定）"
        
        total_items = len(out)
        total_value = out['price'].sum() if 'price' in out.columns else 0
        missing_store = out['store_id'].isna().sum()
        
        info += f" · {total_items} 项产品 · 总值: ${total_value:.2f}"
        if missing_store > 0:
            info += f" · ⚠️ {missing_store} 项缺少store_id"
        
        st.markdown(info)
        st.dataframe(out, use_container_width=True)
        
        # Download individual file
        csv_data = out.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label=f"⬇️ 下载 {Path(f.name).stem}.csv",
            data=csv_data,
            file_name=f"{Path(f.name).stem}.csv",
            mime="text/csv",
            key=f"dl_{f.name}"
        )

        results.append(out)

    # Merge all results
    if results:
        merged = pd.concat(results, ignore_index=True)
        # Final cleanup of merged data
        merged = merged.dropna(subset=['item_id'])
        merged = merged[merged['item_id'] != '']
        
        st.markdown("---")
        st.markdown(f"**📊 合并统计**: {len(merged)} 项产品，总值 ${merged['price'].sum():.2f}")
        
        st.download_button(
            "⬇️ 下载合并 CSV（orders.csv）",
            merged.to_csv(index=False).encode("utf-8-sig"),
            file_name="orders.csv",
            mime="text/csv"
        )
    
    progress_bar.empty()
else:
    st.info("上传 PDF 开始解析。")

# Show helpful tips
with st.expander("💡 使用提示"):
    st.markdown("""
    **修复内容 (v5.3)**:
    - ✅ 修复 WWNZ 缺少价格列的问题
    - ✅ 修复 MyFoodBag 小数数量错误
    - ✅ 清理 CSV 中的空行
    - ✅ 改进店铺名称清理
    - ✅ 增加数据验证和错误处理
    
    **支持格式**:
    - **Foodstuffs**: Article Number, Order Qty, Price Per Ord. Unit
    - **WWNZ**: ITEM NO, ORD QTY, PRICE EXCL TAXES  
    - **MyFoodBag**: Item No, QTY, PRICE
    
    **故障排除**:
    - 确保 PDF 是文本格式（非扫描图片）
    - 检查 store_map.csv 格式：name,store_id
    - 如自动识别失败，请手动选择供应商
    """)
