# app.py (robust v2)
# PO PDF → CSV 统一解析与导出（三家供应商 + 双导入模版）
# 重点增强：
# - 文本提取增加 `pdftotext -layout` 回退（packages.txt 已包含 poppler-utils）
# - MFB 与 WWNZ 的逐行正则更宽松（支持 $、千分位、可变空格）
# - Foodstuffs/MFB 的抬头/店面正则更准确
# - ItemNo 列广谱识别并保留，避免导出 item_id 为空
# - Tabula 启用 stream=True 提高无边框表解析成功率

import re
import json
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

st.set_page_config(page_title="PO PDF → CSV Converter", layout="wide")
st.title("PO PDF → CSV 转换器（三家供应商 & 双导入模版｜增强版）")

VENDOR_PROFILES = {
    "Foodstuffs_NI": {
        "detect_keywords": ["Foodstuffs North Island Limited", "Order Forecast", "O/F"],
        "engine": "camelot-lattice",
        "pages": "all",
        # 抓到整行直到换行，避免把后续的 "Order Forecast Number" 拼进来
        "store_regex": r"(?:Delivery\s+To|Delivery\s+Address)[:：]?\s*([^\n]+)",
        "column_map": {
            "Line": "Line",
            "Item #": "ItemNo",
            "Item#": "ItemNo",
            "Article Number": "Article",
            "Product Description": "Item",
            "Order Qty": "Qty",
            "Purchasing Unit of Measure": "UoM",
            "Units Per Purchasing UoM": "UnitsPerUoM",
            "Price Per Ord. Unit": "Unit Price",
            "Total Price": "Line Total"
        },
        "skip_total_rows": True,
        "header_extract": {
            "PO_Number": r"Order\s+Forecast\s+Number[:：]?\s*([0-9]+)",
            "Order_Date": r"Date\s+of\s+Order[:：]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
            "Delivery_Date": r"Delivery\s+Date[:：]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})"
        },
        "line_regex": r"""
          ^\s*(\d+)\s+                 # Line
          (\d+)\s+                     # Item #
          ([A-Za-z0-9\-]+)\s+          # Article
          (.+?)\s+                     # Product Description
          ([0-9,]+)\s+                 # Order Qty
          ([A-Z]{2,4})\s+              # UoM
          ([0-9,]+)\s+                 # Units Per UoM
          \$?([\d,]+\.\d{2})\s+        # Unit Price
          \$?([\d,]+\.\d{2})\s+        # Net (忽略)
          -?\$?([\d,]+\.\d{2})\s+      # Term Fee (忽略)
          \$?([\d,]+\.\d{2})\s*$       # Line Total
        """
    },
    "MyFoodBag": {
        "detect_keywords": ["My Food Bag Limited", "PURCHASE ORDER", "GST Reg. No:"],
        "engine": "camelot-stream",
        "pages": "all",
        # 只取 Delivery Instructions 行；若没有就留空，后续用 Vendor 名兜底
        "store_regex": r"Delivery\s+Instructions[:：]?\s*([^\n]+)",
        "column_map": {
            "Item No.": "ItemNo",
            "ITEM NO.": "ItemNo",
            "Item No": "ItemNo",
            "QTY": "Qty",
            "DESCRIPTION": "Item",
            "Delivery Date": "Delivery_Date",
            "PRICE": "Unit Price",
            "TOTAL": "Line Total"
        },
        "skip_total_rows": True,
        "header_extract": {
            "PO_Number": r"Purchase\s+No[:：]?\s*([0-9]+)",
            "Order_Date": r"Order\s*Date[:：]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})"
        },
        "line_regex": r"""
          ^\s*([0-9]{6,})\s+           # Item No
          ([0-9,]+)\s+                 # QTY
          (.+?)\s+                     # DESCRIPTION
          ([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})\s+  # Delivery Date
          \$?([\d,]+\.\d{2})\s+        # PRICE
          \$?([\d,]+\.\d{2})\s*$       # TOTAL
        """
    },
    "WWNZ": {
        "detect_keywords": [
            "WOOLWORTHS NZ", "NEW PURCHASE ORDER", "VENDOR COPY",
            "WOOLWORTHS NEW ZEALAND", "PRODUCE ORDER NUMBER"
        ],
        "engine": "camelot-stream",
        "pages": "all",
        "store_regex": r"Deliver\s+To:\s*([0-9]{3,6})\s*\n\s*([^\n]+)",
        "column_map": {
            "ITEM DESCRIPTION": "Item",
            "ITEM NO": "ItemNo",
            "ITEM#": "ItemNo",
            "ORD QTY": "Qty",
            "PRICE EXCL": "Unit Price"
        },
        "skip_total_rows": True,
        "header_extract": {
            "PO_Number": r"PRODUCE\s+ORDER\s+NUMBER\s*:\s*([0-9]+)",
            "Order_Date": r"Order\s+Date\s*:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
            "Delivery_Date": r"Delivery\s+Date\s*:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})",
            "Delivery_Time": r"Delivery\s+Time\s*:\s*([0-9:\s]{4,})"
        },
        "line_regex": r"""
          ^\s*\d+\s+                    # 行号
          (?P<gtin>\d{8,14})\s+         # GTIN
          (?P<item>.+?)\s+              # 描述
          (?P<itemno>\d+)\s+            # ITEM NO
          (?P<om>[\d\.]+)\s+            # OM (可忽略)
          (?P<tihi>\d+x\d+)\s+          # TIxHI (可忽略)
          (?P<ordqty>[\d,]+)\s+         # ORD QTY
          (?P<taxes>\d+)\s+             # TAXES (可忽略)
          \$?(?P<price>[\d,]+\.\d{2})\s*$ # PRICE EXCL
        """
    }
}

# ---------- 工具函数 ----------
def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    return df

def normalize_numeric(series):
    """稳健数值清洗"""
    s = pd.Series(series)
    s = s.apply(lambda x: (x[0] if isinstance(x, (list, tuple)) and len(x) > 0 else x))
    s = s.astype(str)
    s = s.str.replace(r"[\s,\$]", "", regex=True)
    s = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(s, errors="coerce")

def pick_first_existing(df, candidates):
    for c in candidates or []:
        if c in df.columns:
            return c
    return None

def find_col_by_regex(df, pattern_list):
    for pat in pattern_list:
        for c in df.columns:
            if re.search(pat, str(c), re.I):
                return c
    return None

def parse_date_safe(s: str):
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date().isoformat()
        except Exception:
            continue
    return s

def extract_by_regex(pattern, text):
    if not pattern or not text:
        return None
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip() if m.lastindex else m.group(0).strip()
    return None

def extract_store(text, store_re):
    val = extract_by_regex(store_re, text)
    site_code, store_name = None, None
    if isinstance(val, list):
        if len(val) >= 2 and re.fullmatch(r"\d{3,6}", val[0] or ""):
            site_code, store_name = val[0], val[1]
        elif len(val) >= 1:
            store_name = val[0]
    elif isinstance(val, str):
        if re.fullmatch(r"\d{3,6}", val):
            site_code = val
        else:
            store_name = val
    return site_code, store_name

def detect_vendor(text_concat: str) -> str | None:
    text_upper = (text_concat or "").upper()
    for vendor, cfg in VENDOR_PROFILES.items():
        for kw in cfg.get("detect_keywords", []):
            if kw.upper() in text_upper:
                return vendor
    return None

# ---------- 解析引擎 ----------
def parse_with_pdfplumber(pdf_path, pages_spec):
    import pdfplumber
    dfs, raw_texts = [], []
    with pdfplumber.open(pdf_path) as pdf:
        idx = range(len(pdf.pages))
        if pages_spec != "all":
            wanted = set()
            for seg in pages_spec.split(","):
                seg = seg.strip()
                if "-" in seg:
                    a, b = seg.split("-")
                    wanted |= set(range(int(a)-1, int(b)))
                else:
                    wanted.add(int(seg)-1)
            idx = [i for i in idx if i in wanted]
        for i in idx:
            page = pdf.pages[i]
            raw_texts.append(page.extract_text() or "")
            # 优先尝试 extract_tables（多表）
            tables = []
            try:
                tables = page.extract_tables() or []
            except Exception:
                tbl = page.extract_table()
                if tbl and len(tbl) > 1:
                    tables = [tbl]
            for table in tables:
                if table and len(table) > 1:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    dfs.append(df)
    return dfs, "\n".join(raw_texts)

def parse_with_camelot(pdf_path, pages_spec, flavor):
    import camelot
    pages_arg = pages_spec if pages_spec != "all" else "all"
    tables = camelot.read_pdf(pdf_path, pages=pages_arg, flavor=flavor, strip_text="\n")
    dfs = []
    for t in tables:
        df = t.df
        if df.shape[0] > 1:
            new_df = df.copy()
            new_df.columns = new_df.iloc[0]
            new_df = new_df.iloc[1:].reset_index(drop=True)
            dfs.append(new_df)
    return dfs

def parse_with_tabula(pdf_path, pages_spec):
    from tabula import read_pdf
    pages_arg = pages_spec if pages_spec != "all" else "all"
    # 打开 stream 模式，适合无边框表格
    dfs = read_pdf(pdf_path, pages=pages_arg, multiple_tables=True, lattice=False, stream=True, guess=True) or []
    return dfs

def ocr_pages_to_text(pdf_path):
    from pdf2image import convert_from_path
    import pytesseract
    images = convert_from_path(pdf_path, dpi=200)
    texts = []
    for img in images:
        texts.append(pytesseract.image_to_string(img))
    return "\n".join(texts)

def pdftotext_layout(pdf_path):
    try:
        out = subprocess.check_output(["pdftotext", "-layout", pdf_path, "-"], stderr=subprocess.STDOUT)
        return out.decode("utf-8", errors="ignore")
    except Exception:
        return ""

def coerce_to_standard(df, item_candidates, qty_candidates, price_candidates, fallback_store, id_candidates=None):
    df = clean_cols(df)

    # 款式更松的列名识别
    item_col = pick_first_existing(df, item_candidates) or find_col_by_regex(df, [r"\bdesc(ription)?\b", r"product", r"品名"]) or df.columns[0]
    qty_col = pick_first_existing(df, qty_candidates) or find_col_by_regex(df, [r"\bqty\b", r"quantity", r"订单?数量", r"ord\s*qty"])
    price_col = pick_first_existing(df, price_candidates) or find_col_by_regex(df, [r"unit\s*price|price\s*excl|价格|单价"])

    out = pd.DataFrame()
    out["Item"] = df[item_col].astype(str)
    out["Qty"] = normalize_numeric(df[qty_col]) if (qty_col and qty_col in df.columns) else pd.NA
    out["Unit Price"] = normalize_numeric(df[price_col]) if (price_col and price_col in df.columns) else pd.NA

    # ItemNo 识别
    if id_candidates is None:
        id_candidates = ["ItemNo","Item #","Item#","Item No.","ITEM NO","Article Number","Article","SKU","PLU","GTIN","UPC","Barcode"]
    id_col = pick_first_existing(df, id_candidates) or find_col_by_regex(df, [r"item\s*#?", r"item\s*no", r"article", r"sku", r"gtin|upc|plu|barcode"])
    out["ItemNo"] = df[id_col].astype(str) if id_col and id_col in df.columns else pd.NA

    out["Store"] = fallback_store
    return out

# ---------- 导出模版 ----------
def map_store_to_ids(site_code, store_name, store_map_df):
    if store_map_df is None:
        return None, None
    df = store_map_df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    sid, sname = None, None
    if site_code:
        m = df[df.get("site_code", "").astype(str).str.strip() == str(site_code).strip()]
        if not m.empty:
            return m.iloc[0].get("store_id"), m.iloc[0].get("name")
    if store_name:
        key = str(store_name).strip().lower()
        m = df[df.get("name", "").astype(str).str.lower().str.contains(re.escape(key))]
        if not m.empty:
            return m.iloc[0].get("store_id"), m.iloc[0].get("name")
    return None, None

def to_DCorder_template(df):
    return pd.DataFrame({
        "store_id": df.get("store_id"),
        "name": df.get("name"),
        "sales_id": df.get("PO_Number"),
        "order_date": df.get("Delivery_Date").fillna(df.get("Order_Date")),
        "item_id": df.get("ItemNo"),
        "quantity": df.get("Qty"),
        "price": df.get("Unit Price")
    })

def to_CDDCorder_template(df):
    return pd.DataFrame({
        "store_id": df.get("store_id"),
        "name": df.get("name"),
        "sales_id": df.get("PO_Number"),
        "order_date": df.get("Delivery_Date").fillna(df.get("Order_Date")),
        "item_id": df.get("ItemNo"),
        "quantity": df.get("Qty")
    })

def choose_format_by_vendor(vendor: str) -> str:
    if (vendor or "").upper() == "WWNZ":
        return "CDDCorder"
    return "DCorder"

# ---------- 主解析 ----------
def try_parse(pdf_bytes, file_name, vendor_choice, enable_ocr, default_engine, default_pages, default_store_regex,
              item_cols_guess, qty_cols_guess, price_cols_guess):
    with tempfile.TemporaryDirectory() as td:
        fp = Path(td) / file_name
        fp.write_bytes(pdf_bytes)

        # 文本提取：pdfplumber → pdftotext → OCR
        text_concat = ""
        try:
            import pdfplumber
            with pdfplumber.open(str(fp)) as pdf:
                text_concat = "\n".join([p.extract_text() or "" for p in pdf.pages])
        except Exception:
            text_concat = ""
        if not (text_concat or "").strip():
            text_concat = pdftotext_layout(str(fp))
        if enable_ocr and not (text_concat or "").strip():
            try:
                text_concat = ocr_pages_to_text(str(fp))
            except Exception:
                pass

        # 供应商配置
        active_vendor = detect_vendor(text_concat) if vendor_choice == "Auto" else (vendor_choice if vendor_choice in VENDOR_PROFILES else None)
        profile = VENDOR_PROFILES.get(active_vendor, {}) if active_vendor else {}
        engine_used = profile.get("engine", default_engine)
        pages_used = profile.get("pages", default_pages)
        store_re_used = profile.get("store_regex", default_store_regex)
        colmap = profile.get("column_map", {})
        skip_total_rows = profile.get("skip_total_rows", True)
        header_extract = profile.get("header_extract", {})
        line_regex = profile.get("line_regex", "")

        # 抬头字段
        header = {}
        for k, pat in header_extract.items():
            v = extract_by_regex(pat, text_concat)
            header[k] = v

        # 店面/仓
        site_code, store_name = extract_store(text_concat, store_re_used) if text_concat else (None, None)

        # 表格解析：Camelot → Tabula(stream) → pdfplumber
        tables = []
        try:
            if engine_used.startswith("camelot"):
                flavor = "lattice" if engine_used.endswith("lattice") else "stream"
                tables = parse_with_camelot(str(fp), pages_used, flavor)
        except Exception as e:
            st.warning(f"[{file_name}] Camelot 解析失败：{e}")
        if not tables:
            try:
                tables = parse_with_tabula(str(fp), pages_used)
            except Exception:
                pass
        if not tables:
            try:
                tables, text2 = parse_with_pdfplumber(str(fp), pages_used)
                if text2 and (not text_concat):
                    text_concat = text2
            except Exception:
                pass

        # 构建列候选
        item_candidates = [k for k, v in colmap.items() if v == "Item"] or [c.strip() for c in item_cols_guess.split(",")]
        qty_candidates = [k for k, v in colmap.items() if v == "Qty"] or [c.strip() for c in qty_cols_guess.split(",")]
        price_candidates = [k for k, v in colmap.items() if v == "Unit Price"] or [c.strip() for c in price_cols_guess.split(",")]
        id_candidates = [k for k, v in colmap.items() if v in ("ItemNo","ITEM NO")] or ["ItemNo","Item #","Item#","Item No.","ITEM NO","Article Number","Article","SKU","PLU","GTIN","UPC","Barcode"]

        # 规范化
        normalized_tables = []
        if tables:
            for t in tables:
                try:
                    df = pd.DataFrame(t)
                    if df.empty or df.shape[1] < 1:
                        continue
                    df = clean_cols(df)
                    # 列重命名
                    if colmap:
                        rename_map = {src: dst for src, dst in colmap.items() if src in df.columns}
                        if rename_map:
                            df = df.rename(columns=rename_map)
                    norm = coerce_to_standard(df, item_candidates, qty_candidates, price_candidates, store_name, id_candidates=id_candidates)
                    norm = norm.dropna(how="all")
                    norm = norm[norm["Item"].astype(str).str.strip() != ""]
                    if skip_total_rows:
                        norm = norm[~norm["Item"].astype(str).str.fullmatch(r"(?i)(total|合计)")]
                    if not norm.empty:
                        normalized_tables.append(norm)
                except Exception:
                    continue

        # 逐行正则回退
        if not normalized_tables and line_regex and text_concat:
            rows = []
            pat = re.compile(line_regex, re.X | re.I)
            for line in text_concat.splitlines():
                m = pat.match(line)
                if not m:
                    continue
                if active_vendor == "Foodstuffs_NI":
                    Line, ItemNo, Article, Item, Qty, UoM, UnitsPerUoM, UnitPrice, _Net, _TermFee, LineTotal = m.groups()
                    rows.append({
                        "Line": int(Line),
                        "ItemNo": ItemNo,
                        "Article": Article,
                        "Item": Item.strip(),
                        "Qty": float(Qty.replace(",", "")),
                        "UoM": UoM,
                        "UnitsPerUoM": float(UnitsPerUoM.replace(",", "")),
                        "Unit Price": float(UnitPrice.replace(",", "")),
                        "Line Total": float(LineTotal.replace(",", "")),
                        "Store": store_name
                    })
                elif active_vendor == "MyFoodBag":
                    ItemNo, Qty, Item, DeliveryDate, UnitPrice, LineTotal = m.groups()
                    rows.append({
                        "ItemNo": ItemNo,
                        "Item": Item.strip(),
                        "Qty": float(Qty.replace(",", "")),
                        "Delivery_Date": DeliveryDate,
                        "Unit Price": float(UnitPrice.replace(",", "")),
                        "Line Total": float(LineTotal.replace(",", "")),
                        "Store": store_name
                    })
                elif active_vendor == "WWNZ":
                    gd = m.groupdict()
                    rows.append({
                        "ItemNo": gd.get("itemno"),
                        "Item": gd.get("item").strip(),
                        "Qty": float(gd.get("ordqty").replace(",", "")),
                        "Unit Price": float(gd.get("price").replace(",", "")),
                        "Store": store_name
                    })
            if rows:
                normalized_tables.append(pd.DataFrame(rows))

        merged = pd.concat(normalized_tables, ignore_index=True) if normalized_tables else pd.DataFrame(columns=["Item","Qty","Unit Price","Store"])

        # 标准字段
        merged.insert(0, "Vendor", active_vendor or "Unknown")
        merged.insert(1, "PO_Number", header.get("PO_Number"))
        merged.insert(2, "Order_Date", parse_date_safe(header.get("Order_Date")))
        merged.insert(3, "Delivery_Date", parse_date_safe(header.get("Delivery_Date")))
        merged.insert(4, "Delivery_Time", header.get("Delivery_Time"))
        merged.insert(5, "SiteCode", site_code)

        # 数值清洗（双保险）
        if "Qty" in merged.columns:
            merged["Qty"] = normalize_numeric(merged["Qty"])
        if "Unit Price" in merged.columns:
            merged["Unit Price"] = normalize_numeric(merged["Unit Price"])
        if "Line Total" in merged.columns:
            merged["Line Total"] = normalize_numeric(merged["Line Total"])

        # 去除表头噪声
        if "Item" in merged.columns:
            merged = merged[~merged["Item"].astype(str).str.match(r"(?i)^(item|sku|code|description|品名|合计|total)$")]

        # 识别提示
        if active_vendor:
            st.caption(f"识别到供应商：**{active_vendor}**（引擎：{engine_used} | 页码：{pages_used}）")
        else:
            st.caption(f"未识别供应商（使用当前选择/默认：引擎 {engine_used} | 页码 {pages_used}）")

        return merged

# ---------- 侧边栏 ----------
with st.sidebar:
    st.header("解析设置")
    vendor_choice = st.selectbox("供应商", ["Auto"] + list(VENDOR_PROFILES.keys()), index=0)

    default_engine = "camelot-stream"
    default_pages = "all"
    default_store_re = r"(Store|门店|店面)[:：]?\s*([A-Za-z0-9\- _]+)"
    if vendor_choice != "Auto":
        vcfg = VENDOR_PROFILES[vendor_choice]
        default_engine = vcfg.get("engine", default_engine)
        default_pages = vcfg.get("pages", default_pages)
        default_store_re = vcfg.get("store_regex", default_store_re)

    engine = st.selectbox("首选解析引擎", ["camelot-lattice", "camelot-stream", "tabula", "pdfplumber"],
                          index=["camelot-lattice","camelot-stream","tabula","pdfplumber"].index(default_engine))
    pages = st.text_input("页码范围（如 1 或 1-3 或 all）", value=default_pages)
    enable_ocr = st.checkbox("扫描件启用 OCR（较慢）", value=False)

    item_cols_guess = st.text_input("商品列候选（逗号分隔）", "Item,Description,DESCRIPTION,Product Description,品名")
    qty_cols_guess = st.text_input("数量列候选（逗号分隔）", "Qty,Quantity,QTY,数量,Order Qty,ORD QTY,ORD QTY.")
    price_cols_guess = st.text_input("单价列候选（逗号分隔）", "Unit Price,PRICE,Price,单价,Price Per Ord. Unit,PRICE EXCL")
    store_regex = st.text_input("店面/仓库识别正则（可自动识别SiteCode+Store）", default_store_re)
    merge_to_one = st.checkbox("合并多个文件为一个 CSV", value=True)

    st.subheader("导出设置")
    export_format = st.selectbox("导出模版", ["Auto by Vendor", "DCorder (含 price)", "CDDCorder (不含 price)"], index=0)
    store_map_file = st.file_uploader("上传门店映射 store_map.csv（可选）", type=["csv"], key="store_map_uploader")
    store_map_df = None
    if store_map_file:
        try:
            store_map_df = pd.read_csv(store_map_file)
            store_map_df.columns = [c.strip().lower() for c in store_map_df.columns]
            st.success("已加载门店映射，将优先用 site_code 匹配，其次用 name 模糊匹配。")
        except Exception as e:
            st.error(f"门店映射读取失败：{e}")

uploaded = st.file_uploader("上传一个或多个 PO PDF", type=["pdf"], accept_multiple_files=True)

# ---------- 主流程 ----------
results = []
if uploaded:
    st.info("提示：WWNZ/My Food Bag 多为无边框文本列，优先用 camelot-stream；Foodstuffs 若表格带边框，用 camelot-lattice。失败会自动回退到其他方式。")
    for f in uploaded:
        st.markdown(f"**📄 {f.name}**")
        df = try_parse(
            pdf_bytes=f.read(),
            file_name=f.name,
            vendor_choice=vendor_choice,
            enable_ocr=enable_ocr,
            default_engine=engine,
            default_pages=pages,
            default_store_regex=store_regex,
            item_cols_guess=item_cols_guess,
            qty_cols_guess=qty_cols_guess,
            price_cols_guess=price_cols_guess
        )
        if df.empty:
            st.warning("未识别到有效行，请尝试更换解析引擎或开启 OCR。")

        # —— 门店映射：补全 store_id / name ——
        if not df.empty:
            store_ids, store_names = [], []
            for _, r in df.iterrows():
                sid, sname = map_store_to_ids(r.get("SiteCode"), r.get("Store"), store_map_df)
                store_ids.append(sid)
                store_names.append(sname)
            df["store_id"] = store_ids
            # name 优先映射，其次 Store；MFB 若仍空则用 Vendor 名兜底
            base_name = df.get("Store")
            df["name"] = pd.Series(store_names).fillna(base_name)
            if (df["name"].isna().all() or (df["name"].astype(str) == "None").all()) and (df.get("Vendor").astype(str).str.contains("MyFoodBag").any()):
                df["name"] = "My Food Bag"

        # —— 选择导出格式 ——
        _fmt = export_format
        if _fmt == "Auto by Vendor":
            v0 = df["Vendor"].iloc[0] if not df.empty and "Vendor" in df.columns else None
            _fmt = "DCorder (含 price)" if choose_format_by_vendor(v0) == "DCorder" else "CDDCorder (不含 price)"

        # —— 生成导入模版 ——
        export_df = to_DCorder_template(df) if _fmt.startswith("DCorder") else to_CDDCorder_template(df)

        # 校验提示
        need_cols = ["store_id", "name", "sales_id", "order_date", "item_id", "quantity"]
        lack = [c for c in need_cols if c not in export_df.columns]
        if lack:
            st.warning(f"当前数据缺少必要列：{lack}")

        st.write("👇 请在下方核对/补齐导入模版需要的字段（可直接编辑）：")
        edited = st.data_editor(export_df, use_container_width=True, num_rows="dynamic", key=f"editor_{f.name}")

        err_qty = edited["quantity"].isna().sum() if "quantity" in edited.columns else 0
        err_item = edited["item_id"].isna().sum() if "item_id" in edited.columns else 0
        st.write(f"校验：缺少 `item_id` 行数 **{err_item}**；缺少 `quantity` 行数 **{err_qty}**。")
        if "price" in edited.columns:
            err_price = edited["price"].isna().sum()
            st.write(f"校验：缺少 `price` 行数 **{err_price}**。")

        results.append((f.name, edited, _fmt))

    # —— 导出 ——
    c1, c2 = st.columns(2)
    with c1:
        if results:
            for name, df_out, fmt_name in results:
                stub = Path(name).stem
                file_name = f"{stub}_{'DCorder' if 'DCorder' in fmt_name else 'CDDCorder'}.csv"
                csv_bytes = df_out.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    label=f"⬇️ 下载 {file_name}",
                    data=csv_bytes,
                    file_name=file_name,
                    mime="text/csv",
                    key=f"dl_{name}"
                )
    with c2:
        if results and merge_to_one:
            merged = pd.concat([df_out.assign(Source=name) for name, df_out, _fmt in results], ignore_index=True)
            csv_bytes = merged.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                label="⬇️ 下载合并 CSV（merged_orders.csv）",
                data=csv_bytes,
                file_name="merged_orders.csv",
                mime="text/csv",
            )
else:
    st.info("请在上方上传 PDF 文件。")
