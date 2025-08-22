# app.py
# PO PDF → CSV 统一解析与导出（适配三家供应商 + 双导入模版）
# - 解析：Camelot/Tabula/pdfplumber 多级回退 + 正则逐行 + 可选 OCR
# - 供应商：Foodstuffs_NI / MyFoodBag / WWNZ(Countdown)
# - 导出：DCorder(含 price) 与 CDDCorder(不含 price)
# - 门店映射：侧栏上传 store_map.csv（site_code/name → store_id/name）

import re
import json
import tempfile
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd

# 可选依赖（安装后可取消注释相应导入）
# import camelot
# from tabula import read_pdf
# import pdfplumber
# from pdf2image import convert_from_path
# import pytesseract
# from PIL import Image

# -----------------------
# 基础设置
# -----------------------
st.set_page_config(page_title="PO PDF → CSV Converter", layout="wide")
st.title("PO PDF → CSV 转换器（适配三家供应商 & 双导入模版）")

# -----------------------
# 供应商配置（可在侧栏导入/导出 JSON）
# -----------------------
VENDOR_PROFILES = {
    "Foodstuffs_NI": {
        "detect_keywords": ["Foodstuffs North Island Limited", "Order Forecast", "O/F"],
        "engine": "camelot-lattice",
        "pages": "all",
        "store_regex": r"(Delivery\s+To|Delivery\s+Address)[:：]?\s*([A-Za-z0-9\-\&\(\)\/., ]+)",
        "column_map": {
            "Line": "Line",
            "Item #": "ItemNo",
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
            "PO_Number": r"Order\s+Forecast\s+Number:\s*([0-9]+)",
            "Order_Date": r"Date\s+of\s+Order:\s*([0-9/]+)",
            "Delivery_Date": r"Delivery\s+Date:\s*([0-9/]+)"
        },
        "line_regex": r"""
          ^\s*(\d+)\s+                 # Line
          (\d+)\s+                     # Item #
          ([A-Za-z0-9\-]+)\s+          # Article
          (.+?)\s+                     # Product Description
          (\d+)\s+                     # Order Qty
          ([A-Z]{2,4})\s+              # UoM
          (\d+)\s+                     # Units Per UoM
          ([0-9]+\.[0-9]{2})\s+        # Unit Price
          ([0-9]+\.[0-9]{2})\s+        # Net (忽略)
          (-?[0-9]+\.[0-9]{2})\s+      # Term Fee (忽略)
          ([0-9]+\.[0-9]{2})\s*$       # Line Total
        """
    },
    "MyFoodBag": {
        "detect_keywords": ["My Food Bag Limited", "PURCHASE ORDER", "GST Reg. No:"],
        "engine": "camelot-stream",
        "pages": "all",
        "store_regex": r"(Delivery\s+Instructions:|My\s+Food\s+Bag)\s*([A-Za-z0-9\-\&\(\)\/., ]+)",
        "column_map": {
            "Item No.": "ItemNo",
            "QTY": "Qty",
            "DESCRIPTION": "Item",
            "Delivery Date": "Delivery_Date",
            "PRICE": "Unit Price",
            "TOTAL": "Line Total"
        },
        "skip_total_rows": True,
        "header_extract": {
            "PO_Number": r"Purchase\s+No:\s*([0-9]+)",
            "Order_Date": r"Order\s+Date\s*([0-9/]+)"
        },
        "line_regex": r"""
          ^\s*([0-9]{6,})\s+           # Item No
          ([0-9,]+)\s+                 # QTY
          (.+?)\s+                     # DESCRIPTION
          ([0-9/]{6,})\s+              # Delivery Date
          ([0-9]+\.[0-9]{2})\s+        # PRICE
          ([0-9,]+\.[0-9]{2})\s*$      # TOTAL
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
            "ORD QTY": "Qty",
            "PRICE EXCL": "Unit Price"
        },
        "skip_total_rows": True,
        "header_extract": {
            "PO_Number": r"PRODUCE\s+ORDER\s+NUMBER\s*:\s*([0-9]+)",
            "Order_Date": r"Order\s+Date\s*:\s*([0-9/]{8,})",
            "Delivery_Date": r"Delivery\s+Date\s*:\s*([0-9/]{8,})",
            "Delivery_Time": r"Delivery\s+Time\s*:\s*([0-9\s:]{4,})"
        },
        "line_regex": r"""
          ^\s*\d+\s+                    # 行号
          (?P<gtin>\d{8,14})\s+         # GTIN
          (?P<item>.+?)\s+              # 描述
          (?P<itemno>\d+)\s+            # ITEM NO
          (?P<om>[\d\.]+)\s+            # OM (可忽略)
          (?P<tihi>\d+x\d+)\s+          # TIxHI (可忽略)
          (?P<ordqty>\d+)\s+            # ORD QTY
          (?P<taxes>\d+)\s+             # TAXES (可忽略)
          (?P<price>[\d]+\.[\d]{2})\s*$ # PRICE EXCL
        """
    }
}

# -----------------------
# 工具函数
# -----------------------
def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()
    return df

def normalize_numeric(series):
    """Robust numeric cleaner:
    - Accepts Series/list-like/scalars
    - Unwraps list/tuple cells (takes first element)
    - Strips currency/thousand separators
    - Extracts first numeric token
    - Coerces invalids to NA
    """
    s = pd.Series(series)
    s = s.apply(lambda x: (x[0] if isinstance(x, (list, tuple)) and len(x) > 0 else x))
    s = s.astype(str)
    s = s.str.replace(r"[\s,\$]", "", regex=True)
    s = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(s, errors="coerce")

def pick_first_existing(df, candidates):
    for c in candidates:
        if c in df.columns:
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
    return s  # 解析失败则保留原值

def extract_by_regex(pattern, text):
    if not pattern or not text:
        return None
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if m:
        if m.lastindex and m.lastindex >= 1:
            if m.lastindex == 1:
                return m.group(1).strip()
            return [g.strip() if isinstance(g, str) else g for g in m.groups()]
        return m.group(0).strip()
    return None

def extract_store(text, store_re):
    val = extract_by_regex(store_re, text)
    site_code, store_name = None, None
    if isinstance(val, list):
        if len(val) >= 2:
            if re.fullmatch(r"\d{3,6}", val[0]):
                site_code, store_name = val[0], val[1]
            else:
                store_name = val[1]
        elif len(val) == 1:
            store_name = val[0]
    elif isinstance(val, str):
        store_name = val
    return site_code, store_name

def detect_vendor(text_concat: str) -> str | None:
    text_upper = (text_concat or "").upper()
    for vendor, cfg in VENDOR_PROFILES.items():
        for kw in cfg.get("detect_keywords", []):
            if kw.upper() in text_upper:
                return vendor
    return None

# 解析引擎们
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
            table = page.extract_table()
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
    dfs = read_pdf(pdf_path, pages=pages_arg, multiple_tables=True, lattice=False) or []
    return dfs

def ocr_pages_to_text(pdf_path):
    from pdf2image import convert_from_path
    import pytesseract
    images = convert_from_path(pdf_path, dpi=200)
    texts = []
    for img in images:
        texts.append(pytesseract.image_to_string(img))
    return "\n".join(texts)

def coerce_to_standard(df, item_candidates, qty_candidates, price_candidates, fallback_store, id_candidates=None):
    df = clean_cols(df)
    item_col = pick_first_existing(df, item_candidates) or df.columns[0]
    qty_col = pick_first_existing(df, qty_candidates)
    price_col = pick_first_existing(df, price_candidates)

    # 自动猜测
    if qty_col is None:
        maybe = [c for c in df.columns if re.search(r"\b(qty|quantity|数量)\b", str(c), re.I)]
        qty_col = maybe[0] if maybe else None
    if price_col is None:
        maybe = [c for c in df.columns if re.search(r"(unit\s*price|price|单价|价格)", str(c), re.I)]
        price_col = maybe[0] if maybe else None

    out = pd.DataFrame()
    out["Item"] = df[item_col].astype(str)
    out["Qty"] = normalize_numeric(df[qty_col]) if (qty_col and qty_col in df.columns) else pd.NA
    out["Unit Price"] = normalize_numeric(df[price_col]) if (price_col and price_col in df.columns) else pd.NA
    if id_candidates is None:
        id_candidates = ["ItemNo","Item #","Item No.","ITEM NO","Article Number","Article","SKU","PLU","GTIN","UPC","Barcode"]
    id_col = pick_first_existing(df, id_candidates)
    out["ItemNo"] = df[id_col].astype(str) if id_col and id_col in df.columns else pd.NA
    out["Store"] = fallback_store
    return out

# 导出模版工具
def map_store_to_ids(site_code, store_name, store_map_df):
    """根据 site_code 或 store_name 在映射表中找到 store_id 和 name。"""
    if store_map_df is None:
        return None, None

    df = store_map_df.copy()
    df.columns = [c.strip().lower() for c in df.columns]

    sid, sname = None, None
    # 1) site_code 精确匹配
    if site_code:
        m = df[df.get("site_code", "").astype(str).str.strip() == str(site_code).strip()]
        if not m.empty:
            sid = m.iloc[0].get("store_id")
            sname = m.iloc[0].get("name")
            return sid, sname
    # 2) store_name 模糊匹配
    if store_name:
        key = str(store_name).strip().lower()
        m = df[df.get("name", "").astype(str).str.lower().str.contains(re.escape(key))]
        if not m.empty:
            sid = m.iloc[0].get("store_id")
            sname = m.iloc[0].get("name")
            return sid, sname
    return None, None

def to_DCorder_template(df):
    """导出为 DCorder（含 price）"""
    out = pd.DataFrame({
        "store_id": df.get("store_id"),
        "name": df.get("name"),
        "sales_id": df.get("PO_Number"),
        "order_date": df.get("Delivery_Date").fillna(df.get("Order_Date")),
        "item_id": df.get("ItemNo"),
        "quantity": df.get("Qty"),
        "price": df.get("Unit Price")
    })
    return out

def to_CDDCorder_template(df):
    """导出为 CDDCorder（不含 price）"""
    out = pd.DataFrame({
        "store_id": df.get("store_id"),
        "name": df.get("name"),
        "sales_id": df.get("PO_Number"),
        "order_date": df.get("Delivery_Date").fillna(df.get("Order_Date")),
        "item_id": df.get("ItemNo"),
        "quantity": df.get("Qty")
    })
    return out

def choose_format_by_vendor(vendor: str) -> str:
    """按供应商自动选择导出格式：WWNZ→CDDCorder，其它→DCorder"""
    if (vendor or "").upper() == "WWNZ":
        return "CDDCorder"
    return "DCorder"

# 主解析
def try_parse(pdf_bytes, file_name, vendor_choice, enable_ocr, default_engine, default_pages, default_store_regex,
              item_cols_guess, qty_cols_guess, price_cols_guess):
    with tempfile.TemporaryDirectory() as td:
        fp = Path(td) / file_name
        fp.write_bytes(pdf_bytes)

        # 先提纯文本（用于识别供应商与抬头抽取）
        text_concat = ""
        try:
            import pdfplumber
            with pdfplumber.open(str(fp)) as pdf:
                text_concat = "\n".join([p.extract_text() or "" for p in pdf.pages])
        except Exception:
            text_concat = ""

        # 供应商选择与配置
        active_vendor = detect_vendor(text_concat) if vendor_choice == "Auto" else (vendor_choice if vendor_choice in VENDOR_PROFILES else None)
        profile = VENDOR_PROFILES.get(active_vendor, {}) if active_vendor else {}
        engine_used = profile.get("engine", default_engine)
        pages_used = profile.get("pages", default_pages)
        store_re_used = profile.get("store_regex", default_store_regex)
        colmap = profile.get("column_map", {})
        skip_total_rows = profile.get("skip_total_rows", True)
        header_extract = profile.get("header_extract", {})
        line_regex = profile.get("line_regex", "")

        # 抬头字段抽取
        header = {}
        for k, pat in header_extract.items():
            v = extract_by_regex(pat, text_concat)
            if isinstance(v, list):
                v = " ".join([str(x) for x in v if x])
            header[k] = v

        # 店面/仓信息
        site_code, store_name = extract_store(text_concat, store_re_used) if text_concat else (None, None)

        # 解析表格：多级回退
        tables = []
        # 1) 首选 Camelot
        try:
            if engine_used.startswith("camelot"):
                flavor = "lattice" if engine_used.endswith("lattice") else "stream"
                tables = parse_with_camelot(str(fp), pages_used, flavor)
        except Exception as e:
            st.warning(f"[{file_name}] Camelot 解析失败：{e}")

        # 2) Tabula 回退
        if not tables:
            try:
                tables = parse_with_tabula(str(fp), pages_used)
            except Exception:
                pass

        # 3) pdfplumber 回退
        if not tables:
            try:
                tables, text2 = parse_with_pdfplumber(str(fp), pages_used)
                if text2 and (not text_concat):
                    text_concat = text2
            except Exception:
                pass

        # 4) OCR 纯文本（开启时）
        if enable_ocr and not (text_concat or "").strip():
            try:
                text_concat = ocr_pages_to_text(str(fp))
            except Exception as e:
                st.error(f"[{file_name}] OCR 失败：{e}")

        # 构建列候选
        item_candidates = [k for k, v in colmap.items() if v == "Item"] or [c.strip() for c in item_cols_guess.split(",")]
        qty_candidates = [k for k, v in colmap.items() if v == "Qty"] or [c.strip() for c in qty_cols_guess.split(",")]
        price_candidates = [k for k, v in colmap.items() if v == "Unit Price"] or [c.strip() for c in price_cols_guess.split(",")]
        id_candidates = [k for k, v in colmap.items() if v in ("ItemNo", "ITEM NO")] or ["ItemNo","Item #","Item No.","ITEM NO","Article Number","Article","SKU","PLU","GTIN","UPC","Barcode"]

        # 规范化表格
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

        # 若仍为空，回退到逐行正则
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
                        "Qty": float(Qty),
                        "UoM": UoM,
                        "UnitsPerUoM": float(UnitsPerUoM),
                        "Unit Price": float(UnitPrice),
                        "Line Total": float(LineTotal),
                        "Store": store_name
                    })
                elif active_vendor == "MyFoodBag":
                    ItemNo, Qty, Item, DeliveryDate, UnitPrice, LineTotal = m.groups()
                    rows.append({
                        "ItemNo": ItemNo,
                        "Item": Item.strip(),
                        "Qty": float(Qty.replace(",", "")),
                        "Delivery_Date": DeliveryDate,
                        "Unit Price": float(UnitPrice),
                        "Line Total": float(LineTotal.replace(",", "")),
                        "Store": store_name
                    })
                elif active_vendor == "WWNZ":
                    gd = m.groupdict()
                    rows.append({
                        "ItemNo": gd.get("itemno"),
                        "Item": gd.get("item").strip(),
                        "Qty": float(gd.get("ordqty")),
                        "Unit Price": float(gd.get("price")),
                        "Store": store_name
                    })
            if rows:
                normalized_tables.append(pd.DataFrame(rows))

        # 合并结果
        merged = pd.concat(normalized_tables, ignore_index=True) if normalized_tables else pd.DataFrame(columns=["Item","Qty","Unit Price","Store"])

        # 标准字段补充
        merged.insert(0, "Vendor", active_vendor or "Unknown")
        # 抬头字段
        po_num = header.get("PO_Number")
        ord_date = parse_date_safe(header.get("Order_Date"))
        deliv_date = parse_date_safe(header.get("Delivery_Date"))
        deliv_time = header.get("Delivery_Time")

        merged.insert(1, "PO_Number", po_num)
        merged.insert(2, "Order_Date", ord_date)
        merged.insert(3, "Delivery_Date", deliv_date)
        merged.insert(4, "Delivery_Time", deliv_time)
        # 站点/门店
        merged.insert(5, "SiteCode", site_code)

        # 数值清洗
        if "Qty" in merged.columns:
            merged["Qty"] = normalize_numeric(merged["Qty"])
        if "Unit Price" in merged.columns:
            merged["Unit Price"] = normalize_numeric(merged["Unit Price"])
        if "Line Total" in merged.columns:
            merged["Line Total"] = normalize_numeric(merged["Line Total"])

        # 去除表头噪声
        merged = merged[~merged["Item"].astype(str).str.match(r"(?i)^(item|sku|code|description|品名|合计|total)$")]

        # 提示识别
        if active_vendor:
            st.caption(f"识别到供应商：**{active_vendor}**（引擎：{engine_used} | 页码：{pages_used}）")
        else:
            st.caption(f"未识别供应商（使用当前选择/默认：引擎 {engine_used} | 页码 {pages_used}）")

        return merged

# -----------------------
# 侧边栏设置
# -----------------------
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
    qty_cols_guess = st.text_input("数量列候选（逗号分隔）", "Qty,Quantity,QTY,数量,Order Qty,ORD QTY")
    price_cols_guess = st.text_input("单价列候选（逗号分隔）", "Unit Price,PRICE,Price,单价,Price Per Ord. Unit,PRICE EXCL")
    store_regex = st.text_input("店面/仓库识别正则（可自动识别SiteCode+Store）", default_store_re)
    merge_to_one = st.checkbox("合并多个文件为一个 CSV", value=True)

    st.subheader("配置管理")
    if st.button("导出供应商配置 JSON"):
        st.download_button("下载 vendor_profiles.json",
                           data=json.dumps(VENDOR_PROFILES, ensure_ascii=False, indent=2),
                           file_name="vendor_profiles.json",
                           mime="application/json")
    uploaded_cfg = st.file_uploader("导入/覆盖供应商配置 JSON", type=["json"], accept_multiple_files=False, key="cfg_uploader")
    if uploaded_cfg:
        try:
            VENDOR_PROFILES.clear()
            VENDOR_PROFILES.update(json.loads(uploaded_cfg.read().decode("utf-8")))
            st.success("已导入并应用新的供应商配置。")
        except Exception as e:
            st.error(f"导入失败：{e}")

    st.subheader("导出设置")
    export_format = st.selectbox(
        "导出模版",
        ["Auto by Vendor", "DCorder (含 price)", "CDDCorder (不含 price)"],
        index=0
    )

    # 门店映射：site_code 或 name → store_id/name
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

# -----------------------
# 主流程：解析 → 门店映射 → 导出模版
# -----------------------
results = []
if uploaded:
    st.info("提示：WWNZ/MyFoodBag 多为无边框文本列，优先用 camelot-stream；Foodstuffs 若表格带边框，用 camelot-lattice。失败会自动回退到其他方式。")
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
            df["name"] = pd.Series(store_names).fillna(df.get("Store"))

        # —— 选择导出格式（自动/手动） ——
        _fmt = export_format
        if _fmt == "Auto by Vendor":
            v0 = df["Vendor"].iloc[0] if not df.empty and "Vendor" in df.columns else None
            _fmt = "DCorder (含 price)" if choose_format_by_vendor(v0) == "DCorder" else "CDDCorder (不含 price)"

        # —— 生成导入模版数据帧 ——
        if _fmt.startswith("DCorder"):
            export_df = to_DCorder_template(df)
        else:
            export_df = to_CDDCorder_template(df)

        # 简单校验：核心列（价格列仅 DCorder）
        need_cols = ["store_id", "name", "sales_id", "order_date", "item_id", "quantity"]
        lack = [c for c in need_cols if c not in export_df.columns]
        if lack:
            st.warning(f"当前数据缺少必要列：{lack}")

        # 展示可编辑表格
        st.write("👇 请在下方核对/补齐导入模版需要的字段（可直接编辑）：")
        edited = st.data_editor(export_df, use_container_width=True, num_rows="dynamic", key=f"editor_{f.name}")

        # 校验提示
        err_qty = edited["quantity"].isna().sum() if "quantity" in edited.columns else 0
        err_item = edited["item_id"].isna().sum() if "item_id" in edited.columns else 0
        st.write(f"校验：缺少 `item_id` 行数 **{err_item}**；缺少 `quantity` 行数 **{err_qty}**。")
        if "price" in edited.columns:
            err_price = edited["price"].isna().sum()
            st.write(f"校验：缺少 `price` 行数 **{err_price}**。")

        results.append((f.name, edited, _fmt))
    # —— 导出按钮 ——
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
