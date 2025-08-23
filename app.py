# app.py (v5.4 GitHub Compatible) – CSV columns: store_id,name,sales_id,order_date,item_id,quantity,price
# GitHub环境兼容修复:
# - 移除 Python 3.10+ 类型注解语法
# - 添加 pdftotext 替代方案 (PyPDF2)
# - 增强错误处理

import re
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import io

import streamlit as st
import pandas as pd

# 尝试导入PDF处理库
try:
    import PyPDF2
    PDF_FALLBACK_AVAILABLE = True
except ImportError:
    PDF_FALLBACK_AVAILABLE = False

st.set_page_config(page_title="PO PDF → CSV (GitHub版)", layout="wide")
st.title("PO PDF → CSV（GitHub兼容版）")

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

# ---------- PDF处理函数 ----------
def extract_pdf_text(pdf_path: str) -> str:
    """尝试多种方法提取PDF文本"""
    
    # 方法1: 尝试 pdftotext (如果可用)
    try:
        out = subprocess.check_output(
            ["pdftotext", "-layout", pdf_path, "-"], 
            stderr=subprocess.STDOUT,
            timeout=30
        )
        text = out.decode("utf-8", errors="ignore")
        if text.strip():
            return text
    except Exception:
        pass
    
    # 方法2: PyPDF2 备用方案
    if PDF_FALLBACK_AVAILABLE:
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                if text.strip():
                    return text
        except Exception as e:
            st.warning(f"PyPDF2 提取失败: {e}")
    
    return ""

def extract_pdf_from_bytes(pdf_bytes: bytes) -> str:
    """直接从字节数据提取PDF文本"""
    
    # 方法1: 保存临时文件用 pdftotext
    try:
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp.flush()
            return extract_pdf_text(tmp.name)
    except Exception:
        pass
    
    # 方法2: PyPDF2 直接处理字节流
    if PDF_FALLBACK_AVAILABLE:
        try:
            pdf_stream = io.BytesIO(pdf_bytes)
            pdf_reader = PyPDF2.PdfReader(pdf_stream)
            text = ""
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
            return text
        except Exception as e:
            st.warning(f"PyPDF2 字节流处理失败: {e}")
    
    return ""

# ---------- 辅助函数 ----------
def parse_date_safe(s: str) -> Optional[str]:
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date().isoformat()
        except Exception:
            continue
    return s

def extract(pattern: str, text: str, flags=re.I|re.S) -> Optional[str]:
    if not pattern or not text:
        return None
    m = re.search(pattern, text, flags=flags)
    if not m:
        return None
    return m.group(1).strip() if m.lastindex else m.group(0).strip()

def detect_vendor(text_concat: str) -> Optional[str]:
    if not text_concat:
        return None
    
    t = text_concat.upper()
    # prefer WWNZ first
    if any(kw in t for kw in [k.upper() for k in VENDOR_PROFILES["WWNZ"]["detect_keywords"]]):
        return "WWNZ"
    for v in ("Foodstuffs_NI","MyFoodBag"):
        if any(kw.upper() in t for kw in VENDOR_PROFILES[v]["detect_keywords"]):
            return v
    return None

def keyword_hits(text: str) -> Dict[str, int]:
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

def clean_store_name(vendor: str, raw_name: Optional[str]) -> Optional[str]:
    """清理店铺名称"""
    if not raw_name:
        return raw_name
        
    if vendor == "WWNZ":
        # 移除供应商编号等额外文本
        cleaned = re.sub(r'\d{4}-?\s*-?\s*Vendor\s*Number:?\s*\d+', '', raw_name)
        cleaned = re.sub(r'^\d{3,5}\s*\n?\s*', '', cleaned)
        return cleaned.strip()
    
    return raw_name.strip() if raw_name else raw_name

def store_lookup(name_text: str, store_map_df: Optional[pd.DataFrame]):
    """查找店铺ID"""
    if store_map_df is None or not name_text:
        return None, None
    
    try:
        df = store_map_df.copy()
        df.columns = [c.strip().lower() for c in df.columns]
        key = str(name_text).strip().lower()
        
        # 正向包含匹配
        m = df[df.get("name","").astype(str).str.lower().str.contains(re.escape(key[:40]), na=False)]
        if not m.empty:
            return m.iloc[0].get("store_id"), m.iloc[0].get("name")
        
        # 反向包含匹配
        m = df[df.get("name","").astype(str).str.lower().apply(lambda x: key in x if x else False)]
        if not m.empty:
            return m.iloc[0].get("store_id"), m.iloc[0].get("name")
        
        return None, None
    except Exception as e:
        st.warning(f"店铺查找错误: {e}")
        return None, None

# ---------- 供应商解析器 ----------
def parse_foodstuffs(text: str) -> pd.DataFrame:
    """解析 Foodstuffs PDF"""
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

def parse_wwnz(text: str) -> pd.DataFrame:
    """解析 WWNZ PDF - 修复价格提取"""
    rows = []
    lines = text.splitlines()
    
    # 查找数据部分
    data_started = False
    for ln in lines:
        # 查找表头
        if re.search(r'LINE.*ITEM NO.*ORD QTY.*PRICE', ln, re.I):
            data_started = True
            continue
            
        if not data_started:
            continue
            
        # 遇到总计时停止
        if re.search(r'Order Totals|Total Value', ln, re.I):
            break
            
        # 解析数据行 - 更灵活的模式
        match = re.match(r'^\s*(\d+)\s+(\d{8,14})\s+(.*?)\s+(\d{5,})\s+([\d.]+)\s+(\S+)\s+(\d+)\s+(\d+)\s+([\d.]+)', ln.strip())
        
        if match:
            line_no, gtin, desc, item_no, tu, sfx, om, qty, price = match.groups()
            rows.append({
                "item_id": item_no,
                "quantity": qty,
                "price": price,
            })
    
    return pd.DataFrame(rows)

def parse_mfb(text: str) -> pd.DataFrame:
    """解析 MyFoodBag PDF - 修复数量提取"""
    rows = []
    lines = [ln.rstrip() for ln in text.splitlines() if ln.strip()]
    
    # 查找包含 Item No, QTY 等的表头
    header_found = False
    for i, ln in enumerate(lines):
        if re.search(r'item\s*no.*qty.*description', ln, re.I):
            header_found = True
            # 从下一行开始处理数据
            for data_line in lines[i+1:]:
                # 遇到总计或结束标记时停止
                if re.search(r'\btotal\b|balance\s+due|page\s+\d+', data_line, re.I):
                    break
                
                # 查找以物品编号开头的行 (MFB 格式为 10xxxxxxx)
                if re.match(r'^\s*10\d{6,}', data_line):
                    # 仔细分割行
                    parts = re.split(r'\s{2,}', data_line.strip())
                    
                    if len(parts) >= 5:  # Item No, QTY, Description, Date, Price, [Total]
                        item_no = parts[0].strip()
                        qty = parts[1].strip()
                        price = parts[4].strip() if len(parts) > 4 else parts[-2].strip()
                        
                        # 清理数值
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

def validate_dataframe(df: pd.DataFrame, vendor_name: str) -> pd.DataFrame:
    """验证和清理解析后的数据框"""
    if df.empty:
        return df
    
    # 移除完全空的行
    df = df.dropna(how='all')
    
    # 移除没有 item_id 的行
    df = df[df['item_id'].notna() & (df['item_id'] != '')]
    
    # 确保数字列格式正确
    if 'quantity' in df.columns:
        df['quantity'] = normalize_numeric(df['quantity'])
    if 'price' in df.columns:
        df['price'] = normalize_numeric(df['price'])
    
    # 移除无效数据的行
    df = df[(df.get('quantity', 0) > 0) & (df.get('price', 0) > 0)]
    
    return df.reset_index(drop=True)

# ---------- Streamlit 界面 ----------
with st.sidebar:
    st.header("选项")
    vendor_choice = st.selectbox("供应商", ["Auto","Foodstuffs_NI","WWNZ","MyFoodBag"], index=0)
    
    # PDF处理状态显示
    if not PDF_FALLBACK_AVAILABLE:
        st.warning("⚠️ PyPDF2 未安装，仅支持 pdftotext")
        st.code("pip install PyPDF2")
    
    store_map_file = st.file_uploader("上传 store_map.csv（列：name,store_id）", type=["csv"])
    
    store_map_df = None
    if store_map_file:
        try:
            store_map_df = pd.read_csv(store_map_file)
            # 验证必需列
            required_cols = ['name', 'store_id']
            missing_cols = [col for col in required_cols if col not in store_map_df.columns]
            if missing_cols:
                st.error(f"store_map.csv 缺少必需列: {missing_cols}")
                store_map_df = None
            else:
                st.success(f"✅ 已加载门店映射，共 {len(store_map_df)} 条记录")
        except Exception as e:
            st.error(f"store_map 读取失败：{e}")

uploaded = st.file_uploader("上传一个或多个 PO PDF", type=["pdf"], accept_multiple_files=True)

# ---------- 主处理逻辑 ----------
results = []
if uploaded:
    progress_bar = st.progress(0)
    
    for idx, f in enumerate(uploaded):
        progress_bar.progress((idx + 1) / len(uploaded))
        
        # 提取PDF文本
        text = extract_pdf_from_bytes(f.read())
        
        if not text or not text.strip():
            st.error(f"❌ {f.name}: PDF 文本提取失败（可能是扫描版或加密的PDF）")
            continue

        # 供应商识别
        hits = keyword_hits(text)
        active_vendor = detect_vendor(text) if vendor_choice == "Auto" else vendor_choice

        # 自动回退：尝试所有解析器并选择结果最多的
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
            st.error(f"❌ {f.name}: 未识别供应商（关键词命中：{hits}）。请在左侧手动选择供应商。")
            continue

        # 提取头部信息
        prof = VENDOR_PROFILES[active_vendor]
        sales_id = extract(prof["header_extract"].get("PO_Number"), text)
        order_date = extract(prof["header_extract"].get("Delivery_Date") or prof["header_extract"].get("Order_Date"), text)
        order_date = parse_date_safe(order_date)
        
        # 提取并清理店铺名称
        raw_name = extract(prof["store_regex"], text)
        name_txt = clean_store_name(active_vendor, raw_name)

        # 解析供应商数据（如果尚未完成）
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
                st.error(f"❌ {f.name}: 解析失败 - {e}")
                continue

        if body.empty:
            st.warning(f"⚠️ {f.name}: 未解析到任何产品数据")
            continue

        # 店铺查找
        store_id, mapped_name = store_lookup(name_txt, store_map_df)
        final_name = mapped_name or name_txt

        # 创建输出数据框
        out = pd.DataFrame({
            "store_id": store_id,
            "name": final_name,
            "sales_id": sales_id,
            "order_date": order_date,
            "item_id": body.get("item_id"),
            "quantity": body.get("quantity"),
            "price": body.get("price")
        })

        # 最终清理 - 移除剩余的空行
        out = out.dropna(subset=['item_id'])
        out = out[out['item_id'] != '']

        # 显示结果
        info = f"**📄 {f.name}** · 供应商：**{active_vendor}**"
        if chosen_by_rows:
            info += "（自动回退：按解析行数判定）"
        
        total_items = len(out)
        total_value = out['price'].sum() if 'price' in out.columns else 0
        missing_store = out['store_id'].isna().sum()
        
        info += f" · {total_items} 项产品"
        if total_value > 0:
            info += f" · 总值: ${total_value:.2f}"
        if missing_store > 0:
            info += f" · ⚠️ {missing_store} 项缺少store_id"
        
        st.markdown(info)
        st.dataframe(out, use_container_width=True)
        
        # 下载单个文件
        csv_data = out.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label=f"⬇️ 下载 {Path(f.name).stem}.csv",
            data=csv_data,
            file_name=f"{Path(f.name).stem}.csv",
            mime="text/csv",
            key=f"dl_{f.name}"
        )

        results.append(out)

    # 合并所有结果
    if results:
        merged = pd.concat(results, ignore_index=True)
        # 最终清理合并数据
        merged = merged.dropna(subset=['item_id'])
        merged = merged[merged['item_id'] != '']
        
        st.markdown("---")
        total_value = merged['price'].sum() if 'price' in merged.columns else 0
        st.markdown(f"**📊 合并统计**: {len(merged)} 项产品，总值 ${total_value:.2f}")
        
        st.download_button(
            "⬇️ 下载合并 CSV（orders.csv）",
            merged.to_csv(index=False).encode("utf-8-sig"),
            file_name="orders.csv",
            mime="text/csv"
        )
    
    progress_bar.empty()
else:
    st.info("📤 上传 PDF 文件开始解析")

# 使用说明和故障排除
with st.expander("💡 GitHub环境使用指南"):
    st.markdown("""
    **GitHub环境兼容版 (v5.4)**:
    - ✅ 兼容 Python 3.8+ 
    - ✅ 自动降级到 PyPDF2（如果 pdftotext 不可用）
    - ✅ 增强错误处理
    
    **如果遇到问题**:
    1. **PDF提取失败**: 安装 PyPDF2
       ```bash
       pip install PyPDF2
       ```
    
    2. **扫描版PDF**: 需要OCR工具，建议转换为文本版PDF
    
    3. **权限错误**: 确保有文件写入权限
    
    **支持的PDF格式**:
    - 文本型PDF（推荐）
    - 部分图像型PDF（通过PyPDF2）
    """)
