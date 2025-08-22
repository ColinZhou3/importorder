# PO PDF → CSV (Minimal)

Exports exactly 7 columns: `store_id,name,sales_id,order_date,item_id,quantity,price`.
Vendor-specific parsing is line-based from `pdftotext -layout`.

## Deploy
1) Put these files in your GitHub repo root:
   - app.py
   - requirements.txt
   - packages.txt
2) (optional) Add `store_map.csv` with columns `name,store_id` for fuzzy mapping.
3) Streamlit Cloud → New app → point to `app.py`.

