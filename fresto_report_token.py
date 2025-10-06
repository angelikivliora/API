#!/usr/bin/env python3
"""
Fresto Final Sales Report (OAuth Client Credentials)
- Gets a fresh access token using Client ID + Secret
- Pulls orders, orderlines, staff, products, salepoints
- Builds an Excel workbook (FinalReport + raw sheets)

Usage:
  python fresto_report.py --start 2025-08-01 --end 2025-08-31 --out final_sales_report.xlsx
"""

import os
import sys
import time
import base64
import argparse
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

CLIENT_ID = os.getenv("FRESTO_CLIENT_ID")
CLIENT_SECRET = os.getenv("FRESTO_CLIENT_SECRET")
SCOPE = os.getenv("FRESTO_SCOPE", "fresto")
TOKEN_URL = os.getenv("FRESTO_TOKEN_URL", "https://backend.fresto.io/data-api-service/auth/token")

BASE_URL = os.getenv("FRESTO_BASE_URL", "https://data.fresto.io/data-api-service").rstrip("/")
SALEPOINT_ID = os.getenv("FRESTO_SALEPOINT_ID", "").strip() or None

def get_access_token() -> str:
    if not CLIENT_ID or not CLIENT_SECRET:
        raise SystemExit("Missing FRESTO_CLIENT_ID or FRESTO_CLIENT_SECRET in environment/.env")
    auth = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {"Content-Type": "application/json", "Authorization": f"Basic {auth}"}
    data = {"grant_type": "client_credentials", "scope": SCOPE}
    r = requests.post(TOKEN_URL, headers=headers, json=data, timeout=30)
    if r.status_code >= 400:
        raise SystemExit(f"Token error {r.status_code}: {r.text}")
    j = r.json()
    tok = j.get("access_token")
    if not tok:
        raise SystemExit("Token response missing 'access_token'")
    return tok

def auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}

def paged_get(path: str, params: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page, pagesize = 0, 500
    while True:
        p = dict(params)
        p.update({"page": page, "pagesize": pagesize})
        r = requests.get(f"{BASE_URL}{path}", params=p, headers=headers, timeout=60)
        if r.status_code == 401:
            raise SystemExit(f"401 Unauthorized at {path}. Token expired or invalid.")
        if r.status_code >= 400:
            raise SystemExit(f"HTTP {r.status_code} at {path}: {r.text}")
        j = r.json() if r.content else {}
        data = j.get("data", [])
        if isinstance(j, list) and not data:
            data = j
        out.extend(data)
        if len(data) < pagesize:
            break
        page += 1
        time.sleep(0.15)
    return out

def fetch_orders(h, start, end):
    params = {"startDate": start, "endDate": end}
    if SALEPOINT_ID: params["salePointID"] = SALEPOINT_ID
    return paged_get("/sales/orders", params, h)

def fetch_orderlines(h, start, end):
    params = {"startDate": start, "endDate": end}
    if SALEPOINT_ID: params["salePointID"] = SALEPOINT_ID
    return paged_get("/sales/orderlines", params, h)

def fetch_staff(h):
    return paged_get("/staff", {}, h)

def fetch_products(h):
    return paged_get("/menu/products", {}, h)

def fetch_salepoints(h, start, end):
    params = {"startDate": start, "endDate": end}
    if SALEPOINT_ID: params["salePointID"] = SALEPOINT_ID
    return paged_get("/salepoints", params, h)

def build_report(start: str, end: str, out_path: str):
    token = get_access_token()
    headers = auth_headers(token)

    print("Fetching orders…")
    orders = fetch_orders(headers, start, end)
    print("Fetching orderlines…")
    orderlines = fetch_orderlines(headers, start, end)
    print("Fetching staff…")
    staff = fetch_staff(headers)
    print("Fetching products…")
    products = fetch_products(headers)
    print("Fetching salepoints…")
    salepoints = fetch_salepoints(headers, start, end)

    df_orders = pd.DataFrame(orders)
    df_ol = pd.DataFrame(orderlines)
    df_staff = pd.DataFrame(staff)
    df_prod = pd.DataFrame(products)
    df_sp = pd.DataFrame(salepoints)

    # Friendly columns
    if not df_staff.empty:
        df_staff = df_staff.rename(columns={
            "uid": "staff_uid",
            "name": "staff_name",
            "email": "staff_email",
            "posName": "staff_pos_name",
            "role": "staff_role"
        })
    if not df_prod.empty:
        df_prod = df_prod.rename(columns={"id": "productID", "title": "productTitle"})
    if not df_sp.empty:
        df_sp = df_sp.rename(columns={"title": "salePointTitle"})

    report = df_ol.copy()
    if "price" in report.columns:
        report = report.rename(columns={"price": "price_line"})

    if not df_orders.empty and "orderID" in report.columns and "orderID" in df_orders.columns:
        df_orders2 = df_orders.copy()
        if "price" in df_orders2.columns:
            df_orders2 = df_orders2.rename(columns={"price": "price_order"})
        report = report.merge(df_orders2, on="orderID", how="left")

    if not df_staff.empty and "userID" in report.columns and "staff_uid" in df_staff.columns:
        report = report.merge(df_staff, left_on="userID", right_on="staff_uid", how="left")

    if not df_prod.empty and "productID" in report.columns and "productID" in df_prod.columns:
        report = report.merge(df_prod, on="productID", how="left")

    if not df_sp.empty and "salePointID" in report.columns and "salePointID" in df_sp.columns:
        report = report.merge(df_sp[["salePointID", "salePointTitle"]].drop_duplicates(),
                              on="salePointID", how="left")

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        report.to_excel(xw, sheet_name="FinalReport", index=False)
        if not df_orders.empty:    df_orders.to_excel(xw, "Orders", index=False)
        if not df_ol.empty:        df_ol.to_excel(xw, "Orderlines", index=False)
        if not df_staff.empty:     df_staff.to_excel(xw, "Staff", index=False)
        if not df_prod.empty:      df_prod.to_excel(xw, "Products", index=False)
        if not df_sp.empty:        df_sp.to_excel(xw, "SalePoints", index=False)

    print(f"Saved: {out_path}")

def main():
    ap = argparse.ArgumentParser(description="Fresto final sales report (OAuth client credentials)")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", default="final_sales_report.xlsx")
    args = ap.parse_args()
    try:
        datetime.fromisoformat(args.start); datetime.fromisoformat(args.end)
    except ValueError:
        raise SystemExit("Dates must be YYYY-MM-DD")
    build_report(args.start, args.end, args.out)

if __name__ == "__main__":
    main()
