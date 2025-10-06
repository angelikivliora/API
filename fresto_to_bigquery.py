#!/usr/bin/env python3
"""
Fresto → BigQuery Loader

- Gets a fresh access token using Client ID + Secret
- Pulls orders, orderlines, staff, products, salepoints
- Loads into BigQuery tables
- Adds `location_slug` and `loaded_at`

Usage:
  python fresto_to_bigquery.py --project laposatametabase --dataset fresto_raw \
      --start 2025-02-01 --location_slug la-posata
"""

import os
import sys
import time
import base64
import argparse
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# ---- Env vars (from .env) ----
CLIENT_ID     = os.getenv("FRESTO_CLIENT_ID")
CLIENT_SECRET = os.getenv("FRESTO_CLIENT_SECRET")
SCOPE         = os.getenv("FRESTO_SCOPE", "fresto")
TOKEN_URL     = os.getenv("FRESTO_TOKEN_URL", "https://backend.fresto.io/data-api-service/auth/token")
BASE_URL      = os.getenv("FRESTO_BASE_URL", "https://data.fresto.io/data-api-service").rstrip("/")
SALEPOINT_ID  = os.getenv("FRESTO_SALEPOINT_ID", "").strip() or None


# ---- Token retrieval ----
def get_token():
    if not CLIENT_ID or not CLIENT_SECRET:
        sys.exit("Missing FRESTO_CLIENT_ID or FRESTO_CLIENT_SECRET in env/.env")
    auth = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        TOKEN_URL,
        headers={"Content-Type":"application/json","Authorization":f"Basic {auth}"},
        json={"grant_type":"client_credentials","scope":SCOPE},
        timeout=30
    )
    if r.status_code >= 400:
        sys.exit(f"Token error {r.status_code}: {r.text}")
    return r.json()["access_token"]


# ---- API fetch with paging ----
def paged_get(path, params, token):
    out, page, pagesize = [], 0, 500
    headers = {"Authorization": f"Bearer {token}"}
    while True:
        p = dict(params); p.update({"page": page, "pagesize": pagesize})
        r = requests.get(f"{BASE_URL}{path}", params=p, headers=headers, timeout=60)
        if r.status_code >= 400:
            sys.exit(f"HTTP {r.status_code} at {path}: {r.text}")
        j = r.json() if r.content else {}
        data = j.get("data", []) if isinstance(j, dict) else (j or [])
        out.extend(data)
        if len(data) < pagesize: break
        page += 1; time.sleep(0.1)
    return out


# ---- BigQuery load ----
def load_df(df, table_id):
    if df is None or df.empty:
        print(f"  (no rows) -> {table_id}")
        return
    client = bigquery.Client()
    job = client.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"  loaded {len(df)} rows -> {table_id}")


# ---- Main ----
def main():
    ap = argparse.ArgumentParser(description="Load Fresto to BigQuery")
    ap.add_argument("--project", required=True, help="GCP project id")
    ap.add_argument("--dataset", required=True, help="BigQuery dataset")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD business date start")
    ap.add_argument("--end", required=False, help="YYYY-MM-DD business date end (default: today)")
    ap.add_argument("--location_slug", required=True, help="Label for this location, e.g. la-posata")
    args = ap.parse_args()

    # --- Date sanity ---
    try:
        datetime.fromisoformat(args.start)
    except ValueError:
        sys.exit("Start date must be YYYY-MM-DD")

    if args.end:
        try:
            datetime.fromisoformat(args.end)
        except ValueError:
            sys.exit("End date must be YYYY-MM-DD")
    else:
        args.end = datetime.today().strftime("%Y-%m-%d")  # default = today

    token = get_token()
    params = {"startDate": args.start, "endDate": args.end}
    if SALEPOINT_ID:
        params["salePointID"] = SALEPOINT_ID

    print(f"Fetching data for {args.location_slug} from {args.start} to {args.end}...")

    orders     = paged_get("/sales/orders", params, token)
    orderlines = paged_get("/sales/orderlines", params, token)
    staff      = paged_get("/staff", {}, token)
    products   = paged_get("/menu/products", {}, token)
    salepoints = paged_get("/salepoints", params, token)

    def enrich(data):
        df = pd.DataFrame(data)
        if not df.empty:
            df["location_slug"] = args.location_slug
            df["loaded_at"] = pd.Timestamp.utcnow()
        return df

    df_orders     = enrich(orders)
    df_orderlines = enrich(orderlines)
    df_staff      = enrich(staff)
    df_products   = enrich(products)
    df_salepoints = enrich(salepoints)

    base = f"{args.project}.{args.dataset}"
    load_df(df_orders,     f"{base}.raw_orders")
    load_df(df_orderlines, f"{base}.raw_orderlines")
    load_df(df_staff,      f"{base}.dim_staff")
    load_df(df_products,   f"{base}.dim_products")
    load_df(df_salepoints, f"{base}.dim_salepoints")

    print("Done.")


if __name__ == "__main__":
    main()
