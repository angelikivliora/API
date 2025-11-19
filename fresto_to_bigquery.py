import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import json
import os

# -----------------------
# CONFIG
# -----------------------
CLIENT_ID = os.environ["FRESTO_CLIENT_ID"]
CLIENT_SECRET = os.environ["FRESTO_CLIENT_SECRET"]

TOKEN_URL = "https://auth.fresto.io/oauth/token"
API_URL = "https://api.fresto.io/sales/daily"

PROJECT_ID = "laposatametabase"
DATASET = "fresto_raw"
TABLE = "daily_sales_cleaned"

# -----------------------
# PRODUCT GROUP RULES
# -----------------------
def assign_group(title):
    t = title.lower()

    if t.startswith("f"):
        return "Focaccia"

    if "pasta" in t:
        return "Pasta"

    if any(x in t for x in ["wine", "vino", "beer", "birra", "spritz", "gin", "rum"]):
        return "Drinks"

    if any(x in t for x in ["cappuccino", "latte", "espresso", "americano"]):
        return "Coffee"

    if any(x in t for x in ["tiramisu", "cookie", "dessert"]):
        return "Dessert"

    if "staff" in t:
        return "Staff"

    return "Other"


# -----------------------
# CLEAN NAME MAPPING (add more over time)
# -----------------------
NAME_MAP = {
    "F1 - Bella Vita": "F1. Bella Vita",
    "F1. Bella Vita Combo": "F1. Bella Vita",
    "F2 - Summer Vibe": "F2. Summer Vibe",
    "F3. Doppia Combo": "F3. La Doppia",
    "Gp1. Gigante Pasta Carbonara": "P1. Pasta Carbonara",
}

def normalize_title(x):
    x = x.strip().title()
    return NAME_MAP.get(x, x)


# -----------------------
# AUTHENTICATION
# -----------------------
def get_token():
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }
    )
    return response.json()["access_token"]


# -----------------------
# GET RAW DATA FROM API
# -----------------------
def get_sales(date):
    token = get_token()

    r = requests.get(
        API_URL,
        headers={"Authorization": f"Bearer {token}"},
        params={"date": date}
    )

    data = r.json().get("data", [])
    if not data:
        print("⚠ No data returned for date", date)

    return pd.DataFrame(data)


# -----------------------
# CLEAN & TRANSFORM
# -----------------------
def transform(df):
    # Only keep the columns we need
    keep = ["businessDate", "productTitle", "quantity", "location_slug"]
    df = df[[col for col in keep if col in df.columns]]

    # Convert date
    df["businessDate"] = pd.to_datetime(df["businessDate"]).dt.date

    # Normalize names
    df["productTitle"] = df["productTitle"].astype(str).apply(normalize_title)

    # Add group category
    df["group"] = df["productTitle"].apply(assign_group)

    # Deduplicate
    df = df.drop_duplicates()

    # Timestamp
    df["loaded_at"] = datetime.utcnow()

    return df


# -----------------------
# LOAD TO BIGQUERY
# -----------------------
def load_to_bq(df):
    client = bigquery.Client(project=PROJECT_ID)

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )

    job.result()
    print(f"✔ Loaded {len(df)} rows into {table_id}")


# -----------------------
# MAIN EXECUTION
# -----------------------
if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")

    print("📡 Pulling data for:", today)
    df = get_sales(today)

    if not df.empty:
        df_clean = transform(df)
        load_to_bq(df_clean)
    else:
        print("⚠ No data to load.")
