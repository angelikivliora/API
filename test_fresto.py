import os
import requests

BASE_URL = "https://data.fresto.io/data-api-service"
SECRET = "shDdFCm4zLgbWG3pS1uR9Nr62MWNscu4vbS2TVFOL1hTccWRMfPz932iI1rd"  # your secret-as-token

headers = {"Authorization": f"Bearer {SECRET}"}

# Test: fetch daily sales
resp = requests.get(
    f"{BASE_URL}/sales/daily",
    headers=headers,
    params={"startDate": "2025-01-01", "endDate": "2025-01-05"}
)

print("Status code:", resp.status_code)
print("Response JSON:", resp.json())
