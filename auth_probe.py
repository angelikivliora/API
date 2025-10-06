import os, json, base64, itertools, requests
from textwrap import shorten

BASE = os.environ.get("FRESTO_BASE_URL", "https://data.fresto.io/data-api-service").rstrip("/")
CLIENT_ID   = os.environ.get("FRESTO_CLIENT_ID",   "0814db5f-5f98-48bb-ae79-9b66884d1184")
CLIENT_SECRET = os.environ.get("FRESTO_CLIENT_SECRET", "REPLACE_WITH_YOUR_SECRET")  # <= put your secret
SLUG       = os.environ.get("FRESTO_SLUG", "")  # if you know it, put it here (e.g., "fabrik")

def try_call(label, path, headers=None, params=None):
    headers = headers or {}
    params = params or {}
    try:
        r = requests.get(f"{BASE}{path}", headers=headers, params=params, timeout=20)
        try:
            body = r.json()
            body_str = json.dumps(body)[:300]
        except Exception:
            body_str = r.text[:300]
        print(f"[{label}] {r.status_code} {r.reason}  {shorten(body_str, 200)}")
        return r.status_code
    except Exception as e:
        print(f"[{label}] ERROR {e}")
        return None

def build_headers():
    # Variants to try
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    auth_variants = [
        ("Authorization", f"Bearer {CLIENT_SECRET}"),
        ("Authorization", f"Bearer {CLIENT_ID}:{CLIENT_SECRET}"),
        ("Authorization", f"Basic {basic}"),
        ("Authorization", f"Fresto {CLIENT_SECRET}"),      # custom scheme guess
        ("Authorization", CLIENT_SECRET),                   # raw
    ]
    key_variants = [
        ("X-Api-Key", CLIENT_SECRET),
        ("X-API-Key", CLIENT_SECRET),
        ("X-Client-Secret", CLIENT_SECRET),
        ("X-Auth-Token", CLIENT_SECRET),
        ("fresto", CLIENT_SECRET),                          # custom header name guess
    ]
    id_variants = [
        None,
        ("X-Client-Id", CLIENT_ID),
        ("X-Client-ID", CLIENT_ID),
        ("Client-Id", CLIENT_ID),
    ]
    slug_variants = [
        None,
        ("X-Slug", SLUG) if SLUG else None,
        ("X-Tenant", SLUG) if SLUG else None,
    ]
    # Produce header dicts to try
    headers_list = []
    # 1) Authorization-only patterns
    for a in auth_variants:
        headers_list.append({a[0]: a[1]})
    # 2) Key-only patterns
    for k in key_variants:
        headers_list.append({k[0]: k[1]})
    # 3) Combine with Client ID and optional slug
    combos = []
    for base_hdr in headers_list:
        for idh in id_variants:
            for slh in slug_variants:
                h = dict(base_hdr)
                if idh: h[idh[0]] = idh[1]
                if slh and SLUG: h[slh[0]] = slh[1]
                combos.append(h)
    # also try “no auth” once (control)
    combos.append({})
    return combos

def main():
    if CLIENT_SECRET == "REPLACE_WITH_YOUR_SECRET":
        print("Please edit CLIENT_SECRET at the top of this script (or set FRESTO_CLIENT_SECRET in env).")
        return
    headers_list = build_headers()

    # tiny date window
    daily_params = {"startDate": "2025-01-01", "endDate": "2025-01-02"}

    print(f"Probing {len(headers_list)} header combos against /misc/ping and /sales/daily …")
    best = []
    for i, h in enumerate(headers_list, 1):
        label = f"#{i:02d} " + "; ".join([f"{k}={h[k][:12]}…" for k in h]) if h else "#%02d NO_AUTH" % i
        # 1) ping first (auth required per manual)
        code1 = try_call(label + " [PING]", "/misc/ping", headers=h)
        # 2) daily sales endpoint
        code2 = try_call(label + " [DAILY]", "/sales/daily", headers=h, params=daily_params)
        if code1 and code1 < 400 or code2 and code2 < 400:
            best.append((label, code1, code2, h))
    print("\n=== Summary of any non-401 / promising results ===")
    if not best:
        print("No auth pattern worked. Likely needs tenant slug, OAuth token exchange, activation, or IP allowlisting.")
    else:
        for b in best:
            print(b[0], "=>", b[1], b[2], b[3])

if __name__ == "__main__":
    main()
