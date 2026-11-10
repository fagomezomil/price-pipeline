"""_probe_coto.py 12 — browse crudo de un grupo hijo."""
import json

try:
    import truststore

    truststore.inject_into_ssl()
except Exception:
    pass

import httpx

KEY = "key_r6xzz4IAoTWcipni"
cli = httpx.Client(headers={"User-Agent": "Mozilla/5.0 Chrome/131.0"}, timeout=40)
KEYARG = f"?key={KEY}"

for gid, npp in (("catv00001259", 200),):
    r = cli.get(f"https://ac.cnstrc.com/browse/group/{gid}{KEYARG}&num_results_per_page={npp}&page=1&section=Products")
    print(f"{gid}: status={r.status_code}")
    try:
        j = r.json()
        resp = j.get("response") or j
        print("  total:", resp.get("total_num_results"),
              "| results:", len(resp.get("results") or []),
              "| groups:", len(resp.get("groups") or []))
        res = resp.get("results") or []
        if res:
            d = res[0]["data"]
            print("  sample:", d.get("sku_description"), "|", d.get("product_main_ean"), "|", d.get("image_url", "")[:80])
    except Exception:
        print("  raw:", r.text[:300])
