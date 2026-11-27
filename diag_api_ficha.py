"""Diagnóstico 2: (A) respuesta COMPLETA de la API de expediente (autores, coautores, comisiones...)
y (B) cómo la página genera los identificadores cifrados de la URL (se buscan en los .js de la página)."""
import json, re, time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

BASE = "https://wb2server.congreso.gob.pe"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
res = {"expedientes": [], "bundle": []}

# ---------------- A) respuesta completa de la API (vía Chrome) ----------------
opts = Options()
for a in ("--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
          "--window-size=1920,1080", f"user-agent={UA}"):
    opts.add_argument(a)
opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
d = webdriver.Chrome(options=opts)
d.execute_cdp_cmd("Network.enable", {})
for per, num in [(2021, 14671), (2021, 14288)]:
    d.get("about:blank")
    d.get(f"{BASE}/spley-portal/#/expediente/{per}/{num}")
    try:
        WebDriverWait(d, 25).until(lambda x: x.find_elements(By.CSS_SELECTOR, "div.estado-valor"))
    except Exception:
        continue
    time.sleep(3)
    for e in d.get_log("performance"):
        m = json.loads(e["message"])["message"]
        if m["method"] == "Network.responseReceived" and "/spley-portal-service/expediente/" in m["params"]["response"]["url"]:
            try:
                body = d.execute_cdp_cmd("Network.getResponseBody", {"requestId": m["params"]["requestId"]})["body"]
                res["expedientes"].append({"ficha": f"{per}-{num}", "url": m["params"]["response"]["url"],
                                           "json": json.loads(body)})
            except Exception as ex:
                res["expedientes"].append({"ficha": f"{per}-{num}", "error": str(ex)[:200]})
d.quit()

# ---------------- B) buscar en los .js cómo se arma la URL / se cifra el id ----------------
H = {"User-Agent": UA}
try:
    html = requests.get(f"{BASE}/spley-portal/", headers=H, timeout=30).text
    srcs = re.findall(r'<script[^>]+src="([^"]+\.js)"', html)
    print("scripts:", srcs)
    pats = [r"spley-portal-service", r"expediente/\$\{", r"CryptoJS|crypto-js", r"\bAES\b", r"encrypt\w*\(",
            r"subtle\.encrypt|importKey|createCipher", r"secretKey|secret_key|SECRET|cryptoKey|encKey"]
    for s in srcs:
        url = s if s.startswith("http") else f"{BASE}/spley-portal/{s.lstrip('/')}"
        try:
            js = requests.get(url, headers=H, timeout=60).text
        except Exception as ex:
            res["bundle"].append({"archivo": url, "error": str(ex)[:100]}); continue
        info = {"archivo": url, "bytes": len(js), "hits": []}
        for p in pats:
            for mt in list(re.finditer(p, js))[:6]:
                a, b = max(0, mt.start() - 250), min(len(js), mt.end() + 350)
                info["hits"].append({"patron": p, "contexto": js[a:b]})
        res["bundle"].append(info)
except Exception as ex:
    res["bundle"].append({"error": str(ex)[:200]})

json.dump(res, open("diag_api_ficha2.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print("expedientes:", len(res["expedientes"]), "| archivos js:", len(res["bundle"]),
      "| hits:", sum(len(x.get("hits", [])) for x in res["bundle"]))
