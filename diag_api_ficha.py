"""Diagnóstico: ¿de qué API(s) saca la FICHA sus datos? Abre fichas con Chrome, registra las llamadas
XHR/Fetch a api.congreso.gob.pe (URL, método, cuerpo enviado y respuesta) y las guarda en diag_api_ficha.json."""
import json, time, sys, os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

BASE = "https://wb2server.congreso.gob.pe"
FICHAS = [(2021, 14671), (2021, 14288)]          # una reciente y una más antigua
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"

opts = Options()
for a in ("--headless=new", "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
          "--window-size=1920,1080", f"user-agent={UA}"):
    opts.add_argument(a)
opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
d = webdriver.Chrome(options=opts)
d.execute_cdp_cmd("Network.enable", {})
salida = []


def recoger(etiqueta):
    reqs, resp = {}, {}
    for e in d.get_log("performance"):
        m = json.loads(e["message"])["message"]
        p = m.get("params", {})
        if m["method"] == "Network.requestWillBeSent":
            r = p["request"]
            reqs[p["requestId"]] = {"url": r["url"], "metodo": r["method"], "cuerpo": r.get("postData"),
                                    "tipo": p.get("type")}
        elif m["method"] == "Network.responseReceived":
            resp[p["requestId"]] = {"status": p["response"]["status"], "mime": p["response"].get("mimeType")}
    for rid, rq in reqs.items():
        if rq["tipo"] not in ("XHR", "Fetch") or "congreso.gob.pe" not in rq["url"]:
            continue
        item = {"ficha": etiqueta, **rq, **resp.get(rid, {})}
        try:
            body = d.execute_cdp_cmd("Network.getResponseBody", {"requestId": rid}).get("body", "")
            item["respuesta_inicio"] = body[:3000]
            item["respuesta_largo"] = len(body)
        except Exception as ex:
            item["respuesta_inicio"] = f"(no disponible: {str(ex)[:80]})"
        salida.append(item)


for per, num in FICHAS:
    for ruta in ("expediente", "congreso/expediente"):
        d.get("about:blank")
        d.get(f"{BASE}/spley-portal/#/{ruta}/{per}/{num}")
        try:
            WebDriverWait(d, 25).until(lambda x: x.find_elements(By.CSS_SELECTOR, "div.estado-valor"))
        except Exception:
            continue
        time.sleep(4)
        recoger(f"{per}-{num}")
        for el in d.find_elements(By.CSS_SELECTOR, "a.ver-mas")[:1]:      # captura también la llamada de "Ver más..."
            d.execute_script("arguments[0].click();", el)
            time.sleep(3)
            recoger(f"{per}-{num} (clic Ver más)")
        break
d.quit()
json.dump(salida, open("diag_api_ficha.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2)
print(f"Llamadas a la API registradas: {len(salida)}")
for x in salida:
    print(f"- {x['ficha']}: {x['metodo']} {x['url']}  [{x.get('status')}] {x.get('respuesta_largo')} bytes")
