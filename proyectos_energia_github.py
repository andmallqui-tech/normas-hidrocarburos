"""
=============================================================================
PROYECTOS DE LEY DE ENERGÍA - CONGRESO DEL PERÚ (SPLEY) - SEGUIMIENTO
=============================================================================
Todo vive en tu Google Sheet (no se guarda nada en el repo):
  · Pestaña "Proyectos": una fila por proyecto energético, actualizada en cada corrida.
    Es también la memoria: de ahí se lee lo que ya se conocía para detectar cambios.
  · Pestaña "Corridas": una fila por ejecución.
  · Telegram: UN mensaje por corrida con proyectos nuevos y cambios (el cierre
    aparece como una línea dentro del cambio). Sin novedades, no se envía nada.
  · La primera corrida (pestaña Proyectos vacía) sólo llena la base de datos.

Pasos de cada corrida:
  1) Captura (Selenium) la API interna del portal y descarga TODOS los proyectos.
  2) Filtra los del sector energía (mismo filtro de palabras de siempre).
  3) Lee la FICHA de cada proyecto energético (la API de la ficha también se
     captura sola desde el portal).
  4) Compara contra la pestaña Proyectos y detecta: nuevo, cambio de estado o
     comisión, dictamen, cerrado y cambio de sumilla.

Variables de entorno:
  GOOGLE_CREDENTIALS_JSON (JSON o base64) / SPREADSHEET_ID   (obligatorias)
  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
  HOJA_PROYECTOS / HOJA_CORRIDAS   nombres de las pestañas (Proyectos / Corridas)
  PAGE_SIZE            registros por página (500)
  FILTROS_JSON         JSON que se mezcla con el cuerpo de la búsqueda
  INCLUIR_MINERIA=1    INCLUIR_AMBIENTE=1
  SUMILLA_EN_ALERTAS   "nuevos" (defecto: sumilla en nuevos y si cambia) | "todas"
  ALERTAR_PRIMERA=1    alerta también en la primera corrida (por defecto no)
  ESTADOS_TERMINALES   regex que marca un proyecto como cerrado
  FICHA_PEDIDO         p.ej. "14288" o "14288/2025-CR": envía esa ficha completa
Uso: python proyectos_energia_github.py [--dry-run]   (dry-run: no escribe ni envía)
=============================================================================
"""
import os
import re
import sys
import json
import argparse
import html as htmllib
import time
import copy
import base64
import unicodedata
from datetime import date, datetime
from urllib.parse import urlsplit, parse_qsl, urlencode
from zoneinfo import ZoneInfo

import requests
import pandas as pd

print("=" * 100)
print("🏛️  PROYECTOS DE LEY DE ENERGÍA - CONGRESO DEL PERÚ (seguimiento)")
print("=" * 100)

T0 = time.time()
AHORA = datetime.now(ZoneInfo("America/Lima"))
HOY = AHORA.date()
BASE = "https://wb2server.congreso.gob.pe"
PORTAL = BASE + "/spley-portal/#/expediente/search"
SERVICIO = "spley-portal-service"
CACHE_API = "api_spley_capturada.json"
CACHE_DETALLE = "api_spley_detalle.json"

PAGE_SIZE = int(os.getenv("PAGE_SIZE") or 500)
FILTROS_EXTRA = json.loads(os.getenv("FILTROS_JSON") or "{}")
_SI = ("1", "true", "si", "sí")
INCLUIR_MINERIA = (os.getenv("INCLUIR_MINERIA") or "").lower() in _SI
INCLUIR_AMBIENTE = (os.getenv("INCLUIR_AMBIENTE") or "").lower() in _SI
ALERTAR_PRIMERA = (os.getenv("ALERTAR_PRIMERA") or "").lower() in _SI
SUMILLA_EN_ALERTAS = (os.getenv("SUMILLA_EN_ALERTAS") or "nuevos").lower()
FICHA_PEDIDO = (os.getenv("FICHA_PEDIDO") or "").strip()
TERMINAL = re.compile(
    os.getenv("ESTADOS_TERMINALES")
    or r"publicad\w+ en (?:el )?(?:diario oficial )?el peruano|publicad\w+ como ley", re.I)

CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
HOJA_PROY = (os.getenv("HOJA_PROYECTOS") or "Proyectos").strip()
HOJA_CORR = (os.getenv("HOJA_CORRIDAS") or "Corridas").strip()
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")


# =============================================================================
# 1) CAPTURAR LA API QUE USA EL PORTAL
# =============================================================================

def _lista_mas_grande(obj):
    """Devuelve la lista de diccionarios más grande dentro de un JSON (los registros)."""
    mejor = []
    if isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            mejor = obj
        for x in obj:
            cand = _lista_mas_grande(x)
            if len(cand) > len(mejor):
                mejor = cand
    elif isinstance(obj, dict):
        for v in obj.values():
            cand = _lista_mas_grande(v)
            if len(cand) > len(mejor):
                mejor = cand
    return mejor


def _total(obj):
    """Busca un campo tipo total/count/totalElements dentro del JSON."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, int) and re.search(r"total|count|cantidad|registros", k, re.I):
                return v
        for v in obj.values():
            t = _total(v)
            if t:
                return t
    return None


def _parece_proyectos(registros):
    if not registros:
        return False
    claves = " ".join(registros[0].keys()).lower()
    return "titulo" in claves or "pley" in claves or "proyecto" in claves


def capturar_api():
    """Abre el portal con Selenium y captura la petición que trae la lista de proyectos."""
    if os.path.exists(CACHE_API):
        print(f"♻️  Usando API capturada previamente ({CACHE_API})")
        with open(CACHE_API, encoding="utf-8") as f:
            return json.load(f)

    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By

    print("\n🌐 Abriendo el portal para capturar su API interna...")
    opts = Options()
    for a in ("--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
              "--disable-gpu", "--window-size=1920,1080", f"user-agent={UA}"):
        opts.add_argument(a)
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(90)

    try:
        driver.get(PORTAL)
        time.sleep(15)
        # Si el portal exige pulsar "Buscar", lo pulsamos
        for b in driver.find_elements(By.XPATH, "//button[contains(., 'Buscar') or contains(., 'BUSCAR')]"):
            try:
                driver.execute_script("arguments[0].click();", b)
                time.sleep(10)
                break
            except Exception:
                pass

        peticiones, respuestas = {}, set()
        for entrada in driver.get_log("performance"):
            msg = json.loads(entrada["message"])["message"]
            p = msg.get("params", {})
            if msg.get("method") == "Network.requestWillBeSent":
                req = p.get("request", {})
                if SERVICIO in req.get("url", ""):
                    peticiones[p["requestId"]] = req
            elif msg.get("method") == "Network.responseReceived":
                if SERVICIO in p.get("response", {}).get("url", ""):
                    respuestas.add(p["requestId"])

        print(f"   🔎 Peticiones al servicio detectadas: {len(peticiones)}")
        mejor, mejor_n = None, 0
        for rid, req in peticiones.items():
            print(f"      • {req.get('method')} {req.get('url')}")
            if rid not in respuestas:
                continue
            try:
                cuerpo = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": rid})
                datos = json.loads(cuerpo.get("body") or "null")
            except Exception:
                continue
            regs = _lista_mas_grande(datos)
            if _parece_proyectos(regs) and len(regs) >= mejor_n:
                post = req.get("postData")
                if post is None and req.get("hasPostData"):
                    try:
                        post = driver.execute_cdp_cmd("Network.getRequestPostData",
                                                      {"requestId": rid}).get("postData")
                    except Exception:
                        post = None
                mejor_n = len(regs)
                mejor = {
                    "url": req["url"],
                    "method": req.get("method", "GET"),
                    "headers": {k: v for k, v in req.get("headers", {}).items()
                                if k.lower() in ("content-type", "accept", "authorization")},
                    "body": json.loads(post) if post else None,
                    "ejemplo_claves": list(regs[0].keys()),
                    "total_detectado": _total(datos),
                }
    finally:
        driver.quit()

    if not mejor:
        raise RuntimeError("No se pudo capturar la API del portal. Revisa el log de peticiones de arriba.")

    print(f"   ✅ API capturada: {mejor['method']} {mejor['url']}")
    print(f"   📦 Cuerpo: {json.dumps(mejor['body'], ensure_ascii=False)[:300]}")
    print(f"   🔑 Campos: {mejor['ejemplo_claves']}")
    print(f"   🔢 Total según el portal: {mejor['total_detectado']}")
    with open(CACHE_API, "w", encoding="utf-8") as f:
        json.dump(mejor, f, ensure_ascii=False, indent=2)
    return mejor


# =============================================================================
# 2) DESCARGAR TODOS LOS PROYECTOS PÁGINA POR PÁGINA
# =============================================================================

CLAVES_TAMANO = ("pageSize", "size", "limit", "rows", "tamanio", "tamano", "cantidad", "length")
CLAVES_DESDE = ("rowStart", "start", "offset", "inicio", "first")
CLAVES_PAGINA = ("page", "pageNumber", "pagina", "numPagina", "pageIndex")


def _buscar_clave(d, candidatas):
    if not isinstance(d, dict):
        return None
    for k in d:
        if k.lower() in [c.lower() for c in candidatas]:
            return k
    return None


def descargar_todo(api):
    sesion = requests.Session()
    sesion.headers.update({"User-Agent": UA, "Origin": BASE, "Referer": BASE + "/spley-portal/"})
    sesion.headers.update(api.get("headers") or {})

    cuerpo = dict(api.get("body") or {})
    cuerpo.update(FILTROS_EXTRA)
    k_tam = _buscar_clave(cuerpo, CLAVES_TAMANO)
    k_desde = _buscar_clave(cuerpo, CLAVES_DESDE)
    k_pag = _buscar_clave(cuerpo, CLAVES_PAGINA)
    base_pag = cuerpo.get(k_pag, 0) if k_pag else 0   # ¿la paginación empieza en 0 o en 1?
    print(f"\n📥 Descargando (tamaño='{k_tam}', desde='{k_desde}', página='{k_pag}', {PAGE_SIZE}/pág)")

    if not (k_tam and (k_desde or k_pag)):
        print("   ⚠️ No se reconocieron los campos de paginación; se hará una sola llamada.")

    todos, vistos, i = [], set(), 0
    total = api.get("total_detectado")
    while True:
        if k_tam:
            cuerpo[k_tam] = PAGE_SIZE
        if k_desde:
            cuerpo[k_desde] = i * PAGE_SIZE
        elif k_pag:
            cuerpo[k_pag] = base_pag + i

        for intento in range(4):
            try:
                if api["method"].upper() == "POST":
                    r = sesion.post(api["url"], json=cuerpo, timeout=60)
                else:
                    r = sesion.get(api["url"], params=cuerpo, timeout=60)
                r.raise_for_status()
                datos = r.json()
                break
            except Exception as e:
                print(f"   ⚠️ Página {i + 1}, intento {intento + 1}: {e}")
                time.sleep(5 * (intento + 1))
        else:
            print("   ❌ Se abandona la descarga en esta página.")
            break

        regs = _lista_mas_grande(datos)
        total = total or _total(datos)
        nuevos = 0
        for reg in regs:
            clave = json.dumps(reg, sort_keys=True, ensure_ascii=False)[:400]
            if clave not in vistos:
                vistos.add(clave)
                todos.append(reg)
                nuevos += 1
        print(f"   Página {i + 1}: {len(regs)} registros (+{nuevos}) — acumulado {len(todos)}/{total or '?'}")

        if not regs or nuevos == 0 or not k_tam or (total and len(todos) >= total):
            break
        i += 1
        time.sleep(0.7)   # cortesía con el servidor del Congreso

    print(f"   ✅ Proyectos descargados: {len(todos)}")
    return todos
# =============================================================================
# 3) NORMALIZAR CAMPOS (los nombres exactos dependen del portal)
# =============================================================================

def _plano(d, prefijo=""):
    out = {}
    for k, v in d.items():
        k2 = f"{prefijo}{k}"
        if isinstance(v, dict):
            out.update(_plano(v, k2 + "."))
        elif isinstance(v, list):
            out[k2] = "; ".join(
                " ".join(str(x) for x in e.values() if x not in (None, "")) if isinstance(e, dict) else str(e)
                for e in v)
        else:
            out[k2] = v
    return out


def _tomar(fila, patrones, excluir=()):
    for p in patrones:
        for k, v in fila.items():
            kl = k.lower()
            if re.search(p, kl) and not any(re.search(x, kl) for x in excluir) and v not in (None, ""):
                return v
    return ""


# =============================================================================
# 4) FILTRO DE ENERGÍA (mismo estilo que normas_github.py)
# =============================================================================

def normalizar_texto(texto):
    if not isinstance(texto, str):
        return ""
    texto = unicodedata.normalize("NFKD", texto.lower()).encode("ascii", "ignore").decode("utf-8")
    texto = re.sub(r"[^a-z0-9\s]", " ", texto)
    return re.sub(r"\s+", " ", texto).strip()


def _rx(patrones):
    return re.compile(r"\b(?:" + "|".join(sorted(patrones, key=len, reverse=True)) + r")\b")


SUBSECTORES = {
    "Hidrocarburos": _rx([
        r"hidrocarbur\w*", r"petrole\w*", r"petroquimic\w*", r"petroperu", r"perupetro", r"gas natural",
        r"gas licuado\w*", r"glp", r"gnv", r"gnl", r"gasocentro\w*", r"combustibl\w*", r"gasolin\w*",
        r"gasohol", r"diesel", r"kerosene", r"crudo", r"oleoducto\w*", r"gasoducto\w*", r"poliducto\w*",
        r"refineri\w*", r"banda de precios", r"canon gasifero", r"camisea", r"lotes? petroler\w*",
        r"fise", r"bonogas", r"masificacion del gas", r"estaciones? de servicio", r"grifos?",
        r"fondo de estabilizacion de precios", r"lubricante\w*", r"talara",
    ]),
    "Electricidad": _rx([
        r"electricidad", r"energia electrica", r"sector electrico", r"subsector electricidad",
        r"servicio publico de electricidad", r"concesion(?:es)? electrica\w*", r"ley de concesiones electricas",
        r"tarifas? electrica\w*", r"generacion electrica", r"transmision electrica", r"distribucion electrica",
        r"electrificacion\w*", r"suministro electrico", r"sistema electrico\w*", r"sein", r"coes",
        r"hidroelectric\w*", r"termoelectric\w*", r"subestacion\w*", r"lineas? de transmision", r"kv",
        r"empresas? (?:de|distribuidoras de) electricidad", r"osinergmin", r"alumbrado publico",
        r"fose", r"mercado electrico", r"usuarios? libres?", r"electro\s?(?:sur|norte|centro|oriente|ucayali|puno)\w*",
    ]),
    "Renovables y transición": _rx([
        r"energias? renovables?", r"recursos energeticos renovables", r"rer", r"energia (?:solar|eolica)",
        r"fotovoltaic\w*", r"paneles? solares?", r"eolic\w*", r"geotermi\w*", r"biocombustibl\w*",
        r"biodiesel", r"etanol", r"hidrogeno\w*", r"transicion energetica", r"eficiencia energetica",
        r"electromovilidad", r"vehiculos? electric\w*", r"estaciones? de carga", r"generacion distribuida",
        r"autogeneracion", r"cogeneracion", r"almacenamiento de energia", r"litio",
    ]),
    "Energía (general)": _rx([
        r"energia y minas", r"politica energetica", r"seguridad energetica", r"crisis energetica",
        r"matriz energetica", r"sector energetico", r"recursos energeticos", r"minem",
        r"planeamiento energetico", r"pobreza energetica", r"acceso a la energia",
    ]),
}
MINERIA = _rx([r"mineri\w*", r"miner[oa]s?", r"reinfo", r"ingemmet", r"relaves?", r"pequena mineria",
               r"mineria artesanal", r"formalizacion minera", r"canon minero", r"concesion(?:es)? minera\w*"])
AMBIENTE = _rx([r"ambiental\w*", r"cambio climatico", r"oefa", r"senace", r"minam", r"pasivos ambientales",
                r"emisiones", r"gases de efecto invernadero", r"limites? maximos? permisibles?"])
ENERGIA_SUELTA = _rx([r"energia\w*", r"energetic\w*", r"electric\w*"])
# Falsos positivos frecuentes en títulos legislativos
RUIDO = _rx([r"energia vital", r"sillas? electric\w*", r"carro electrico de golf"])


def clasificar(titulo, sumilla=""):
    """Clasifica SOLO con título + sumilla (la comisión no cuenta).
    Confianza: Alta  = el subsector aparece en el TÍTULO;
               Media = sólo aparece en la SUMILLA;
               Revisar = sólo palabras sueltas tipo "energía/eléctrico"."""
    t_tit = normalizar_texto(titulo)
    t = normalizar_texto(f"{titulo} {sumilla}")
    tags = [nombre for nombre, rx in SUBSECTORES.items() if rx.search(t)]
    if INCLUIR_MINERIA and MINERIA.search(t):
        tags.append("Minería")
    if INCLUIR_AMBIENTE and AMBIENTE.search(t):
        tags.append("Ambiente")
    en_titulo = any(rx.search(t_tit) for rx in SUBSECTORES.values()) \
        or (INCLUIR_MINERIA and MINERIA.search(t_tit)) or (INCLUIR_AMBIENTE and AMBIENTE.search(t_tit))
    confianza = ("Alta" if en_titulo else "Media") if tags else ""
    if not tags and ENERGIA_SUELTA.search(t) and not RUIDO.search(t):
        tags, confianza = ["Energía (revisar)"], "Revisar"
    claves = sorted({m.group(0) for rx in list(SUBSECTORES.values()) + [ENERGIA_SUELTA] for m in rx.finditer(t)})
    return ", ".join(tags), confianza, ", ".join(claves)


# =============================================================================
# 3b) NORMALIZAR LA LISTA Y LEER LA FICHA DE CADA PROYECTO
# =============================================================================

def a_fecha(v):
    """Convierte epoch-ms, ISO o dd/mm/aaaa a Timestamp (o None)."""
    if v in (None, ""):
        return None
    try:
        if isinstance(v, (int, float)) and v > 10**11:
            return pd.to_datetime(v, unit="ms")
        s = str(v).strip()
        if re.match(r"\d{2}/\d{2}/\d{4}", s):
            return pd.to_datetime(s[:10], format="%d/%m/%Y")
        return pd.to_datetime(s[:19])
    except Exception:
        return None


def fmt(v):
    f = a_fecha(v)
    return f.strftime("%d/%m/%Y") if f is not None and not pd.isna(f) else ""


SUFIJOS = [(r"congres", "CR"), (r"ejecutivo", "PE"), (r"judicial", "PJ"), (r"defensor", "DP"),
           (r"ministerio publico", "MP"), (r"contralor", "CG"), (r"jurado nacional", "JNE"),
           (r"onpe|procesos electorales", "ONPE"), (r"reniec", "RENIEC"), (r"ciudadan", "IC"),
           (r"gobierno regional", "GR"), (r"municipalidad", "GL"), (r"tribunal constitucional", "TC")]


def sufijo_proponente(prop):
    t = normalizar_texto(prop)
    for rx, suf in SUFIJOS:
        if re.search(rx, t):
            return suf
    return ""


def normalizar(reg):
    """Campos que ya vienen en la lista general (la ficha completa los enriquece)."""
    f = _plano(reg)
    periodo = _tomar(f, [r"perpar", r"periodo"])
    numero = _tomar(f, [r"pleynum", r"numproyecto", r"^numero", r"nroproyecto", r"num"], excluir=[r"id$"])
    titulo = _tomar(f, [r"titulo"], excluir=[r"id$"])
    sumilla = _tomar(f, [r"sumilla"])
    if not titulo:
        titulo = _tomar(f, [r"descripcion"]) or sumilla
    fecha = fmt(_tomar(f, [r"fecpresent", r"fechapresent", r"fecha"]))
    proponente = _tomar(f, [r"desproponente", r"proponente"], excluir=[r"id$"])
    anio = fecha[-4:] if fecha else ""
    suf = sufijo_proponente(str(proponente))
    num_fmt = f"{numero}/{anio}" + (f"-{suf}" if suf else "") if numero else ""
    txt = normalizar_texto(f"{titulo} {sumilla}")
    tipo = "Resolución legislativa" if "resolucion legislativa" in txt else "Ley"
    link = f"{BASE}/spley-portal/#/expediente/{periodo}/{numero}" if periodo and numero else ""
    return {
        "id": f"{periodo}-{numero}", "periodo": periodo, "numero": numero, "numero_fmt": num_fmt,
        "tipo": tipo, "fecha_pres": fecha, "sumilla": sumilla, "titulo": titulo,
        "estado": _tomar(f, [r"desestado", r"estado"], excluir=[r"id$"]),
        "proponente": proponente,
        "autores_lista": _tomar(f, [r"autor", r"congresista", r"firmante"], excluir=[r"id$"]),
        "grupo": _tomar(f, [r"grupo", r"bancada"], excluir=[r"id$"]),
        "comision": _tomar(f, [r"comision"], excluir=[r"id$"]),
        "link": link,
    }


# ---- capturar las llamadas que hace la FICHA del portal --------------------

def _escalares(obj, ruta=()):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from _escalares(v, ruta + (k,))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            yield from _escalares(v, ruta + (i,))
    else:
        yield ruta, obj


def _fijar(obj, ruta, valor):
    for k in ruta[:-1]:
        obj = obj[k]
    obj[ruta[-1]] = valor


def _enlaces(pl, plano_muestra):
    """Dónde aparecen (en la URL o el cuerpo) los datos del proyecto de muestra."""
    inv = {}
    for k, v in plano_muestra.items():
        if v in (None, "") or isinstance(v, bool):
            continue
        s = str(v)
        if len(s) >= 3 or re.search(r"num|per", k.lower()):
            inv.setdefault(s, k)
    u = urlsplit(pl["url"])
    enl = [(("seg", i), inv[s]) for i, s in enumerate(u.path.split("/")) if s in inv]
    enl += [(("q", n), inv[v]) for n, v in parse_qsl(u.query) if v in inv]
    if isinstance(pl.get("body"), (dict, list)):
        enl += [(("body", r), inv[str(v)]) for r, v in _escalares(pl["body"]) if str(v) in inv]
    return enl


def _instanciar(pl, plano_destino):
    u = urlsplit(pl["url"])
    segs = u.path.split("/")
    qs = dict(parse_qsl(u.query))
    cuerpo = copy.deepcopy(pl.get("body"))
    for (donde, ref), clave in pl["enlaces"]:
        nuevo = plano_destino.get(clave)
        if nuevo in (None, ""):
            continue
        if donde == "seg":
            segs[ref] = str(nuevo)
        elif donde == "q":
            qs[ref] = str(nuevo)
        elif donde == "body" and cuerpo is not None:
            _fijar(cuerpo, tuple(ref), nuevo)
    url = u._replace(path="/".join(segs), query=urlencode(qs)).geturl()
    return url, cuerpo


def capturar_detalle(api_lista, link_muestra, plano_muestra):
    """Abre la ficha de un proyecto y guarda las llamadas que hace el portal."""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    print("\n🌐 Capturando la API de la ficha de un proyecto...")
    opts = Options()
    for a in ("--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
              "--disable-gpu", "--window-size=1920,1080", f"user-agent={UA}"):
        opts.add_argument(a)
    opts.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(90)
    plantillas = []
    try:
        driver.get(BASE + "/spley-portal/")
        time.sleep(8)
        driver.execute_script("window.location.hash = arguments[0];", link_muestra.split("#", 1)[1])
        time.sleep(12)
        peticiones, respuestas = {}, set()
        for entrada in driver.get_log("performance"):
            msg = json.loads(entrada["message"])["message"]
            p = msg.get("params", {})
            if msg.get("method") == "Network.requestWillBeSent":
                req = p.get("request", {})
                if SERVICIO in req.get("url", ""):
                    peticiones[p["requestId"]] = req
            elif msg.get("method") == "Network.responseReceived":
                if SERVICIO in p.get("response", {}).get("url", ""):
                    respuestas.add(p["requestId"])
        lista_base = api_lista["url"].split("?")[0]
        vistos = set()
        for rid, req in peticiones.items():
            url, metodo = req.get("url", ""), req.get("method", "GET")
            if metodo == "OPTIONS" or url.split("?")[0] == lista_base or rid not in respuestas:
                continue
            try:
                datos = json.loads(driver.execute_cdp_cmd(
                    "Network.getResponseBody", {"requestId": rid}).get("body") or "null")
            except Exception:
                continue
            if not datos:
                continue
            post = req.get("postData")
            if post is None and req.get("hasPostData"):
                try:
                    post = driver.execute_cdp_cmd("Network.getRequestPostData", {"requestId": rid}).get("postData")
                except Exception:
                    post = None
            try:
                cuerpo = json.loads(post) if post else None
            except Exception:
                cuerpo = None
            clave = (metodo, url.split("?")[0])
            if clave in vistos:
                continue
            vistos.add(clave)
            pl = {"url": url, "method": metodo, "body": cuerpo,
                  "headers": {k: v for k, v in req.get("headers", {}).items()
                              if k.lower() in ("content-type", "accept", "authorization")}}
            pl["enlaces"] = _enlaces(pl, plano_muestra)
            print(f"   • {metodo} {url}  (enlaces al proyecto: {len(pl['enlaces'])})")
            pl["muestra"] = json.dumps(datos, ensure_ascii=False)[:20000]
            plantillas.append(pl)
    finally:
        driver.quit()
    with open(CACHE_DETALLE, "w", encoding="utf-8") as f:
        json.dump(plantillas, f, ensure_ascii=False, indent=2)
    if not plantillas:
        print("   ⚠️ No se capturó ninguna llamada de ficha: se usarán sólo los datos de la lista.")
    return [p for p in plantillas if p["enlaces"]]


def leer_ficha(sesion, plantillas, plano_destino):
    """Repite las llamadas de la ficha para otro proyecto. Devuelve (datos, n_errores)."""
    out, errores = {}, 0
    for i, pl in enumerate(plantillas):
        url, cuerpo = _instanciar(pl, plano_destino)
        for intento in range(3):
            try:
                if pl["method"].upper() == "POST":
                    r = sesion.post(url, json=cuerpo, headers=pl["headers"], timeout=60)
                else:
                    r = sesion.get(url, headers=pl["headers"], timeout=60)
                r.raise_for_status()
                out[f"ep{i}"] = r.json()
                break
            except Exception as e:
                if intento == 2:
                    errores += 1
                    print(f"      ⚠️ ficha {url[-60:]}: {e}")
                time.sleep(2 * (intento + 1))
    return (out or None), errores


# ---- interpretar la ficha (nombres de campo por patrón, como _tomar) --------

def recorrer(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k, v
            yield from recorrer(v)
    elif isinstance(obj, list):
        for x in obj:
            yield from recorrer(x)


def primero(det, patrones, excluir=r"id$"):
    for p in patrones:
        for k, v in recorrer(det):
            if re.search(p, k.lower()) and not re.search(excluir, k.lower()) \
                    and isinstance(v, (str, int, float)) and not isinstance(v, bool) and v != "":
                return v
    return ""


def _nombre_rol(el):
    if isinstance(el, str):
        return el.strip(), ""
    if not isinstance(el, dict):
        return "", ""
    nombre = " ".join(str(v) for k, v in el.items()
                      if re.search(r"nom|ape", k.lower()) and not re.search(r"id$|tipo|grupo|partido|bancada|comision", k.lower())
                      and isinstance(v, str) and v)
    if not nombre:
        nombre = " ".join(str(v) for v in el.values() if isinstance(v, str) and v and not v.isdigit())[:80]
    rol = " ".join(str(v) for k, v in el.items()
                   if re.search(r"tipo|rol|condicion|calidad", k.lower()) and isinstance(v, str))
    return nombre.strip(), rol.lower()


def extraer_autores(det):
    principal, co, vistos = "", [], set()
    for k, v in recorrer(det):
        if isinstance(v, list) and v and re.search(r"autor|firmante|congresista|proponente", k.lower()):
            for el in v:
                nombre, rol = _nombre_rol(el)
                if not nombre or nombre in vistos:
                    continue
                vistos.add(nombre)
                es_co = re.search(r"co.?autor|adherente", rol)
                if not principal and not es_co and (re.search(r"autor|principal|primer", rol) or not rol):
                    principal = nombre
                else:
                    co.append(nombre)
    if not principal and co:
        principal, co = co[0], co[1:]
    return principal, co


def extraer_eventos(det):
    """Lista de movimientos del expediente: [{'fecha': Timestamp, 'estado': str, 'detalle': str}]."""
    mejor, mejor_pref = [], False
    for k, v in recorrer(det):
        if not (isinstance(v, list) and v and all(isinstance(x, dict) for x in v)):
            continue
        if not any(re.search(r"fec", kk.lower()) for kk in v[0]):
            continue
        pref = bool(re.search(r"seguimiento|historial|movimiento|tramite|accion", k.lower()))
        if (pref, len(v)) > (mejor_pref, len(mejor)):
            mejor, mejor_pref = v, pref
    eventos = []
    for el in mejor:
        fecha = a_fecha(next((x for kk, x in el.items() if re.search(r"fec", kk.lower()) and x), None))
        estado = next((str(x) for kk, x in el.items()
                       if re.search(r"estado", kk.lower()) and not re.search(r"id$", kk.lower())
                       and isinstance(x, str) and x), "")
        detalle = next((str(x) for kk, x in el.items()
                        if re.search(r"desc|detalle|accion|asunto|observ|sumilla", kk.lower())
                        and isinstance(x, str) and x), "")
        if fecha is not None and not pd.isna(fecha):
            eventos.append({"fecha": fecha, "estado": estado.strip(), "detalle": detalle.strip()})
    eventos.sort(key=lambda e: e["fecha"])
    return eventos


def extraer_comisiones(det, fallback=""):
    nombres = []
    for k, v in recorrer(det):
        if re.search(r"comision", k.lower()) and not re.search(r"id$|fec", k.lower()):
            for x in (v if isinstance(v, list) else [v]):
                if isinstance(x, str) and x.strip():
                    nombres.append(x.strip())
                elif isinstance(x, dict):
                    n = next((str(y) for kk, y in x.items() if re.search(r"nom|desc|comision", kk.lower())
                              and not re.search(r"id$", kk.lower()) and isinstance(y, str) and y), "")
                    if n:
                        nombres.append(n)
    if not nombres and fallback:
        nombres = [str(fallback)]
    return "; ".join(dict.fromkeys(nombres))


RX_DICTAMEN = re.compile(r"dictamen")
RX_NO_DICTAMEN = re.compile(r"(?:sin|pendiente de|solicita|pedido de|ampliacion de plazo para)\s+(?:emitir\s+)?dictamen")
HITOS = [("Admitido", re.compile(r"admitid|admisib")),
         ("Dictamen", None),
         ("Aprobado en Pleno", re.compile(r"aprobad\w+ (?:en|por) (?:el )?pleno|primera votacion|segunda votacion|exonerad\w+ de segunda")),
         ("Autógrafa", re.compile(r"autografa")),
         ("Publicada", TERMINAL)]


def analizar_tramite(eventos, estado_lista):
    """Hitos, dictamen sí/no, historial resumido y cierre a partir de los movimientos."""
    hitos, dictamen, hist, cierre_txt, cierre_fecha = [], False, [], "", ""
    for e in eventos:
        t = normalizar_texto(f"{e['estado']} {e['detalle']}")
        d = e["fecha"].strftime("%d/%m/%Y")
        for nombre, rx in HITOS:
            hit = (RX_DICTAMEN.search(t) and not RX_NO_DICTAMEN.search(t)) if nombre == "Dictamen" else \
                  (rx.search(f"{e['estado']} {e['detalle']}" if nombre == "Publicada" else t))
            if hit:
                if nombre == "Dictamen":
                    dictamen = True
                etiqueta = f"{nombre} ({d})"
                if etiqueta not in hitos:
                    hitos.append(etiqueta)
        if TERMINAL.search(f"{e['estado']} {e['detalle']}"):
            m = re.search(r"ley\s*(?:n[°ºo.]*)?\s*(\d{5})", f"{e['estado']} {e['detalle']}", re.I)
            cierre_txt = f"Ley N° {m.group(1)}" if m else "Publicada en El Peruano"
            cierre_fecha = d
        nombre_estado = e["estado"] or e["detalle"][:60]
        if nombre_estado and (not hist or hist[-1][0] != nombre_estado):
            hist.append((nombre_estado, d))
    cerrado = bool(cierre_txt) or bool(TERMINAL.search(str(estado_lista or "")))
    if cerrado and not cierre_txt:
        cierre_txt = "Publicada en El Peruano"
    historial = " → ".join(f"{n} ({d})" for n, d in hist)
    return "; ".join(hitos), dictamen, historial, cerrado, cierre_txt, cierre_fecha


def puntaje(tags, claves, comision, confianza):
    p = {"Alta": 40, "Media": 30}.get(confianza, 15)
    p += min(30, 10 * len([c for c in claves.split(", ") if c]))
    p += 10 * max(0, len([t for t in tags.split(", ") if t]) - 1)
    if "energia y minas" in normalizar_texto(comision):
        p += 20
    return min(100, p)


def construir_fila(base, det, prev):
    """Une lista + ficha en una fila final. Si la ficha falló, conserva lo de la corrida anterior."""
    r = {k: v for k, v in base.items() if k not in ("autores_lista", "comision", "_reg")}
    principal, co, eventos = "", [], []
    if det:
        principal, co = extraer_autores(det)
        eventos = extraer_eventos(det)
        r["sumilla"] = r["sumilla"] or str(primero(det, [r"sumilla"]))
        r["titulo"] = r["titulo"] or str(primero(det, [r"titulo"]))
        r["grupo"] = str(r["grupo"] or primero(det, [r"grupo", r"bancada"]))
        r["proponente"] = r["proponente"] or str(primero(det, [r"desproponente", r"proponente"]))
        r["comisiones"] = extraer_comisiones(det, base["comision"])
        r["estado"] = str(r["estado"] or primero(det, [r"desestado", r"estado"]) or
                          (eventos[-1]["estado"] if eventos else ""))
        hitos, dictamen, hist, cerrado, cierre_txt, cierre_fecha = analizar_tramite(eventos, r["estado"])
        r.update(hitos=hitos, dictamen=dictamen, historial=hist, cerrado=cerrado,
                 cierre_txt=cierre_txt, cierre_fecha=cierre_fecha)
        r["fecha_mov"] = eventos[-1]["fecha"].strftime("%d/%m/%Y") if eventos else ""
        r["autores"] = "; ".join([principal] + co) if principal else str(base["autores_lista"])
        r["principal"], r["coautores"] = principal or str(base["autores_lista"]), co
    elif prev:
        for k in ("sumilla", "autores", "principal", "coautores", "grupo", "comisiones", "hitos",
                  "dictamen", "historial", "cerrado", "cierre_txt", "cierre_fecha", "fecha_mov"):
            r[k] = prev.get(k, r.get(k))
        r["estado"] = r["estado"] or prev.get("estado", "")
        if not r.get("cerrado") and TERMINAL.search(str(r["estado"] or "")):   # cierre visto sólo en la lista
            r["cerrado"], r["cierre_txt"] = True, "Publicada en El Peruano"
    else:
        r.update(comisiones=str(base["comision"]), hitos="", dictamen=False, historial="", cerrado=False,
                 cierre_txt="", cierre_fecha="", fecha_mov="", autores=str(base["autores_lista"]),
                 principal=str(base["autores_lista"]), coautores=[])
        r["cerrado"] = bool(TERMINAL.search(str(r["estado"] or "")))
        if r["cerrado"]:
            r["cierre_txt"] = "Publicada en El Peruano"
    r["puntaje"] = puntaje(base["subsector"], base["claves"], r.get("comisiones", ""), base["confianza"])
    r["primera_deteccion"] = prev["primera_deteccion"] if prev else str(HOY)
    r["ultimo_cambio"] = prev.get("ultimo_cambio", "") if prev else ""
    return r


# =============================================================================
# 5) DETECCIÓN DE CAMBIOS (compara contra lo que ya está en la hoja Proyectos)
# =============================================================================

def _n(s):
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def comparar(prev, r):
    """Devuelve la lista de eventos de este proyecto y actualiza r['ultimo_cambio']."""
    hoy = HOY.strftime("%d/%m/%Y")
    if prev is None:
        r["ultimo_cambio"] = f"{hoy}: detectado por primera vez"
        return [{"tipo": "nuevo", "r": r, "prev": None, "cambios": []}]
    cambios = []
    cierra = not prev.get("cerrado") and r["cerrado"]
    if not cierra:      # al cerrar, el estado final ya lo dice la línea de cierre
        if prev.get("estado") and r["estado"] and _n(prev["estado"]) != _n(r["estado"]):
            cambios.append(("estado", f"Estado: {prev['estado']} → {r['estado']}"))
        if prev.get("comisiones") and r["comisiones"] and _n(prev["comisiones"]) != _n(r["comisiones"]):
            cambios.append(("comision", f"Comisión: {prev['comisiones']} → {r['comisiones']}"))
    if not prev.get("dictamen") and r["dictamen"]:
        cambios.append(("dictamen", "📝 Dictamen emitido"))
    if prev.get("sumilla") and r["sumilla"] and _n(prev["sumilla"]) != _n(r["sumilla"]):
        cambios.append(("sumilla", "✏️ Sumilla modificada (¿texto sustitutorio?)"))
    if cierra:
        cambios.append(("cerrado", f"✅ CERRADO: {r['cierre_txt']} "
                                   f"({r.get('cierre_fecha') or r.get('fecha_mov') or hoy})."))
    if not cambios:
        return []
    fecha = r.get("fecha_mov") or hoy
    r["ultimo_cambio"] = f"{fecha}: " + " | ".join(re.sub(r"^[^\w]+\s*", "", c[1]) for c in cambios)
    return [{"tipo": "cambio", "r": r, "prev": prev, "cambios": cambios}]


# =============================================================================
# 6) MENSAJE DE TELEGRAM (HTML, un solo mensaje por corrida)
# =============================================================================

def _h(t):
    return htmllib.escape(str(t or ""), quote=False)


def _enlace(url):
    return f'🔗 <a href="{htmllib.escape(url, quote=True)}">Ver ficha</a>'


def _sub(r):
    return r["subsector"] or "Energía"


def bloque_nuevo(r):
    return (f"<b>PL {_h(r['numero_fmt'])} · {_h(_sub(r))}</b>\nPresentado: {_h(r['fecha_pres'])}\n"
            f"Sumilla: {_h(r['sumilla']) or '—'}\nAutores: {_h(r['autores']) or '—'}\n"
            f"Grupo: {_h(r['grupo']) or '—'} · Comisión: {_h(r['comisiones']) or '—'}\n{_enlace(r['link'])}")


def bloque_cambio(ev):
    r = ev["r"]
    txt = f"<b>PL {_h(r['numero_fmt'])} · {_h(_sub(r))}</b>\n" + "\n".join(_h(c[1]) for c in ev["cambios"])
    if r.get("sumilla") and (SUMILLA_EN_ALERTAS == "todas" or any(c[0] == "sumilla" for c in ev["cambios"])):
        txt += f"\nSumilla: {_h(r['sumilla'])}"
    return txt + f"\n{_enlace(r['link'])}"


def mensaje_corrida(eventos):
    """Lista de bloques (separador_previo, texto). Un solo mensaje; si pasa el límite, se parte entre bloques."""
    nuevos = [e for e in eventos if e["tipo"] == "nuevo"]
    cambios = [e for e in eventos if e["tipo"] == "cambio"]
    bloques = [("", "<b>Proyectos de Ley del sector hidrocarburos y energía</b>")]
    for lista, titulo, fn in ((nuevos, "🆕 NUEVOS", lambda e: bloque_nuevo(e["r"])),
                              (cambios, "🔄 CAMBIOS", bloque_cambio)):
        for k, e in enumerate(lista):
            if k == 0:
                bloques.append(("\n\n", f"<b>{titulo} ({len(lista)})</b>\n\n{fn(e)}"))
            else:
                bloques.append(("\n──────────\n", fn(e)))
    return bloques


def msg_ficha(r):
    co = "\n".join(f"  · {_h(c)}" for c in r.get("coautores", [])) or "  —"
    return (f"<b>📄 PROYECTO DE LEY {_h(r['numero_fmt'])}</b>\nSubsector: {_h(_sub(r))}\n\n"
            f"<b>SUMILLA</b>\n{_h(r['sumilla']) or '—'}\n\n<b>TÍTULO</b>\n{_h(r['titulo']) or '—'}\n\n"
            f"<b>AUTORES</b>\nPrincipal: {_h(r.get('principal')) or '—'}\nCoautores:\n{co}\n"
            f"Grupo parlamentario: {_h(r['grupo']) or '—'}\nProponente: {_h(r['proponente']) or '—'}\n\n"
            f"<b>TRÁMITE</b>\nPresentado: {_h(r['fecha_pres'])}\nEstado actual: {_h(r['estado']) or '—'} "
            f"(último movimiento {_h(r.get('fecha_mov')) or '—'})\nComisiones: {_h(r['comisiones']) or '—'}\n"
            f"Hitos: {_h(r['hitos']) or '—'}\nDictamen: {'sí' if r['dictamen'] else 'no'}\n\n"
            f"<b>ÚLTIMO CAMBIO DETECTADO</b>\n{_h(r['ultimo_cambio']) or '—'}\n\n{_enlace(r['link'])}")


def enviar_telegram_html(bloques, bot_token, chat_id, limite=3800):
    """Envía en HTML, partiendo entre bloques para no pasar el límite de Telegram (4096)."""
    if not bot_token or not chat_id:
        print("   ⚠️  Sin TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID: no se envió el mensaje")
        return False
    partes, actual = [], ""
    for sep, texto in bloques:
        if actual and len(actual) + len(sep) + len(texto) > limite:
            partes.append(actual.strip()); actual = ""
        actual += (sep if actual else "") + texto
    if actual.strip():
        partes.append(actual.strip())
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    for k, parte in enumerate(partes, 1):
        try:
            r = requests.post(url, data={"chat_id": chat_id, "text": parte[:4096], "parse_mode": "HTML",
                                         "disable_web_page_preview": "true"}, timeout=15)
            if r.status_code == 400:   # HTML mal formado: reenviar como texto plano en vez de perder el aviso
                plano = re.sub(r"<[^>]+>", "", parte)
                r = requests.post(url, data={"chat_id": chat_id, "text": htmllib.unescape(plano)[:4096],
                                             "disable_web_page_preview": "true"}, timeout=15)
            r.raise_for_status()
            print(f"   ✅ Telegram parte {k}/{len(partes)} enviada")
            if k < len(partes):
                time.sleep(1)
        except Exception as e:
            print(f"   ❌ Error Telegram (parte {k}): {e}")
            return False
    return True


def lineas_novedades(eventos):
    out = []
    for e in eventos:
        r = e["r"]
        if e["tipo"] == "nuevo":
            out.append(f"• {r['numero']}: nuevo ({_sub(r)})")
        else:
            out.append(f"• {r['numero']}: " + "; ".join(re.sub(r"^[^\w]+\s*", "", t) for _, t in e["cambios"]))
    return out


# =============================================================================
# 7) GOOGLE SHEETS (pestañas Proyectos y Corridas)
# =============================================================================

COLUMNAS = [("numero_fmt", "N° PROYECTO", 110), ("tipo", "TIPO", 90), ("fecha_pres", "FECHA PRESENTACIÓN", 110),
            ("sumilla", "SUMILLA", 420), ("titulo", "TÍTULO", 300), ("subsector", "SUBSECTOR", 140),
            ("puntaje", "PUNTAJE", 80), ("autores", "AUTORES", 300), ("grupo", "GRUPO PARLAMENTARIO", 150),
            ("proponente", "PROPONENTE", 150), ("estado", "ESTADO ACTUAL", 170),
            ("fecha_mov", "FECHA ÚLTIMO MOVIMIENTO", 110), ("comisiones", "COMISIONES", 200),
            ("hitos", "HITOS", 260), ("dictamen", "DICTAMEN", 80), ("ultimo_cambio", "ÚLTIMO CAMBIO", 300),
            ("historial", "HISTORIAL DE ESTADOS", 360), ("cerrado", "¿CERRADO?", 80),
            ("primera_deteccion", "PRIMERA DETECCIÓN", 110), ("link", "ENLACE A LA FICHA", 260)]
ENC_CORR = [("FECHA Y HORA", 130), ("REVISADOS", 90), ("DESCARTADOS", 100), ("NUEVOS", 80),
            ("MODIFICADOS", 100), ("CERRADOS", 90), ("DETALLE DE NOVEDADES", 480), ("ERRORES Y DURACIÓN", 200)]
CLAVES_CORR = ["Fecha y hora", "Revisados", "Descartados", "Nuevos", "Modificados", "Cerrados",
               "Detalle de novedades", "Errores y duración"]


def conectar(credentials_raw):
    if not credentials_raw:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON")
    t = credentials_raw.strip()
    info = json.loads(t) if t.startswith("{") else json.loads(base64.b64decode(t))
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _formato_inicial(sheet_id, anchos):
    reqs = [
        {"updateSheetProperties": {"properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                                   "fields": "gridProperties.frozenRowCount"}},
        {"repeatCell": {"range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                        "cell": {"userEnteredFormat": {
                            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                            "backgroundColor": {"red": 0.20, "green": 0.42, "blue": 0.35},
                            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
                        "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment,verticalAlignment)"}},
    ]
    for i, ancho in enumerate(anchos):
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": ancho}, "fields": "pixelSize"}})
    return reqs


def asegurar_hoja(servicio, sid, nombre, encabezados, anchos):
    """Crea la pestaña (con encabezados y formato) si no existe. Si existe con otros encabezados, se detiene
    en vez de escribir en columnas equivocadas. Devuelve True si la pestaña se creó ahora."""
    meta = servicio.spreadsheets().get(spreadsheetId=sid, fields="sheets.properties(sheetId,title)").execute()
    existente = next((x["properties"] for x in meta["sheets"] if x["properties"]["title"] == nombre), None)
    ultima = chr(ord("A") + len(encabezados) - 1)
    if existente:
        resp = servicio.spreadsheets().values().get(spreadsheetId=sid, range=f"'{nombre}'!A1:{ultima}1").execute()
        actuales = [c.strip().upper() for c in (resp.get("values") or [[]])[0]]
        if actuales == [h.upper() for h in encabezados]:
            return False
        if actuales:
            raise RuntimeError(
                f"La pestaña '{nombre}' ya existe pero sus encabezados no coinciden.\n   Tiene:    {actuales}\n"
                f"   Esperado: {encabezados}\n   Renómbrala o bórrala y vuelve a correr: se creará sola "
                f"(o cambia el nombre con HOJA_PROYECTOS / HOJA_CORRIDAS).")
        sheet_id = existente["sheetId"]          # pestaña vacía: solo se ponen encabezados
    else:
        print(f"   🆕 Creando pestaña '{nombre}'...")
        resp = servicio.spreadsheets().batchUpdate(spreadsheetId=sid, body={
            "requests": [{"addSheet": {"properties": {"title": nombre}}}]}).execute()
        sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    servicio.spreadsheets().values().update(spreadsheetId=sid, range=f"'{nombre}'!A1:{ultima}1",
                                            valueInputOption="RAW", body={"values": [encabezados]}).execute()
    servicio.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": _formato_inicial(sheet_id, anchos)}).execute()
    return True


ULT_PROY = chr(ord("A") + len(COLUMNAS) - 1)


def leer_proyectos(servicio, sid):
    """Lo que ya se conocía (id -> fila). Esta lectura es la memoria entre corridas."""
    resp = servicio.spreadsheets().values().get(spreadsheetId=sid, range=f"'{HOJA_PROY}'!A2:{ULT_PROY}").execute()
    prev = {}
    for i, fila in enumerate(resp.get("values", []), start=2):
        fila = [str(x) for x in fila] + [""] * (len(COLUMNAS) - len(fila))
        r = {k: fila[j] for j, (k, _, _) in enumerate(COLUMNAS)}
        m = re.search(r"expediente/(\d+)/(\d+)", r["link"])
        if not m:
            continue
        r["periodo"], r["numero"] = m.groups()
        r["id"] = f"{m.group(1)}-{m.group(2)}"
        r["dictamen"] = r["dictamen"].strip().lower() in ("sí", "si")
        r["cerrado"] = r["cerrado"].strip().lower() in ("sí", "si")
        r["puntaje"] = int(r["puntaje"]) if r["puntaje"].strip().isdigit() else 0
        partes = [x.strip() for x in r["autores"].split(";") if x.strip()]
        r["principal"], r["coautores"] = (partes[0] if partes else ""), partes[1:]
        r["cierre_txt"], r["cierre_fecha"] = "", ""
        r["_fila"], r["_valores"] = i, fila
        prev[r["id"]] = r
    return prev


def fila_hoja(r):
    out = []
    for k, _, _ in COLUMNAS:
        v = r.get(k, "")
        if k in ("dictamen", "cerrado"):
            v = "Sí" if v else "No"
        out.append(str(v)[:49000])
    return out


def guardar_proyectos(servicio, sid, filas):
    """filas: lista de (r, prev|None). Actualiza en su sitio lo que cambió y agrega lo nuevo al final."""
    datos, nuevas = [], []
    for r, prev in filas:
        valores = fila_hoja(r)
        if prev is None:
            nuevas.append(valores)
        elif [_n(x) for x in valores] != [_n(x) for x in prev["_valores"]]:
            datos.append({"range": f"'{HOJA_PROY}'!A{prev['_fila']}:{ULT_PROY}{prev['_fila']}", "values": [valores]})
    if datos:
        servicio.spreadsheets().values().batchUpdate(spreadsheetId=sid, body={
            "valueInputOption": "RAW", "data": datos}).execute()
    if nuevas:
        servicio.spreadsheets().values().append(
            spreadsheetId=sid, range=f"'{HOJA_PROY}'!A:{ULT_PROY}", valueInputOption="RAW",
            insertDataOption="INSERT_ROWS", body={"values": nuevas}).execute()
    print(f"   ✅ '{HOJA_PROY}': {len(nuevas)} filas nuevas, {len(datos)} actualizadas")


def guardar_corrida(servicio, sid, corrida):
    ultima = chr(ord("A") + len(CLAVES_CORR) - 1)
    servicio.spreadsheets().values().append(
        spreadsheetId=sid, range=f"'{HOJA_CORR}'!A:{ultima}", valueInputOption="RAW",
        insertDataOption="INSERT_ROWS", body={"values": [[str(corrida[k]) for k in CLAVES_CORR]]}).execute()
    print(f"   ✅ Corrida registrada en '{HOJA_CORR}'")


# =============================================================================
# MAIN
# =============================================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="no escribe en Sheets ni envía Telegram")
    args = ap.parse_args()
    errores = 0

    sid, creds = os.getenv("SPREADSHEET_ID"), os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not args.dry_run and (not sid or not creds):
        raise RuntimeError("Falta SPREADSHEET_ID o GOOGLE_CREDENTIALS_JSON")
    servicio = conectar(creds) if (sid and creds) else None

    previos = {}
    if servicio:
        if not args.dry_run:
            asegurar_hoja(servicio, sid, HOJA_PROY, [c[1] for c in COLUMNAS], [c[2] for c in COLUMNAS])
            asegurar_hoja(servicio, sid, HOJA_CORR, [c[0] for c in ENC_CORR], [c[1] for c in ENC_CORR])
        try:
            previos = leer_proyectos(servicio, sid)
        except Exception as e:
            print(f"   ⚠️ No se pudo leer '{HOJA_PROY}' (se trata como primera corrida): {e}")
    primera = not previos
    print(f"\n🗂️  Proyectos ya registrados en la hoja: {len(previos)}"
          + (" → PRIMERA CORRIDA: sólo se llena la base de datos" if primera and not ALERTAR_PRIMERA else ""))

    api = capturar_api()
    registros = descargar_todo(api)
    if not registros:
        raise RuntimeError("No se descargó ningún proyecto.")

    # --- lista general: normalizar + clasificar -------------------------------
    filas = []
    for reg in registros:
        b = normalizar(reg)
        b["_reg"] = reg
        tags, conf, claves = clasificar(b["titulo"], b["sumilla"])
        b.update(subsector=tags, confianza=conf, claves=claves)
        filas.append(b)
    revisados = len(filas)
    con_sumilla = sum(1 for f in filas if f["sumilla"])
    print(f"   📝 Proyectos con sumilla en la lista general: {con_sumilla}/{revisados}")
    if con_sumilla == 0:
        print("   ⚠️ La lista general no trae sumilla: el filtro usa sólo el título (la sumilla se lee en la ficha).")
    energia = [f for f in filas if f["subsector"] or f["id"] in previos]
    descartados = revisados - len(energia)
    print(f"\n📊 Revisados: {revisados} · Energía: {len(energia)} · Descartados: {descartados}")

    # --- API de la ficha -------------------------------------------------------
    plantillas = []
    muestra = next((f for f in energia if not previos.get(f["id"], {}).get("cerrado") and f["link"]), None)
    if muestra:
        try:
            plantillas = capturar_detalle(api, muestra["link"], _plano(muestra["_reg"]))
        except Exception as e:
            errores += 1
            print(f"   ⚠️ No se pudo capturar la ficha: {e}")
    sesion = requests.Session()
    sesion.headers.update({"User-Agent": UA, "Origin": BASE, "Referer": BASE + "/spley-portal/"})
    sesion.headers.update(api.get("headers") or {})

    # --- leer cada ficha y detectar cambios -----------------------------------
    rows, a_guardar, eventos = [], [], []
    for i, f in enumerate(energia, 1):
        prev = previos.get(f["id"])
        if prev and prev.get("cerrado"):            # cerrado: deja de monitorearse
            rows.append(prev)
            continue
        det = None
        if plantillas:
            det, n_err = leer_ficha(sesion, plantillas, _plano(f["_reg"]))
            errores += n_err
            if det is None:
                errores += 1
            time.sleep(0.5)
        r = construir_fila(f, det, prev)
        eventos += comparar(prev, r)
        rows.append(r)
        a_guardar.append((r, prev))
        if i % 20 == 0:
            print(f"   … fichas leídas: {i}/{len(energia)}")

    nuevos = [e for e in eventos if e["tipo"] == "nuevo"]
    modificados = [e for e in eventos if e["tipo"] == "cambio"]
    cerrados = [e for e in modificados if any(c[0] == "cerrado" for c in e["cambios"])]
    linea_base = primera and not ALERTAR_PRIMERA
    detalle = (f"Primera corrida: {len(rows)} proyectos registrados (sin mensaje)"
               if linea_base else "\n".join(lineas_novedades(eventos)))

    # --- Telegram (primero: si falla, no se actualiza la hoja y se reintenta) -----
    tg_ok = True
    bloques = mensaje_corrida(eventos) if (eventos and not linea_base) else None
    if args.dry_run:
        print("\n[dry-run] Mensaje de Telegram que se enviaría:\n")
        print("".join(sep + t for sep, t in bloques) if bloques else "(ninguno)")
        return
    if bloques:
        tg_ok = enviar_telegram_html(bloques, os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID"))
    else:
        print("   (primera corrida o sin novedades: no se envía mensaje)")
    if FICHA_PEDIDO:
        n = FICHA_PEDIDO.split("/")[0].replace("PL", "").strip()
        todos = {**previos, **{r["id"]: r for r in rows}}
        hallado = next((r for r in todos.values() if str(r["numero"]) == n or r["numero_fmt"] == FICHA_PEDIDO), None)
        enviar_telegram_html([("", msg_ficha(hallado) if hallado else
                               f"No encontré el proyecto {_h(FICHA_PEDIDO)} entre los energéticos.")],
                             os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID"))

    # --- Google Sheets ---------------------------------------------------------
    dur = int(time.time() - T0)
    corrida = {"Fecha y hora": AHORA.strftime("%d/%m/%Y %H:%M"), "Revisados": revisados,
               "Descartados": descartados, "Nuevos": len(nuevos), "Modificados": len(modificados),
               "Cerrados": len(cerrados), "Detalle de novedades": detalle,
               "Errores y duración": f"{errores} errores · {dur // 60} min {dur % 60} s"}
    if not tg_ok:
        corrida["Errores y duración"] += " · Telegram falló: no se actualizó Proyectos"
        guardar_corrida(servicio, sid, corrida)
        raise RuntimeError("Telegram falló. No se actualizó la pestaña Proyectos para no perder las alertas; "
                           "se reintentará en la próxima corrida.")
    guardar_proyectos(servicio, sid, a_guardar)
    guardar_corrida(servicio, sid, corrida)
    print("\n🎉 PROCESO COMPLETADO")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)
