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

COMPLETA = (os.getenv("COMPLETA") or "").strip().lower() in ("1", "true", "si", "sí")   # forzar lectura completa
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


# ---- leer la FICHA tal como la muestra el portal (HTML renderizado) -----------
# La ficha es una página Angular con bloques rotulados (SUMILLA, TÍTULO, AUTOR PRINCIPAL...).
# Se lee esa página con Selenium y se interpreta con los rótulos exactos.

URLS_FICHA = ["{base}/spley-portal/#/expediente/{p}/{n}",
              "{base}/spley-portal/#/congreso/expediente/{p}/{n}"]
_url_ok = {"i": 0}                     # cuál de las dos rutas funcionó (se recuerda)
_TXT_VACIO = {"", "-", "--", "- -"}


def _txt(nodo):
    return re.sub(r"\s+", " ", nodo.get_text(" ", strip=True)).strip() if nodo else ""


def _limpiar_celda(td):
    for t in td.select(".p-column-title"):
        t.decompose()
    return td


def parsear_ficha(html):
    """HTML renderizado de la ficha -> dict con los datos del proyecto."""
    from bs4 import BeautifulSoup
    sp = BeautifulSoup(html, "html.parser")
    fi = {"numero_fmt": "", "tipo": "Ley", "fecha_pres": "", "proponente": "", "titulo": "", "sumilla": "",
          "principal": "", "coautores": [], "adherentes": [], "grupo": "", "estado": "", "eventos": [],
          "comisiones": "", "ver_mas": ""}
    leg = sp.select_one("fieldset legend")
    m = re.match(r"^(.*?)\s+N[°º.]*\s*(\S+)\s*$", _txt(leg)) if leg else None
    if m:
        fi["numero_fmt"] = m.group(2)
        if "resolu" in normalizar_texto(m.group(1)):
            fi["tipo"] = "Resolución legislativa"
    campos = {}
    for div in sp.select("div.campo-detalle"):
        lab = div.select_one("label.campo-label")
        val = div.select_one(".campo-valor")
        if not lab or not val:
            continue
        clave = normalizar_texto(_txt(lab))
        if val.select_one("ul.firmantes-list"):
            campos[clave] = [_txt(a) for a in val.select("a.link-firmante") if _txt(a)]
            vm = val.select_one("a.ver-mas")
            if vm and vm.get("href"):
                fi["ver_mas"] = vm["href"]
        else:
            t = _txt(val)
            campos[clave] = "" if t in _TXT_VACIO else t
    g = lambda k: campos.get(k, "") if isinstance(campos.get(k, ""), str) else ""
    fi.update(fecha_pres=g("fecha de presentacion"), proponente=g("proponente"), titulo=g("titulo"),
              sumilla=g("sumilla"), grupo=g("grupo parlamentario"), estado=g("ultimo estado procesal"))
    pr = campos.get("autor principal") or []
    fi["principal"] = pr[0] if pr else ""
    fi["coautores"] = campos.get("coautores") or []
    fi["adherentes"] = campos.get("adherentes") if isinstance(campos.get("adherentes"), list) else []

    # SEGUIMIENTOS: la tabla viene con lo más nuevo arriba -> se invierte para dejar el orden cronológico
    ultimas_com = ""
    for fs in sp.select("fieldset"):
        lg = fs.select_one("legend")
        if lg and normalizar_texto(_txt(lg)).startswith("seguimientos"):
            filas = []
            for tr in fs.select("tbody tr"):
                tds = tr.find_all("td", recursive=False)
                if len(tds) < 4:
                    continue
                tds = [_limpiar_celda(td) for td in tds]
                fecha = a_fecha(_txt(tds[0]))
                if fecha is None or pd.isna(fecha):
                    continue
                coms = [_txt(li) for li in tds[2].select("li") if _txt(li)]
                filas.append({"fecha": fecha, "estado": _txt(tds[1]), "detalle": _txt(tds[3]),
                              "comisiones": coms})
            filas.reverse()
            filas.sort(key=lambda e: e["fecha"])          # estable: conserva el orden entre misma fecha
            fi["eventos"] = filas
            for e in reversed(filas):
                if e["comisiones"]:
                    ultimas_com = "; ".join(e["comisiones"])
                    break
    # ESTUDIO EN COMISIONES (comisión que lo estudia ahora)
    estudio = [re.sub(r"^Comisi[oó]n\s+(?:de\s+(?:la\s+|los\s+|las\s+)?|del\s+)?", "", _txt(n))
               for n in sp.select(".ec-comision-nombre")]
    fi["comisiones"] = "; ".join(dict.fromkeys(x for x in estudio if x)) or ultimas_com
    if not fi["estado"] and fi["eventos"]:
        fi["estado"] = fi["eventos"][-1]["estado"]
    return fi


def _nuevo_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    for a in ("--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
              "--disable-gpu", "--window-size=1920,1080", f"user-agent={UA}"):
        opts.add_argument(a)
    opts.page_load_strategy = "eager"          # no espera imágenes/estilos pesados
    d = webdriver.Chrome(options=opts)
    d.set_page_load_timeout(60)
    try:                                       # no descargar imágenes ni fuentes (no se usan)
        d.execute_cdp_cmd("Network.enable", {})
        d.execute_cdp_cmd("Network.setBlockedURLs", {"urls": [
            "*.png", "*.jpg", "*.jpeg", "*.gif", "*.svg", "*.webp", "*.ico",
            "*.woff", "*.woff2", "*.ttf", "*fonts.googleapis.com*", "*fonts.gstatic.com*"]})
    except Exception:
        pass
    return d


def _cargar_ficha(driver, periodo, numero):
    """Abre la ficha y devuelve su HTML ya renderizado (o None)."""
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.common.by import By
    for k in range(len(URLS_FICHA)):
        i = (_url_ok["i"] + k) % len(URLS_FICHA)
        url = URLS_FICHA[i].format(base=BASE, p=periodo, n=numero)
        driver.get("about:blank")          # fuerza a Angular a cargar el expediente de cero
        driver.get(url)
        try:
            WebDriverWait(driver, 20).until(lambda d: d.find_elements(By.CSS_SELECTOR, "div.estado-valor"))
        except Exception:
            continue
        _url_ok["i"] = i
        try:                               # la tabla de seguimientos carga después (espera sólo lo necesario)
            WebDriverWait(driver, 6, poll_frequency=0.2).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "p-table tbody tr td[colspan]") or
                          len(d.find_elements(By.CSS_SELECTOR, "p-table tbody tr")) >= 1)
        except Exception:
            pass
        try:                               # la comisión que estudia el proyecto (puede no existir: máx. 1.5 s)
            WebDriverWait(driver, 1.5, poll_frequency=0.2).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, ".ec-comision-nombre"))
        except Exception:
            pass
        return driver.page_source
    return None


def leer_fichas_selenium(items):
    """items: filas de la lista general. Devuelve ({id: ficha}, n_errores)."""
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.common.by import By
    try:                                   # falla de inmediato (antes de abrir Chrome) si falta la librería
        import bs4  # noqa: F401
    except ImportError:
        raise RuntimeError("Falta la librería beautifulsoup4. Agrégala a requirements.txt "
                           "o al paso 'pip install' del workflow (pip install beautifulsoup4).")
    out, errores, driver, muestras = {}, 0, None, 0
    print(f"\n📄 Leyendo {len(items)} fichas del portal (≈6-10 s cada una)...")
    try:
        for k, f in enumerate(items, 1):
            try:
                if driver is None or k % 60 == 0:      # reinicia Chrome de vez en cuando (memoria)
                    if driver:
                        driver.quit()
                    driver = _nuevo_driver()
                html = _cargar_ficha(driver, f["periodo"], f["numero"])
                if not html:
                    errores += 1
                    print(f"   ⚠️ No cargó la ficha de {f['id']}")
                    continue
                if muestras == 0:
                    with open("ficha_muestra.html", "w", encoding="utf-8") as fh:
                        fh.write(html)
                fi = parsear_ficha(html)
                if not fi["sumilla"] and not fi["titulo"]:
                    errores += 1
                    print(f"   ⚠️ Ficha de {f['id']} sin sumilla ni título (¿cambió el diseño?)")
                    continue
                # "Ver más...": la ficha muestra sólo algunos coautores; se hace clic para ver la lista completa
                if fi["ver_mas"]:
                    try:
                        enlaces = driver.find_elements(By.CSS_SELECTOR, "a.ver-mas")
                        if enlaces:
                            driver.execute_script("arguments[0].click();", enlaces[0])
                            time.sleep(0.8)
                            html2 = driver.page_source
                            if muestras == 0:
                                with open("ficha_ver_mas_muestra.html", "w", encoding="utf-8") as fh:
                                    fh.write(html2)
                            from bs4 import BeautifulSoup
                            sp2 = BeautifulSoup(html2, "html.parser")
                            candidatos = [parsear_ficha(html2)["coautores"],
                                          [_txt(x) for x in sp2.select(".p-dialog a.link-firmante") if _txt(x)]]
                            co2 = [x for x in max(candidatos, key=len) if x != fi["principal"]]
                            if len(co2) > len(fi["coautores"]):
                                fi["coautores"] = co2
                    except Exception:
                        pass
                muestras += 1
                out[f["id"]] = fi
            except Exception as e:
                errores += 1
                print(f"   ⚠️ Error leyendo {f['id']}: {str(e)[:120]}")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None
            if k % 20 == 0:
                print(f"   … fichas leídas: {k}/{len(items)}")
    finally:
        if driver:
            driver.quit()
    print(f"   ✅ Fichas leídas: {len(out)}/{len(items)} · con sumilla: {sum(1 for x in out.values() if x['sumilla'])}")
    return out, errores


# ---- leer la FICHA por la API del portal (rápido, sin Chrome) ---------------------------------
# La página pide: GET {API}/expediente/{AES(perParId)}/{AES(numero)}?codTipoParl=C
# El cifrado (AES-128-ECB, base64 "url-safe") y su clave los entrega la propia página a todo navegador.
API_FICHA = "https://api.congreso.gob.pe/spley-portal-service"
USAR_API = (os.getenv("USAR_API") or "1").strip().lower() not in ("0", "false", "no")
_CLAVE_ID = {"k": os.getenv("ENCRYPTION_KEY_PORTAL") or "ProdALg5ZrAsxBMD"}


def _cifrar_id(valor):
    """Reproduce encrypt() de la página: AES-128-ECB + PKCS7 -> base64 url-safe sin '='."""
    clave, datos = _CLAVE_ID["k"].encode("utf-8"), str(valor).encode("utf-8")
    try:
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import pad
        cif = AES.new(clave, AES.MODE_ECB).encrypt(pad(datos, 16))
    except ImportError:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.primitives import padding
        pd_ = padding.PKCS7(128).padder()
        e = Cipher(algorithms.AES(clave), modes.ECB()).encryptor()
        cif = e.update(pd_.update(datos) + pd_.finalize()) + e.finalize()
    return base64.b64encode(cif).decode().replace("+", "-").replace("/", "_").replace("=", "")


def _refrescar_clave():
    """Si la clave cambió, la vuelve a leer del JavaScript de la página. True si encontró una distinta."""
    try:
        h = {"User-Agent": UA}
        html = requests.get(f"{BASE}/spley-portal/", headers=h, timeout=30).text
        for src in re.findall(r'<script[^>]+src="([^"]*main[^"]*\.js)"', html):
            js = requests.get(src if src.startswith("http") else f"{BASE}/spley-portal/{src.lstrip('/')}",
                              headers=h, timeout=90).text
            m = re.search(r'ENCRYPTION_KEY\s*:\s*"([^"]+)"', js)
            if m and m.group(1) != _CLAVE_ID["k"]:
                _CLAVE_ID["k"] = m.group(1)
                print("   🔑 Clave de cifrado actualizada desde la página")
                return True
    except Exception as e:
        print(f"   ⚠️ No se pudo releer la clave: {str(e)[:100]}")
    return False


def ficha_desde_api(data):
    """JSON de la API (data) -> mismo dict que parsear_ficha()."""
    g = data.get("general") or {}
    fi = {"numero_fmt": g.get("proyectoLey") or "", "tipo": "Ley", "fecha_pres": "",
          "proponente": g.get("desProponente") or "", "titulo": g.get("titulo") or "",
          "sumilla": g.get("sumilla") or "", "principal": "", "coautores": [], "adherentes": [],
          "grupo": g.get("desGpar") or "", "estado": g.get("desEstado") or "", "eventos": [],
          "comisiones": "", "ver_mas": ""}
    if "resolu" in normalizar_texto(fi["titulo"][:40]):
        fi["tipo"] = "Resolución legislativa"
    f = a_fecha(g.get("fecPresentacion"))
    fi["fecha_pres"] = f.strftime("%d/%m/%Y") if f is not None and not pd.isna(f) else ""
    for x in data.get("firmantes") or []:
        nom = (x.get("nombre") or "").strip()
        if not nom:
            continue
        t = x.get("tipoFirmanteId")
        if t == 1 and not fi["principal"]:
            fi["principal"] = nom
        elif t == 3:
            fi["adherentes"].append(nom)
        else:
            fi["coautores"].append(nom)
    evs, ultimas_com = [], ""
    for sg in data.get("seguimientos") or []:
        fecha = a_fecha(str(sg.get("fecha") or "")[:10])
        if fecha is None or pd.isna(fecha):
            continue
        det = (sg.get("detalle") or "").strip()
        obs = (sg.get("observacion") or "").strip()
        if obs and obs != det:
            det = f"{det} {obs}".strip()
        coms = [c.strip() for c in (sg.get("desComisiones") or "").split(";") if c.strip()]
        evs.append({"fecha": fecha, "estado": (sg.get("desEstado") or "").strip(), "detalle": det,
                    "comisiones": coms, "_id": sg.get("seguimientoPleyId") or 0})
    evs.sort(key=lambda e: (e["fecha"], e["_id"]))        # cronológico; el id desempata el mismo día
    fi["eventos"] = evs
    for e in reversed(evs):
        if e["comisiones"]:
            ultimas_com = "; ".join(e["comisiones"])
            break
    estudio = [re.sub(r"^Comisi[oó]n\s+(?:de\s+(?:la\s+|los\s+|las\s+)?|del\s+)?", "", (c.get("comisionNombre") or "").strip())
               for c in data.get("estudioComisiones") or []]
    fi["comisiones"] = "; ".join(dict.fromkeys(x for x in estudio if x)) or ultimas_com
    if not fi["estado"] and evs:
        fi["estado"] = evs[-1]["estado"]
    return fi


def _pedir_expediente(periodo, numero):
    url = f"{API_FICHA}/expediente/{_cifrar_id(periodo)}/{_cifrar_id(numero)}"
    h = {"User-Agent": UA, "Accept": "application/json", "Origin": BASE, "Referer": BASE + "/"}
    ultimo = None
    for k in range(3):
        try:
            r = requests.get(url, params={"codTipoParl": "C"}, headers=h, timeout=30)
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, dict) and isinstance(j.get("data"), dict) and j["data"].get("general"):
                    return j["data"], None
                ultimo = f"respuesta sin datos ({str(j)[:80]})"
            else:
                ultimo = f"HTTP {r.status_code}"
                if r.status_code in (400, 401, 403, 404):
                    break                                  # no tiene sentido reintentar
        except Exception as e:
            ultimo = str(e)[:100]
        time.sleep(1.5 * (k + 1))
    return None, ultimo


def leer_fichas_api(items):
    """items: filas de la lista general. Devuelve ({id: ficha}, n_errores)."""
    out, errores = {}, 0
    print(f"\n⚡ Leyendo {len(items)} fichas por la API del portal...")
    clave_refrescada = False
    for k, f in enumerate(items, 1):
        data, err = _pedir_expediente(f["periodo"], f["numero"])
        if data is None and not clave_refrescada and not out:     # nada ha funcionado aún: ¿cambió la clave?
            clave_refrescada = True
            if _refrescar_clave():
                data, err = _pedir_expediente(f["periodo"], f["numero"])
        if data is None:
            errores += 1
            print(f"   ⚠️ API sin datos para {f['id']}: {err}")
        else:
            try:
                fi = ficha_desde_api(data)
                if fi["sumilla"] or fi["titulo"]:
                    out[f["id"]] = fi
                else:
                    errores += 1
                    print(f"   ⚠️ API de {f['id']} sin sumilla ni título")
            except Exception as e:
                errores += 1
                print(f"   ⚠️ Error interpretando {f['id']}: {str(e)[:100]}")
        if k % 50 == 0:
            print(f"   … fichas leídas: {k}/{len(items)}")
        time.sleep(0.15)                                          # trato amable con el servidor
        if not out and errores >= 5:                              # la API no sirve: se abandona rápido
            print("   ⛔ La API de fichas no responde; se usará Chrome")
            break
    print(f"   ✅ Fichas por API: {len(out)}/{len(items)} · con sumilla: {sum(1 for x in out.values() if x['sumilla'])}")
    return out, errores


def leer_fichas(items):
    """API primero; lo que falle se lee con Chrome (más lento). Devuelve ({id: ficha}, n_errores)."""
    out = {}
    if USAR_API and items:
        out, _ = leer_fichas_api(items)
    faltan = [f for f in items if f["id"] not in out]
    if faltan:
        print(f"\n🌐 {len(faltan)} fichas por Chrome (respaldo)")
        o2, _ = leer_fichas_selenium(faltan)
        out.update(o2)
    return out, len(items) - len(out)


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


def construir_fila(base, fi, prev):
    """Une lista + ficha en una fila final. Si la ficha falló, conserva lo de la corrida anterior."""
    r = {k: v for k, v in base.items() if k not in ("autores_lista", "comision", "_reg")}
    r["link"] = URLS_FICHA[_url_ok["i"]].format(base=BASE, p=base["periodo"], n=base["numero"]) \
        if base.get("periodo") and base.get("numero") else base.get("link", "")
    if fi:
        r["numero_fmt"] = fi["numero_fmt"] or r["numero_fmt"]
        r["tipo"] = fi["tipo"]
        r["fecha_pres"] = fi["fecha_pres"] or r["fecha_pres"]
        r["sumilla"] = fi["sumilla"] or r["sumilla"]
        r["titulo"] = fi["titulo"] or r["titulo"]
        r["grupo"] = fi["grupo"] or str(r["grupo"])
        r["proponente"] = fi["proponente"] or str(r["proponente"])
        r["comisiones"] = fi["comisiones"] or str(base["comision"])
        r["estado"] = fi["estado"] or str(r["estado"])
        hitos, dictamen, hist, cerrado, cierre_txt, cierre_fecha = analizar_tramite(fi["eventos"], r["estado"])
        r.update(hitos=hitos, dictamen=dictamen, historial=hist, cerrado=cerrado,
                 cierre_txt=cierre_txt, cierre_fecha=cierre_fecha)
        r["fecha_mov"] = fi["eventos"][-1]["fecha"].strftime("%d/%m/%Y") if fi["eventos"] else ""
        principal = fi["principal"] or str(base["autores_lista"])
        r["principal"], r["coautores"] = principal, fi["coautores"]
        r["autores"] = "; ".join([principal] + fi["coautores"]) if principal else "; ".join(fi["coautores"])
    elif prev:
        for k in ("sumilla", "autores", "principal", "coautores", "grupo", "comisiones", "hitos",
                  "dictamen", "historial", "cerrado", "cierre_txt", "cierre_fecha", "fecha_mov",
                  "numero_fmt", "tipo"):
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

    # --- leer las fichas (Selenium) -------------------------------------------
    abiertos = [f for f in energia if not (previos.get(f["id"], {}).get("cerrado"))]
    completa = COMPLETA or primera or HOY.day <= 7 or USAR_API   # con API se leen todas (≈2 min); con Chrome, mixta
    pendientes = []
    for f in abiertos:
        prev = previos.get(f["id"])
        if completa or not prev or not prev.get("sumilla") or \
                normalizar_texto(str(f["estado"] or "")) != normalizar_texto(str(prev.get("estado") or "")):
            pendientes.append(f)
        else:
            f["estado"] = prev.get("estado", f["estado"])   # mismo estado: se conserva el texto ya guardado
    print(f"\n🔎 Lectura {'COMPLETA' if completa else 'parcial (nuevos y con cambio de estado)'}: "
          f"{len(pendientes)} fichas por leer de {len(abiertos)} abiertas")
    fichas, n_err = leer_fichas(pendientes)
    errores += n_err

    # --- construir filas y detectar cambios -----------------------------------
    rows, a_guardar, eventos = [], [], []
    for f in energia:
        prev = previos.get(f["id"])
        if prev and prev.get("cerrado"):            # cerrado: deja de monitorearse
            rows.append(prev)
            continue
        r = construir_fila(f, fichas.get(f["id"]), prev)
        eventos += comparar(prev, r)
        rows.append(r)
        a_guardar.append((r, prev))

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
