"""
=============================================================================
BÚSQUEDA DE NOTICIAS DEL SECTOR (Google News RSS)
=============================================================================
- Consulta Google News RSS (sin API key) con varias búsquedas del sector.
- Reusa el filtro por puntaje de normas_github.evaluar_relevancia.
- Escribe en la pestaña "Noticias" con el MISMO formato que lee enviar_correos.py
  (sigue el orden de la pestaña de Normas: captura primero, decisión y envío al final):
    A=FECHA CAPTURA (AAAA-MM-DD, día en que se encontró; define el día del boletín)
    B=TITULAR | C=FECHA PUBLICACIÓN | D=FUENTE | E=RESUMEN (automático; editable por ti)
    F=ENLACE | G=PUNTAJE FILTRO (por qué el filtro la aceptó)
    H=ENVIAR (S/N, lo marcas tú) | I=ENVIADO (lo escribe enviar_correos.py)
    J=OTRAS FUENTES (otros diarios que publicaron lo mismo; informativa)
- Resuelve el enlace real de cada noticia (Google News solo entrega un redirect), abre la
  página y extrae un resumen (meta description / primeros párrafos). Si falla, queda vacío.
- Deduplica contra lo que ya está en la hoja (por enlace y por titular).
- Lunes: busca sábado, domingo y lunes. Martes a viernes: ayer y hoy.
- Una misma noticia cubierta por varios diarios se agrupa y se queda UNA sola: la mejor según
  el diario (FUENTES_TIER) y lo completo del resumen. Las otras fuentes quedan anotadas en
  la columna OTRAS FUENTES.
- Manda un mensaje aparte a Telegram con las noticias encontradas.

Variables de entorno: GOOGLE_CREDENTIALS_JSON, SPREADSHEET_ID,
                      TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
Uso: python noticias_github.py [--dry-run]
=============================================================================
"""

import os
import re
import sys
import json
import base64
import argparse
import html as htmllib
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus, urlsplit, urlunsplit, parse_qsl, urlencode

import time
import requests
from bs4 import BeautifulSoup
from google.oauth2 import service_account
from googleapiclient.discovery import build

try:                                   # pip install googlenewsdecoder (está en requirements.txt)
    from googlenewsdecoder import gnewsdecoder
except Exception as _e:               # ImportError, pero también incompatibilidades (selectolax>=1.0)
    gnewsdecoder = None
    print(f"⚠️  googlenewsdecoder no se pudo importar: {type(_e).__name__}: {str(_e)[:120]}", flush=True)

# Reutiliza el filtro y el envío a Telegram del scraper de normas (no se modifica ese archivo)
from normas_github import evaluar_relevancia, enviar_telegram, normalizar_texto, _alias

HOJA_NOTICIAS = "Noticias"
# Orden de columnas (única fuente de verdad en este archivo). Debe coincidir con
# CAMPOS_NOTICIAS de enviar_correos.py: ["captura","titular","fecha_pub","fuente","resumen",
#                                         "enlace","puntaje","enviar","enviado"]
CAMPOS = ["captura", "titular", "fecha_pub", "fuente", "resumen",
          "enlace", "puntaje", "enviar", "enviado", "otras_fuentes"]
N_BASE = 9          # A:I = columnas que lee enviar_correos.py. La J (OTRAS FUENTES) es solo informativa.
ENCABEZADOS = ["FECHA CAPTURA", "TITULAR", "FECHA PUBLICACIÓN", "FUENTE", "RESUMEN (opcional)",
               "ENLACE", "PUNTAJE FILTRO", "ENVIAR (S/N)", "ENVIADO", "OTRAS FUENTES"]
ULTIMA_COL = chr(ord("A") + len(CAMPOS) - 1)         # "I"
ANCHOS = [110, 480, 120, 130, 320, 260, 260, 95, 110, 260]  # píxeles por columna
ZONA = ZoneInfo("America/Lima")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

HOY = datetime.now(ZONA).date()   # Lima, igual que enviar_correos.py (el runner está en UTC)
# Lunes: sábado, domingo y lunes (el viernes ya lo cubrió la corrida del viernes).
# Martes a viernes: ayer y hoy.
DIAS_ATRAS = 2 if HOY.weekday() == 0 else 1
FECHA_DESDE = HOY - timedelta(days=DIAS_ATRAS)       # filtro exacto por fecha de publicación (Lima)
# Google filtra por "últimas N*24 horas" contadas desde AHORA, no por fecha. Se pide 1 día de más
# para no perder lo de la madrugada del primer día; el filtro exacto por fecha lo hace FECHA_DESDE.
VENTANA = f"{DIAS_ATRAS + 1}d"
LARGO_RESUMEN = 320                                  # caracteres máximos del resumen
MAX_POR_QUERY = 100
MAX_NOTICIAS = 15                                    # tope por corrida (evita saturar la hoja)

# Varias búsquedas cortas rinden más que una sola gigante (Google recorta resultados por query)
QUERIES = [
    '(hidrocarburos OR Osinergmin OR Perupetro OR Petroperú) Perú',
    '("gas natural" OR GLP OR GNV OR Camisea OR combustibles) Perú',
    '(Minem OR "energía y minas" OR electricidad OR "transición energética") Perú',
    '(OEFA OR "derrame de petróleo" OR "lote 192" OR "lote 95") Perú',
]

HEADERS_HTTP = {"User-Agent": "Mozilla/5.0 (compatible; normas-hidrocarburos/1.0)"}
HEADERS_WEB = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
               "Accept-Language": "es-PE,es;q=0.9"}


def log(m=""):
    print(m, flush=True)


# -----------------------------------------------------------------------------
# GOOGLE NEWS RSS
# -----------------------------------------------------------------------------
def url_rss(query, ventana):
    q = quote_plus(f"{query} when:{ventana}")
    return f"https://news.google.com/rss/search?q={q}&hl=es-419&gl=PE&ceid=PE:es-419"


def parsear_rss(xml_texto):
    """Devuelve lista de dicts {titular, fuente, enlace, fecha_pub}."""
    items = []
    root = ET.fromstring(xml_texto)
    for it in root.iter("item"):
        titulo = htmllib.unescape((it.findtext("title") or "").strip())
        enlace = (it.findtext("link") or "").strip()
        src_el = it.find("source")
        fuente = (src_el.text or "").strip() if src_el is not None and src_el.text else ""
        fuente_url = (src_el.get("url") or "").strip() if src_el is not None else ""

        titulo = re.sub(r"\s*\|\s*[A-ZÁÉÍÓÚÑ ]{3,25}$", "", titulo)      # "… | ECONOMIA" (sección del medio)
        # Google agrega " - Fuente" al final del titular: se quita para no duplicarlo
        if fuente and titulo.endswith(f" - {fuente}"):
            titulo = titulo[: -len(fuente) - 3].strip()
        elif " - " in titulo and not fuente:
            titulo, fuente = titulo.rsplit(" - ", 1)

        try:
            fecha_pub = parsedate_to_datetime(it.findtext("pubDate") or "").astimezone(ZONA).date()
        except Exception:
            fecha_pub = None

        titulo = re.sub(r"\s*\|\s*[A-ZÁÉÍÓÚÑ ]{3,25}$", "", titulo)
        if titulo and enlace:
            items.append({"titular": titulo, "fuente": fuente.strip(), "fuente_url": fuente_url,
                          "enlace": enlace, "fecha_pub": fecha_pub})
    return items


def buscar_noticias():
    todas = []
    for q in QUERIES:
        try:
            r = requests.get(url_rss(q, VENTANA), headers=HEADERS_HTTP, timeout=20)
            r.raise_for_status()
            items = parsear_rss(r.content)
            antes = len(items)
            items = [n for n in items if not n["fecha_pub"] or n["fecha_pub"] >= FECHA_DESDE][:MAX_POR_QUERY]
            log(f"   🔎 {q[:55]}... → {len(items)} resultados (de {antes}; desde {FECHA_DESDE})")
            todas.extend(items)
        except Exception as e:
            log(f"   ⚠️  Falló la consulta '{q[:40]}...': {e}")
    return todas


def deduplicar(items):
    """Quita repetidos entre queries (mismo enlace o titular casi idéntico)."""
    vistos, unicos = set(), []
    for n in items:
        clave = normalizar_texto(n["titular"])[:80]
        if n["enlace"] in vistos or clave in vistos:
            continue
        vistos.add(n["enlace"])
        vistos.add(clave)
        unicos.append(n)
    return unicos


# -----------------------------------------------------------------------------
# FILTRO DE RELEVANCIA PARA NOTICIAS
# -----------------------------------------------------------------------------
# Las normas son siempre peruanas; las noticias no. Sobre el puntaje de normas_github se agrega:
#   - exigir señal de Perú (en el titular o diario peruano)  -> evita Colombia, México, Argentina...
#   - restar ruido que usa el nombre del sector sin ser del sector (elecciones, fútbol, metáforas)
#   - sumar los temas que hoy mueven al sector (crisis del gas, reorganización del Minem)
EXTRANJERO = re.compile(r"\b(?:estados unidos|eeuu|ee uu|europa|europea|rusia|ruso|rusa|ucrania|china|"
                        r"india|japon|corea|argentina|brasil|colombia|colombiano|mexico|mexicano|chile|"
                        r"bolivia|ecuador|venezuela|arabia|iran|israel|gazprom|texas|nigeria|reino unido|"
                        r"alemania|francia|espana|canada|noruega|qatar|angola|argelia|portugal|belgica|"
                        r"huachicol\w*|sheinbaum|petrobras|pemex|ecopetrol|ypf|enap|"
                        r"espriella|valledupar|zapopan|puebla|mendoza|neuquen|rio negro|caracas|margarita|"
                        r"cuba|malvinas|londres|irani|senegal|oran|villazon|la quiaca|"
                        r"ministerio de hidrocarburos|ministro de hidrocarburos|unam|asea|milei|"
                        r"cerro dragon|vaca muerta|portugal|halliburton)\b")
PERU = re.compile(r"\b(?:peru|peruano|peruana|peruanos|lima|callao|piura|talara|cusco|arequipa|loreto|tacna|"
                  r"ucayali|tumbes|junin|camisea|minem|osinergmin|perupetro|petroperu|oefa|senace|indecopi|"
                  r"minam|pluspetrol|tgp|calidda|fise|proinversion|coes|energia y minas|lotes? \d+|lotes? [ivx]+)\b")
# Diarios peruanos: si el titular no menciona a Perú pero viene de uno de estos, se acepta
FUENTES_PERUANAS = ["gestion", "el comercio", "la republica", "rpp", "andina", "peru21", "correo",
                    "exitosa", "energiminas", "rumbo minero", "proactivo", "canal n", "el peruano",
                    "comexperu", "semana economica", "convoca", "ojo publico", "peru retail"]
# ✏️ EDITABLE: palabras que delatan una noticia que NO es del sector aunque nombre Petroperú,
# hidrocarburos, etc. (elecciones, fútbol...). Si te llega una noticia que no va, agrega aquí la
# palabra que la delata (en minúsculas y sin tilde). Cada coincidencia resta AJUSTE_NO_SECTOR puntos.
PALABRAS_EXCLUIR = [
    # elecciones / política electoral
    "erm", "elecciones", "eleccion", "sufragar", "sufragio", "votar", "votacion", "miembros de mesa",
    "local de votacion", "local de petroperu", "onpe", "jne", "candidato", "candidata",
    # metáforas y comparaciones con Petroperú
    "parece el petroperu",
    # deportes
    "futbol", "liga", "partido", "goleada", "beisbol", "campeonato", "guaiqueri\\w*", "navegantes",
    # educación / espectáculos / clima
    "universidad\\w*", "horoscopo", "farandula", "clima en",
]
NO_SECTOR = re.compile(r"\b(?:" + "|".join(PALABRAS_EXCLUIR) + r")\b")
# Temas que hoy mueven al sector (aunque el titular no nombre una entidad)
TEMA_PERU = re.compile(r"\b(?:balon(?:es)? de gas|escasez de (?:gas|gasolina|combustibles?|glp|gnv)|"
                       r"crisis (?:del |de )?(?:gas|gnv|glp|combustibles?)|gasoducto|desabastecimiento de "
                       r"(?:gas|combustibles?)|restablecimiento (?:de|del) (?:gas|suministro))\b")
# Sector peruano que evaluar_relevancia (hecho para títulos de normas) no puntúa: +3 una sola vez
VOCAB_PERU = re.compile(r"\b(?:lotes? \d+|tgp|reguladores?|oleoducto norperuano|talara|balon(?:es)? de (?:gas|glp)|gas domiciliario|"
                        r"calidda|contugas|pluspetrol|petrotal|canon (?:petrolero|gasifero)|gas de camisea|"
                        r"parque eolico|gigante eolico|energia renovable|energias renovables)\b")
# Minem: reorganización y lo que la acompaña (destrabar inversiones, meritocracia)
REORG_MINEM = (re.compile(r"\b(?:reorganiz\w*|destrab\w*|meritocr\w*)\b"), re.compile(r"\bminem\b|energia y minas"))
# Notas diarias de precios: son la misma noticia aunque cambie la fecha
_PRECIO_TEMA = re.compile(r"\b(?:gasolina\w*|gnv|glp|diesel|combustibles?)\b")
_PRECIO_PALABRA = re.compile(r"\b(?:precios?|cuanto (?:esta|cuesta|cuestan)|mas barata|mas cara)\b")
_PRECIO_DIA = re.compile(r"\b(?:hoy|lunes|martes|miercoles|jueves|viernes|sabado|domingo|\d{1,2} de [a-z]+)\b")


class _PrecioDia:
    """Nota diaria de precios de combustibles ('...precios... hoy, lunes 5 de octubre'). Una nota de política
    como 'Indecopi ante pico de precios de combustibles' NO califica (no trae referencia al día)."""
    @staticmethod
    def search(t):
        return bool(_PRECIO_TEMA.search(t) and _PRECIO_PALABRA.search(t) and _PRECIO_DIA.search(t))


PRECIOS_DIA = _PrecioDia()
# Precio internacional del crudo (Brent/OPEP/Ormuz): mueve los precios locales y SÍ lo enviaste (Infobae, 29/09).
# No se trata como "extranjera" ni exige señal de Perú. Ojo: solo el mercado, no noticias de otros países.
MERCADO_INTL = re.compile(r"\b(?:precio del petroleo|precios del petroleo|petroleo (?:brent|wti)|brent|wti|opep|"
                          r"estrecho de ormuz|barril(?:es)? de petroleo)\b")
AJUSTE_NO_SECTOR, AJUSTE_TEMA, AJUSTE_SIN_PERU, AJUSTE_EXTRANJERO, AJUSTE_VOCAB = 5.0, 3.0, 2.5, 4.0, 3.0
# Servicio al consumidor / cobertura de masas: no van en un boletín sectorial (ninguna se envió en 12 días)
SERVICIO = re.compile(r"\b(?:clases (?:virtuales|presenciales)|minedu|colegios?|bono\b|taxistas?|cuanto cuesta|"
                      r"pronostico|corte de luz|feriados?|horario de atencion)\b")
AJUSTE_SERVICIO = 6.0
# --- Capa editorial: el boletín es para empresas e instituciones del sector, no para el público ---
# Precio/consumo al público y programas sociales (se suman a SERVICIO)
CONSUMO_PUBLICO = re.compile(r"\b(?:precios? (?:del|de los|de) balon(?:es)?|ollas comunes|comedores populares|"
                             r"vaso de leche|subsidio (?:al|a los) (?:hogares|usuarios)|pasajes?|tarifa del taxi)\b")
# Transferencias de canon a regiones/municipios (finanzas locales; la regulación del canon es otro tema)
CANON_TRANSFERENCIA = re.compile(r"\b(?:recib\w+|transfer\w+|distribuy\w+|girar\w*|trasladar\w*)\b.*\bcanon\b|"
                                 r"\bcanon\b.*\b(?:recib\w+|transferenc\w+)\b")
AJUSTE_CANON = 4.0
# Hechos que le sirven a una empresa del sector: regulación, contratos/lotes, inversión/proyectos,
# decisiones institucionales, mercado y suministro a nivel sistema.
EVENTO_SECTOR = re.compile(
    r"\b(?:adenda\w*|contrat\w+|convenios?|concesion\w*|licitacion\w*|subasta\w*|adjudic\w+|lotes? \w+|"
    r"decretos? (?:supremos?|de urgencia|legislativo)|du \d|reglament\w+|resolucion\w*|proyectos? de ley|dictamen|"
    r"congreso|consejo directivo|comites?|concursos?|directorio|reorganiz\w+|capital privado|activos|proinversion|"
    r"inversion(?:es)?|gasoducto\w*|oleoducto\w*|refineria|exploracion|explotacion|yacimientos?|cuenca|reservas|"
    r"descubrimiento|candamo|estudio de mercado|competencia|tarifa\w*|peaje\w*|fiscaliz\w+|sancion\w*|multas?|"
    r"arbitraje|reguladores?|emergencia|masificacion|suministro|transmision|interconexion|generacion|eolic\w+|"
    r"solar\w*|hidroelectric\w+|osinergmin|indecopi|perupetro|minem|oefa|senace|petroperu|tgp|calidda|contugas|"
    r"coes|pcm|contraloria|camisea|talara)\b")
AJUSTE_SIN_HECHO = 1.5
# Temas que el boletín sí publica (según lo enviado): regulación, contratos/lotes, proyectos, mercado, reorganización.
# NO decide si entra (eso lo hace el puntaje); solo sube su prioridad dentro del tope de MAX_NOTICIAS.
TEMA_EDITORIAL = re.compile(r"\b(?:osinergmin|indecopi|reguladores?|pcm|concursos?|adenda\w*|contratos?|convenios?|"
                            r"lotes? \d+|decretos? supremos?|concesion\w*|licitacion\w*|proinversion|perupetro|"
                            r"gasoducto\w*|refineria|talara|masificacion|siete regiones|exploracion|reservas|"
                            r"cuenca|mercado de combustibles|reorganiz\w*|calidda|tgp|camisea)\b")
BONO_EDITORIAL = 2.0
GRIS_MIN = 1.0           # entre GRIS_MIN y el umbral (3.0) y sin señal extranjera: "revisar" en Telegram
GRISES = []


def es_fuente_peruana(fuente, url=""):
    host = re.sub(r"^https?://(?:www\.)?", "", (url or "").lower()).split("/")[0]
    if host.endswith(".pe"):                       # gestion.pe, rpp.pe, andina.pe, larepublica.pe...
        return True
    f = normalizar_texto(fuente)
    return any(x in f for x in FUENTES_PERUANAS)


def evaluar_noticia(titular, fuente="", fuente_url=""):
    """evaluar_relevancia de normas_github + ajustes propios de noticias. Devuelve (ok, razón)."""
    _, razon = evaluar_relevancia(titular, "")
    m = re.search(r"(-?[\d.]+) pts \[(.*)\]", razon)
    pts, motivos = (float(m.group(1)), m.group(2)) if m else (0.0, "")
    motivos = [] if motivos == "sin señales" else [motivos]
    t = normalizar_texto(titular)

    ruido_n = len(set(NO_SECTOR.findall(t)))
    if ruido_n:
        resta = AJUSTE_NO_SECTOR * min(ruido_n, 3)      # cada palabra distinta resta; tope 3 palabras
        pts -= resta; motivos.append(f"no sectorial -{resta:g}")
    if TEMA_PERU.search(t):
        pts += AJUSTE_TEMA; motivos.append(f"tema del momento +{AJUSTE_TEMA:g}")
    if REORG_MINEM[0].search(t) and REORG_MINEM[1].search(t):
        pts += AJUSTE_TEMA; motivos.append(f"reorganización Minem +{AJUSTE_TEMA:g}")
    if VOCAB_PERU.search(t):
        pts += AJUSTE_VOCAB; motivos.append(f"vocabulario del sector +{AJUSTE_VOCAB:g}")
    mercado = bool(MERCADO_INTL.search(t))
    if mercado:
        motivos.append("mercado internacional del petróleo")
    extranjera = bool(EXTRANJERO.search(t) and not PERU.search(t)) and not mercado
    if mercado:
        pass
    elif extranjera:
        pts -= AJUSTE_EXTRANJERO; motivos.append(f"extranjera -{AJUSTE_EXTRANJERO:g}")
    elif not (PERU.search(t) or TEMA_PERU.search(t) or VOCAB_PERU.search(t)) and not es_fuente_peruana(fuente, fuente_url):
        pts -= AJUSTE_SIN_PERU; motivos.append(f"sin señal de Perú -{AJUSTE_SIN_PERU:g}")

    if PRECIOS_DIA.search(t) or SERVICIO.search(t) or CONSUMO_PUBLICO.search(t):
        pts -= AJUSTE_SERVICIO; motivos.append(f"servicio al consumidor -{AJUSTE_SERVICIO:g}")
    if CANON_TRANSFERENCIA.search(t):
        pts -= AJUSTE_CANON; motivos.append(f"transferencia de canon -{AJUSTE_CANON:g}")
    # Sin entidad del sector ni hecho concreto (regulación, contrato, proyecto...): nota genérica de opinión/contexto
    txt_motivos = " ".join(motivos)
    ancla = ("entidad" in txt_motivos or EVENTO_SECTOR.search(t) or TEMA_PERU.search(t) or VOCAB_PERU.search(t)
             or mercado)
    if not ancla:
        pts -= AJUSTE_SIN_HECHO; motivos.append(f"sin hecho concreto -{AJUSTE_SIN_HECHO:g}")

    ok = pts >= 3.0
    txt = " ".join(motivos)
    # zona gris: dudosas útiles. Se excluyen las que ya se sabe que son ruido (electoral, minería, extranjera, redes)
    ruido = extranjera or any(k in txt for k in ("no sectorial", "mineria", "ruido local", "servicio al consumidor",
                                                "transferencia de canon", "sin hecho concreto"))
    evaluar_noticia.gris = (not ok) and (not ruido) and pts >= GRIS_MIN
    return ok, f"{'✅' if ok else '❌'} {pts:.1f} pts [{', '.join(x for x in motivos if x) or 'sin señales'}]"


def filtrar_relevantes(items):
    ok = []
    for n in items:
        if TITULAR_GENERICO.search(n["titular"]) and not n.get("_enriquecida"):
            enriquecer(n)          # el titular real está en la página; con el genérico el puntaje sería 0
        relevante, razon = evaluar_noticia(n["titular"], n.get("fuente", ""), n.get("fuente_url", ""))
        log(f"   {'✅' if relevante else '❌'} {razon[2:].strip()[:70]:<70} | {n['titular'][:90]}")
        if not relevante and evaluar_noticia.gris:
            n["puntaje"] = razon.lstrip("✅❌ ").strip()
            GRISES.append(n)
        if relevante:
            n["puntaje"] = razon.lstrip("✅❌ ").strip()   # ej: "7.0 pts [hidrocarburos x1, entidad fuerte]"
            ok.append(n)
    return ok


# -----------------------------------------------------------------------------
# AGRUPAR LA MISMA NOTICIA CUBIERTA POR VARIOS DIARIOS Y ELEGIR LA MEJOR
# -----------------------------------------------------------------------------
# Prioridad de diarios (editable). 3 = primera opción ... 0 = evitar (agregadores/redes).
# Se busca el nombre como texto dentro de la fuente que informa Google (sin tildes, minúsculas).
FUENTES_TIER = {
    3: ["energiminas", "rumbo minero", "gestion", "el comercio", "andina", "reuters",
        "bloomberg", "semana economica", "gob pe", "el peruano", "el gas noticias"],
    2: ["la republica", "rpp", "infobae", "peru21", "proactivo", "minería y energía",
        "mineria y energia", "convoca", "ojo publico", "desde adentro", "desdeadentro"],
    0: ["msn", "yahoo", "facebook", "youtube", "tiktok", "twitter", "x.com", "dailyhunt", "newsbreak"],
}
TIER_DEFECTO = 1
MAX_CANDIDATOS_POR_GRUPO = 3          # a cuántos diarios del grupo se les abre la página para comparar

_STOP = set("""de la el en y a los las del por con para un una al se que su sus lo como mas o es
sobre entre tras ante desde hasta este esta estos estas ser fue han hay son sin tambien segun
asi muy ya le les nos hoy ayer""".split())


def _tokens(t):
    """Palabras significativas del titular, recortadas a 6 letras (agrupa singular/plural/verbos)."""
    # _alias unifica "Ministerio de Energía y Minas" = Minem, "Organismo Supervisor..." = Osinergmin, etc.
    base = normalizar_texto(t)
    palabras = base.split() + _alias(base).split()          # forma larga y sigla, ambas cuentan
    # los números de día (1-31) no distinguen noticias: "lunes 5" y "domingo 4" son la misma nota
    return {w[:6] for w in palabras if w not in _STOP and (len(w) >= 3 or w.isdigit())
            and not (w.isdigit() and int(w) <= 31)}


# Pares de significados opuestos: si un titular dice una cosa y el otro la contraria, NO son la misma noticia
_OPUESTOS = [
    (("sube", "suben", "alza", "aument", "increm", "crece", "dispar", "elev"),
     ("baja", "bajan", "cae", "caen", "caid", "reduc", "dismin", "recort", "desplom", "descen")),
    (("utilid", "gananc", "superav"), ("perdid", "deficit", "pierd")),
    (("aprueb", "aprob", "autoriz", "otorg"), ("rechaz", "denieg", "archiv", "anula", "suspend", "revoc")),
]


def _tiene(tokens, prefijos):
    return any(t.startswith(p) for t in tokens for p in prefijos)


def _contradicen(a, b):
    for x, y in _OPUESTOS:
        ax, ay, bx, by = _tiene(a, x), _tiene(a, y), _tiene(b, x), _tiene(b, y)
        if (ax and not ay and by and not bx) or (ay and not ax and bx and not by):
            return True
    na, nb = {t for t in a if t.isdigit()}, {t for t in b if t.isdigit()}
    return bool(na and nb and not (na & nb))        # cifras distintas (220 kV vs 500 kV)


def misma_noticia(titular_a, titular_b):
    """True si dos titulares hablan del mismo hecho: >=3 palabras en común y alto solapamiento."""
    if PRECIOS_DIA.search(normalizar_texto(titular_a)) and PRECIOS_DIA.search(normalizar_texto(titular_b)):
        return True
    a, b = _tokens(titular_a), _tokens(titular_b)
    comunes = a & b
    if len(comunes) < 3 or not a or not b:
        return False
    if _contradicen(a, b):
        return False
    return len(comunes) / min(len(a), len(b)) >= 0.4 and len(comunes) / len(a | b) >= 0.25


def agrupar(noticias):
    """Agrupa (enlace simple) las noticias que son el mismo hecho. Devuelve lista de listas."""
    padre = list(range(len(noticias)))

    def raiz(i):
        while padre[i] != i:
            padre[i] = padre[padre[i]]
            i = padre[i]
        return i

    for i in range(len(noticias)):
        for j in range(i + 1, len(noticias)):
            if misma_noticia(noticias[i]["titular"], noticias[j]["titular"]):
                padre[raiz(j)] = raiz(i)
    grupos = {}
    for i, n in enumerate(noticias):
        grupos.setdefault(raiz(i), []).append(n)
    return list(grupos.values())


def tier_fuente(fuente):
    f = normalizar_texto(fuente)
    for tier, nombres in FUENTES_TIER.items():
        if any(normalizar_texto(x) in f for x in nombres):
            return tier
    return TIER_DEFECTO


def calidad(n):
    """Diario (0-3) + qué tan completo es el resumen (0-2). Mayor = mejor."""
    return tier_fuente(n.get("fuente", "")) + 2 * min(len(n.get("resumen", "")) / LARGO_RESUMEN, 1)


def elegir_mejor(grupo):
    """De un grupo de la misma noticia, deja la mejor y anota en qué otros diarios salió."""
    if len(grupo) == 1:
        return grupo[0]
    # a los mejores diarios (y mayor puntaje) se les abre la página para comparar lo completo
    candidatos = sorted(grupo, key=lambda n: (tier_fuente(n.get("fuente", "")),
                                              float(n.get("puntaje", "0").split()[0])),
                        reverse=True)[:MAX_CANDIDATOS_POR_GRUPO]
    for n in candidatos:
        if not n.get("_enriquecida"):
            enriquecer(n)
    mejor = max(candidatos, key=calidad)
    otras = sorted({n["fuente"] for n in grupo if n is not mejor and n.get("fuente")} - {mejor.get("fuente")})
    if otras:
        mejor["otras_fuentes"] = ", ".join(otras)
    log(f"   🔀 Grupo de {len(grupo)} → se queda: [{mejor.get('fuente', '?')}] {mejor['titular'][:60]}")
    for n in grupo:
        if n is not mejor:
            log(f"        ✖ [{n.get('fuente', '?')}] {n['titular'][:70]}")
    return mejor


def elegir_unicas(noticias):
    grupos = agrupar(noticias)
    log(f"\n🧩 Agrupando la misma noticia: {len(noticias)} → {len(grupos)} noticias distintas")
    elegidas = []
    for g in grupos:
        n = elegir_mejor(g)
        n["cobertura"] = len(g)          # cuántos diarios la publicaron: mide la importancia
        elegidas.append(n)
    return elegidas


MAX_POR_ENTIDAD = 4      # que una sola entidad (p. ej. Petroperú) no ocupe todo el boletín
ENTIDADES_TOPE = ["petroperu", "perupetro", "osinergmin", "minem", "oefa", "camisea"]


def prioridad(n):
    """Puntaje + tema editorial + calidad del diario + algo de cobertura (tope 3 diarios).
    Antes se ordenaba por cobertura primero: una nota de precios en 9 diarios le ganaba a una adenda en 1."""
    pts = float(n["puntaje"].split()[0])
    tema = BONO_EDITORIAL if TEMA_EDITORIAL.search(normalizar_texto(n["titular"])) else 0.0
    return pts + tema + tier_fuente(n.get("fuente", "")) * 0.5 + min(n.get("cobertura", 1), 3) * 0.5


def ordenar_y_limitar(noticias):
    """Más diarios la publicaron = más importante; empata el puntaje. Máx. MAX_POR_ENTIDAD por entidad."""
    orden = sorted(noticias, key=prioridad, reverse=True)
    cuenta, final, omitidas = {}, [], 0
    for n in orden:
        t = normalizar_texto(n["titular"])
        ent = next((e for e in ENTIDADES_TOPE if e in t), None)
        if ent and cuenta.get(ent, 0) >= MAX_POR_ENTIDAD:
            omitidas += 1
            continue
        cuenta[ent] = cuenta.get(ent, 0) + 1
        final.append(n)
    if omitidas:
        log(f"   ✂️  {omitidas} noticia(s) omitidas por tope de {MAX_POR_ENTIDAD} por entidad")
    return final[:MAX_NOTICIAS]


# -----------------------------------------------------------------------------
# ENLACE REAL + RESUMEN
# -----------------------------------------------------------------------------
def resolver_url(link):
    """Google News entrega un redirect cifrado; esto obtiene la URL real del medio."""
    if "news.google.com" not in link or gnewsdecoder is None:
        return link
    try:
        r = gnewsdecoder(link, interval=1)
        if r.get("status") and r.get("decoded_url"):
            return r["decoded_url"]
    except Exception as e:
        log(f"      ⚠️  No se pudo resolver el enlace: {e}")
    return link


_TRACK = {"ref", "outputtype", "oc", "fbclid", "gclid", "source", "mc_cid", "mc_eid", "igshid", "amp"}


def limpiar_url(url):
    """Quita rastreadores (?ref=rpp, ?outputType=amp-type, utm_*...) y el #fragmento. Conserva el resto."""
    try:
        u = urlsplit(url)
        q = [(k, v) for k, v in parse_qsl(u.query, keep_blank_values=True)
             if k.lower() not in _TRACK and not k.lower().startswith("utm_")]
        return urlunsplit((u.scheme, u.netloc, u.path, urlencode(q), ""))
    except Exception:
        return url


_CACHE_CORTOS = {}


def acortar(url, largo_max=60):
    """Acorta con is.gd (o TinyURL si falla). Si no se puede, devuelve el enlace tal cual. Solo para Telegram."""
    if len(url) <= largo_max or os.getenv("NO_ACORTAR"):
        return url
    if url in _CACHE_CORTOS:
        return _CACHE_CORTOS[url]
    for api in ("https://is.gd/create.php?format=simple&url={}", "https://tinyurl.com/api-create.php?url={}"):
        try:
            r = requests.get(api.format(quote_plus(url)), headers=HEADERS_HTTP, timeout=8)
            corto = r.text.strip()
            if r.ok and corto.startswith("http") and len(corto) < len(url):
                _CACHE_CORTOS[url] = corto
                return corto
        except Exception:
            continue
    return url


def _limpiar(t):
    return re.sub(r"\s+", " ", htmllib.unescape(t or "")).strip()


def _recortar(t, largo=LARGO_RESUMEN):
    """Recorta en fin de oración (o palabra) para que no quede cortado a la mitad."""
    t = _limpiar(t)
    if len(t) <= largo:
        return t
    corte = t[:largo]
    punto = max(corte.rfind(". "), corte.rfind("? "), corte.rfind("! "))
    if punto >= largo * 0.5:
        return corte[:punto + 1]
    return corte.rsplit(" ", 1)[0].rstrip(",;:") + "…"


def _palabras(t):
    return {w for w in normalizar_texto(t).split() if len(w) > 4}


def resumen_desde_html(html_bytes, titular):
    """
    1º meta description/og:description, solo si habla de la noticia (comparte palabras con el
    titular; muchos medios ponen la descripción genérica del sitio). 2º primeros párrafos.
    """
    soup = BeautifulSoup(html_bytes, "html.parser")
    clave = _palabras(titular)
    for attrs in ({"property": "og:description"}, {"name": "description"},
                  {"name": "twitter:description"}):
        tag = soup.find("meta", attrs=attrs)
        txt = _limpiar(tag.get("content")) if tag else ""
        if len(txt) >= 60 and len(clave & _palabras(txt)) >= 2:
            return _recortar(txt)
    zona = soup.find("article") or soup
    parrafos = [_limpiar(p.get_text(" ")) for p in zona.find_all("p")]
    parrafos = [p for p in parrafos if len(p) >= 80]
    return _recortar(" ".join(parrafos[:2])) if parrafos else ""


_FECHA_META = ({"property": "article:published_time"}, {"name": "article:published_time"},
               {"itemprop": "datePublished"}, {"name": "date"}, {"property": "og:published_time"})
TITULAR_GENERICO = re.compile(r"puede reproducirse|todos los derechos reservados|^agencia andina$|^noticias?$", re.I)


def fecha_desde_html(html_bytes):
    """Fecha de publicación REAL según la página (Google a veces re-fecha notas viejas). None si no la encuentra."""
    soup = BeautifulSoup(html_bytes, "html.parser")
    candidatos = []
    for attrs in _FECHA_META:
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content"):
            candidatos.append(tag["content"])
    t = soup.find("time", attrs={"datetime": True})
    if t:
        candidatos.append(t["datetime"])
    for sc in soup.find_all("script", type="application/ld+json"):
        m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', sc.string or sc.get_text() or "")
        if m:
            candidatos.append(m.group(1))
    for c in candidatos:
        m = re.match(r"\s*(\d{4})-(\d{2})-(\d{2})", c)
        if m:
            try:
                return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                pass
    return None


def titular_desde_html(html_bytes):
    soup = BeautifulSoup(html_bytes, "html.parser")
    tag = soup.find("meta", attrs={"property": "og:title"})
    return _limpiar(tag.get("content")) if tag and tag.get("content") else ""


def enriquecer(n):
    """Reemplaza el enlace de Google por el real y agrega n['resumen'] (vacío si no se pudo)."""
    n["resumen"] = ""
    n["_enriquecida"] = True
    n["enlace"] = limpiar_url(resolver_url(n["enlace"]))
    if "news.google.com" in n["enlace"]:
        log(f"      ⚠️  Sin enlace real, no se extrae resumen: {n['titular'][:50]}")
        return
    try:
        r = requests.get(n["enlace"], headers=HEADERS_WEB, timeout=15)
        r.raise_for_status()
        if TITULAR_GENERICO.search(n["titular"]):          # p. ej. Andina: "Este contenido puede reproducirse..."
            nuevo = titular_desde_html(r.content)
            if nuevo and not TITULAR_GENERICO.search(nuevo):
                n["titular"] = nuevo
        n["resumen"] = resumen_desde_html(r.content, n["titular"])
        real = fecha_desde_html(r.content)
        if real:
            n["fecha_real"] = real
            if real < FECHA_DESDE - timedelta(days=2):    # holgura: notas actualizadas un día después
                n["obsoleta"] = True
    except Exception as e:
        log(f"      ⚠️  No se pudo leer la página ({type(e).__name__}): {n['titular'][:50]}")
    time.sleep(1)


def enriquecer_todas(noticias):
    log(f"\n📝 Resolviendo enlaces y extrayendo resúmenes ({len(noticias)})...")
    if gnewsdecoder is None:
        log("   ⚠️  Falta 'googlenewsdecoder' (pip install googlenewsdecoder): sin resúmenes.")
    for i, n in enumerate(noticias, 1):
        if not n.get("_enriquecida"):
            enriquecer(n)
        log(f"   [{i}/{len(noticias)}] {'✅' if n['resumen'] else '➖ sin resumen'} {n['titular'][:60]}")


# -----------------------------------------------------------------------------
# GOOGLE SHEETS
# -----------------------------------------------------------------------------
def conectar(credentials_raw):
    if not credentials_raw:
        raise RuntimeError("Falta GOOGLE_CREDENTIALS_JSON")
    t = credentials_raw.strip()
    info = json.loads(t) if t.startswith("{") else json.loads(base64.b64decode(t))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _formato_inicial(sheet_id):
    """Congela encabezado, lo resalta, fija anchos y pone lista desplegable S/N en ENVIAR."""
    col_enviar = CAMPOS.index("enviar")
    reqs = [
        {"updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
            "fields": "gridProperties.frozenRowCount"}},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "backgroundColor": {"red": 0.20, "green": 0.42, "blue": 0.35},
                "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
            "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment,verticalAlignment)"}},
        {"setDataValidation": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 5000,
                      "startColumnIndex": col_enviar, "endColumnIndex": col_enviar + 1},
            "rule": {"condition": {"type": "ONE_OF_LIST",
                                   "values": [{"userEnteredValue": "S"}, {"userEnteredValue": "N"}]},
                     "showCustomUi": True, "strict": True}}},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 5000,
                      "startColumnIndex": col_enviar, "endColumnIndex": col_enviar + 1},
            "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
            "fields": "userEnteredFormat.horizontalAlignment"}},
    ]
    for i, ancho in enumerate(ANCHOS):
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": ancho}, "fields": "pixelSize"}})
    return reqs


def asegurar_hoja(servicio, spreadsheet_id):
    """
    Crea la pestaña 'Noticias' (con encabezados, formato y lista S/N) si no existe.
    Si ya existe, verifica que tenga el MISMO orden de columnas; si no, se detiene en vez
    de escribir datos en columnas equivocadas (enviar_correos.py lee por posición).
    """
    meta = servicio.spreadsheets().get(
        spreadsheetId=spreadsheet_id, fields="sheets.properties(sheetId,title)").execute()
    if HOJA_NOTICIAS in [s["properties"]["title"] for s in meta["sheets"]]:
        resp = servicio.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"'{HOJA_NOTICIAS}'!A1:{ULTIMA_COL}1").execute()
        actuales = [c.strip().upper() for c in (resp.get("values") or [[]])[0]]
        esperados = [c.upper() for c in ENCABEZADOS]
        if actuales == esperados:
            return
        if actuales == esperados[:N_BASE]:
            # Hoja creada con la versión anterior (A:I): solo falta la columna J
            log("   🔧 Agregando columna 'OTRAS FUENTES' (J) a la pestaña existente...")
            servicio.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=f"'{HOJA_NOTICIAS}'!J1",
                valueInputOption="RAW", body={"values": [[ENCABEZADOS[-1]]]}).execute()
            sid = next(x["properties"]["sheetId"] for x in meta["sheets"]
                       if x["properties"]["title"] == HOJA_NOTICIAS)
            servicio.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [
                {"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1,
                                          "startColumnIndex": N_BASE, "endColumnIndex": N_BASE + 1},
                                "cell": {"userEnteredFormat": {
                                    "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                                    "backgroundColor": {"red": 0.20, "green": 0.42, "blue": 0.35},
                                    "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE"}},
                                "fields": "userEnteredFormat(textFormat,backgroundColor,horizontalAlignment,verticalAlignment)"}},
                {"updateDimensionProperties": {
                    "range": {"sheetId": sid, "dimension": "COLUMNS", "startIndex": N_BASE, "endIndex": N_BASE + 1},
                    "properties": {"pixelSize": ANCHOS[-1]}, "fields": "pixelSize"}}]}).execute()
            return
        raise RuntimeError(
            f"La pestaña '{HOJA_NOTICIAS}' ya existe pero sus encabezados no coinciden con el "
            f"formato actual.\n   Tiene:    {actuales}\n   Esperado: {esperados}\n"
            f"   Renómbrala (p. ej. 'Noticias_old') o bórrala y vuelve a correr: se creará sola.")

    log(f"   🆕 Creando pestaña '{HOJA_NOTICIAS}'...")
    resp = servicio.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={
        "requests": [{"addSheet": {"properties": {"title": HOJA_NOTICIAS}}}]}).execute()
    sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    servicio.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"'{HOJA_NOTICIAS}'!A1:{ULTIMA_COL}1",
        valueInputOption="RAW", body={"values": [ENCABEZADOS]}).execute()
    servicio.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body={"requests": _formato_inicial(sheet_id)}).execute()


def leer_existentes(servicio, spreadsheet_id):
    """Enlaces y titulares ya guardados (cualquier fecha) para no repetir."""
    resp = servicio.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{HOJA_NOTICIAS}'!A2:{ULTIMA_COL}").execute()
    i_enlace, i_titular = CAMPOS.index("enlace"), CAMPOS.index("titular")
    i_captura = CAMPOS.index("captura")
    desde = (HOY - timedelta(days=7)).strftime("%Y-%m-%d")
    enlaces, titulares = set(), []
    for f in resp.get("values", []):
        f = list(f) + [""] * (len(CAMPOS) - len(f))
        if f[i_enlace].strip():
            enlaces.add(f[i_enlace].strip())
        # para "misma noticia" solo se comparan los últimos 7 días (un hecho nuevo del mismo tema sí entra)
        if f[i_titular].strip() and f[i_captura].strip() >= desde:
            titulares.append(f[i_titular].strip())
    return enlaces, titulares


def _fila(n, captura):
    """Una noticia -> lista de celdas en el orden de CAMPOS."""
    valores = {
        "captura": captura,
        "titular": n["titular"],
        "fecha_pub": n["fecha_pub"].strftime("%Y-%m-%d") if n.get("fecha_pub") else "",
        "fuente": n.get("fuente", ""),
        "resumen": n.get("resumen", ""),     # automático; puedes editarlo en la hoja
        "enlace": n["enlace"],
        "puntaje": n.get("puntaje", ""),
        "enviar": "",                        # tú marcas S (o N)
        "enviado": "",                       # lo escribe enviar_correos.py
        "otras_fuentes": n.get("otras_fuentes", ""),
    }
    return [valores[c] for c in CAMPOS]


def guardar(servicio, spreadsheet_id, noticias):
    captura = HOY.strftime("%Y-%m-%d")
    filas = [_fila(n, captura) for n in noticias]
    servicio.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id, range=f"'{HOJA_NOTICIAS}'!A:{ULTIMA_COL}",
        valueInputOption="RAW", insertDataOption="INSERT_ROWS",
        body={"values": filas}).execute()
    log(f"   ✅ {len(filas)} noticias agregadas a '{HOJA_NOTICIAS}' (ENVIAR vacío: marca S las que quieras)")


# -----------------------------------------------------------------------------
# TELEGRAM
# -----------------------------------------------------------------------------
def _h(t):
    """Escapa texto para Telegram parse_mode=HTML."""
    return htmllib.escape(t or "", quote=False)


def _enlace_tg(url):
    """Enlace con texto corto: la URL (aunque sea larguísima) queda oculta tras 'Leer nota'."""
    return f'🔗 <a href="{htmllib.escape(url, quote=True)}">Leer nota</a>'


def bloque_grises(max_n=6):
    vistos, out = set(), []
    for n in sorted(GRISES, key=lambda x: float(x["puntaje"].split()[0]), reverse=True):
        k = normalizar_texto(n["titular"])[:60]
        if k in vistos:
            continue
        vistos.add(k)
        out.append(f"• {_h(n['titular'][:140])} ({_h(n.get('fuente', ''))})\n  {_enlace_tg(n['enlace'])}")
        if len(out) >= max_n:
            break
    return ("\n\n🔎 <b>Posibles</b> (el filtro dudó, revisa):\n" + "\n".join(out)) if out else ""


def mensaje_telegram(noticias):
    """Mensaje en HTML de Telegram. Cada noticia es un bloque autónomo (separado por línea en blanco)."""
    if not noticias:
        return "📰 Noticias del sector: hoy no se encontraron noticias relevantes." + bloque_grises()
    msg = f"📰 <b>Noticias del sector {HOY.strftime('%d/%m/%y')}</b>\n\n"
    for i, n in enumerate(noticias, 1):
        fuente = f" <i>({_h(n['fuente'])})</i>" if n["fuente"] else ""
        resumen = f"{_h(n['resumen'])}\n" if n.get("resumen") else ""
        otras = f"📎 También en: {_h(n['otras_fuentes'])}\n" if n.get("otras_fuentes") else ""
        msg += f"{i}. <b>{_h(n['titular'])}</b>{fuente}\n{resumen}{otras}{_enlace_tg(n['enlace'])}\n\n"
    return msg.strip() + bloque_grises()


def enviar_telegram_html(mensaje, bot_token, chat_id, limite=3800):
    """Envía en HTML, partiendo por bloques para no pasar el límite de Telegram (4096).
    Sin vista previa de enlaces (con enlaces largos de Google News ocupaba media pantalla)."""
    if not bot_token or not chat_id:
        log("   ⚠️  Sin TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID: no se envió el mensaje")
        return False
    partes, actual = [], ""
    for bloque in mensaje.split("\n\n"):
        if actual and len(actual) + len(bloque) + 2 > limite:
            partes.append(actual.strip()); actual = ""
        actual += bloque + "\n\n"
    if actual.strip():
        partes.append(actual.strip())
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    for k, parte in enumerate(partes, 1):
        try:
            r = requests.post(url, data={"chat_id": chat_id, "text": parte, "parse_mode": "HTML",
                                         "disable_web_page_preview": "true"}, timeout=15)
            if r.status_code == 400:   # HTML mal formado: reenviar como texto plano en vez de perder el aviso
                plano = re.sub(r"<[^>]+>", "", parte)
                r = requests.post(url, data={"chat_id": chat_id, "text": htmllib.unescape(plano),
                                             "disable_web_page_preview": "true"}, timeout=15)
            r.raise_for_status()
            log(f"   ✅ Telegram parte {k}/{len(partes)} enviada")
            if k < len(partes):
                time.sleep(1)
        except Exception as e:
            log(f"   ❌ Error Telegram (parte {k}): {e}")
            return False
    return True


# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="no escribe en Sheets ni envía Telegram")
    args = ap.parse_args()

    log(f"\n📰 BÚSQUEDA DE NOTICIAS — hoy {HOY.strftime('%a %d/%m')}, desde {FECHA_DESDE.strftime('%a %d/%m')} (when:{VENTANA})")
    crudas = deduplicar(buscar_noticias())
    log(f"   Únicas tras deduplicar: {len(crudas)}")

    log("\n🔬 Filtrando relevancia...")
    relevantes = filtrar_relevantes(crudas)

    if not args.dry_run:
        spreadsheet_id = os.getenv("SPREADSHEET_ID")
        if not spreadsheet_id:
            raise RuntimeError("Falta SPREADSHEET_ID")
        servicio = conectar(os.getenv("GOOGLE_CREDENTIALS_JSON"))
        asegurar_hoja(servicio, spreadsheet_id)
        enlaces, titulares = leer_existentes(servicio, spreadsheet_id)
        no_repetidas = [n for n in relevantes
                        if n["enlace"] not in enlaces
                        and not any(misma_noticia(n["titular"], t) for t in titulares)]
        log(f"\n   Ya estaban en la hoja (últimos 7 días): {len(relevantes) - len(no_repetidas)}")
        nuevas = ordenar_y_limitar(elegir_unicas(no_repetidas))
        log(f"   Nuevas para guardar: {len(nuevas)}")
        enriquecer_todas(nuevas)
        viejas = [n for n in nuevas if n.get("obsoleta")]
        for n in viejas:
            log(f"   🕰️  Descartada por fecha real {n['fecha_real']} (Google la mostró como reciente): {n['titular'][:60]}")
        nuevas = [n for n in nuevas if not n.get("obsoleta")]
        if nuevas:
            guardar(servicio, spreadsheet_id, nuevas)
        enviar_telegram_html(mensaje_telegram(nuevas),
                             os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID"))
    else:
        top = ordenar_y_limitar(elegir_unicas(relevantes))
        enriquecer_todas(top)
        for n in [n for n in top if n.get("obsoleta")]:
            log(f"   🕰️  Descartada por fecha real {n['fecha_real']}: {n['titular'][:60]}")
        top = [n for n in top if not n.get("obsoleta")]
        log("\n[dry-run] Mensaje de Telegram que se enviaría:\n")
        log(mensaje_telegram(top))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"\n❌ ERROR: {e}")
        sys.exit(1)
