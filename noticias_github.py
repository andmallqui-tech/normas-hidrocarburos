"""
=============================================================================
BÚSQUEDA DE NOTICIAS DEL SECTOR (Google News RSS)
=============================================================================
- Consulta Google News RSS (sin API key) con varias búsquedas del sector.
- Reusa el vocabulario y el puntaje de normas_github (CORE, entidades, evaluar_relevancia) y lo AFINA
  para el público del boletín (instituciones que siguen el sector HIDROCARBUROS):
    · Etapa 1 (titular, laxa): decide a quién se le abre la página.
    · Etapa 2 (titular + resumen + sección/URL, estricta): exige ancla de hidrocarburos y un hecho
      concreto (regulación, contrato/lote, inversión, decisión de empresa...).
    · Usa tus normas recientes (hoja de Normas) y tus S/N de la hoja Noticias para ajustar.
- Verifica la fecha REAL (URL/página) ANTES de elegir cuáles entran; no hay tope fijo de noticias.
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
from collections import Counter
from math import log as _ln
from normas_github import (evaluar_relevancia, enviar_telegram, normalizar_texto, _alias,
                           CORE, ENTIDAD_FUERTE, INFRA_ENERGIA, MINERIA, AMBIENTE, ENERGIA_GENERAL)

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


def _env_int(nombre, defecto):
    try:
        return int(os.getenv(nombre) or defecto)
    except ValueError:
        return defecto


# --- Cuántas noticias entran --------------------------------------------------------------------
# YA NO hay un "siempre 15": entra todo lo que supere UMBRAL_FINAL. MAX_NOTICIAS es solo un techo de
# seguridad por si un día el filtro se descontrola (0 = sin techo). Se puede cambiar por variable de entorno.
MAX_NOTICIAS = _env_int("NOTICIAS_TOPE", 40)
MAX_ENRIQUECER = _env_int("NOTICIAS_MAX_ENRIQUECER", 70)     # a cuántas se les abre la página (enlace real, fecha, resumen)
MAX_POR_ENTIDAD = _env_int("NOTICIAS_MAX_POR_ENTIDAD", 6)    # que una sola entidad no ocupe todo el boletín
UMBRAL_PRE = 2.0          # etapa 1 (solo titular): laxo, para no perder candidatas
UMBRAL_FINAL = 3.0        # etapa 2 (titular + resumen + sección): estricto
HOLGURA_FECHA = 1         # días de tolerancia sobre FECHA_DESDE al verificar la fecha real
# Electricidad/renovables: por defecto FUERA (el boletín es de hidrocarburos). INCLUIR_ELECTRICIDAD=1 los reactiva.
INCLUIR_ELECTRICIDAD = (os.getenv("INCLUIR_ELECTRICIDAD") or "").strip().lower() in ("1", "true", "si", "sí")

# Varias búsquedas cortas rinden más que una sola gigante (Google recorta ~100 resultados por query).
# Todas apuntan a HIDROCARBUROS; ya no se busca "electricidad" ni "transición energética" a secas.
QUERIES = [
    '(hidrocarburos OR Perupetro OR Petroperú) Perú',
    '(Osinergmin OR Minem) (hidrocarburos OR "gas natural" OR combustibles OR GLP OR GNV)',
    '("gas natural" OR GNV OR GLP OR Camisea OR TGP OR Calidda OR Contugas) Perú',
    '(Petroperú OR "Refinería Talara" OR "Oleoducto Norperuano") (directorio OR deuda OR contrato OR reorganización OR producción OR crudo)',
    '(adenda OR contrato OR licitación OR lote) (Perupetro OR hidrocarburos OR petróleo OR "gas natural") Perú',
    '(OEFA OR "derrame de petróleo" OR "lote 192" OR "lote 95" OR "lote 8" OR "lote 56") Perú',
    '("banda de precios" OR "precio del petróleo" OR Brent) (combustibles OR Perú)',
    '("proyecto de ley" OR "decreto supremo" OR reglamento OR dictamen) (hidrocarburos OR "gas natural" OR combustibles OR GLP OR Petroperú)',
]
QUERIES_ELECTRICIDAD = [
    '(Minem OR "energía y minas") (electricidad OR "transición energética" OR renovables) Perú',
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


_RX_SECCION = re.compile(r"\s*\|\s*([A-ZÁÉÍÓÚÑ ]{3,25})$")


def _quitar_seccion(titulo):
    """'Titular | OPINIÓN' -> ('Titular', 'OPINIÓN'). La sección se conserva: sirve para descartar opinión/deportes."""
    m = _RX_SECCION.search(titulo)
    return (titulo[:m.start()], m.group(1).strip()) if m else (titulo, "")


def parsear_rss(xml_texto):
    """Devuelve lista de dicts {titular, fuente, enlace, fecha_pub, seccion}."""
    items = []
    root = ET.fromstring(xml_texto)
    for it in root.iter("item"):
        titulo = htmllib.unescape((it.findtext("title") or "").strip())
        enlace = (it.findtext("link") or "").strip()
        src_el = it.find("source")
        fuente = (src_el.text or "").strip() if src_el is not None and src_el.text else ""
        fuente_url = (src_el.get("url") or "").strip() if src_el is not None else ""

        titulo, seccion = _quitar_seccion(titulo)                          # "… | ECONOMIA" (sección del medio)
        # Google agrega " - Fuente" al final del titular: se quita para no duplicarlo
        if fuente and titulo.endswith(f" - {fuente}"):
            titulo = titulo[: -len(fuente) - 3].strip()
        elif " - " in titulo and not fuente:
            titulo, fuente = titulo.rsplit(" - ", 1)

        try:
            fecha_pub = parsedate_to_datetime(it.findtext("pubDate") or "").astimezone(ZONA).date()
        except Exception:
            fecha_pub = None

        titulo, seccion2 = _quitar_seccion(titulo)
        if titulo and enlace:
            items.append({"titular": titulo, "fuente": fuente.strip(), "fuente_url": fuente_url,
                          "enlace": enlace, "fecha_pub": fecha_pub, "seccion": seccion or seccion2})
    return items


def buscar_noticias(queries=None):
    todas = []
    for q in (queries or QUERIES):
        try:
            r = requests.get(url_rss(q, VENTANA), headers=HEADERS_HTTP, timeout=20)
            r.raise_for_status()
            items = parsear_rss(r.content)
            antes = len(items)
            # Sin fecha de Google => se conserva, pero se verificará con la fecha real de la página/URL.
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
                        r"calidda|contugas|pluspetrol|petrotal|canon (?:petrolero|gasifero)|gas de camisea)\b")
# Solo suma si INCLUIR_ELECTRICIDAD=1 (el boletín es de hidrocarburos)
VOCAB_RENOVABLE = re.compile(r"\b(?:parque eolico|gigante eolico|energia renovable|energias renovables)\b")
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
# Hechos que le sirven a una institución del sector: decisiones, normas, contratos/lotes, inversión, finanzas
# de las empresas, producción/reservas, incidentes de suministro. OJO: los NOMBRES de entidades ya no cuentan
# como "hecho" (antes "Petroperú: la historia de la empresa" pasaba solo por nombrar a Petroperú).
EVENTO_INST = re.compile(
    r"\b(?:aprueb\w+|aprob\w+|oficializ\w+|promulg\w+|dispon\w+|modific\w+|derog\w+|emit\w+|emision\w*|"
    r"reglament\w+|decretos? (?:supremos?|de urgencia|legislativos?)|du \d+|leyes?|proyectos? de ley|dictamen\w*|"
    r"adenda\w*|contrat\w+|convenios?|licitacion\w*|concurso\w*|subasta\w*|adjudic\w+|lotes? \w+|concesion\w*|"
    r"firm\w+|suscrib\w+|otorg\w+|autoriz\w+|cesion\w*|sancion\w*|multa\w*|fiscaliz\w+|supervis\w+|"
    r"emergencia|arbitraje|ciadi|laudo|demandan?|"
    r"directorio|reorganiz\w+|capitaliz\w+|deuda|garantia\w*|rescate|refinanc\w+|calificacion|"
    r"produccion|reservas|descubri\w+|exploracion|perforacion|pozos?|"
    r"inversion\w*|gasoducto\w*|oleoducto\w*|refineria|masificacion|"
    r"restablec\w+|suspend\w+|paraliz\w+|falla|rotura|derrame\w*|paro|bloque\w*|"
    r"destrab\w+|privatiz\w+|venta de activos|subsidio\w*|fondo de estabilizacion|banda de precios|"
    r"importacion\w*|exportacion\w*|"
    r"anunci\w+|present\w+|propon\w+|propuesta\w*|plan(?:es)?|salvataje|reestructur\w+|solicit\w+|pid\w+|"
    r"investig\w+|denunci\w+|concertacion|conect\w+|ampli\w+|construc\w+|obras?|inici\w+|culmin\w+|lanz\w+|"
    r"prepublic\w+|norma\w*|acuerd\w+|negoci\w+|cierr\w+|evalu\w+|plantea\w*|convoc\w+|"
    r"fij\w+|tarifa\w*|peaje\w*|cargos?|pagos?|cronograma\w*|ejecut\w+|impuls\w+|promuev\w+|promover|"
    r"incumpl\w+|infraccion\w*|riesgo\w*|alerta\w*|incendio\w*|explosion\w*|accidente\w*)\b")
# Notas explicativas / históricas / de consejos: contexto general, no un hecho para la institución
NO_HECHO_TIT = re.compile(r"\b(?:historia de|asi llego|claves?|todo lo que (?:debes|necesitas)|que es|como funciona|"
                          r"por que|guia|paso a paso|preguntas|razones|conoce|descubre|como (?:ahorrar|elegir|cargar)|"
                          r"consejos|tips|trucos)\b")
AJUSTE_EXPLICATIVA = 3.0
# "bono" es consumo SOLO si no habla de deuda/emisión (p. ej. "bono de Petroperú" es finanzas de la empresa)
BONO = re.compile(r"\bbonos?\b")
BONO_CORP = re.compile(r"\b(?:petroperu|perupetro|emision\w*|soberanos?|corporativ\w+|deuda|garantia\w*|mef|capital)\b")
SERVICIO_SIN_BONO = re.compile(SERVICIO.pattern.replace("bono\\b|", ""))
# Opinión / secciones que no son noticia del sector (se leen de la sección que informa Google y del PATH de la URL)
OPINION_SEC = re.compile(r"\b(?:opinion|columnistas?|editorial|blogs?|cartas?|tribuna)\b")
OPINION_TIT = re.compile(r"^(?:opinion|editorial|columna)\b")
SECCION_RUIDO = re.compile(r"\b(?:deportes?|futbol|espectaculos?|entretenimiento|farandula|horoscopo|virales?|lifestyle|"
                           r"tendencias|policiales?|gastronomia|loterias?|tv|television|salud|cultura|estilo de vida)\b")
SECCION_MUNDO = re.compile(r"\b(?:mundo|internacional(?:es)?|global)\b")
AJUSTE_OPINION, AJUSTE_SECCION, AJUSTE_MUNDO, AJUSTE_NORMAS = 4.0, 6.0, 4.0, 1.5
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


# -----------------------------------------------------------------------------
# CONTEXTO: normas recientes + feedback S/N de la hoja Noticias
# -----------------------------------------------------------------------------
# Términos específicos que aparecen seguido en las normas que el sistema de normas capturó (y no marcaste N).
# Una noticia del sector que coincide con uno de ellos sube +AJUSTE_NORMAS: es "contexto" de lo que se está regulando.
TEMAS_NORMAS = []                      # [(termino, regex)], lo llena cargar_temas_normas()
_GENERICOS = {"hidrocarburos", "hidrocarburo", "petroleo", "combustible", "combustibles", "gas natural",
              "ductos", "ducto", "crudo", "grifo", "grifos"}   # demasiado amplios para distinguir un tema


def temas_desde_normas(textos_pesos, n_max=6, min_veces=2):
    """textos_pesos: [(texto_norma, peso)]. Devuelve [(termino, veces)] de términos específicos recurrentes."""
    cuenta = Counter()
    for texto, peso in textos_pesos:
        t = _alias(normalizar_texto(texto))
        hallados = {m.group(0) for m in CORE.finditer(t)} | {m.group(0) for m in ENTIDAD_FUERTE.finditer(t)}
        hallados |= set(re.findall(r"\blotes? \d+\b", t))
        for h in hallados:
            if h not in _GENERICOS and len(h) >= 3:
                cuenta[h] += peso
    return [(w, c) for w, c in cuenta.most_common(n_max) if c >= min_veces]


def cargar_temas_normas(temas):
    TEMAS_NORMAS.clear()
    for term, _ in temas:
        TEMAS_NORMAS.append((term, re.compile(r"\b" + re.escape(term) + r"\b")))


class _Feedback:
    """Aprende de tus S/N en la hoja Noticias: tokens que aparecen más en S suben, los de N bajan.
    Solo se activa con >= MIN_ETIQUETAS de cada lado (con pocos datos sería ruido)."""
    MIN_ETIQUETAS = 10

    def __init__(self):
        self.reset()

    def reset(self):
        self.pos, self.neg = Counter(), Counter()
        self.n_pos = self.n_neg = 0
        self.titulares_n = []
        self.activo = False

    def entrenar(self, positivos, negativos):
        self.reset()
        for t in positivos:
            self.pos.update(_tokens(t)); self.n_pos += 1
        for t in negativos:
            self.neg.update(_tokens(t)); self.n_neg += 1
        self.titulares_n = list(negativos)[-80:]
        self.activo = self.n_pos >= self.MIN_ETIQUETAS and self.n_neg >= self.MIN_ETIQUETAS

    def ajuste(self, titular):
        """(delta, motivo). Muy parecida a una N reciente => -4. Si no, log-odds de tokens, acotado a ±3."""
        if not self.activo:
            return 0.0, ""
        for tn in self.titulares_n:
            if misma_noticia(titular, tn):
                return -4.0, "parecida a una que marcaste N -4"
        score = 0.0
        for w in _tokens(titular):
            if self.pos[w] + self.neg[w] >= 3:
                score += _ln((self.pos[w] + 1) / (self.n_pos + 2)) - _ln((self.neg[w] + 1) / (self.n_neg + 2))
        delta = max(-3.0, min(3.0, 0.5 * score))
        return (delta, f"feedback S/N {delta:+.1f}") if abs(delta) >= 0.5 else (0.0, "")


FEEDBACK = _Feedback()


# -----------------------------------------------------------------------------
# PUNTAJE DE UNA NOTICIA
# -----------------------------------------------------------------------------
def _hay_ancla_hc(t, mercado):
    """Ancla de HIDROCARBUROS: la noticia trata del sector, no solo de energía/minería/ambiente en general."""
    reorg = bool(REORG_MINEM[0].search(t) and REORG_MINEM[1].search(t))
    if CORE.search(t) or VOCAB_PERU.search(t) or TEMA_PERU.search(t) or mercado or reorg:
        return True
    # Osinergmin también regula electricidad y minería: solo ancla si la nota no es de eso
    if ENTIDAD_FUERTE.search(t) and not (INFRA_ENERGIA.search(t) or MINERIA.search(t)):
        return True
    return bool(INCLUIR_ELECTRICIDAD and (INFRA_ENERGIA.search(t) or VOCAB_RENOVABLE.search(t)))


def puntuar_noticia(titular, fuente="", fuente_url="", contexto="", seccion="", url="", etapa="final"):
    """Devuelve (pts, motivos, ok, gris).
    etapa='pre'  : solo titular, umbral laxo (decide a quién se le abre la página).
    etapa='final': titular + resumen + sección/URL, umbral estricto y exige hecho concreto."""
    texto = f"{titular}. {contexto}" if contexto else titular
    _, razon = evaluar_relevancia(texto, "")                 # vocabulario/entidades/puntaje de normas
    m = re.search(r"(-?[\d.]+) pts \[(.*)\]", razon)
    pts, motivos = (float(m.group(1)), m.group(2)) if m else (0.0, "")
    motivos = [] if motivos == "sin señales" else [motivos]
    t = normalizar_texto(texto)
    tt = normalizar_texto(titular)

    consumidor = bool(PRECIOS_DIA.search(tt) or SERVICIO_SIN_BONO.search(t) or CONSUMO_PUBLICO.search(t)
                      or (BONO.search(t) and not BONO_CORP.search(t)))
    ruido_n = len(set(NO_SECTOR.findall(t)))
    if ruido_n:
        resta = AJUSTE_NO_SECTOR * min(ruido_n, 3)
        pts -= resta; motivos.append(f"no sectorial -{resta:g}")
    if TEMA_PERU.search(t) and not consumidor:       # "precio del balón de gas hoy" ya no se premia por decir "balón de gas"
        pts += AJUSTE_TEMA; motivos.append(f"tema del momento +{AJUSTE_TEMA:g}")
    reorg = bool(REORG_MINEM[0].search(t) and REORG_MINEM[1].search(t))
    if reorg:
        pts += AJUSTE_TEMA; motivos.append(f"reorganización Minem +{AJUSTE_TEMA:g}")
    if (VOCAB_PERU.search(t) or (INCLUIR_ELECTRICIDAD and VOCAB_RENOVABLE.search(t))) and not consumidor:
        pts += AJUSTE_VOCAB; motivos.append(f"vocabulario del sector +{AJUSTE_VOCAB:g}")
    mercado = bool(MERCADO_INTL.search(t))
    if mercado:
        motivos.append("mercado internacional del petróleo")
    extranjera = bool(EXTRANJERO.search(tt) and not PERU.search(t)) and not mercado
    if extranjera:
        pts -= AJUSTE_EXTRANJERO; motivos.append(f"extranjera -{AJUSTE_EXTRANJERO:g}")
    elif not mercado and not (PERU.search(t) or TEMA_PERU.search(t) or VOCAB_PERU.search(t)) \
            and not es_fuente_peruana(fuente, fuente_url):
        pts -= AJUSTE_SIN_PERU; motivos.append(f"sin señal de Perú -{AJUSTE_SIN_PERU:g}")

    if consumidor:
        pts -= AJUSTE_SERVICIO; motivos.append(f"servicio al consumidor -{AJUSTE_SERVICIO:g}")
    if NO_HECHO_TIT.search(tt):
        pts -= AJUSTE_EXPLICATIVA; motivos.append(f"nota explicativa/consejos -{AJUSTE_EXPLICATIVA:g}")
    if CANON_TRANSFERENCIA.search(t):
        pts -= AJUSTE_CANON; motivos.append(f"transferencia de canon -{AJUSTE_CANON:g}")

    # --- Sección del medio / PATH de la URL (sin el slug, para no confundir palabras del titular) ---
    secs = normalizar_texto(seccion)
    try:
        segs = [x for x in urlsplit(url).path.lower().split("/") if x][:-1] if url else []
    except Exception:
        segs = []
    secs = (secs + " " + " ".join(normalizar_texto(x) for x in segs)).strip()
    es_opinion = bool(OPINION_SEC.search(secs) or OPINION_TIT.search(tt))
    if es_opinion:
        pts -= AJUSTE_OPINION; motivos.append(f"opinión -{AJUSTE_OPINION:g}")
    if SECCION_RUIDO.search(secs):
        pts -= AJUSTE_SECCION; motivos.append(f"sección no sectorial -{AJUSTE_SECCION:g}")
    if not mercado and SECCION_MUNDO.search(secs):
        pts -= AJUSTE_MUNDO; motivos.append(f"sección internacional -{AJUSTE_MUNDO:g}")

    # --- Ancla de hidrocarburos: electricidad/renovables/minería/ambiente solos NO son el sector ---
    hc = _hay_ancla_hc(t, mercado)
    if not hc:
        fuera = bool(INFRA_ENERGIA.search(t) or MINERIA.search(t) or AMBIENTE.search(t) or ENERGIA_GENERAL.search(t))
        pts = min(pts, 1.5)
        motivos.append("fuera de hidrocarburos (energía/minería/ambiente)" if fuera else "sin ancla de hidrocarburos")

    # --- Hecho concreto (solo etapa final: el titular suelto no siempre lo muestra) ---
    evento = bool(EVENTO_INST.search(t) or TEMA_PERU.search(t) or mercado or reorg)
    if etapa == "final" and hc and not evento:
        pts = min(pts, UMBRAL_FINAL - 0.1); motivos.append("sin hecho concreto (tope)")

    # --- Contexto: normas recientes y tu feedback (solo suman si la noticia es de hidrocarburos) ---
    if hc:
        for term, rx in TEMAS_NORMAS:
            if rx.search(t):
                pts += AJUSTE_NORMAS; motivos.append(f"coincide con normas recientes ({term}) +{AJUSTE_NORMAS:g}")
                break
        delta, why = FEEDBACK.ajuste(titular)
        if delta:
            pts += delta; motivos.append(why)

    umbral = UMBRAL_FINAL if etapa == "final" else UMBRAL_PRE
    ok = pts >= umbral
    txt = " ".join(motivos)
    # zona gris: dudosas útiles (no las que ya se sabe que son ruido)
    ruido = (extranjera or es_opinion or any(k in txt for k in (
        "no sectorial", "mineria", "ruido local", "servicio al consumidor", "transferencia de canon",
        "fuera de hidrocarburos", "sección no sectorial", "nota explicativa", "parecida a una que marcaste N")))
    gris = (not ok) and (not ruido) and pts >= GRIS_MIN
    return pts, motivos, ok, gris


def evaluar_noticia(titular, fuente="", fuente_url="", **kw):
    """Compatibilidad: (ok, razón). Deja .gris y .pts como atributos de la función."""
    pts, motivos, ok, gris = puntuar_noticia(titular, fuente, fuente_url, **kw)
    evaluar_noticia.gris, evaluar_noticia.pts = gris, pts
    return ok, f"{'✅' if ok else '❌'} {pts:.1f} pts [{', '.join(x for x in motivos if x) or 'sin señales'}]"


def filtrar_pre(items):
    """Etapa 1 (solo titular, umbral laxo). Las que pasan se enriquecen (enlace real, fecha, resumen)."""
    ok = []
    for n in items:
        if TITULAR_GENERICO.search(n["titular"]) and not n.get("_enriquecida"):
            enriquecer(n)          # el titular real está en la página; con el genérico el puntaje sería 0
        pts, motivos, relevante, gris = puntuar_noticia(
            n["titular"], n.get("fuente", ""), n.get("fuente_url", ""), seccion=n.get("seccion", ""), etapa="pre")
        razon = f"{pts:.1f} pts [{', '.join(x for x in motivos if x) or 'sin señales'}]"
        log(f"   {'✅' if relevante else '❌'} {razon[:70]:<70} | {n['titular'][:90]}")
        n["puntaje"], n["pre"] = razon, pts
        if relevante:
            ok.append(n)
        elif gris:
            GRISES.append(n)
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


ENTIDADES_TOPE = ["petroperu", "perupetro", "osinergmin", "minem", "oefa", "camisea"]


def prioridad(n):
    """Puntaje + tema editorial + calidad del diario + algo de cobertura (tope 3 diarios).
    Antes se ordenaba por cobertura primero: una nota de precios en 9 diarios le ganaba a una adenda en 1."""
    pts = float(n["puntaje"].split()[0])
    tema = BONO_EDITORIAL if TEMA_EDITORIAL.search(normalizar_texto(n["titular"])) else 0.0
    return pts + tema + tier_fuente(n.get("fuente", "")) * 0.5 + min(n.get("cobertura", 1), 3) * 0.5


def ordenar_y_limitar(noticias):
    """Ordena por prioridad. Máx. MAX_POR_ENTIDAD por entidad. Solo hay un techo de seguridad
    (MAX_NOTICIAS, 0 = sin techo): la cantidad la decide el umbral de relevancia, no un número fijo."""
    # Las de fecha NO verificada van siempre detrás de las verificadas (una nota de marzo re-fechada por Google
    # no debe ganarle a una verificada, por buen puntaje que tenga); además es lo primero que cae con el techo.
    orden = sorted(noticias, key=lambda n: (bool(n.get("fecha_verificada", True)), prioridad(n)), reverse=True)
    cuenta, final, omitidas = {}, [], 0
    for n in orden:
        t = normalizar_texto(n["titular"])
        ent = next((e for e in ENTIDADES_TOPE if e in t), None)
        if ent and MAX_POR_ENTIDAD and cuenta.get(ent, 0) >= MAX_POR_ENTIDAD:
            omitidas += 1
            continue
        cuenta[ent] = cuenta.get(ent, 0) + 1
        final.append(n)
    if omitidas:
        log(f"   ✂️  {omitidas} noticia(s) omitidas por tope de {MAX_POR_ENTIDAD} por entidad")
    if MAX_NOTICIAS and len(final) > MAX_NOTICIAS:
        log(f"   🛑 Techo de seguridad: {len(final)} relevantes, se guardan las {MAX_NOTICIAS} de mayor prioridad")
        final = final[:MAX_NOTICIAS]
    return final


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


_FECHA_URL = (re.compile(r"/(20\d{2})[/-](\d{1,2})[/-](\d{1,2})(?:[/-]|$)"),    # /2026/03/12/  o  /2026-03-12-
              re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)"))               # ...-20260312
def fecha_en_url(url):
    """Fecha de publicación que muchos diarios ponen en el PATH de la URL. None si no hay."""
    try:
        path = urlsplit(url).path
    except Exception:
        return None
    for rx in _FECHA_URL:
        for m in rx.finditer(path):
            try:
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                continue
            if date(2015, 1, 1) <= d <= HOY + timedelta(days=1):
                return d
    return None


def aplicar_fechas(n, fecha_pagina=None):
    """Fija n['fecha_real'] (la MÁS ANTIGUA entre URL y página: Google re-fecha notas viejas, nunca al revés),
    n['fecha_verificada'] y n['obsoleta']. Sin ninguna fecha real queda 'sin verificar' (se confía en Google, con aviso)."""
    conocidas = [d for d in (fecha_en_url(n.get("enlace", "")), fecha_pagina) if d]
    if conocidas:
        n["fecha_real"] = min(conocidas)
        n["fecha_verificada"] = True
        n["obsoleta"] = n["fecha_real"] < FECHA_DESDE - timedelta(days=HOLGURA_FECHA)
    else:
        n["fecha_verificada"] = False
        n["obsoleta"] = False


def enriquecer(n):
    """Reemplaza el enlace de Google por el real, verifica la fecha real y agrega n['resumen'] (vacío si no se pudo)."""
    n["resumen"] = ""
    n["_enriquecida"] = True
    n["enlace"] = limpiar_url(resolver_url(n["enlace"]))
    aplicar_fechas(n)                                   # 1º por la URL (no cuesta abrir la página)
    if "news.google.com" in n["enlace"]:
        log(f"      ⚠️  Sin enlace real, no se extrae resumen ni se verifica la fecha: {n['titular'][:50]}")
        return
    if n["obsoleta"]:                                   # ya se sabe que es vieja: no gastar tiempo en abrirla
        return
    pagina = None
    try:
        r = requests.get(n["enlace"], headers=HEADERS_WEB, timeout=15)
        r.raise_for_status()
        if TITULAR_GENERICO.search(n["titular"]):          # p. ej. Andina: "Este contenido puede reproducirse..."
            nuevo = titular_desde_html(r.content)
            if nuevo and not TITULAR_GENERICO.search(nuevo):
                n["titular"] = nuevo
        n["resumen"] = resumen_desde_html(r.content, n["titular"])
        pagina = fecha_desde_html(r.content)
    except Exception as e:
        log(f"      ⚠️  No se pudo leer la página ({type(e).__name__}): {n['titular'][:50]}")
    aplicar_fechas(n, pagina)
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
        # fecha REAL (URL/página) cuando se pudo verificar; si no, la de Google
        "fecha_pub": (n.get("fecha_real") or n.get("fecha_pub")).strftime("%Y-%m-%d") if (n.get("fecha_real") or n.get("fecha_pub")) else "",
        "fuente": n.get("fuente", ""),
        "resumen": n.get("resumen", ""),     # automático; puedes editarlo en la hoja
        "enlace": n["enlace"],
        "puntaje": n.get("puntaje", "") + ("" if n.get("fecha_verificada", True) else " | ⚠ fecha sin verificar"),
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


def _nombre_hoja_normas(servicio, spreadsheet_id):
    """Misma regla que generar_boletin.py: HOJA_NORMAS o la primera pestaña (que no sea Noticias)."""
    nombre = (os.getenv("HOJA_NORMAS") or "").strip()
    if nombre:
        return nombre
    meta = servicio.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title").execute()
    titulos = [x["properties"]["title"] for x in meta["sheets"]]
    return next((t for t in titulos if t != HOJA_NOTICIAS), titulos[0])


def leer_temas_normas(servicio, spreadsheet_id, dias=30):
    """Temas recurrentes de las normas capturadas en los últimos `dias` (hoja de Normas: A=corrida, B=título,
    D=sumilla, G=S/N). Las marcadas N no cuentan; las S pesan el doble."""
    hoja = _nombre_hoja_normas(servicio, spreadsheet_id)
    resp = servicio.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{hoja}'!A2:H").execute()
    desde = HOY - timedelta(days=dias)
    textos = []
    for f in resp.get("values", []):
        f = list(f) + [""] * (8 - len(f))
        try:
            d = datetime.strptime(f[0].strip(), "%Y-%m-%d").date()
        except ValueError:
            continue
        marca = f[6].strip().upper()
        if d < desde or marca == "N":
            continue
        textos.append((f"{f[1]} {f[3]}", 2 if marca == "S" else 1))
    log(f"   📚 Normas de los últimos {dias} días usadas como contexto: {len(textos)}")
    return temas_desde_normas(textos)


def leer_feedback_noticias(servicio, spreadsheet_id):
    """Titulares que marcaste S y N en la hoja Noticias (columna ENVIAR)."""
    resp = servicio.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{HOJA_NOTICIAS}'!A2:{ULTIMA_COL}").execute()
    i_tit, i_env = CAMPOS.index("titular"), CAMPOS.index("enviar")
    pos, neg = [], []
    for f in resp.get("values", []):
        f = list(f) + [""] * (len(CAMPOS) - len(f))
        marca = f[i_env].strip().upper()
        if f[i_tit].strip() and marca == "S":
            pos.append(f[i_tit].strip())
        elif f[i_tit].strip() and marca == "N":
            neg.append(f[i_tit].strip())
    return pos, neg


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
        sin_fecha = " ⚠️ <i>fecha sin verificar</i>" if not n.get("fecha_verificada", True) else ""
        msg += f"{i}. <b>{_h(n['titular'])}</b>{fuente}{sin_fecha}\n{resumen}{otras}{_enlace_tg(n['enlace'])}\n\n"
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
def procesar(crudas, enlaces_existentes=(), titulares_existentes=()):
    """Pipeline de 2 etapas. La fecha REAL se verifica ANTES de ordenar/limitar (antes se recortaba a 15
    primero y las viejas descartadas después dejaban hueco, o pasaban si la página no tenía fecha)."""
    log("\n🔬 Etapa 1: filtro por titular (laxo)...")
    candidatas = filtrar_pre(crudas)
    nuevas = [n for n in candidatas if not any(misma_noticia(n["titular"], t) for t in titulares_existentes)]
    log(f"\n   Pasan la etapa 1: {len(candidatas)} de {len(crudas)} | ya estaban en la hoja (7 días, por titular): "
        f"{len(candidatas) - len(nuevas)}")

    unicas = elegir_unicas(nuevas)
    unicas.sort(key=lambda n: n.get("pre", 0), reverse=True)
    if MAX_ENRIQUECER and len(unicas) > MAX_ENRIQUECER:
        log(f"   ⏱️  Se verifican las {MAX_ENRIQUECER} de mayor puntaje (de {len(unicas)})")
        unicas = unicas[:MAX_ENRIQUECER]
    enriquecer_todas(unicas)

    log("\n🔬 Etapa 2: titular + resumen + sección/URL (estricto), fecha real y enlace...")
    finales, enlaces = [], set(enlaces_existentes)
    for n in unicas:
        if n.get("obsoleta"):
            log(f"   🕰️  Descartada por fecha real {n['fecha_real']} (Google la mostró como reciente): {n['titular'][:60]}")
            continue
        if n["enlace"] in enlaces:       # ahora sí se compara el enlace REAL (antes era el redirect de Google)
            log(f"   ♻️  Ya estaba en la hoja o repetida: {n['titular'][:60]}")
            continue
        pts, motivos, ok, gris = puntuar_noticia(
            n["titular"], n.get("fuente", ""), n.get("fuente_url", ""), contexto=n.get("resumen", ""),
            seccion=n.get("seccion", ""), url=n["enlace"], etapa="final")
        razon = f"{pts:.1f} pts [{', '.join(x for x in motivos if x) or 'sin señales'}]"
        n["puntaje"] = razon
        log(f"   {'✅' if ok else '❌'} {razon[:78]:<78} | {n['titular'][:80]}")
        if ok:
            enlaces.add(n["enlace"])
            finales.append(n)
        elif gris:
            GRISES.append(n)
    return ordenar_y_limitar(finales)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="no escribe en Sheets ni envía Telegram")
    args = ap.parse_args()

    log(f"\n📰 BÚSQUEDA DE NOTICIAS — hoy {HOY.strftime('%a %d/%m')}, desde {FECHA_DESDE.strftime('%a %d/%m')} (when:{VENTANA})")
    log(f"   Techo de seguridad: {MAX_NOTICIAS or 'sin techo'} | electricidad: {'sí' if INCLUIR_ELECTRICIDAD else 'no'}")

    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    creds = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not args.dry_run and (not spreadsheet_id or not creds):
        raise RuntimeError("Falta SPREADSHEET_ID o GOOGLE_CREDENTIALS_JSON")
    servicio = conectar(creds) if (spreadsheet_id and creds) else None

    queries = list(QUERIES) + (QUERIES_ELECTRICIDAD if INCLUIR_ELECTRICIDAD else [])
    enlaces, titulares = set(), []
    if servicio:
        if not args.dry_run:
            asegurar_hoja(servicio, spreadsheet_id)
        # Contexto (nunca debe frenar la corrida: si algo falla se sigue sin él)
        try:
            enlaces, titulares = leer_existentes(servicio, spreadsheet_id)
        except Exception as e:
            log(f"   ⚠️  No se pudo leer la hoja Noticias: {e}")
        try:
            temas = leer_temas_normas(servicio, spreadsheet_id)
            cargar_temas_normas(temas)
            if temas:
                log("   🧭 Temas recurrentes en tus normas: " + ", ".join(f"{t} ({c})" for t, c in temas))
                base = normalizar_texto(" ".join(queries))
                queries += [f'"{t}" Perú' for t, _ in temas[:4] if t not in base]
        except Exception as e:
            log(f"   ⚠️  No se pudo leer el contexto de normas: {e}")
        try:
            pos, neg = leer_feedback_noticias(servicio, spreadsheet_id)
            FEEDBACK.entrenar(pos, neg)
            log(f"   🗳️  Feedback en Noticias: {len(pos)} S / {len(neg)} N → "
                f"{'activo' if FEEDBACK.activo else f'inactivo (necesita >= {FEEDBACK.MIN_ETIQUETAS} de cada uno)'}")
        except Exception as e:
            log(f"   ⚠️  No se pudo leer tu feedback de Noticias: {e}")

    crudas = deduplicar(buscar_noticias(queries))
    log(f"   Únicas tras deduplicar: {len(crudas)}")
    nuevas = procesar(crudas, enlaces, titulares)
    log(f"\n   Noticias a guardar: {len(nuevas)}")

    if args.dry_run:
        log("\n[dry-run] Mensaje de Telegram que se enviaría:\n")
        log(mensaje_telegram(nuevas))
        return
    if nuevas:
        guardar(servicio, spreadsheet_id, nuevas)
    enviar_telegram_html(mensaje_telegram(nuevas), os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID"))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"\n❌ ERROR: {e}")
        sys.exit(1)
