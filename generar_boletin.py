#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
GENERADOR DEL BOLETÍN — arma boletin_generado.html con las normas que el
sistema de normas guardó en Google Sheets (hoja de normas, columnas A:H).
=============================================================================

Uso:
    python generar_boletin.py                       # última corrida del scraper
    python generar_boletin.py --fecha 2026-09-21    # una corrida específica
    python generar_boletin.py --salida boletin_generado.html

Variables de entorno:
    GOOGLE_CREDENTIALS_JSON : JSON de la service account (base64 o texto plano)
    SPREADSHEET_ID          : ID del Google Sheet (el mismo del scraper)
    HOJA_NORMAS             : (opcional) nombre de la pestaña con las normas.
                              Si no se define, usa la PRIMERA pestaña del Sheet
                              (que es donde escribe el scraper con rango 'A:G').

Columnas de la hoja de normas (las escribe el scraper):
    A=Fecha de corrida (YYYY-MM-DD) | B=Título | C=FechaPublicación | D=Sumilla
    E=Link | F=Tipo edición | G=Relevante S/N | H=Sector (opcional)

Reglas:
    • Solo toma las filas cuya columna A coincide con la fecha elegida.
    • Si no se indica --fecha, usa la fecha MÁS RECIENTE que exista en la columna A
      (así no depende de zona horaria: el runner de GitHub está en UTC).
    • Descarta las filas marcadas con "N" en la columna G.
    • Si no hay normas, NO genera archivo y avisa al workflow (hay_normas=false).
"""

import os
import re
import sys
import json
import base64
import argparse
import unicodedata
from datetime import datetime, date
from html import escape

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
PLANTILLA_DEFECTO = "boletin_plantilla.html"
SALIDA_DEFECTO = "boletin_generado.html"

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "setiembre", "octubre", "noviembre", "diciembre"]

ORDEN_SECTORES = ["Energía y Minas", "Medio Ambiente", "Normas Técnicas", "Otras normas"]


def log(msg=""):
    print(msg, flush=True)


# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------
def normalizar(t):
    t = unicodedata.normalize("NFKD", (t or "").lower()).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9/\- ]", " ", t)).strip()


def fecha_larga(d):
    return f"{d.day} {MESES[d.month - 1]} {d.year}"


def parsear_fecha(txt):
    txt = (txt or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            pass
    return None


def clasificar_sector(titulo, sumilla, sector_hoja=""):
    """Etiqueta (badge) de cada norma. Si la hoja trae sector en col. H se usa tal cual."""
    if sector_hoja.strip():
        return sector_hoja.strip()
    t = normalizar(f"{titulo} {sumilla}")
    if any(k in t for k in ("oefa", "minam", "senace", "ambiental", "ambiente", "cambio climatico")):
        return "Medio Ambiente"
    if any(k in t for k in ("inacal", "norma tecnica peruana", "normas tecnicas peruanas")):
        return "Normas Técnicas"
    if (re.search(r"-em\b|/em\b|-minem|minem|os/cd|os/gg|osinergmin|perupetro", t)
            or any(k in t for k in ("hidrocarburo", "gas natural", "combustible", "electric",
                                    "petroleo", "glp", "gnv", "energia", "minera"))):
        return "Energía y Minas"
    return "Otras normas"


# -----------------------------------------------------------------------------
# Google Sheets
# -----------------------------------------------------------------------------
def conectar(credentials_raw):
    if not credentials_raw:
        raise RuntimeError("Falta la variable de entorno GOOGLE_CREDENTIALS_JSON")
    texto = credentials_raw.strip()
    info = json.loads(texto) if texto.startswith("{") else json.loads(base64.b64decode(texto))
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def nombre_hoja_normas(servicio, spreadsheet_id):
    nombre = (os.getenv("HOJA_NORMAS") or "").strip()
    if nombre:
        return nombre
    meta = servicio.spreadsheets().get(
        spreadsheetId=spreadsheet_id, fields="sheets.properties.title").execute()
    return meta["sheets"][0]["properties"]["title"]      # primera pestaña


def leer_filas(servicio, spreadsheet_id):
    hoja = nombre_hoja_normas(servicio, spreadsheet_id)
    log(f"📊 Leyendo normas de la pestaña '{hoja}'...")
    resp = servicio.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{hoja}'!A2:H").execute()
    return resp.get("values", [])


# -----------------------------------------------------------------------------
# Selección de normas
# -----------------------------------------------------------------------------
def seleccionar_normas(filas, fecha_pedida=None):
    """Devuelve (fecha_corrida, [normas]) según las reglas del docstring."""
    parseadas = []
    for f in filas:
        f = list(f) + [""] * (8 - len(f))
        d = parsear_fecha(f[0])
        if d:
            parseadas.append((d, f))
    if not parseadas:
        return None, []

    fecha = fecha_pedida or max(d for d, _ in parseadas)
    normas, vistos, descartadas_n = [], set(), 0

    for d, f in parseadas:
        if d != fecha:
            continue
        titulo, pub, sumilla, link, tipo, feedback, sector_h = (
            f[1].strip(), f[2].strip(), f[3].strip(), f[4].strip(),
            f[5].strip(), f[6].strip().upper(), f[7].strip())
        if not titulo:
            continue
        if feedback == "N":
            descartadas_n += 1
            continue
        clave = link or titulo.lower()
        if clave in vistos:
            continue
        vistos.add(clave)
        normas.append({
            "titulo": titulo, "sumilla": sumilla or titulo, "link": link,
            "extraordinaria": tipo.lower() == "extraordinaria",
            "sector": clasificar_sector(titulo, sumilla, sector_h),
        })

    if descartadas_n:
        log(f"   🚫 {descartadas_n} norma(s) marcadas 'N' en columna G, excluidas")

    def orden(n):
        try:
            return ORDEN_SECTORES.index(n["sector"])
        except ValueError:
            return len(ORDEN_SECTORES)
    normas.sort(key=orden)          # sort estable: conserva el orden original dentro de cada sector
    return fecha, normas


# -----------------------------------------------------------------------------
# HTML
# -----------------------------------------------------------------------------
BLOQUE_NORMA = """  <!-- Norma {n} -->
  <tr>
    <td style="padding:0 24px 20px 24px;">
      <span style="background-color:#173e3c; color:#ffffff; font-size:12px; font-weight:bold; letter-spacing:0.5px; text-transform:uppercase; padding:2px 10px; display:inline-block;">{sector}</span>
      <p style="margin:12px 0 4px 0; font-size:13px; color:#999999; font-weight:bold;">{titulo}{ext}</p>
      <p style="margin:0 0 8px 0; font-size:15px; color:#000000; font-weight:bold; line-height:1.4;">
        {sumilla}
      </p>{enlace}
    </td>
  </tr>
"""
ENLACE = """
      <a href="{link}" target="_blank" style="font-size:13px; color:#173e3c; text-decoration:none; font-weight:bold;">Ver disposición →</a>"""
SEPARADOR = """  <tr><td style="padding:0 24px;"><hr style="border:none; border-top:1px solid #e7e7e7; margin:0 0 20px 0;"></td></tr>
"""


def construir_bloques(normas):
    partes = []
    for i, n in enumerate(normas, 1):
        ext = (' <span style="font-weight:normal; text-transform:none;">· Edición extraordinaria</span>'
               if n["extraordinaria"] else "")
        enlace = ENLACE.format(link=escape(n["link"], quote=True)) if n["link"] else ""
        partes.append(BLOQUE_NORMA.format(
            n=i, sector=escape(n["sector"]), titulo=escape(n["titulo"].upper()),
            ext=ext, sumilla=escape(n["sumilla"]), enlace=enlace))
        if i < len(normas):
            partes.append(SEPARADOR)
    return "\n".join(partes)


def construir_html(plantilla, normas, fecha):
    html = plantilla.replace("{{NORMAS}}", construir_bloques(normas))
    html = html.replace("{{FECHA}}", fecha_larga(fecha))
    html = html.replace("{{FECHA_YYYYMMDD}}", fecha.strftime("%Y%m%d"))
    return html


def escribir_outputs(**kv):
    """Deja variables para los siguientes pasos del workflow de GitHub Actions."""
    ruta = os.getenv("GITHUB_OUTPUT")
    if ruta:
        with open(ruta, "a", encoding="utf-8") as fh:
            for k, v in kv.items():
                fh.write(f"{k}={v}\n")


# -----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Genera el boletín HTML desde el Sheet de normas")
    ap.add_argument("--fecha", default="", help="YYYY-MM-DD de la corrida (vacío = la más reciente)")
    ap.add_argument("--plantilla", default=PLANTILLA_DEFECTO)
    ap.add_argument("--salida", default=SALIDA_DEFECTO)
    args = ap.parse_args()

    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not spreadsheet_id:
        raise RuntimeError("Falta la variable de entorno SPREADSHEET_ID")
    if not os.path.exists(args.plantilla):
        raise RuntimeError(f"No existe la plantilla: {args.plantilla}")

    fecha_pedida = None
    if args.fecha.strip():
        fecha_pedida = parsear_fecha(args.fecha)
        if not fecha_pedida:
            raise RuntimeError(f"--fecha inválida: {args.fecha} (usa YYYY-MM-DD)")

    servicio = conectar(os.getenv("GOOGLE_CREDENTIALS_JSON"))
    filas = leer_filas(servicio, spreadsheet_id)
    fecha, normas = seleccionar_normas(filas, fecha_pedida)

    if not normas:
        log("⚠️  No hay normas para el boletín (hoja vacía, fecha sin filas o todas marcadas 'N').")
        escribir_outputs(hay_normas="false", cantidad="0")
        return

    with open(args.plantilla, encoding="utf-8") as fh:
        html = construir_html(fh.read(), normas, fecha)
    with open(args.salida, "w", encoding="utf-8") as fh:
        fh.write(html)

    log(f"✅ Boletín generado: {args.salida}  ({len(normas)} normas, corrida {fecha.isoformat()})")
    for n in normas:
        log(f"   • [{n['sector']}] {n['titulo']}")
    escribir_outputs(hay_normas="true", cantidad=str(len(normas)),
                     fecha_txt=fecha.strftime("%d/%m/%Y"))


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        log(f"\n❌ {e}")
        sys.exit(1)
