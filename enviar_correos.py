"""
=============================================================================
ENVÍO DE BOLETÍN POR CORREO
  - Contactos : hoja "Contactos"  (columna ACTIVO = S/N)
  - Normas    : la hoja donde el buscador guarda las normas (columna G = S/N)
  - Noticias  : hoja "Noticias"   (columna A = S/N)
Se envían las filas con S que aún no tengan fecha en la columna ENVIADO.
Tras un envío real, el script escribe la fecha en ENVIADO para no repetirlas.
=============================================================================
"""

import os
import re
import ssl
import sys
import json
import time
import base64
import smtplib
import argparse
import html as htmllib
from urllib.parse import urlparse
from datetime import datetime
from email.utils import formataddr, make_msgid
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

from google.oauth2 import service_account
from googleapiclient.discovery import build

try:
    from zoneinfo import ZoneInfo
    ZONA = ZoneInfo("America/Lima")
except Exception:          # si no hay zoneinfo, se usa la hora del servidor
    ZONA = None

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

HOJA_CONTACTOS = "Contactos"
RANGO_LECTURA = f"{HOJA_CONTACTOS}!A1:J"
COLUMNA_ULTIMO_ENVIO = "J"

# A: N° | B: EMPRESA | C: RUC | D: TIPO | E: CONTACTO | F: CARGO
# G: TELÉFONO | H: CORREO ELECTRÓNICO | I: ACTIVO | J: ULTIMOENVIO
COL_N = 0
COL_EMPRESA = 1
COL_RUC = 2
COL_TIPO = 3
COL_CONTACTO = 4
COL_CARGO = 5
COL_TELEFONO = 6
COL_CORREO = 7
COL_ACTIVO = 8
NUM_COLUMNAS = 10

# --- Normas: la MISMA hoja donde escribe el buscador de normas (primera pestaña).
# A: Fecha captura | B: Título | C: Fecha publicación | D: Sumilla | E: Link
# F: Tipo edición  | G: Relevante (S/N) | H: ENVIADO (lo escribe este script)
HOJA_NORMAS = ""      # "" = primera pestaña. Si tiene otro nombre, escríbelo aquí.
CAMPOS_NORMAS = ["captura", "norma", "fecha_pub", "resumen", "enlace",
                 "tipo", "enviar", "enviado"]

# --- Noticias (pestaña nueva): A: ENVIAR | B: TITULAR | C: RESUMEN | D: ENLACE
# E: FUENTE (opcional) | F: ENVIADO (lo escribe este script)
HOJA_NOTICIAS = "Noticias"
CAMPOS_NOTICIAS = ["enviar", "titular", "resumen", "enlace", "fuente", "enviado"]

VALORES_SI = ("S", "SI", "SÍ", "Y", "YES", "TRUE", "1")

SMTP_HOST_DEFAULT = "smtp.titan.email"
SMTP_PORT_DEFAULT = 465
PAUSA_ENTRE_ENVIOS = 2.0
REINTENTOS = 2
LIMITE_GMAIL_KB = 102

REGEX_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
SEPARADORES = re.compile(r"[,;\n\r/]+")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

MESES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
         "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

COLOR_PRINCIPAL = "#173e3c"
COLOR_CLARO = "#d5ead9"


def log(msg=""):
    print(msg, flush=True)


# =============================================================================
# GOOGLE SHEETS
# =============================================================================

def conectar_sheets(credentials_raw):
    """Acepta el JSON de credenciales en base64 o en texto plano."""
    log("\n🔐 Conectando con Google Sheets...")
    if not credentials_raw:
        raise RuntimeError("Falta la variable de entorno GOOGLE_CREDENTIALS_JSON")

    texto = credentials_raw.strip()
    try:
        if texto.startswith("{"):
            info = json.loads(texto)
        else:
            info = json.loads(base64.b64decode(texto))
    except Exception as e:
        raise RuntimeError(f"GOOGLE_CREDENTIALS_JSON no es un JSON válido: {e}")

    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    servicio = build("sheets", "v4", credentials=creds, cache_discovery=False)
    log(f"   ✅ Conectado como: {info.get('client_email', 'desconocido')}")
    return servicio


def leer_contactos(servicio, spreadsheet_id):
    """
    Devuelve una lista de dicts con los destinatarios activos (Activo = S).
    Una celda con varios correos genera varias entradas apuntando a la misma fila.
    """
    log("\n📇 Leyendo hoja 'Contactos'...")
    try:
        resp = servicio.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=RANGO_LECTURA
        ).execute()
    except Exception as e:
        raise RuntimeError(
            f"No se pudo leer la hoja '{HOJA_CONTACTOS}': {e}\n"
            "   Revisa que la pestaña se llame exactamente 'Contactos' y que la "
            "service account esté compartida como Editor en el Sheet."
        )

    filas = resp.get("values", [])[1:]
    log(f"   📄 Filas con datos: {len(filas)}")

    destinatarios = []
    vistos = set()
    inactivos = sin_email = invalidos = duplicados = 0

    for i, fila in enumerate(filas, start=2):
        fila = list(fila) + [""] * (NUM_COLUMNAS - len(fila))
        empresa = (fila[COL_EMPRESA] or "").strip()
        ruc = (fila[COL_RUC] or "").strip()
        tipo = (fila[COL_TIPO] or "").strip()
        contacto = (fila[COL_CONTACTO] or "").strip()
        cargo = (fila[COL_CARGO] or "").strip()
        telefono = (fila[COL_TELEFONO] or "").strip()
        celda_email = (fila[COL_CORREO] or "").strip()
        activo = (fila[COL_ACTIVO] or "").strip().upper()
        nombre = contacto or empresa

        if activo not in VALORES_SI:
            inactivos += 1
            continue

        if not celda_email:
            log(f"   ⚠️  Fila {i} ({nombre or 'sin nombre'}): activa pero sin correo, se omite")
            sin_email += 1
            continue

        for bruto in SEPARADORES.split(celda_email):
            email = bruto.strip().strip("<>").strip().strip('"').strip()
            if not email:
                continue
            if not REGEX_EMAIL.match(email):
                log(f"   ⚠️  Fila {i}: correo inválido descartado → {email}")
                invalidos += 1
                continue
            clave = email.lower()
            if clave in vistos:
                duplicados += 1
                continue
            vistos.add(clave)
            destinatarios.append({
                "nombre": nombre, "empresa": empresa, "ruc": ruc, "tipo": tipo,
                "contacto": contacto, "cargo": cargo, "telefono": telefono,
                "email": email, "fila": i,
            })

    log(f"   ✅ Destinatarios válidos: {len(destinatarios)}")
    log(f"   ⏸️  Inactivos (Activo≠S): {inactivos}")
    log(f"   ␀  Activos sin correo:   {sin_email}")
    log(f"   ❌ Correos inválidos:    {invalidos}")
    log(f"   ♻️  Duplicados omitidos:  {duplicados}")
    return destinatarios


def _rango(hoja, celdas):
    return f"'{hoja}'!{celdas}" if hoja else celdas


def leer_seleccion(servicio, spreadsheet_id, hoja, campos, principal, etiqueta, obligatorio=True):
    """
    Devuelve las filas con ENVIAR = S que todavía no tienen fecha en ENVIADO.
    Cada elemento es un dict con las claves de `campos` + 'fila'.
    """
    nombre = hoja or "primera pestaña (normas)"
    log(f"\n📰 Leyendo {nombre}...")
    ultima_col = chr(ord("A") + len(campos) - 1)
    try:
        resp = servicio.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=_rango(hoja, f"A1:{ultima_col}")
        ).execute()
    except Exception as e:
        msg = (f"No se pudo leer {nombre}: {e}\n"
               f"   Revisa que exista y tenga las columnas: "
               f"{' | '.join(c.upper() for c in campos)}")
        if obligatorio:
            raise RuntimeError(msg)
        log(f"   ⚠️  {msg}\n   Se continúa sin {etiqueta}.")
        return []

    filas = resp.get("values", [])[1:]
    elegidas = []
    ya_enviadas = 0
    for i, fila in enumerate(filas, start=2):
        fila = list(fila) + [""] * (len(campos) - len(fila))
        item = {c: (fila[k] or "").strip() for k, c in enumerate(campos)}
        if item["enviar"].upper() not in VALORES_SI:
            continue
        if item["enviado"]:
            ya_enviadas += 1
            continue
        if not item[principal]:
            log(f"   ⚠️  Fila {i}: marcada con S pero sin '{principal}', se omite")
            continue
        item["fila"] = i
        elegidas.append(item)

    log(f"   ✅ {etiqueta.capitalize()} por enviar (S y sin fecha en ENVIADO): {len(elegidas)}")
    if ya_enviadas:
        log(f"   ♻️  Con S pero ya enviadas antes: {ya_enviadas} (no se repiten)")
    return elegidas


def marcar_enviado(servicio, spreadsheet_id, hoja, campos, filas, sello):
    """Escribe la fecha en la columna ENVIADO de las filas incluidas en el boletín."""
    if not filas:
        return
    col = chr(ord("A") + campos.index("enviado"))
    data = [{"range": _rango(hoja, f"{col}{n}:{col}{n}"), "values": [[sello]]}
            for n in sorted(set(filas))]
    try:
        servicio.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data}
        ).execute()
        log(f"   ✅ {len(data)} filas marcadas como ENVIADO en {hoja or 'hoja de normas'}")
    except Exception as e:
        log(f"   ⚠️  No se pudo marcar ENVIADO en {hoja or 'hoja de normas'}: {e}")


def marcar_ultimo_envio(servicio, spreadsheet_id, filas, sello):
    """Escribe la fecha/hora de envío en la columna J de cada fila enviada."""
    if not filas:
        return
    log("\n🕒 Actualizando columna UltimoEnvio...")
    data = [
        {"range": f"{HOJA_CONTACTOS}!{COLUMNA_ULTIMO_ENVIO}{n}:{COLUMNA_ULTIMO_ENVIO}{n}",
         "values": [[sello]]}
        for n in sorted(set(filas))
    ]
    try:
        servicio.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data}
        ).execute()
        log(f"   ✅ {len(data)} filas actualizadas")
    except Exception as e:
        log(f"   ⚠️  No se pudo actualizar UltimoEnvio: {e}")
        log("      (los correos SÍ se enviaron; revisa permisos de Editor)")


# =============================================================================
# ARMADO DEL BOLETÍN (normas + noticias)
# =============================================================================

def _e(texto):
    """Escapa HTML y respeta los saltos de línea de la celda."""
    return htmllib.escape(texto or "", quote=True).replace("\r\n", "\n").replace("\n", "<br>")


def _enlace_valido(url):
    return url if re.match(r"^https?://", url or "", re.I) else ""


def html_normas(normas):
    if not normas:
        return ('  <tr><td style="padding:8px 24px 16px 24px; font-size:14px; color:#666666;">'
                'No hay normas seleccionadas para este boletín.</td></tr>')
    bloques = []
    for n in normas:
        url = _enlace_valido(n["enlace"])
        boton = (f'<a href="{_e(url)}" target="_blank" style="font-size:13px; color:{COLOR_PRINCIPAL}; '
                 f'font-weight:bold; text-decoration:underline;">Ver norma &rarr;</a>') if url else ""
        etiquetas = []
        if n["fecha_pub"]:
            etiquetas.append(n["fecha_pub"])
        if n["tipo"].lower().startswith("extra"):
            etiquetas.append("Edición extraordinaria")
        cabecera = (f'<p style="margin:0 0 4px 0; font-size:11px; letter-spacing:0.5px; '
                    f'text-transform:uppercase; color:#6b7f7d;">{_e(" · ".join(etiquetas))}</p>') if etiquetas else ""
        # El buscador copia el título como sumilla cuando no hay sumilla: no repetirlo
        resumen = ""
        if n["resumen"] and n["resumen"].strip().lower() != n["norma"].strip().lower():
            resumen = (f'<p style="margin:0 0 10px 0; font-size:14px; color:#444444; line-height:1.5;">'
                       f'{_e(n["resumen"])}</p>')
        bloques.append(f"""  <tr>
    <td style="padding:0 24px 14px 24px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e7e7e7;">
        <tr>
          <td style="padding:14px 16px;">
            {cabecera}
            <p style="margin:0 0 8px 0; font-size:15px; font-weight:bold; color:#1a1a1a; line-height:1.4;">{_e(n["norma"])}</p>
            {resumen}
            {boton}
          </td>
        </tr>
      </table>
    </td>
  </tr>""")
    return "\n".join(bloques)


def html_noticias(noticias):
    """
    Sección 'Noticia del sector': etiqueta verde + tarjeta con acento de color y botón.
    Solo usa tablas y estilos en línea (compatible con Gmail y Outlook).
    Si no hay noticias marcadas con S, no se muestra nada.
    """
    if not noticias:
        return ""

    bloques = [f"""  <!-- Título de sección: Noticia del sector -->
  <tr>
    <td style="padding:18px 24px 14px 24px;">
      <table role="presentation" cellpadding="0" cellspacing="0" width="100%">
        <tr>
          <td width="6" style="background-color:{COLOR_PRINCIPAL}; font-size:1px; line-height:1px;">&nbsp;</td>
          <td style="padding:2px 0 2px 12px;">
            <p style="margin:0; font-size:11px; letter-spacing:1px; text-transform:uppercase; color:#6b7f7d; font-weight:bold;">Actualidad</p>
            <h2 style="margin:2px 0 0 0; font-size:18px; color:#1a1a1a;">Noticia del sector</h2>
          </td>
        </tr>
      </table>
    </td>
  </tr>"""]

    for n in noticias:
        url = _enlace_valido(n["enlace"])
        nombre_fuente = n["fuente"] or re.sub(r"^www\.", "", urlparse(url).netloc)

        fuente = (f'<span style="display:inline-block; background-color:{COLOR_CLARO}; color:{COLOR_PRINCIPAL}; '
                  f'font-size:11px; font-weight:bold; letter-spacing:0.5px; text-transform:uppercase; '
                  f'padding:4px 10px;">{_e(nombre_fuente)}</span>') if nombre_fuente else ""

        resumen = (f'<p style="margin:0 0 18px 0; font-size:14px; color:#444444; line-height:1.6;">'
                   f'{_e(n["resumen"])}</p>') if n["resumen"] else ""

        boton = (f"""<table role="presentation" cellpadding="0" cellspacing="0">
              <tr>
                <td align="center" bgcolor="{COLOR_PRINCIPAL}" style="background-color:{COLOR_PRINCIPAL};">
                  <a href="{_e(url)}" target="_blank" style="display:inline-block; padding:10px 22px; font-size:14px; color:#ffffff; text-decoration:none; font-weight:bold;">
                    Leer noticia completa &rarr;
                  </a>
                </td>
              </tr>
            </table>""") if url else ""

        bloques.append(f"""  <tr>
    <td style="padding:0 24px 20px 24px;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#f1f8f3; border:1px solid {COLOR_CLARO}; border-top:4px solid {COLOR_PRINCIPAL};">
        <tr>
          <td style="padding:20px 20px 22px 20px;">
            <p style="margin:0 0 12px 0;">{fuente}</p>
            <h3 style="margin:0 0 10px 0; font-size:17px; line-height:1.4; color:#1a1a1a;">{_e(n["titular"])}</h3>
            {resumen}
            {boton}
          </td>
        </tr>
      </table>
    </td>
  </tr>""")
    return "\n".join(bloques)


def fecha_larga(dt):
    return f"{dt.day} de {MESES[dt.month - 1]} de {dt.year}"


def armar_html(plantilla, normas, noticias, fecha_dt):
    """Rellena los marcadores de la plantilla. Si no hay marcadores, la devuelve igual."""
    out = plantilla
    out = out.replace("{{FECHA}}", fecha_larga(fecha_dt))
    out = out.replace("{{FECHA_YYYYMMDD}}", fecha_dt.strftime("%Y%m%d"))
    out = out.replace("{{NORMAS}}", html_normas(normas))
    out = out.replace("{{NOTICIAS}}", html_noticias(noticias))
    return out


# =============================================================================
# PREPARACIÓN DEL HTML
# =============================================================================

def extraer_imagenes_base64(html):
    """
    Reemplaza cada src="data:image/...;base64,XXXX" por src="cid:imagenN"
    y devuelve (html_limpio, [(cid, bytes, subtipo), ...]).
    """
    patron = re.compile(r'src=(["\'])data:image/([A-Za-z0-9.+-]+);base64,([^"\']+)\1')
    imagenes = []

    def reemplazar(m):
        subtipo = m.group(2).lower()
        datos = m.group(3)
        idx = len(imagenes) + 1
        cid = make_msgid(idstring=f"img{idx}")[1:-1]
        try:
            binario = base64.b64decode(datos)
        except Exception:
            return m.group(0)
        imagenes.append((cid, binario, subtipo))
        return f'src="cid:{cid}"'

    html_limpio = patron.sub(reemplazar, html)
    return html_limpio, imagenes


def html_a_texto(html):
    """Versión de respaldo en texto plano, para clientes que no muestran HTML."""
    texto = re.sub(r"(?is)<(script|style).*?</\1>", " ", html)
    texto = re.sub(r"(?i)<br\s*/?>", "\n", texto)
    texto = re.sub(r"(?i)</(p|tr|div|h[1-6]|li)>", "\n", texto)
    texto = re.sub(r"<[^>]+>", " ", texto)
    texto = htmllib.unescape(texto)
    texto = re.sub(r"[ \t]+", " ", texto)
    texto = re.sub(r"\n\s*\n\s*\n+", "\n\n", texto)
    return texto.strip()


def construir_mensaje(remitente, nombre_remitente, destino, asunto,
                      html, texto_plano, imagenes):
    """Arma un multipart/related con alternativa de texto plano e imágenes inline."""
    raiz = MIMEMultipart("related")
    raiz["Subject"] = asunto
    raiz["From"] = formataddr((nombre_remitente, remitente))
    raiz["To"] = destino

    alternativa = MIMEMultipart("alternative")
    raiz.attach(alternativa)
    alternativa.attach(MIMEText(texto_plano, "plain", "utf-8"))
    alternativa.attach(MIMEText(html, "html", "utf-8"))

    for cid, binario, subtipo in imagenes:
        img = MIMEImage(binario, _subtype=subtipo)
        img.add_header("Content-ID", f"<{cid}>")
        img.add_header("Content-Disposition", "inline", filename=f"{cid.split('@')[0]}.{subtipo}")
        raiz.attach(img)

    return raiz


# =============================================================================
# ENVÍO
# =============================================================================

def enviar_todo(destinatarios, asunto, html, imagenes, texto_plano,
                usuario, password, nombre_remitente, pausa, smtp_host, smtp_port):
    exitosos, fallidos, filas_ok = [], [], []
    contexto = ssl.create_default_context()

    log(f"\n📤 Iniciando envío a {len(destinatarios)} destinatarios...")
    log(f"   Servidor SMTP: {smtp_host}:{smtp_port}")
    try:
        servidor = smtplib.SMTP_SSL(smtp_host, smtp_port, context=contexto, timeout=60)
        servidor.login(usuario, password)
        log("   ✅ Autenticado correctamente")
    except smtplib.SMTPAuthenticationError:
        raise RuntimeError(
            f"El servidor {smtp_host} rechazó las credenciales.\n"
            "   • Revisa que EMAIL_USER sea el correo completo (usuario@tudominio.com).\n"
            "   • Revisa que EMAIL_PASSWORD sea correcta (para Gmail debe ser una "
            "contraseña de aplicación de 16 caracteres, no la normal).\n"
            "   • Pégala sin espacios."
        )

    try:
        for i, d in enumerate(destinatarios, 1):
            mensaje = construir_mensaje(usuario, nombre_remitente, d["email"],
                                        asunto, html, texto_plano, imagenes)
            enviado = False
            for intento in range(1, REINTENTOS + 2):
                try:
                    servidor.sendmail(usuario, [d["email"]], mensaje.as_string())
                    enviado = True
                    break
                except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError):
                    log(f"      ↻ Reconectando (intento {intento})...")
                    servidor = smtplib.SMTP_SSL(smtp_host, smtp_port, context=contexto, timeout=60)
                    servidor.login(usuario, password)
                except smtplib.SMTPRecipientsRefused:
                    break
                except Exception as e:
                    log(f"      ⚠️  Intento {intento} falló: {e}")
                    time.sleep(3)

            if enviado:
                log(f"   [{i}/{len(destinatarios)}] ✅ {d['email']}  ({d['nombre'] or 's/n'})")
                exitosos.append(d["email"])
                filas_ok.append(d["fila"])
            else:
                log(f"   [{i}/{len(destinatarios)}] ❌ {d['email']}  ({d['nombre'] or 's/n'})")
                fallidos.append(d["email"])

            if i < len(destinatarios):
                time.sleep(pausa)
    finally:
        try:
            servidor.quit()
        except Exception:
            pass

    return exitosos, fallidos, filas_ok


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Envía un boletín HTML a los contactos del Sheet")
    parser.add_argument("--html", required=True, help="Ruta a la plantilla HTML")
    parser.add_argument("--asunto", required=True, help="Asunto del correo")
    parser.add_argument("--dry-run", action="store_true", help="No envía nada, solo simula")
    parser.add_argument("--remitente", default="", help="Nombre visible del remitente")
    parser.add_argument("--pausa", type=float, default=PAUSA_ENTRE_ENVIOS,
                        help="Segundos entre envíos")
    parser.add_argument("--limite", type=int, default=0,
                        help="Enviar solo a los primeros N (0 = todos)")
    parser.add_argument("--fecha", default="",
                        help="Fecha del boletín AAAA-MM-DD (por defecto: hoy en Lima)")
    args = parser.parse_args()

    log("=" * 78)
    log("📧 ENVÍO DE BOLETÍN")
    log("=" * 78)
    log(f"   Archivo : {args.html}")
    log(f"   Asunto  : {args.asunto}")
    log(f"   Modo    : {'🧪 PRUEBA (dry-run, no se envía nada)' if args.dry_run else '🚀 ENVÍO REAL'}")
    log(f"   SMTP    : {os.getenv('SMTP_HOST') or SMTP_HOST_DEFAULT}:{os.getenv('SMTP_PORT') or SMTP_PORT_DEFAULT}")

    credenciales = os.getenv("GOOGLE_CREDENTIALS_JSON")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    usuario = (os.getenv("EMAIL_USER") or os.getenv("GMAIL_USER") or "").strip()
    password = (os.getenv("EMAIL_PASSWORD") or os.getenv("GMAIL_APP_PASSWORD") or "").replace(" ", "")
    smtp_host = (os.getenv("SMTP_HOST") or SMTP_HOST_DEFAULT).strip()
    smtp_port = int(os.getenv("SMTP_PORT") or SMTP_PORT_DEFAULT)

    faltantes = [n for n, v in [("SPREADSHEET_ID", spreadsheet_id),
                                ("EMAIL_USER", usuario)] if not v]
    if not args.dry_run and not password:
        faltantes.append("EMAIL_PASSWORD")
    if faltantes:
        log(f"\n❌ Faltan variables de entorno: {', '.join(faltantes)}")
        sys.exit(1)

    # --- 1. Leer la plantilla ---
    if not os.path.exists(args.html):
        log(f"\n❌ No existe el archivo: {args.html}")
        log(f"   Archivos HTML en el repo: {[f for f in os.listdir('.') if f.endswith('.html')] or 'ninguno'}")
        sys.exit(1)

    with open(args.html, "r", encoding="utf-8") as f:
        plantilla = f.read()

    if args.fecha:
        try:
            fecha_dt = datetime.strptime(args.fecha, "%Y-%m-%d")
        except ValueError:
            log("\n❌ --fecha debe tener el formato AAAA-MM-DD (ej. 2026-09-24)")
            sys.exit(1)
    else:
        fecha_dt = datetime.now(ZONA) if ZONA else datetime.now()

    # --- 2. Conectar y leer normas / noticias / contactos ---
    servicio = conectar_sheets(credenciales)

    normas, noticias = [], []
    if "{{NORMAS}}" in plantilla or "{{NOTICIAS}}" in plantilla:
        normas = leer_seleccion(servicio, spreadsheet_id, HOJA_NORMAS,
                                CAMPOS_NORMAS, "norma", "normas", obligatorio=True)
        noticias = leer_seleccion(servicio, spreadsheet_id, HOJA_NOTICIAS,
                                  CAMPOS_NOTICIAS, "titular", "noticias", obligatorio=False)
        if not normas and not noticias:
            log("\n⚠️  No hay ninguna norma ni noticia marcada con S. Nada que enviar.")
            sys.exit(0)
        html_original = armar_html(plantilla, normas, noticias, fecha_dt)
    else:
        log("\nℹ️  La plantilla no tiene {{NORMAS}} ni {{NOTICIAS}}; se envía tal cual.")
        html_original = plantilla

    # --- 3. Preparar HTML (imágenes inline, texto plano) ---
    kb_original = len(html_original.encode("utf-8")) / 1024
    html, imagenes = extraer_imagenes_base64(html_original)
    kb_final = len(html.encode("utf-8")) / 1024
    texto_plano = html_a_texto(html)

    log(f"\n🖼️  Procesando HTML...")
    log(f"   Tamaño original : {kb_original:.1f} KB")
    if imagenes:
        log(f"   Imágenes base64 convertidas a adjuntos inline (cid:): {len(imagenes)}")
        log(f"   Cuerpo HTML final: {kb_final:.1f} KB  ✅ (Gmail recorta sobre {LIMITE_GMAIL_KB} KB)")
    elif kb_final > LIMITE_GMAIL_KB:
        log(f"   ⚠️  {kb_final:.1f} KB — Gmail puede recortar este correo")

    destinatarios = leer_contactos(servicio, spreadsheet_id)

    if args.limite > 0:
        destinatarios = destinatarios[:args.limite]
        log(f"   ✂️  Limitado a los primeros {len(destinatarios)}")

    if not destinatarios:
        log("\n⚠️  No hay destinatarios activos con correo válido. Nada que enviar.")
        sys.exit(0)

    # --- 4. Dry run ---
    if args.dry_run:
        log("\n" + "=" * 78)
        log("🧪 MODO PRUEBA — contenido del boletín:")
        log("=" * 78)
        log(f"   Normas a enviar ({len(normas)}):")
        for n in normas:
            log(f"      • {n['norma'][:80]}")
        log(f"   Noticias a enviar ({len(noticias)}):")
        for n in noticias:
            log(f"      • {n['titular'][:80]}")
        vista = "vista_previa_boletin.html"
        with open(vista, "w", encoding="utf-8") as f:
            f.write(html_original)
        log(f"   👁️  Vista previa guardada en: {vista}")

        log("\n" + "=" * 78)
        log("🧪 Destinatarios:")
        log("=" * 78)
        for i, d in enumerate(destinatarios, 1):
            log(f"   {i:>3}. {d['email']:<45} {d['nombre'][:28]:<28} (fila {d['fila']})")
        log("=" * 78)
        log(f"   TOTAL: {len(destinatarios)} correos")
        log(f"   Tiempo estimado de envío: ~{len(destinatarios) * args.pausa / 60:.1f} min")
        log("\n✅ Prueba completada. No se envió ningún correo.")
        log("   Para enviar de verdad, ejecuta con dry_run = false")
        return

    # --- 5. Envío real ---
    nombre_remitente = args.remitente or usuario.split("@")[0]
    exitosos, fallidos, filas_ok = enviar_todo(
        destinatarios, args.asunto, html, imagenes, texto_plano,
        usuario, password, nombre_remitente, args.pausa,
        smtp_host, smtp_port
    )

    # --- 6. Marcar en el Sheet ---
    sello = datetime.now().strftime("%Y-%m-%d %H:%M")
    marcar_ultimo_envio(servicio, spreadsheet_id, filas_ok, sello)
    if exitosos:
        log("\n✔️  Marcando normas y noticias como ENVIADO...")
        marcar_enviado(servicio, spreadsheet_id, HOJA_NORMAS, CAMPOS_NORMAS,
                       [n["fila"] for n in normas], sello)
        marcar_enviado(servicio, spreadsheet_id, HOJA_NOTICIAS, CAMPOS_NOTICIAS,
                       [n["fila"] for n in noticias], sello)

    # --- 7. Resumen ---
    log("\n" + "=" * 78)
    log("🎉 PROCESO COMPLETADO")
    log("=" * 78)
    log(f"   ✅ Enviados : {len(exitosos)}")
    log(f"   ❌ Fallidos : {len(fallidos)}")
    if fallidos:
        for e in fallidos:
            log(f"        • {e}")
    log("=" * 78)

    if fallidos and not exitosos:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        log(f"\n❌ {e}")
        sys.exit(1)
    except Exception as e:
        log(f"\n❌ ERROR INESPERADO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
