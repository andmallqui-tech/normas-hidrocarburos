#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
ENVÍO DE BOLETÍN POR CORREO — lee contactos desde Google Sheets (hoja "Contactos")
=============================================================================

Uso:
    python enviar_correos.py --html boletin.html --asunto "Normas del día" --dry-run
    python enviar_correos.py --html boletin.html --asunto "Normas del día"

Variables de entorno requeridas:
    GOOGLE_CREDENTIALS_JSON : JSON de la service account (en base64 o en texto plano)
    SPREADSHEET_ID          : ID del Google Sheet
    GMAIL_USER              : tu correo Gmail remitente
    GMAIL_APP_PASSWORD      : contraseña de aplicación de 16 caracteres (NO tu clave normal)

Hoja "Contactos", fila 1 = encabezados:
    A=Nombre | B=Email | C=Activo | D=FechaAgregado | E=Notas | F=UltimoEnvio

La columna B admite VARIOS correos en una sola celda, separados por coma,
punto y coma o salto de línea (Alt+Enter).
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
import mimetypes
from datetime import datetime
from email.utils import formataddr, make_msgid
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

from google.oauth2 import service_account
from googleapiclient.discovery import build

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

HOJA_CONTACTOS = "Contactos"
RANGO_LECTURA = f"{HOJA_CONTACTOS}!A2:F"
COLUMNA_ULTIMO_ENVIO = "F"

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 465
PAUSA_ENTRE_ENVIOS = 2.0      # segundos, para no gatillar el límite de Gmail
REINTENTOS = 2
LIMITE_GMAIL_KB = 102         # Gmail recorta por encima de esto

REGEX_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
SEPARADORES = re.compile(r"[,;\n\r/]+")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


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
    Devuelve una lista de dicts:
        {"nombre":..., "email":..., "fila": n}
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

    filas = resp.get("values", [])
    log(f"   📄 Filas con datos: {len(filas)}")

    destinatarios = []
    vistos = set()
    inactivos = sin_email = invalidos = duplicados = 0

    for i, fila in enumerate(filas, start=2):          # fila 2 = primera de datos
        fila = list(fila) + [""] * (6 - len(fila))     # rellenar columnas faltantes
        nombre = (fila[0] or "").strip()
        celda_email = (fila[1] or "").strip()
        activo = (fila[2] or "").strip().upper()

        if activo not in ("S", "SI", "SÍ", "Y", "YES", "TRUE", "1"):
            inactivos += 1
            continue

        if not celda_email:
            log(f"   ⚠️  Fila {i} ({nombre or 'sin nombre'}): activa pero sin correo, se omite")
            sin_email += 1
            continue

        for bruto in SEPARADORES.split(celda_email):
            email = bruto.strip().strip("<>").strip()
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
            destinatarios.append({"nombre": nombre, "email": email, "fila": i})

    log(f"   ✅ Destinatarios válidos: {len(destinatarios)}")
    log(f"   ⏸️  Inactivos (Activo≠S): {inactivos}")
    log(f"   ␀  Activos sin correo:   {sin_email}")
    log(f"   ❌ Correos inválidos:    {invalidos}")
    log(f"   ♻️  Duplicados omitidos:  {duplicados}")
    return destinatarios


def marcar_ultimo_envio(servicio, spreadsheet_id, filas, sello):
    """Escribe la fecha/hora de envío en la columna F de cada fila enviada."""
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
# PREPARACIÓN DEL HTML
# =============================================================================

def extraer_imagenes_base64(html):
    """
    Reemplaza cada src="data:image/...;base64,XXXX" por src="cid:imagenN"
    y devuelve (html_limpio, [(cid, bytes, subtipo), ...]).

    Motivo: Gmail NO muestra imágenes en data: URI, y un HTML de más de
    ~102 KB se recorta. Con cid: las imágenes viajan como adjuntos inline.
    """
    patron = re.compile(r'src=(["\'])data:image/([A-Za-z0-9.+-]+);base64,([^"\']+)\1')
    imagenes = []

    def reemplazar(m):
        subtipo = m.group(2).lower()
        datos = m.group(3)
        idx = len(imagenes) + 1
        cid = make_msgid(idstring=f"img{idx}")[1:-1]   # sin < >
        try:
            binario = base64.b64decode(datos)
        except Exception:
            return m.group(0)                          # si falla, dejar como estaba
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
    texto = (texto.replace("&nbsp;", " ").replace("&amp;", "&")
                  .replace("&lt;", "<").replace("&gt;", ">").replace("&#39;", "'"))
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
                usuario, password, nombre_remitente, pausa):
    exitosos, fallidos, filas_ok = [], [], []
    contexto = ssl.create_default_context()

    log(f"\n📤 Iniciando envío a {len(destinatarios)} destinatarios...")
    try:
        servidor = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=contexto, timeout=60)
        servidor.login(usuario, password)
        log("   ✅ Autenticado en Gmail SMTP")
    except smtplib.SMTPAuthenticationError:
        raise RuntimeError(
            "Gmail rechazó las credenciales.\n"
            "   • GMAIL_APP_PASSWORD debe ser una 'contraseña de aplicación' de 16 "
            "caracteres, no tu contraseña normal.\n"
            "   • Requiere verificación en 2 pasos activada en la cuenta.\n"
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
                    servidor = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=contexto, timeout=60)
                    servidor.login(usuario, password)
                except smtplib.SMTPRecipientsRefused:
                    break                      # correo rechazado: no reintentar
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
    parser.add_argument("--html", required=True, help="Ruta al archivo HTML a enviar")
    parser.add_argument("--asunto", required=True, help="Asunto del correo")
    parser.add_argument("--dry-run", action="store_true", help="No envía nada, solo simula")
    parser.add_argument("--remitente", default="", help="Nombre visible del remitente")
    parser.add_argument("--pausa", type=float, default=PAUSA_ENTRE_ENVIOS,
                        help="Segundos entre envíos")
    parser.add_argument("--limite", type=int, default=0,
                        help="Enviar solo a los primeros N (0 = todos)")
    args = parser.parse_args()

    log("=" * 78)
    log("📧 ENVÍO DE BOLETÍN")
    log("=" * 78)
    log(f"   Archivo : {args.html}")
    log(f"   Asunto  : {args.asunto}")
    log(f"   Modo    : {'🧪 PRUEBA (dry-run, no se envía nada)' if args.dry_run else '🚀 ENVÍO REAL'}")

    credenciales = os.getenv("GOOGLE_CREDENTIALS_JSON")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    usuario = (os.getenv("GMAIL_USER") or "").strip()
    password = (os.getenv("GMAIL_APP_PASSWORD") or "").replace(" ", "")

    faltantes = [n for n, v in [("SPREADSHEET_ID", spreadsheet_id),
                                ("GMAIL_USER", usuario)] if not v]
    if not args.dry_run and not password:
        faltantes.append("GMAIL_APP_PASSWORD")
    if faltantes:
        log(f"\n❌ Faltan variables de entorno: {', '.join(faltantes)}")
        sys.exit(1)

    # --- 1. Leer el HTML ---
    if not os.path.exists(args.html):
        log(f"\n❌ No existe el archivo: {args.html}")
        log(f"   Archivos HTML en el repo: {[f for f in os.listdir('.') if f.endswith('.html')] or 'ninguno'}")
        sys.exit(1)

    with open(args.html, "r", encoding="utf-8") as f:
        html_original = f.read()

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

    # --- 2. Leer contactos ---
    servicio = conectar_sheets(credenciales)
    destinatarios = leer_contactos(servicio, spreadsheet_id)

    if args.limite > 0:
        destinatarios = destinatarios[:args.limite]
        log(f"   ✂️  Limitado a los primeros {len(destinatarios)}")

    if not destinatarios:
        log("\n⚠️  No hay destinatarios activos con correo válido. Nada que enviar.")
        sys.exit(0)

    # --- 3. Dry run ---
    if args.dry_run:
        log("\n" + "=" * 78)
        log("🧪 MODO PRUEBA — estos serían los destinatarios:")
        log("=" * 78)
        for i, d in enumerate(destinatarios, 1):
            log(f"   {i:>3}. {d['email']:<45} {d['nombre'][:28]:<28} (fila {d['fila']})")
        log("=" * 78)
        log(f"   TOTAL: {len(destinatarios)} correos")
        log(f"   Tiempo estimado de envío: ~{len(destinatarios) * args.pausa / 60:.1f} min")
        log("\n✅ Prueba completada. No se envió ningún correo.")
        log("   Para enviar de verdad, ejecuta con dry_run = false")
        return

    # --- 4. Envío real ---
    nombre_remitente = args.remitente or usuario.split("@")[0]
    exitosos, fallidos, filas_ok = enviar_todo(
        destinatarios, args.asunto, html, imagenes, texto_plano,
        usuario, password, nombre_remitente, args.pausa
    )

    # --- 5. Marcar en el Sheet ---
    sello = datetime.now().strftime("%Y-%m-%d %H:%M")
    marcar_ultimo_envio(servicio, spreadsheet_id, filas_ok, sello)

    # --- 6. Resumen ---
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
