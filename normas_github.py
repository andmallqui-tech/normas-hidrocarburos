"""
=============================================================================
SISTEMA AUTOMATIZADO DE NORMAS - VERSIÓN FINAL CORREGIDA PARA GITHUB ACTIONS
=============================================================================
"""

import os
import re
import io
import json
import time
import base64
import unicodedata
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup
import pandas as pd

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

print("="*100)
print("🚀 SISTEMA DE NORMAS - VERSIÓN FINAL CORREGIDA PARA GITHUB")
print("="*100)

HOY = date.today()
DIA_SEMANA = HOY.weekday()

CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Lunes (0): revisa Viernes, Sábado y Domingo = 3 ediciones
# Otros días: revisa hoy y ayer = 2 ediciones
DIAS_A_REVISAR = 3 if DIA_SEMANA == 0 else 1

print(f"📅 HOY: {HOY.strftime('%d/%m/%Y')} - DÍA: {['Lun','Mar','Mié','Jue','Vie','Sáb','Dom'][DIA_SEMANA]}")
print(f"🔍 DÍAS A REVISAR: {DIAS_A_REVISAR}")
print("="*100)

# =============================================================================
# GOOGLE DRIVE CLIENT
# =============================================================================

class GoogleDriveClient:
    def __init__(self, credentials_json):
        print("\n🔐 INICIALIZANDO GOOGLE DRIVE CLIENT...")
        credentials_dict = json.loads(base64.b64decode(credentials_json))
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        )
        self.drive_service = build('drive', 'v3', credentials=credentials)
        self.sheets_service = build('sheets', 'v4', credentials=credentials)
        print("   ✅ Cliente inicializado correctamente")

    def get_file_by_name(self, folder_id, filename):
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = results.get('files', [])
            if files:
                print(f"   ✅ Archivo encontrado: {filename} (ID: {files[0]['id']})")
            else:
                print(f"   ℹ️ Archivo NO existe: {filename}")
            return files[0]['id'] if files else None
        except Exception as e:
            print(f"   ❌ Error buscando {filename}: {e}")
            return None

    def download_text_file(self, file_id):
        try:
            print(f"   ⬇️ Descargando archivo ID: {file_id}...")
            request = self.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            content = fh.getvalue().decode('utf-8')
            print(f"   ✅ Descargado: {len(content)} chars, {len(content.split())} palabras")
            return content
        except Exception as e:
            print(f"   ❌ Error descargando: {e}")
            return ""

    def upload_text_file(self, folder_id, filename, content):
        try:
            print(f"\n💾 SUBIENDO/ACTUALIZANDO: {filename}")
            print(f"   📊 Tamaño: {len(content)} chars, {len(content.split())} palabras")

            file_metadata = {
                'name': filename,
                'parents': [folder_id],
                'mimeType': 'text/plain'
            }
            media = MediaIoBaseUpload(
                io.BytesIO(content.encode('utf-8')),
                mimetype='text/plain',
                resumable=True
            )
            existing_id = self.get_file_by_name(folder_id, filename)

            if existing_id:
                self.drive_service.files().update(
                    fileId=existing_id,
                    media_body=media
                ).execute()
                print(f"   ✅ Corpus actualizado en Drive (ID: {existing_id})")
            else:
                file = self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                print(f"   ✅ Corpus creado en Drive (ID: {file.get('id')})")
            return True
        except Exception as e:
            print(f"   ❌ Error subiendo: {e}")
            return False

    def upload_pdf(self, folder_id, filename, pdf_bytes):
        try:
            print(f"\n📤 SUBIENDO PDF: {filename}")
            print(f"   📊 Tamaño: {len(pdf_bytes) / 1024:.2f} KB")

            file_metadata = {
                'name': filename,
                'parents': [folder_id],
                'mimeType': 'application/pdf'
            }
            media = MediaIoBaseUpload(
                io.BytesIO(pdf_bytes),
                mimetype='application/pdf',
                resumable=True
            )
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()

            link = file.get('webViewLink', '')
            print(f"   ✅ PDF subido exitosamente")
            print(f"   🔗 Link: {link}")
            return link
        except Exception as e:
            print(f"   ❌ Error subiendo PDF: {e}")
            import traceback
            traceback.print_exc()
            return None

    def create_folder(self, parent_id, folder_name):
        try:
            print(f"\n📁 CREANDO/BUSCANDO CARPETA: {folder_name}")
            existing_id = self.get_file_by_name(parent_id, folder_name)
            if existing_id:
                print(f"   ✅ Carpeta ya existe (ID: {existing_id})")
                return existing_id

            file_metadata = {
                'name': folder_name,
                'parents': [parent_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = self.drive_service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            folder_id = folder.get('id')
            print(f"   ✅ Carpeta creada (ID: {folder_id})")
            return folder_id
        except Exception as e:
            print(f"   ❌ Error creando carpeta: {e}")
            return None

    def append_to_sheet(self, spreadsheet_id, range_name, values):
        try:
            body = {'values': values}
            result = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            print(f"   ✅ {len(values)} filas agregadas a Sheets")
            return result
        except Exception as e:
            print(f"   ❌ Error en Sheets: {e}")
            return None

# =============================================================================
# TELEGRAM
# =============================================================================

def enviar_telegram(mensaje, bot_token, chat_id):
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        data = {'chat_id': chat_id, 'text': mensaje, 'parse_mode': 'HTML'}
        response = requests.post(url, data=data, timeout=10)
        response.raise_for_status()
        print("   ✅ Telegram enviado")
        return True
    except Exception as e:
        print(f"   ❌ Error Telegram: {e}")
        return False

# =============================================================================
# NORMALIZACIÓN
# =============================================================================

def normalizar_texto(texto):
    if not isinstance(texto, str):
        return ""
    texto = texto.lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    texto = re.sub(r'[^a-z0-9\s]', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

# =============================================================================
# KEYWORDS Y FILTROS
# =============================================================================

# Entidades del sector → aceptar SIEMPRE, sin importar keywords ni sector
ENTIDADES_SECTOR = set([normalizar_texto(x) for x in [
    'osinergmin',
    'perupetro',
    'minem',
    'ministerio de energia y minas',
    'energia y minas',
    'oefa',
    'organismo supervisor de la inversion en energia y mineria',
    'organismo de evaluacion y fiscalizacion ambiental'
]])

# Sectores en <h4> que son siempre relevantes
SECTORES_PRIORITARIOS = set([normalizar_texto(x) for x in [
    'energia y minas',
    'energia minas',
    'minem',
    'ministerio de energia y minas',
    'osinergmin',
    'organismo supervisor de la inversion en energia y mineria',
    'perupetro',
    'oefa'
]])

# Sectores que pueden tener normas relevantes → umbral más bajo
SECTORES_SECUNDARIOS = set([normalizar_texto(x) for x in [
    'decretos de urgencia',
    'decreto de urgencia',
    'presidencia del consejo de ministros',
    'pcm',
    'organismos tecnicos especializados',
    'organismo tecnico especializado',
    'organismos reguladores',
    'organismo regulador'
]])

# Sectores que nunca son relevantes → descartar siempre
SECTORES_EXCLUIR = set([normalizar_texto(x) for x in [
    'educacion', 'salud', 'defensa', 'interior', 'mujer',
    'desarrollo social', 'trabajo', 'migraciones', 'cultura',
    'vivienda', 'comunicaciones', 'justicia', 'relaciones exteriores', 'midis'
]])

# Palabras obligatorias ampliadas → al menos una debe aparecer para pasar al TF-IDF
PALABRAS_OBLIGATORIAS = set([normalizar_texto(x) for x in [
    'hidrocarburos', 'hidrocarburo', 'petroleo', 'gas natural',
    'perupetro', 'gnv', 'glp', 'oleoducto', 'gasoducto', 'refineria',
    'osinergmin', 'oefa', 'banda de precios', 'combustible',
    'combustibles liquidos', 'gasolina', 'diesel', 'kerosene',
    'lote petrolero', 'contrato de licencia', 'contrato de servicios',
    'canon gasifero', 'regalia', 'tarifa de distribucion',
    'tarifa de transporte', 'precio de gas', 'precio del gas',
    'electromovilidad', 'vehiculo electrico', 'estacion de carga',
    'biocombustible', 'biodiesel', 'etanol'
]])

KEYWORDS_MANUAL = [normalizar_texto(x) for x in [
    'hidrocarburos', 'hidrocarburo', 'petroleo', 'gas natural', 'gnv', 'glp',
    'perupetro', 'osinergmin', 'minem', 'oefa', 'refineria', 'oleoducto', 'gasoducto',
    'exploracion', 'explotacion', 'combustible', 'diesel', 'gasolina', 'kerosene',
    'canon gasifero', 'banda de precios', 'lote', 'pozo', 'yacimiento',
    'diesel b5', 'turbo', 'residual', 'bunker', 'upstream', 'downstream',
    'fraccionamiento', 'terminal', 'planta de gas', 'contrato de licencia',
    'regalia', 'concesion', 'electromovilidad', 'ductos', 'fijaron precios',
    'recursos energeticos', 'distribucion natural', 'tarifa', 'fiscalizacion',
    'supervision', 'licencia de operacion', 'registro de hidrocarburos',
    'contrato de servicios', 'lote petrolero', 'actividades de hidrocarburos',
    'instalaciones de gas', 'red de distribucion', 'vehiculo electrico',
    'estacion de carga', 'biocombustible', 'combustibles liquidos'
]]

tokens_tecnicos = set()
for kw in KEYWORDS_MANUAL:
    for token in kw.split():
        if len(token) > 2:
            tokens_tecnicos.add(token)

print(f"\n🧠 CONFIGURACIÓN DE FILTRADO:")
print(f"   Entidades sector (siempre aceptar): {len(ENTIDADES_SECTOR)}")
print(f"   Sectores prioritarios: {len(SECTORES_PRIORITARIOS)}")
print(f"   Sectores secundarios: {len(SECTORES_SECUNDARIOS)}")
print(f"   Palabras obligatorias: {len(PALABRAS_OBLIGATORIAS)}")
print(f"   Keywords manuales: {len(KEYWORDS_MANUAL)}")
print(f"   Tokens técnicos: {len(tokens_tecnicos)}")

# =============================================================================
# CORPUS INICIAL ENRIQUECIDO
# =============================================================================

CORPUS_INICIAL = """
osinergmin aprueba procedimiento supervision fiscalizacion hidrocarburos
osinergmin fija tarifas distribucion gas natural red principal
osinergmin establece disposiciones actividades downstream hidrocarburos
osinergmin aprueba norma tecnica instalaciones gas natural vehicular gnv
osinergmin modifica procedimiento registro hidrocarburos liquidos
osinergmin resolucion consejo directivo supervision distribucion glp
osinergmin fiscalizacion actividades upstream downstream petroleo gas
osinergmin fijacion cargo tarifario transporte gas natural ductos
osinergmin aprueba procedimiento atencion solicitudes autorizacion
osinergmin modifica tarifas peajes transporte liquidos gas natural
osinergmin establece metodologia calculo tarifas distribucion gas
osinergmin resolucion gerencia supervision actividades hidrocarburos

perupetro aprueba contrato licencia exploracion explotacion hidrocarburos
perupetro suscribe contrato servicios lote petrolero amazonia
perupetro negocia contrato licencia lote zocalo continental
perupetro aprueba modelo contrato licencia exploracion petroleo gas
perupetro informa resultado ronda licitacion lotes petroleros
perupetro aprueba cesion posicion contractual lote hidrocarburos
perupetro aprueba plan minimo trabajo exploracion lote petrolero
perupetro autoriza transferencia participacion contrato licencia
perupetro aprueba programa trabajo inversiones lote produccion

ministerio energia minas aprueba reglamento actividades hidrocarburos
minem establece disposiciones exploracion explotacion gas natural
minem modifica reglamento transporte hidrocarburos ductos
minem aprueba politica energetica nacional hidrocarburos
minem fija banda precios combustibles derivados petroleo
minem resolucion ministerial concesion distribucion gas natural
minem otorga concesion transporte gas natural gasoducto
minem aprueba estudio impacto ambiental actividades petroleo
minem modifica reglamento seguridad instalaciones petroleo gas
minem establece disposiciones obligatorias mezcla biocombustibles
minem aprueba especificaciones tecnicas calidad combustibles
minem resolucion directoral autorizacion instalacion planta envasado glp

oefa supervisa fiscaliza actividades hidrocarburos impacto ambiental
oefa aprueba instrumento gestion ambiental sector hidrocarburos
oefa resolucion fiscalizacion ambiental refineria petroleo
oefa establece obligaciones ambientales operadores hidrocarburos
oefa aprueba tipificacion infracciones ambientales sector energetico

decreto urgencia medidas extraordinarias sector energetico combustibles
decreto urgencia promueve acceso glp poblacion vulnerable
decreto urgencia establece medidas mitigar impacto precio combustibles
decreto urgencia regula precio gas natural vehicular gnv
decreto urgencia fondo estabilizacion precios combustibles
decreto urgencia medidas reactivacion sector hidrocarburos
decreto urgencia promueve inversion exploracion petroleo amazonia
decreto urgencia acceso masificacion gas natural usuarios residenciales

fijacion banda precios combustibles derivados petroleo gasolina diesel
actualizacion banda precios glp gasolina diesel kerosene turbo
precio referencial combustibles liquidos mercado nacional
precio maximo venta glp cilindro uso domestico
tarifa distribucion gas natural usuarios regulados red secundaria
cargo fijo variable tarifa transporte gas natural ducto principal
actualizacion factor k banda precios combustibles
precio paridad importacion gasolina diesel kerosene

contrato licencia exploracion explotacion lote petrolero
suscripcion contrato servicios actividades hidrocarburos
cesion posicion contractual lote exploracion petroleo gas
aprobacion plan minimo trabajo exploracion lote petrolero
canon gasifero distribucion regiones productoras gas natural
regalia produccion petroleo crudo gas natural lote
participacion estado produccion hidrocarburos contrato licencia
regalias valorizacion produccion fiscalizada petroleo crudo

oleoducto norperuano transporte petroleo crudo amazonia
gasoducto sur peruano transporte gas natural camisea
sistema transporte gas natural liquidos camisea lima
terminal maritimo almacenamiento combustibles liquidos
planta fraccionamiento liquidos gas natural pisco
refineria talara modernizacion proceso petroleo crudo
instalacion almacenamiento distribucion combustibles liquidos
ducto transporte hidrocarburos autorizacion construccion operacion
habilitacion terminal portuario recepcion almacenamiento combustibles

exploracion sismica prospeccion petroleo gas lote amazonia
perforacion pozo exploratorio produccion petroleo crudo
explotacion yacimiento gas natural condensado selva
produccion fiscalizada petroleo crudo gas natural canon
abandono pozo restauracion ambiental actividades hidrocarburos
programa trabajo inversiones exploracion explotacion lote

electromovilidad vehiculo electrico infraestructura carga peru
estacion carga vehiculo electrico via publica concesion
programa promocion vehiculos gas natural vehicular gnv
conversion vehicular sistema glp gnv homologacion
biocombustible biodiesel etanol mezcla obligatoria combustible
energia renovable integracion sistema electrico nacional
vehiculo hibrido electrico homologacion tecnica circulacion

registro hidrocarburos inscripcion operador comercializador
licencia operacion establecimiento venta combustibles retail
autorizacion instalacion planta envasado glp
habilitacion unidad transporte combustibles liquidos
certificacion calidad combustibles laboratorio acreditado
importacion exportacion petroleo crudo derivados arancel
autorizacion construccion operacion ducto transporte hidrocarburos
inscripcion registro agente comercializador combustibles

norma sin relevancia educacion primaria secundaria universidad
resolucion salud hospital medico enfermera vacuna
decreto defensa fuerzas armadas militares
resolucion interior policia nacional orden publico
norma vivienda construccion urbanismo habilitacion urbana
resolucion trabajo empleo laboral sindicato convenio
decreto cultura patrimonio arqueologico museo
resolucion migraciones extranjeria visa residencia
norma agriculture ganaderia riego canal
resolucion pesca acuicultura marina recursos hidrobiologicos
"""

# =============================================================================
# GESTIÓN DE CORPUS CON FEEDBACK
# =============================================================================

def gestionar_corpus(drive_client, spreadsheet_id, drive_folder_id):
    """
    Lee el corpus desde Drive. Si no existe, lo crea con el corpus inicial.
    Lee feedback de columna G de Sheets (S/N) y actualiza el corpus.
    Retorna el texto del corpus listo para entrenar el vectorizador.
    """
    print("\n🧠 GESTIONANDO CORPUS...")

    # Leer corpus existente o crear desde cero
    corpus_file_id = drive_client.get_file_by_name(drive_folder_id, 'corpus_hidrocarburos.txt')

    if corpus_file_id:
        print("   ✅ Corpus existente encontrado en Drive")
        texto_corpus = drive_client.download_text_file(corpus_file_id)
        if len(texto_corpus.strip()) < 200:
            print("   ⚠️ Corpus muy pequeño, reiniciando con corpus inicial enriquecido")
            texto_corpus = CORPUS_INICIAL
    else:
        print("   📝 Corpus no existe — creando con corpus inicial enriquecido")
        texto_corpus = CORPUS_INICIAL

    # Leer feedback de Sheets (columna G = "Relevante S/N")
    try:
        print("   📊 Leyendo feedback de Sheets (columna G)...")
        result = drive_client.sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='A2:G'  # Desde fila 2 para saltar encabezado
        ).execute()

        filas = result.get('values', [])
        textos_positivos = []
        textos_negativos = []

        for fila in filas:
            if len(fila) >= 7:
                feedback = fila[6].strip().upper()
                titulo = fila[1] if len(fila) > 1 else ""
                sumilla = fila[3] if len(fila) > 3 else ""
                texto = normalizar_texto(f"{titulo} {sumilla}")

                if feedback == "S" and texto:
                    textos_positivos.append(texto)
                elif feedback == "N" and texto:
                    textos_negativos.append(texto)

        print(f"   📊 Feedback leído: {len(textos_positivos)} positivos ✅, {len(textos_negativos)} negativos ❌")

        # Reforzar corpus con positivos (x3 para dar más peso al feedback humano)
        if textos_positivos:
            texto_corpus += "\n" + "\n".join(textos_positivos * 3)
            print(f"   ✅ Corpus reforzado con {len(textos_positivos)} normas confirmadas")

        # Los negativos NO se agregan (el TF-IDF no los aprende como relevantes)
        if textos_negativos:
            print(f"   🚫 {len(textos_negativos)} normas marcadas como no relevantes excluidas del corpus")

    except Exception as e:
        print(f"   ⚠️ No se pudo leer feedback de Sheets: {e}")

    # Guardar corpus actualizado en Drive
    drive_client.upload_text_file(drive_folder_id, 'corpus_hidrocarburos.txt', texto_corpus)
    print(f"   ✅ Corpus guardado: {len(texto_corpus)} chars, {len(texto_corpus.split())} palabras")

    return texto_corpus

# =============================================================================
# FUNCIONES DE EVALUACIÓN
# =============================================================================

def es_sector_prioritario(sector):
    sector_norm = normalizar_texto(sector)
    for s in SECTORES_PRIORITARIOS:
        if s in sector_norm:
            return True, s
    return False, None

def es_sector_secundario(sector):
    sector_norm = normalizar_texto(sector)
    for s in SECTORES_SECUNDARIOS:
        if s in sector_norm:
            return True, s
    return False, None

def es_entidad_sector(texto):
    """Detecta si MINEM, OSINERGMIN, PERUPETRO u OEFA aparecen en cualquier parte del texto"""
    texto_norm = normalizar_texto(texto)
    for entidad in ENTIDADES_SECTOR:
        if entidad in texto_norm:
            return True, entidad
    return False, None

def evaluar_relevancia(texto_candidato, sector, vectorizador, X_base):
    texto_norm = normalizar_texto(texto_candidato)
    sector_norm = normalizar_texto(sector)

    # NIVEL 1: Excluir sectores irrelevantes siempre
    for s in SECTORES_EXCLUIR:
        if s in sector_norm:
            return False, f"Sector excluido: {s}"

    # NIVEL 2: Entidad del sector en título o sumilla → aceptar siempre sin más análisis
    encontrada, entidad = es_entidad_sector(texto_candidato)
    if encontrada:
        return True, f"✅ Entidad del sector: {entidad}"

    # NIVEL 3: Verificar palabra obligatoria
    tiene_obligatoria = False
    for palabra in PALABRAS_OBLIGATORIAS:
        if palabra in texto_norm:
            tiene_obligatoria = True
            break

    if not tiene_obligatoria:
        # Sector secundario con tokens técnicos → umbral más permisivo
        es_sec, _ = es_sector_secundario(sector)
        count_tokens = sum(1 for token in tokens_tecnicos if token in texto_norm)
        if es_sec and count_tokens >= 2:
            return True, f"✅ Sector secundario + {count_tokens} tokens técnicos"
        return False, "Sin palabra obligatoria ni entidad del sector"

    # NIVEL 4: Análisis TF-IDF
    count_tokens = sum(1 for token in tokens_tecnicos if token in texto_norm)
    try:
        Y = vectorizador.transform([texto_norm])
        tfidf_score = float(cosine_similarity(X_base, Y)[0][0])
    except:
        tfidf_score = 0.0

    relevante = count_tokens >= 2 or tfidf_score >= 0.15
    razon = (
        f"✅ {count_tokens} tokens, TF-IDF:{tfidf_score:.3f}"
        if relevante else
        f"❌ {count_tokens} tokens, TF-IDF:{tfidf_score:.3f}"
    )
    return relevante, razon

# =============================================================================
# SELENIUM - FUNCIONES AUXILIARES
# =============================================================================

def crear_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)
    return driver

def complete_href(href):
    """Completa URL relativa a absoluta"""
    if not href:
        return None
    href = href.strip()
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return "https://diariooficial.elperuano.pe" + href
    if href.startswith("http"):
        return href
    return "https://diariooficial.elperuano.pe/" + href.lstrip("./")

def sanitize_filename(nombre):
    """Limpia nombre para usar como nombre de archivo"""
    nombre = re.sub(r'[<>:"/\\|?*\n\r\t]', '', nombre)
    nombre = re.sub(r'\s+', '_', nombre.strip())
    return nombre[:150]

# =============================================================================
# SELENIUM - EXTRACCIÓN PRINCIPAL
# =============================================================================

def extraer_normas(driver, fecha_obj, es_extraordinaria=False):
    """
    Extrae normas del Diario El Peruano para una fecha dada.
    - El tipo de edición se detecta directamente del HTML (<strong class="extraordinaria">)
    - La sumilla se extrae del <p> sin <b> según estructura HTML confirmada
    - El checkbox usa .click() para disparar el evento change correctamente
    """
    tipo_edicion = "Extraordinaria" if es_extraordinaria else "Ordinaria"
    fecha_str = fecha_obj.strftime("%d/%m/%Y")

    print(f"\n{'='*100}")
    print(f"🔍 EXTRAYENDO: {tipo_edicion} del {fecha_str}")
    print(f"{'='*100}")

    try:
        print("1️⃣ Cargando página...")
        driver.get("https://diariooficial.elperuano.pe/Normas")
        time.sleep(5)

        print(f"2️⃣ Configurando fechas: {fecha_str}")
        driver.execute_script(f"""
            document.getElementById('cddesde').value = '{fecha_str}';
            document.getElementById('cdhasta').value = '{fecha_str}';
        """)
        time.sleep(1)

        # CORRECCIÓN: usar .click() para disparar el evento change del checkbox
        print(f"3️⃣ Configurando checkbox extraordinaria: {es_extraordinaria}")
        driver.execute_script("""
            var checkbox = document.getElementById('tipo');
            var estadoActual = checkbox.checked;
            var estadoDeseado = arguments[0];
            if (estadoActual !== estadoDeseado) {
                checkbox.click();
            }
        """, es_extraordinaria)
        time.sleep(1)

        print("4️⃣ Ejecutando búsqueda...")
        driver.execute_script("document.getElementById('btnBuscar').click();")
        time.sleep(10)

        # Scroll con detección de estabilidad
        print("5️⃣ Cargando contenido con scroll inteligente...")
        last_count = -1
        stable = 0
        max_scrolls = 40

        for i in range(max_scrolls):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            articles = soup.find_all("article", class_=lambda c: c and "edicionesoficiales_articulos" in c)
            count = len(articles)

            print(f"   Scroll {i+1}/{max_scrolls}: {count} artículos")

            if count == last_count:
                stable += 1
            else:
                stable = 0
                last_count = count

            if stable >= 3:
                print("   ✅ Contenido estable, finalizando scroll")
                break

        print("6️⃣ Parseando HTML final...")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        articles = soup.find_all("article", class_=lambda c: c and "edicionesoficiales_articulos" in c)

        print(f"   📄 TOTAL ARTÍCULOS: {len(articles)}")

        if not articles:
            print("   ⚠️ NO SE ENCONTRARON ARTÍCULOS")
            return []

        print("7️⃣ Extrayendo datos de artículos...")
        candidatos = []

        for idx, art in enumerate(articles, 1):
            try:
                # Extraer sector desde <h4>
                sector = ""
                sector_tag = art.find("h4")
                if sector_tag:
                    sector = sector_tag.get_text(" ", strip=True)

                # Extraer título desde <h5><a>
                titulo = ""
                titulo_tag = art.find("h5")
                if titulo_tag:
                    link = titulo_tag.find("a")
                    titulo = link.get_text(" ", strip=True) if link else titulo_tag.get_text(" ", strip=True)

                # CORRECCIÓN: extraer fecha, sumilla y tipo desde HTML real
                # Estructura confirmada:
                #   <p><b>Fecha: ...</b> <strong class="extraordinaria">Edición Extraordinaria</strong></p>
                #   <p>texto de la sumilla</p>
                p_tags = art.find_all("p")
                fecha_pub = ""
                sumilla = ""
                tipo_edicion_detectado = "Ordinaria"  # default

                for p in p_tags:
                    if p.find("b"):
                        # Campo de fecha
                        texto_fecha = p.get_text(" ", strip=True)
                        if "fecha:" in texto_fecha.lower():
                            fecha_pub = texto_fecha.replace("Fecha:", "").replace("fecha:", "").strip()

                        # Detectar tipo directamente del HTML — más confiable que el checkbox
                        strong_ext = p.find("strong", class_="extraordinaria")
                        if strong_ext:
                            tipo_edicion_detectado = "Extraordinaria"
                    else:
                        # <p> sin <b> = sumilla
                        candidato = p.get_text(" ", strip=True)
                        if len(candidato) > 10:
                            sumilla = candidato

                # Limpiar texto "Extraordinaria" si quedó pegado en fecha_pub
                if "extraordinaria" in fecha_pub.lower():
                    fecha_pub = re.sub(r'(?i)edici[oó]n\s+extraordinaria', '', fecha_pub).strip()

                # Fallback: si sumilla vacía, usar título
                if not sumilla and titulo:
                    sumilla = titulo

                # Buscar PDF URL en inputs
                pdf_url = ""
                for inp in art.find_all("input"):
                    if inp.has_attr("data-url"):
                        val = (inp.get("value", "") or "").lower()
                        if "descarga individual" in val or "descarga" in val:
                            pdf_url = complete_href(inp['data-url'])
                            break
                        if not pdf_url:
                            pdf_url = complete_href(inp['data-url'])

                # Fallback: buscar en enlaces directos
                if not pdf_url:
                    for a in art.find_all("a", href=True):
                        if ".pdf" in a['href'].lower():
                            pdf_url = complete_href(a['href'])
                            break

                if not pdf_url:
                    print(f"   ⚠️ Artículo {idx} sin PDF URL, omitiendo")
                    continue

                texto_completo = f"{sector} {titulo} {sumilla}"
                nombre_archivo = sanitize_filename(titulo or sumilla[:60]) + ".pdf"

                candidatos.append({
                    "sector": sector,
                    "titulo": titulo,
                    "FechaPublicacion": fecha_pub,
                    "Sumilla": sumilla,
                    "pdf_url": pdf_url,
                    "NombreArchivo": nombre_archivo,
                    "TipoEdicion": tipo_edicion_detectado,
                    "texto_completo": texto_completo
                })

                # Debug del primer artículo
                if idx == 1:
                    print(f"\n   📋 DEBUG PRIMER ARTÍCULO:")
                    print(f"      Sector:  {sector[:60]}")
                    print(f"      Título:  {titulo[:60]}")
                    print(f"      Sumilla: {sumilla[:80]}")
                    print(f"      Fecha:   {fecha_pub}")
                    print(f"      Tipo:    {tipo_edicion_detectado}")
                    print(f"      PDF URL: {pdf_url[:80]}")

            except Exception as e:
                print(f"   ⚠️ Error en artículo {idx}: {e}")
                continue

        print(f"\n8️⃣ CANDIDATOS EXTRAÍDOS: {len(candidatos)}")
        print(f"{'='*100}\n")

        return candidatos

    except Exception as e:
        print(f"❌ ERROR CRÍTICO en extracción: {e}")
        import traceback
        traceback.print_exc()
        return []

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "="*100)
    print("🚀 INICIANDO PROCESO PRINCIPAL")
    print("="*100)

    # -------------------------------------------------------------------------
    # PASO 1: CONECTAR A GOOGLE DRIVE
    # -------------------------------------------------------------------------
    print("\n📁 PASO 1: CONECTAR A GOOGLE DRIVE")
    drive_client = GoogleDriveClient(CREDENTIALS_JSON)

    # -------------------------------------------------------------------------
    # PASO 2: GESTIONAR CORPUS (crea, actualiza con feedback de Sheets)
    # -------------------------------------------------------------------------
    print("\n🧠 PASO 2: GESTIONAR CORPUS")
    texto_base = gestionar_corpus(drive_client, SPREADSHEET_ID, DRIVE_FOLDER_ID)

    # -------------------------------------------------------------------------
    # PASO 3: INICIALIZAR VECTORIZADOR TF-IDF
    # -------------------------------------------------------------------------
    print("\n🤖 PASO 3: INICIALIZAR VECTORIZADOR TF-IDF")
    vectorizador = TfidfVectorizer(lowercase=True, ngram_range=(1, 2), max_features=3000)
    vectorizador.fit([texto_base])
    X_base = vectorizador.transform([texto_base])
    print(f"   ✅ Vocabulario: {len(vectorizador.vocabulary_)} términos")

    # -------------------------------------------------------------------------
    # PASO 4: GENERAR FECHAS A REVISAR
    # -------------------------------------------------------------------------
    print("\n📅 PASO 4: GENERAR FECHAS A REVISAR")
    fechas_a_procesar = []

    if DIA_SEMANA == 0:  # Lunes
        print("   📅 ES LUNES — revisando viernes, sábado y domingo:")
        # Ordinarias: viernes(-3), sábado(-2), domingo(-1)
        for dias_atras in range(3, 0, -1):
            fecha = HOY - timedelta(days=dias_atras)
            fechas_a_procesar.append((fecha, False))
            print(f"      • Ordinaria:     {fecha.strftime('%d/%m/%Y')}")
        # Extraordinarias: jueves(-4), viernes(-3), sábado(-2)
        for dias_atras in range(4, 1, -1):
            fecha = HOY - timedelta(days=dias_atras)
            fechas_a_procesar.append((fecha, True))
            print(f"      • Extraordinaria: {fecha.strftime('%d/%m/%Y')}")
    else:  # Martes a viernes
        print("   📅 DÍA NORMAL — revisando hoy y ayer:")
        fechas_a_procesar.append((HOY, False))
        print(f"      • Ordinaria:     {HOY.strftime('%d/%m/%Y')}")
        ayer = HOY - timedelta(days=1)
        fechas_a_procesar.append((ayer, True))
        print(f"      • Extraordinaria: {ayer.strftime('%d/%m/%Y')}")

    # -------------------------------------------------------------------------
    # PASO 5: INICIAR NAVEGADOR
    # -------------------------------------------------------------------------
    print("\n🌐 PASO 5: INICIAR NAVEGADOR")
    driver = crear_driver()
    print("   ✅ Navegador iniciado")

    # -------------------------------------------------------------------------
    # PASO 6: EXTRAER NORMAS
    # -------------------------------------------------------------------------
    print("\n📰 PASO 6: EXTRAER NORMAS")
    todos_candidatos = []

    for i, (fecha, es_ext) in enumerate(fechas_a_procesar, 1):
        tipo = "EXTRAORDINARIA" if es_ext else "ORDINARIA"
        print(f"\n📋 6.{i} — EXTRAYENDO {tipo} DEL {fecha.strftime('%d/%m/%Y')}:")
        candidatos = extraer_normas(driver, fecha, es_extraordinaria=es_ext)
        print(f"   ✅ Extraídos: {len(candidatos)} candidatos")
        todos_candidatos.extend(candidatos)
        time.sleep(3)

    driver.quit()
    print("\n✅ Navegador cerrado")

    # -------------------------------------------------------------------------
    # PASO 7: DEDUPLICAR — incluye TipoEdicion en la clave
    # -------------------------------------------------------------------------
    print("\n🔄 PASO 7: DEDUPLICAR")
    vistos = set()
    candidatos_unicos = []

    for c in todos_candidatos:
        key = (
            c['titulo'].strip().lower(),
            c.get('FechaPublicacion', ''),
            c.get('TipoEdicion', '').strip().lower()
        )
        if key not in vistos and key[0]:
            vistos.add(key)
            candidatos_unicos.append(c)

    print(f"   Total extraído: {len(todos_candidatos)}")
    print(f"   ✅ Únicos: {len(candidatos_unicos)}")

    # -------------------------------------------------------------------------
    # PASO 8: FILTRAR RELEVANCIA
    # -------------------------------------------------------------------------
    print("\n🔬 PASO 8: FILTRAR RELEVANCIA")
    aceptados = []
    prioritarios = []

    for i, c in enumerate(candidatos_unicos, 1):
        # Nivel 1: sector prioritario en <h4>
        es_prioritario, sector_match = es_sector_prioritario(c['sector'])

        if es_prioritario:
            aceptados.append(c)
            prioritarios.append(c)
            print(f"   [{i}/{len(candidatos_unicos)}] ⭐ SECTOR PRIORITARIO: {c['titulo'][:60]}")
        else:
            # Niveles 2-4: entidad en texto, palabras obligatorias, TF-IDF
            relevante, razon = evaluar_relevancia(
                c['texto_completo'], c['sector'], vectorizador, X_base
            )
            if relevante:
                aceptados.append(c)
                print(f"   [{i}/{len(candidatos_unicos)}] ✅ RELEVANTE ({razon}): {c['titulo'][:60]}")
            else:
                print(f"   [{i}/{len(candidatos_unicos)}] ❌ DESCARTADO ({razon}): {c['titulo'][:60]}")

    print(f"\n✅ TOTAL ACEPTADOS: {len(aceptados)}")

    # -------------------------------------------------------------------------
    # PASO 9: DESCARGAR Y SUBIR PDFs
    # -------------------------------------------------------------------------
    folder_id = None
    folder_name = HOY.strftime("%Y-%m-%d")

    if aceptados:
        print("\n📥 PASO 9: DESCARGAR PDFs")
        folder_id = drive_client.create_folder(DRIVE_FOLDER_ID, folder_name)

        if folder_id:
            print(f"   ✅ Carpeta lista: {folder_name}")

            for i, norma in enumerate(aceptados, 1):
                print(f"\n   [{i}/{len(aceptados)}] Procesando: {norma['titulo'][:50]}...")
                try:
                    response = requests.get(
                        norma['pdf_url'],
                        timeout=(10, 60),       # 10s conexión, 60s lectura
                        allow_redirects=True,   # sigue redirecciones explícitamente
                        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                    )
                    print(f"      HTTP Status:  {response.status_code}")
                    print(f"      URL final:    {response.url}")
                    print(f"      Content-Type: {response.headers.get('content-type', 'N/A')}")
                    print(f"      Tamaño:       {len(response.content)} bytes")

                    # Verificar con magic bytes (%PDF) — más confiable que content-type
                    es_pdf_valido = (
                        response.status_code == 200 and
                        len(response.content) > 500 and
                        response.content[:4] == b'%PDF'
                    )

                    if es_pdf_valido:
                        filename = norma['NombreArchivo']
                        link = drive_client.upload_pdf(folder_id, filename, response.content)
                        norma['drive_link'] = link if link else norma['pdf_url']
                        print(f"      ✅ PDF válido subido correctamente")
                    else:
                        print(f"      ⚠️ No es PDF válido (magic bytes: {response.content[:8]})")
                        norma['drive_link'] = norma['pdf_url']

                except requests.exceptions.Timeout:
                    print(f"      ❌ Timeout al descargar PDF")
                    norma['drive_link'] = norma['pdf_url']
                except requests.exceptions.TooManyRedirects:
                    print(f"      ❌ Demasiadas redirecciones: {norma['pdf_url']}")
                    norma['drive_link'] = norma['pdf_url']
                except Exception as e:
                    print(f"      ❌ Error inesperado: {e}")
                    norma['drive_link'] = norma['pdf_url']

    # -------------------------------------------------------------------------
    # PASO 10: GOOGLE SHEETS
    # Columnas: A=Fecha | B=Título | C=FechaPub | D=Sumilla | E=Link | F=Tipo | G=Relevante(S/N)
    # La columna G queda vacía para que puedas marcar feedback manualmente
    # -------------------------------------------------------------------------
    if aceptados:
        print("\n📊 PASO 10: ACTUALIZANDO GOOGLE SHEETS...")
        rows = []
        for norma in aceptados:
            rows.append([
                HOY.strftime("%Y-%m-%d"),
                norma.get('titulo', ''),
                norma.get('FechaPublicacion', ''),
                norma.get('Sumilla', ''),
                norma.get('drive_link', ''),
                norma.get('TipoEdicion', ''),
                ''  # Col G: "Relevante (S/N)" — deja vacío para feedback manual
            ])
        drive_client.append_to_sheet(SPREADSHEET_ID, 'A:G', rows)
        print(f"   ✅ {len(rows)} filas agregadas")
        print(f"   ℹ️  Recuerda: puedes marcar S o N en columna G para mejorar el filtrado")

    # -------------------------------------------------------------------------
    # PASO 11: ACTUALIZAR CORPUS con normas aceptadas del día
    # -------------------------------------------------------------------------
    if aceptados:
        print("\n🧠 PASO 11: ACTUALIZANDO CORPUS CON NORMAS DE HOY...")
        nuevo_contenido = "\n".join([n['texto_completo'] for n in aceptados])
        corpus_actualizado = texto_base + "\n" + nuevo_contenido
        drive_client.upload_text_file(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt', corpus_actualizado)

    # -------------------------------------------------------------------------
    # PASO 12: TELEGRAM
    # -------------------------------------------------------------------------
    print("\n💬 PASO 12: ENVIANDO TELEGRAM...")

    if aceptados:
        if DIA_SEMANA == 0:
            fecha_inicio = (HOY - timedelta(days=3)).strftime('%d/%m/%y')
            fecha_fin = HOY.strftime('%d/%m/%y')
            mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector del {fecha_inicio} al {fecha_fin}\n\n"
        else:
            mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector {HOY.strftime('%d/%m/%y')}\n\n"

        for norma in aceptados:
            tipo_etiqueta = ""
            if str(norma.get('TipoEdicion', '')).strip().lower() == "extraordinaria":
                tipo_etiqueta = " (Extraordinaria)"
            mensaje += f"<b>{norma['titulo']}{tipo_etiqueta}</b>\n"
            mensaje += f"{norma.get('Sumilla', '')}\n\n"
    else:
        if DIA_SEMANA == 0:
            fecha_inicio = (HOY - timedelta(days=3)).strftime('%d/%m/%y')
            fecha_fin = HOY.strftime('%d/%m/%y')
            mensaje = (
                f"Buen día equipo, el día de hoy no se encontraron normas relevantes del sector.\n\n"
                f"📅 Periodo revisado: del {fecha_inicio} al {fecha_fin}"
            )
        else:
            ayer = HOY - timedelta(days=1)
            mensaje = (
                f"Buen día equipo, el día de hoy no se encontraron normas relevantes del sector.\n\n"
                f"📅 Extraordinaria {ayer.strftime('%d/%m/%y')}\n"
                f"📅 Ordinaria {HOY.strftime('%d/%m/%y')}"
            )

    enviar_telegram(mensaje, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

    # -------------------------------------------------------------------------
    # RESUMEN FINAL
    # -------------------------------------------------------------------------
    print("\n" + "="*80)
    print("🎉 PROCESO COMPLETADO")
    print("="*80)
    print(f"   ✅ Normas aceptadas:   {len(aceptados)}")
    print(f"   ⭐ Prioritarias:       {len(prioritarios)}")
    print(f"   📋 Total evaluadas:    {len(candidatos_unicos)}")
    if aceptados and folder_id:
        print(f"   📁 Carpeta Drive:     {folder_name}")
    print("="*80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
