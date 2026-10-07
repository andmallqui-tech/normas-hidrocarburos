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
    """Envía mensaje dividido en partes si supera 4096 caracteres"""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        # Dividir por párrafos dobles (cada norma termina en \n\n)
        if len(mensaje) <= 4096:
            partes = [mensaje]
        else:
            partes = []
            parte_actual = ""
            
            for bloque in mensaje.split("\n\n"):
                bloque = bloque.strip()
                if not bloque:
                    continue
                
                candidato = parte_actual + bloque + "\n\n"
                
                if len(candidato) > 4096:
                    # Guardar parte actual y empezar nueva
                    if parte_actual:
                        partes.append(parte_actual.strip())
                    parte_actual = bloque + "\n\n"
                else:
                    parte_actual = candidato
            
            if parte_actual.strip():
                partes.append(parte_actual.strip())
        
        # Enviar cada parte
        for i, parte in enumerate(partes, 1):
            print(f"   📤 Enviando parte {i}/{len(partes)} ({len(parte)} chars)...")
            data = {
                'chat_id': chat_id,
                'text': parte
            }
            response = requests.post(url, data=data, timeout=10)
            print(f"   🔎 Status: {response.status_code}")
            response.raise_for_status()
            
            if i < len(partes):
                time.sleep(1)  # pausa entre mensajes para no saturar la API
        
        print(f"   ✅ Telegram enviado ({len(partes)} parte(s))")
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
    'organismo de evaluacion y fiscalizacion ambiental',
        # NUEVOS — Ambiente (MINAM y sus organismos adscritos)
    'minam',
    'ministerio del ambiente',
    'senace'
]])

# Sectores en <h4> que son siempre relevantes
SECTORES_PRIORITARIOS = set([normalizar_texto(x) for x in [
    'energia y minas',
    'energia minas',
    'minem',
    'ministerio de energia y minas',
    'osinergmin',
    'organismo supervisor de la inversion en energia y mineria',
        # NUEVOS — Ambiente
    'minam',
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
    'vivienda', 'comunicaciones', 'justicia', 'relaciones exteriores', 'midis', 'midagri','mdlp',
    'osinfor', 'senamhi', 'trabajo y promocion del empleo', 'sernanp', 'desarrollo agrario y riego',
    'municipio', 'SENAMHI', 'SERNANP', 'SERVICIO NACIONAL DE AREAS NATURALES PROTEGIDAS POR EL ESTADO',
    'JURADO NACIONAL DE ELECCIONES', 'GOBIERNOS LOCALES', 'MUNICIPALIDAD DISTRITAL', 'GOBIERNO REGIONAL', 
    'SERVICIO NACIONAL DE CERTIFICACIÓN AMBIENTAL PARA LAS INVERSIONES SOSTENIBLES'
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
    'biocombustible', 'biodiesel', 'etanol',
   # NUEVAS — energía eléctrica y renovables
    'energia electrica', 'generacion electrica', 'transmision electrica',
    'distribucion electrica', 'sistema electrico interconectado nacional', 'sein',
    'tarifa electrica', 'concesion electrica', 'central hidroelectrica',
    'central termoelectrica', 'energia solar', 'energia eolica',
    'energia renovable', 'recursos energeticos renovables', 'rer',
    'autogeneracion', 'cogeneracion', 'coes',
    # NUEVAS — ambiente
    'gestion ambiental', 'evaluacion de impacto ambiental', 'eia',
    'certificacion ambiental', 'estudio de impacto ambiental',
    'estandar de calidad ambiental', 'eca', 'limite maximo permisible', 'lmp',
    'cambio climatico', 'biodiversidad', 'areas naturales protegidas',
    'recursos forestales', 'ordenamiento territorial ambiental',
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
    'estacion de carga', 'biocombustible', 'combustibles liquidos',
    'sistema electrico interconectado', 'concesion definitiva electrica',
    'tarifa de generacion', 'peaje de transmision', 'pliego tarifario electrico',
    'central de generacion', 'planta fotovoltaica', 'parque eolico',
    'certificado de emisiones', 'instrumento de gestion ambiental',
    'clasificacion ambiental', 'consulta previa', 'zonificacion ecologica economica',
    'plan de manejo ambiental', 'monitoreo ambiental', 'huella de carbono',
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

    # Se reconstruye SIEMPRE desde CORPUS_INICIAL + feedback (S) del Sheets.
    # Antes se descargaba el corpus de Drive y se le volvían a sumar los positivos en
    # cada ejecución: el corpus crecía sin control y se contaminaba.
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

# --- FILTRO v3: palabra completa + PUNTAJE. Reemplaza el bloque v2 (es_sector_*, evaluar_relevancia) ---
# Requiere en el archivo principal: re, normalizar_texto, cosine_similarity (ya existen).
def _rx(patrones):
    """Compila patrones ya normalizados con límite de palabra."""
    return re.compile(r'\b(?:' + '|'.join(sorted(patrones, key=len, reverse=True)) + r')\b')

# Nombres largos -> siglas (el de OSINERGMIN contiene "mineria" y se confundiría con minería)
ALIAS = {
    'organismo supervisor de la inversion en energia y mineria': 'osinergmin',
    'ministerio de energia y minas': 'minem', 'energia y minas': 'minem',
    'organismo de evaluacion y fiscalizacion ambiental': 'oefa',
    'ministerio del ambiente': 'minam',
    'servicio nacional de certificacion ambiental para las inversiones sostenibles': 'senace',
}
def _alias(t):
    for largo, corto in ALIAS.items():
        t = t.replace(largo, corto)
    return t

# Hidrocarburos y combustibles (\w* = cualquier terminación)
CORE = _rx([
    r'hidrocarbur\w*', r'petrole\w*', r'petrolifer\w*', r'petroquimic\w*', r'petroperu', r'perupetro',
    r'gas natural', r'gas licuado\w*', r'glp', r'gnv', r'gnl', r'gnc', r'gasocentro\w*', r'combustibl\w*',
    r'gasolin\w*', r'gasohol', r'diesel', r'kerosene', r'turbo a1', r'nafta', r'crudo', r'oleoducto\w*',
    r'gasoducto\w*', r'poliducto\w*', r'refineri\w*', r'banda de precios', r'canon gasifero', r'camisea',
    r'biocombustibl\w*', r'biodiesel', r'etanol', r'lubricante\w*', r'estaciones? de servicio',
    r'grifos?', r'fise', r'bonogas', r'consumidor(?:es)? directo\w*', r'planta(?:s)? de abastecimiento',
    r'planta(?:s)? envasadora\w*', r'planta(?:s)? de fraccionamiento', r'ductos?', r'hidrogeno verde',
    r'electromovilidad', r'vehiculos? electric\w*', r'estaciones? de carga', r'upstream', r'downstream',
])

# Infraestructura / sistema eléctrico y renovables (señal fuerte por sí sola)
INFRA_ENERGIA = _rx([
    r'\d+ kv', r'kv', r'kilovoltios?', r'subestacion\w*', r'lineas? de transmision', r'enlaces? \d+ kv',
    r'transmision electrica', r'generacion electrica', r'distribucion electrica', r'energia electrica',
    r'electricidad', r'sector electrico', r'mercado electrico', r'sistema electrico\w*', r'sein', r'coes',
    r'concesion(?:es)? electrica\w*', r'ley de concesiones electricas', r'tarifas? electrica\w*',
    r'tarifas? en barra', r'peaje\w* de transmision', r'hidroelectric\w*', r'termoelectric\w*',
    r'fotovoltaic\w*', r'parques? eolico\w*', r'centrales? (?:solar|eolica|hidraulica|termica)\w*',
    r'energia (?:solar|eolica|renovable)\w*', r'energias renovables', r'recursos energeticos renovables',
    r'rer', r'geotermic\w*', r'autogeneracion', r'cogeneracion', r'electrificacion rural',
    r'eficiencia energetica', r'transicion energetica', r'matriz energetica', r'sector energetico',
    r'recursos energeticos', r'hidrogeno',
    r'concesion(?:es)? de (?:generacion|transmision|distribucion)',
])
ENERGIA_GENERAL = _rx([r'energia', r'electric\w*', r'energetic\w*'])

AMBIENTE = _rx([
    r'ambiental\w*', r'eia', r'eca', r'lmp', r'cambio climatico', r'biodiversidad', r'consulta previa',
    r'areas naturales protegidas', r'huella de carbono', r'gases de efecto invernadero', r'calidad del aire',
    r'limites? maximos? permisibles?', r'residuos solidos', r'remediacion', r'pasivos ambientales',
])

# Gobernanza de los reguladores (OSINERGMIN, OSITRAN, SUNASS, OSIPTEL): decisión tuya, ver INCLUIR_REGULADORES
REGULADORES = _rx([r'organismos? reguladores? de la inversion privada[a-z ]*', r'consejo directivo de (?:los )?organismos reguladores'])
INCLUIR_REGULADORES = True

DEBILES = _rx([r'tarifas?', r'concesion\w*', r'lotes?', r'pozos?', r'terminal\w*', r'supervision',
               r'fiscalizacion', r'regalias?', r'exploracion', r'explotacion', r'yacimientos?',
               r'licencia de operacion', r'peajes?', r'contratos? de licencia'])

ENTIDAD_FUERTE = _rx([r'osinergmin', r'perupetro', r'petroperu', r'dgh', r'\d{4} os (?:cd|gg|grt|gart|gsm|gse|dsr|gfhl|gfgn)'])
ENTIDAD_AMPLIA = _rx([r'minem', r'oefa', r'minam', r'senace', r'\d{4} em', r'\d{4} minem', r'\d{4} oefa', r'\d{4} minam'])

SECTOR_PRIORITARIO = _rx([r'energia y minas', r'energia minas', r'minem', r'osinergmin', r'perupetro',
                          r'oefa', r'minam', r'ambiente', r'senace'])
SECTOR_SECUNDARIO = _rx([r'decretos? de urgencia', r'presidencia del consejo de ministros', r'pcm',
                         r'organismos? tecnicos? especializados?', r'organismos? reguladores?',
                         r'economia y finanzas', r'mef', r'transportes', r'autoridad portuaria'])
# 'vivienda' y 'comunicaciones' (MTC) ya NO se excluyen: pueden traer GNV, electromovilidad, gas domiciliario.
# Igual necesitan señal de tema para pasar.
SECTOR_EXCLUIR = _rx([
    r'educacion', r'salud', r'defensa', r'interior', r'mujer', r'desarrollo social', r'trabajo',
    r'migraciones', r'cultura', r'justicia', r'relaciones exteriores',
    r'midis', r'midagri', r'mdlp', r'osinfor', r'senamhi', r'sernanp', r'desarrollo agrario',
    r'jurado nacional', r'gobiernos? locales?', r'gobiernos? regional\w*', r'municipalidad\w*', r'municipio\w*',
    r'universidad\w*', r'sunedu', r'poder judicial', r'ministerio publico', r'congreso', r'onpe', r'reniec',
    r'essalud', r'sunafil', r'contraloria', r'defensoria', r'sunarp',
])

# Ruido detectado en el TEXTO (por si el sector viene vacío o con otro nombre)
RUIDO_LOCAL = _rx([r'universidad\w*', r'municipalidad\w*', r'ordenanza\w*', r'distrital', r'sunedu', r'becas?',
                   r'estudiantes?', r'docentes?', r'colegio\w*', r'hospital\w*', r'policia\w*'])
MINERIA = _rx([r'mineri\w*', r'miner[oa]s?', r'petitorios?', r'reinfo', r'ingemmet', r'relaves?', r'mineral\w*'])
ADMIN = re.compile(r'\b(?:designan|designar|encargan|encargar|nombran|aceptan? (?:la )?renuncia|'
                   r'da(?:n)? por concluid\w+|autorizan? viaje|otorgan? licencia|declaran vacante|cesan|'
                   r'ratifican designacion|felicitan)\b')
UMBRAL = 3.0

def _distintos(rx, texto, cap):
    return min(len({m.group(0)[:6] for m in rx.finditer(texto)}), cap)

def _sim_max(vectorizador, X, texto):
    """Máxima similitud contra TODAS las filas del corpus."""
    if vectorizador is None or X is None:
        return 0.0
    try:
        return float(cosine_similarity(X, vectorizador.transform([texto])).max())
    except Exception:
        return 0.0

def es_sector_prioritario(sector):
    m = SECTOR_PRIORITARIO.search(normalizar_texto(sector))
    return (True, m.group(0)) if m else (False, None)

def es_sector_secundario(sector):
    m = SECTOR_SECUNDARIO.search(normalizar_texto(sector))
    return (True, m.group(0)) if m else (False, None)

def es_entidad_sector(texto):
    t = _alias(normalizar_texto(texto))
    m = ENTIDAD_FUERTE.search(t) or ENTIDAD_AMPLIA.search(t)
    return (True, m.group(0)) if m else (False, None)
    
def es_minem_osinergmin(sector, titulo, sumilla):
    """MINEM y OSINERGMIN pasan SIEMPRE, antes de cualquier otro filtro."""
    t = f"{sector} {titulo} {sumilla}".lower()   # sin normalizar: conserva "-OS/" y "-EM"
    patrones = [
        r'minem', r'osinergmin',
        r'energ[ií]a y minas',
        r'organismo supervisor de la inversi[oó]n en energ[ií]a y miner[ií]a',
        r'\d{4}-em\b',                 # DS / RS: 010-2026-EM
        r'\d{4}-os\b', r'\bos/',       # OSINERGMIN: 174-2026-OS/CD
    ]
    for p in patrones:
        if re.search(p, t):
            return True, p
    return False, None

def evaluar_relevancia(texto_candidato, sector, vectorizador=None, X_base=None):
    """texto_candidato = f"{titulo} {sumilla}" (SIN el sector). Devuelve (bool, razon)."""
    t = _alias(normalizar_texto(texto_candidato))
    s = normalizar_texto(sector)
    p, why = 0.0, []

    n_core = _distintos(CORE, t, 2)
    n_infra = _distintos(INFRA_ENERGIA, t, 2)
    fuerte = bool(ENTIDAD_FUERTE.search(t))
    tema_fuerte = n_core >= 2 or n_infra > 0 or fuerte
    if n_core:  p += 3 * n_core;  why.append(f"hidrocarburos x{n_core}")
    if n_infra: p += 3 * n_infra; why.append(f"energia/infra x{n_infra}")
    if fuerte:  p += 4;           why.append("entidad fuerte")
    elif ENTIDAD_AMPLIA.search(t): p += 2; why.append("entidad amplia")
    if INCLUIR_REGULADORES and REGULADORES.search(t): p += 3; why.append("reguladores")
    if es_sector_prioritario(sector)[0]: p += 1.5; why.append("sector prioritario")
    elif es_sector_secundario(sector)[0]: p += 0.5; why.append("sector secundario")
    n = _distintos(ENERGIA_GENERAL, t, 2)
    if n: p += n; why.append(f"energia general x{n}")
    n = _distintos(AMBIENTE, t, 2)
    if n: p += n; why.append(f"ambiente x{n}")
    if p >= 1:                                   # señales débiles y TF-IDF SOLO refuerzan
        n = _distintos(DEBILES, t, 3)
        if n: p += 0.5 * n; why.append(f"debiles x{n}")
        sim = _sim_max(vectorizador, X_base, t)
        if sim >= 0.30: p += 2.5; why.append(f"tfidf {sim:.2f}")
        elif sim >= 0.15: p += 1.5; why.append(f"tfidf {sim:.2f}")

    if SECTOR_EXCLUIR.search(s) and not fuerte:      p -= 6; why.append("sector excluido")
    if RUIDO_LOCAL.search(t) and not tema_fuerte:    p -= 3; why.append("ruido local/educacion")
    if MINERIA.search(t) and not tema_fuerte:        p -= 3; why.append("mineria sin hidrocarburos")
    if ADMIN.search(t[:250]):                        p -= 5; why.append("personal/viaje")

    ok = p >= UMBRAL
    return ok, f"{'✅' if ok else '❌'} {p:.1f} pts [{', '.join(why) or 'sin señales'}]"

# =============================================================================
# RESOLUCIÓN DE URL DE PDF
# =============================================================================

def resolver_pdf_url(url_dispositivo, fecha_pub_str=""):
    """
    Toma la URL del botón de detalle y trata de encontrar el PDF real dentro del HTML.
    Si no lo encuentra, devuelve la URL original.
    """
    if not url_dispositivo:
        return url_dispositivo

    # Si ya es PDF directo, devolver tal cual
    if url_dispositivo.lower().endswith(".pdf"):
        return url_dispositivo

    # Pedir la página HTML del dispositivo
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            )
        }

        resp = requests.get(
            url_dispositivo,
            headers=headers,
            timeout=15,
            allow_redirects=True
        )

        if resp.status_code != 200:
            return url_dispositivo

        soup_page = BeautifulSoup(resp.text, "html.parser")

        # 1) Buscar enlace explícito a PDF en el HTML
        for a in soup_page.find_all("a", href=True):
            href = a["href"].strip()
            texto = a.get_text(" ", strip=True).lower()

            if ".pdf" in href.lower():
                if href.startswith("/"):
                    href = "https://diariooficial.elperuano.pe" + href
                return href

            # A veces el botón viene como enlace al dispositivo con texto de descarga
            if "descarga" in texto and href:
                if href.startswith("/"):
                    href = "https://busquedas.elperuano.pe" + href
                # devolver el enlace del botón, por si allí está el recurso real
                return href

        # 2) Buscar PDF dentro de meta tags
        for meta in soup_page.find_all("meta"):
            content = meta.get("content", "")
            if ".pdf" in content.lower():
                if content.startswith("/"):
                    content = "https://diariooficial.elperuano.pe" + content
                return content.strip()

        # 3) Buscar iframe/embed/object con PDF
        for tag in soup_page.find_all(["iframe", "embed", "object"]):
            src = tag.get("src") or tag.get("data")
            if src and ".pdf" in src.lower():
                if src.startswith("/"):
                    src = "https://diariooficial.elperuano.pe" + src
                return src.strip()

        # 4) Fallback: si el HTML trae alguna URL absoluta a PDF
        match_pdf = re.search(r'https?://[^"\']+\.pdf', resp.text, flags=re.I)
        if match_pdf:
            return match_pdf.group(0)

    except Exception as e:
        print(f"      ⚠️ resolver_pdf_url fallback falló: {e}")

    return url_dispositivo

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

            if stable >= 5:
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

                # Buscar PDF URL en botones de descarga
                pdf_url = ""

                # Método 1 (PRINCIPAL): enlace "Descarga individual" en div.ediciones_botones
                # Estructura actual: <a href="https://busquedas.elperuano.pe/dispositivo/NL/XXXXXXX-1">Descarga individual</a>
                botones_div = art.find("div", class_="ediciones_botones")
                if botones_div:
                    for a in botones_div.find_all("a", href=True):
                        texto_enlace = a.get_text(strip=True).lower()
                        if "descarga individual" in texto_enlace:
                            pdf_url = a['href']
                            if pdf_url.startswith("/"):
                                pdf_url = "https://busquedas.elperuano.pe" + pdf_url
                            break
                    # Si no hay "descarga individual", tomar el primer enlace del div
                    if not pdf_url:
                        primer_a = botones_div.find("a", href=True)
                        if primer_a:
                            pdf_url = primer_a['href']
                            if pdf_url.startswith("/"):
                                pdf_url = "https://busquedas.elperuano.pe" + pdf_url

                # Método 2 (fallback): construir URL desde imagen de portada
                if not pdf_url:
                    img_tag = art.find("div", class_="ediciones_pdf")
                    if img_tag:
                        img = img_tag.find("img")
                        if img and img.has_attr("src"):
                            src = img["src"]
                            match = re.search(r'PortadaFull/(\d{4}/\d{2}/\d{2})/(\d+-\d+)_Portada\.jpg', src)
                            if match:
                                fecha_path = match.group(1)
                                codigo = match.group(2)
                                pdf_url = f"https://diariooficial.elperuano.pe/NormasElperuano/{fecha_path}/{codigo}.pdf"

                # Método 3 (fallback): inputs con data-url (estructura antigua)
                if not pdf_url:
                    for inp in art.find_all("input"):
                        if inp.has_attr("data-url"):
                            val = (inp.get("value", "") or "").lower()
                            if "descarga individual" in val or "descarga" in val:
                                pdf_url = complete_href(inp['data-url'])
                                break
                    if not pdf_url:
                        for inp in art.find_all("input"):
                            if inp.has_attr("data-url"):
                                pdf_url = complete_href(inp['data-url'])
                                break

                # Método 4 (fallback): enlaces directos a PDF
                if not pdf_url:
                    for a in art.find_all("a", href=True):
                        if ".pdf" in a['href'].lower():
                            pdf_url = complete_href(a['href'])
                            break

                if not pdf_url and titulo_tag:
                    a_tit = titulo_tag.find("a", href=True)
                    if a_tit:
                        pdf_url = complete_href(a_tit['href'])
                if not pdf_url:
                    print(f"   ⚠️ Artículo {idx} sin URL alguna, se conserva sin link: {titulo[:50]}")

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
            
        # ====== DIAGNÓSTICO TEMPORAL ======
        print("🔎 DIAGNÓSTICO: analizando HTML recibido...")
        todos_articles = soup.find_all("article")
        print(f"  Total <article> en página: {len(todos_articles)}")
        for art in todos_articles[:3]:
            print(f"  Clases: {art.get('class', 'sin clase')}")
            print(f"  Primeros 200 chars: {str(art)[:200]}")
        
        # Buscar con selector más amplio por si cambió la clase
        divs_posibles = soup.find_all(["article", "div"], class_=lambda c: c and ("articulo" in str(c).lower() or "norma" in str(c).lower() or "edicion" in str(c).lower()))
        print(f"  Elementos con 'articulo/norma/edicion' en clase: {len(divs_posibles)}")
        # ==================================

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
        print("   📅 ES LUNES — revisando viernes, sábado, domingo y lunes:")
        # Ordinarias: sábado(-2), domingo(-1), lunes(0)
        for dias_atras in [2, 1, 0]:
            fecha = HOY - timedelta(days=dias_atras)
            fechas_a_procesar.append((fecha, False))
            print(f"      • Ordinaria:     {fecha.strftime('%d/%m/%Y')}")
        # Extraordinarias: viernes(-3), sábado(-2), domingo(-1)
        for dias_atras in [3, 2, 1]:
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
            c.get('TipoEdicion', '').strip().lower(),
            c.get('pdf_url', '')
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
        # MINEM y OSINERGMIN pasan SIEMPRE
        ok, patron = es_minem_osinergmin(c['sector'], c['titulo'], c['Sumilla'])
        if ok:
            aceptados.append(c)
            prioritarios.append(c)
            print(f"   [{i}/{len(candidatos_unicos)}] ⭐ MINEM/OSINERGMIN ({patron}): {c['titulo'][:60]}")
            continue

        # Resto de normas: filtro normal
        es_prioritario, _ = es_sector_prioritario(c['sector'])
        relevante, razon = evaluar_relevancia(
            f"{c['titulo']} {c['Sumilla']}", c['sector'], vectorizador, X_base
        )
        if relevante:
            aceptados.append(c)
            if es_prioritario:
                prioritarios.append(c)
            print(f"   [{i}/{len(candidatos_unicos)}] ✅ ({razon}): {c['titulo'][:60]}")
        else:
            print(f"   [{i}/{len(candidatos_unicos)}] ❌ ({razon}) [{c['sector'][:25]}]: {c['titulo'][:60]}")

    print(f"\n✅ TOTAL ACEPTADOS: {len(aceptados)}")

    # -------------------------------------------------------------------------
    # PASO 9: GUARDAR SOLO EL URL EN SHEETS (SIN DESCARGAR PDFs)
    # -------------------------------------------------------------------------
    if aceptados:
        print("\n📥 PASO 9: GUARDAR SOLO URL EN EXCEL (SIN DESCARGA)")
    
        for i, norma in enumerate(aceptados, 1):
            print(f"\n   [{i}/{len(aceptados)}] Procesando: {norma['titulo'][:50]}...")
    
            # Guardar el URL que ya se encontró al extraer la norma
            # No se descarga nada
            norma['drive_link'] = norma.get('pdf_url', '')
    
            print(f"      🔗 URL guardada: {norma['drive_link']}")
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
    # PASO 11 desactivado: agregar al corpus lo "aceptado" sin validar realimentaba los falsos
    # positivos. El corpus ahora se aprende SOLO de la columna G (S) del Sheets.

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

        for i, norma in enumerate(aceptados, 1):
            tipo_etiqueta = ""
            if str(norma.get('TipoEdicion', '')).strip().lower() == "extraordinaria":
                tipo_etiqueta = " (Extraordinaria)"
            mensaje += f"{i}. {norma['titulo']}{tipo_etiqueta}\n"
            mensaje += f"{norma.get('Sumilla', '')}\n"
            mensaje += f"🔗 {norma.get('pdf_url', '')}\n\n"
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
    # folder_id ya no se usa (sin descarga de PDFs)
    print("="*80)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
