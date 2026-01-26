"""
=============================================================================
SISTEMA AUTOMATIZADO DE NORMAS DE HIDROCARBUROS
Versión GitHub Actions + Google Drive + Telegram
Con soporte para fin de semana (Lunes revisa Sábado y Domingo)
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

# Core
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Machine Learning
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

print("="*80)
print("🚀 SISTEMA DE NORMAS - GITHUB ACTIONS")
print("="*80)

# Fechas
HOY = date.today()
DIA_SEMANA = HOY.weekday()  # 0=Lunes, 1=Martes, ..., 6=Domingo
AYER = HOY - timedelta(days=1)  # Siempre definir para mensajes

# Determinar qué fechas revisar según el día
if DIA_SEMANA == 0:  # LUNES
    # Revisar todo el fin de semana
    # Las fechas son las que aparecen en el buscador de El Peruano
    FECHA_VIERNES = HOY - timedelta(days=3)   # Viernes (hace 3 días)
    FECHA_SABADO = HOY - timedelta(days=2)    # Sábado (hace 2 días)
    FECHA_DOMINGO = HOY - timedelta(days=1)   # Domingo (hace 1 día)
    
    FECHAS_A_REVISAR = [
        # Viernes
        ('Viernes Extra', FECHA_VIERNES, True),      # Viernes extraordinaria
        # Sábado
        ('Sábado Ord', FECHA_SABADO, False),         # Sábado ordinaria
        ('Sábado Extra', FECHA_SABADO, True),        # Sábado extraordinaria (del viernes)
        # Domingo
        ('Domingo Ord', FECHA_DOMINGO, False),       # Domingo ordinaria
        ('Domingo Extra', FECHA_DOMINGO, True),      # Domingo extraordinaria (del sábado)
    ]
    print(f"📅 ES LUNES - Revisando fin de semana completo:")
    print(f"   🗓️  Viernes {FECHA_VIERNES.strftime('%d/%m/%Y')} (Extra)")
    print(f"   🗓️  Sábado {FECHA_SABADO.strftime('%d/%m/%Y')} (Ord + Extra)")
    print(f"   🗓️  Domingo {FECHA_DOMINGO.strftime('%d/%m/%Y')} (Ord + Extra)")
else:
    # Martes a Viernes: revisar día anterior normal
    FECHAS_A_REVISAR = [
        ('Ayer Extra', AYER, True),   # Extraordinaria del día anterior
        ('Hoy Ord', HOY, False),      # Ordinaria de hoy
    ]
    print(f"📅 HOY: {HOY.strftime('%d/%m/%Y')}")
    print(f"📅 AYER: {AYER.strftime('%d/%m/%Y')}")

# Google Cloud (desde variables de entorno)
CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

print("="*80)

# =============================================================================
# GOOGLE DRIVE CLIENT
# =============================================================================

class GoogleDriveClient:
    """Cliente para Google Drive y Sheets"""
    
    def __init__(self, credentials_json):
        # Decodificar credenciales desde base64
        try:
            # Intentar decodificar base64
            decoded = base64.b64decode(credentials_json)
            credentials_dict = json.loads(decoded.decode('utf-8'))
        except (UnicodeDecodeError, json.JSONDecodeError):
            # Si falla, asumir que ya está en formato JSON
            print("   ⚠️ Decodificación base64 falló, usando JSON directo")
            credentials_dict = json.loads(credentials_json)
        
        credentials = service_account.Credentials.from_service_account_info(
            credentials_dict,
            scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        )
        
        self.drive_service = build('drive', 'v3', credentials=credentials)
        self.sheets_service = build('sheets', 'v4', credentials=credentials)
    
    def get_file_by_name(self, folder_id, filename):
        """Busca archivo por nombre en carpeta"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(
                q=query,
                fields='files(id, name)'
            ).execute()
            
            files = results.get('files', [])
            return files[0]['id'] if files else None
            
        except Exception as e:
            print(f"⚠️ Error buscando archivo: {e}")
            return None
    
    def download_text_file(self, file_id):
        """Descarga archivo de texto"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                _, done = downloader.next_chunk()
            
            return fh.getvalue().decode('utf-8')
            
        except Exception as e:
            print(f"⚠️ Error descargando: {e}")
            return ""
    
    def upload_text_file(self, folder_id, filename, content):
        """Sube o actualiza archivo de texto"""
        try:
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
            
            # Buscar si existe
            existing_id = self.get_file_by_name(folder_id, filename)
            
            if existing_id:
                # Actualizar
                self.drive_service.files().update(
                    fileId=existing_id,
                    media_body=media
                ).execute()
                print(f"   ✅ Actualizado: {filename}")
            else:
                # Crear nuevo
                self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                print(f"   ✅ Creado: {filename}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error subiendo {filename}: {e}")
            return False
    
    def upload_pdf(self, folder_id, filename, pdf_bytes):
        """Sube PDF a Drive"""
        try:
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
            
            return file.get('webViewLink', '')
            
        except Exception as e:
            print(f"❌ Error subiendo PDF: {e}")
            return None
    
    def create_folder(self, parent_id, folder_name):
        """Crea carpeta en Drive"""
        try:
            # Verificar si existe
            existing_id = self.get_file_by_name(parent_id, folder_name)
            if existing_id:
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
            
            return folder.get('id')
            
        except Exception as e:
            print(f"❌ Error creando carpeta: {e}")
            return None
    
    def append_to_sheet(self, spreadsheet_id, range_name, values):
        """Agrega filas a Google Sheets"""
        try:
            body = {'values': values}
            
            result = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            
            return result
            
        except Exception as e:
            print(f"❌ Error actualizando Sheets: {e}")
            return None

# =============================================================================
# TELEGRAM CLIENT
# =============================================================================

def enviar_telegram(mensaje, bot_token, chat_id):
    """Envía mensaje por Telegram"""
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        data = {
            'chat_id': chat_id,
            'text': mensaje,
            'parse_mode': 'HTML'
        }
        
        response = requests.post(url, data=data, timeout=10)
        response.raise_for_status()
        
        print("   ✅ Telegram enviado")
        return True
        
    except Exception as e:
        print(f"❌ Error Telegram: {e}")
        return False

# =============================================================================
# NORMALIZACIÓN Y KEYWORDS
# =============================================================================

def normalizar_texto(texto):
    """Normaliza texto"""
    if not isinstance(texto, str):
        return ""
    texto = texto.lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    texto = re.sub(r'[^a-z0-9\s]', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

# Keywords del sector
KEYWORDS_MANUAL = [normalizar_texto(x) for x in [
    'hidrocarburos','hidrocarburo','petroleo','gas natural','gnv','glp',
    'perupetro','osinergmin','minem','oefa','refineria','oleoducto','gasoducto',
    'exploracion','explotacion','combustible','diesel','gasolina','kerosene',
    'canon gasifero','banda de precios','lote','pozo','yacimiento',
    'diesel b5','turbo','residual','bunker','upstream','downstream',
    'fraccionamiento','terminal','planta de gas','contrato de licencia',
    'regalia','concesion','electromovilidad','ductos','fijaron precios',
    'recursos energeticos','distribucion natural'
]]

PALABRAS_OBLIGATORIAS = set([normalizar_texto(x) for x in [
    'hidrocarburos','hidrocarburo','petroleo','gas natural',
    'perupetro','gnv','glp','oleoducto','gasoducto','refineria',
    'osinergmin','oefa','banda de precios'
]])

SECTORES_EXCLUIR = set([normalizar_texto(x) for x in [
    'educacion','salud','defensa','interior','mujer',
    'desarrollo social','trabajo','migraciones','cultura',
    'vivienda','comunicaciones','justicia','relaciones exteriores','midis'
]])

# Tokens técnicos
tokens_tecnicos = set()
for kw in KEYWORDS_MANUAL:
    for token in kw.split():
        if len(token) > 2:
            tokens_tecnicos.add(token)

print(f"🧠 Keywords cargadas: {len(KEYWORDS_MANUAL)}")
print(f"🧠 Tokens técnicos: {len(tokens_tecnicos)}")

# =============================================================================
# EVALUACIÓN DE RELEVANCIA
# =============================================================================

def evaluar_relevancia(texto_candidato, vectorizador, X_base):
    """Evalúa relevancia"""
    
    texto_norm = normalizar_texto(texto_candidato)
    
    # Filtro 1: Sector excluido
    for sector in SECTORES_EXCLUIR:
        if sector in texto_norm:
            return False, f"Sector excluido: {sector}"
    
    # Filtro 2: Palabra obligatoria
    tiene_obligatoria = False
    palabra_encontrada = None
    for palabra in PALABRAS_OBLIGATORIAS:
        if palabra in texto_norm:
            tiene_obligatoria = True
            palabra_encontrada = palabra
            break
    
    if not tiene_obligatoria:
        return False, "Sin palabra obligatoria"
    
    # Filtro 3: Tokens técnicos
    count_tokens = sum(1 for token in tokens_tecnicos if token in texto_norm)
    
    # Filtro 4: TF-IDF
    try:
        Y = vectorizador.transform([texto_norm])
        tfidf_score = float(cosine_similarity(X_base, Y)[0][0])
    except:
        tfidf_score = 0.0
    
    # Decisión
    relevante = (
        count_tokens >= 3 or
        (count_tokens >= 2 and tfidf_score >= 0.15)
    )
    
    razon = f"✅ {count_tokens} términos, TF-IDF:{tfidf_score:.3f}" if relevante else "❌ Insuficiente"
    return relevante, razon

# =============================================================================
# SELENIUM
# =============================================================================

def crear_driver():
    """Crea driver Chrome headless"""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)
    return driver

def extraer_normas(driver, fecha_obj, es_extraordinaria=False):
    """Extrae normas"""
    
    tipo = "Extraordinaria" if es_extraordinaria else "Ordinaria"
    fecha_str = fecha_obj.strftime("%d/%m/%Y")
    
    print(f"\n🔍 Extrayendo {tipo} del {fecha_str}")
    
    try:
        driver.get("https://diariooficial.elperuano.pe/Normas")
        time.sleep(4)
        
        # Configurar fechas
        driver.execute_script(f"""
            document.getElementById('cddesde').value = '{fecha_str}';
            document.getElementById('cdhasta').value = '{fecha_str}';
        """)
        
        # Checkbox
        if es_extraordinaria:
            driver.execute_script("document.getElementById('tipo').checked = true;")
        else:
            driver.execute_script("document.getElementById('tipo').checked = false;")
        
        time.sleep(1)
        
        # Buscar
        driver.execute_script("document.getElementById('btnBuscar').click();")
        time.sleep(8)
        
        # Scroll
        print("   Cargando contenido...")
        for i in range(30):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
        
        # Parsear
        soup = BeautifulSoup(driver.page_source, "html.parser")
        articles = soup.find_all("article", class_=lambda c: c and "articulos" in c)
        
        print(f"   📄 {len(articles)} artículos encontrados")
        
        candidatos = []
        for art in articles:
            try:
                # Sector
                sector_tag = art.find("h4")
                sector = sector_tag.get_text(" ", strip=True) if sector_tag else ""
                
                # Título
                titulo_tag = art.find("h5")
                titulo = titulo_tag.get_text(" ", strip=True) if titulo_tag else ""
                
                # Sumilla
                p_tags = art.find_all("p")
                sumilla = ""
                fecha_pub = ""
                
                if p_tags:
                    # Fecha
                    b = p_tags[0].find("b")
                    if b:
                        fecha_pub = b.get_text(" ", strip=True).replace("Fecha:", "").strip()
                    
                    # Sumilla
                    if len(p_tags) >= 2:
                        sumilla = p_tags[1].get_text(" ", strip=True)
                
                # PDF URL
                pdf_url = ""
                for inp in art.find_all("input"):
                    if inp.has_attr("data-url"):
                        url = inp['data-url']
                        if url.startswith("//"):
                            pdf_url = "https:" + url
                        elif url.startswith("/"):
                            pdf_url = "https://diariooficial.elperuano.pe" + url
                        elif url.startswith("http"):
                            pdf_url = url
                        else:
                            pdf_url = "https://diariooficial.elperuano.pe/" + url.lstrip("./")
                        break
                
                if not pdf_url:
                    continue
                
                candidatos.append({
                    "sector": sector,
                    "titulo": titulo,
                    "fecha_pub": fecha_pub,
                    "sumilla": sumilla,
                    "pdf_url": pdf_url,
                    "tipo": tipo,
                    "texto_completo": f"{sector} {titulo} {sumilla}"
                })
                
            except Exception as e:
                continue
        
        return candidatos
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

# =============================================================================
# MAIN
# =============================================================================

def main():
    """Proceso principal"""
    
    print("\n" + "="*80)
    print("🚀 INICIANDO PROCESO")
    print("="*80)
    
    # Inicializar Drive
    print("\n📁 Conectando a Google Drive...")
    drive_client = GoogleDriveClient(CREDENTIALS_JSON)
    
    # Cargar o crear corpus
    print("\n🧠 Cargando corpus...")
    corpus_file_id = drive_client.get_file_by_name(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt')
    
    if corpus_file_id:
        texto_base = drive_client.download_text_file(corpus_file_id)
        print(f"   ✅ Corpus cargado ({len(texto_base.split())} palabras)")
    else:
        texto_base = " ".join(KEYWORDS_MANUAL * 3)
        print(f"   📝 Corpus inicial creado")
    
    # Vectorizador TF-IDF
    vectorizador = TfidfVectorizer(lowercase=True, ngram_range=(1,2), max_features=2000)
    vectorizador.fit([texto_base])
    X_base = vectorizador.transform([texto_base])
    
    # Selenium
    print("\n🌐 Iniciando navegador...")
    driver = crear_driver()
    
    # Extraer normas según las fechas determinadas
    print("\n📰 Extrayendo normas...")
    todos_candidatos = []
    
    for label, fecha_obj, es_extraordinaria in FECHAS_A_REVISAR:
        candidatos = extraer_normas(driver, fecha_obj, es_extraordinaria)
        todos_candidatos.extend(candidatos)
        time.sleep(2)
    
    driver.quit()
    print("\n✅ Navegador cerrado")
    
    print(f"\n📊 Total candidatos: {len(todos_candidatos)}")
    
    # Filtrar
    print("\n🔬 Filtrando relevancia...")
    aceptados = []
    
    for c in todos_candidatos:
        relevante, razon = evaluar_relevancia(
            c['texto_completo'],
            vectorizador,
            X_base
        )
        
        if relevante:
            aceptados.append(c)
            print(f"   ✅ {c['titulo'][:60]}")
    
    print(f"\n✅ Normas relevantes: {len(aceptados)}")
    
    # Crear carpeta del día y descargar PDFs
    folder_id = None
    if aceptados:
        print("\n📥 Descargando PDFs...")
        
        folder_name = HOY.strftime("%Y-%m-%d")
        folder_id = drive_client.create_folder(DRIVE_FOLDER_ID, folder_name)
        
        if folder_id:
            for norma in aceptados:
                try:
                    # Descargar PDF
                    response = requests.get(norma['pdf_url'], timeout=30)
                    if response.status_code == 200:
                        # Nombre limpio
                        filename = re.sub(r'[^\w\s-]', '', norma['titulo'][:100])
                        filename = re.sub(r'\s+', '_', filename) + '.pdf'
                        
                        # Subir a Drive
                        link = drive_client.upload_pdf(folder_id, filename, response.content)
                        norma['drive_link'] = link if link else norma['pdf_url']
                        
                        print(f"   ✅ {filename[:50]}")
                    else:
                        norma['drive_link'] = norma['pdf_url']
                        
                except Exception as e:
                    print(f"   ⚠️ Error: {e}")
                    norma['drive_link'] = norma['pdf_url']
    
    # Actualizar Google Sheets
    if aceptados:
        print("\n📊 Actualizando Google Sheets...")
        
        rows = []
        for norma in aceptados:
            rows.append([
                HOY.strftime("%Y-%m-%d"),
                norma['titulo'],
                norma.get('fecha_pub', ''),
                norma['sumilla'],
                norma.get('drive_link', ''),
                norma['tipo']
            ])
        
        drive_client.append_to_sheet(SPREADSHEET_ID, 'A:F', rows)
        print(f"   ✅ {len(rows)} filas agregadas")
    
    # Actualizar corpus
    if aceptados:
        print("\n🧠 Actualizando corpus...")
        nuevo_contenido = "\n".join([n['texto_completo'] for n in aceptados])
        corpus_actualizado = texto_base + "\n" + nuevo_contenido
        drive_client.upload_text_file(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt', corpus_actualizado)
    
    # Generar mensaje
    if aceptados:
        # Determinar rango de fechas en el mensaje
        if DIA_SEMANA == 0:  # Lunes
            rango_fechas = f"del fin de semana ({FECHA_SABADO.strftime('%d/%m/%y')} - {FECHA_DOMINGO.strftime('%d/%m/%y')})"
        else:
            rango_fechas = f"al sector {HOY.strftime('%d/%m/%y')}"
        
        mensaje = f"Buen día equipo, se envía la revisión de normas relevantes {rango_fechas}\n\n"
        
        for i, norma in enumerate(aceptados, 1):
            mensaje += f"<b>{i}. {norma['titulo']}</b>\n"
            mensaje += f"{norma['sumilla'][:200]}...\n\n"
        
        mensaje += f"\n✅ Total: {len(aceptados)} normas\n"
        mensaje += f"📁 <a href='https://drive.google.com/drive/folders/{folder_id}'>Ver PDFs en Drive</a>"
    else:
        # Mensaje cuando no hay normas
        if DIA_SEMANA == 0:  # Lunes
            mensaje = (
                f"Buen día equipo, no se encontraron normas relevantes del sector durante el fin de semana.\n\n"
                f"📅 Sábado {FECHA_SABADO.strftime('%d/%m/%y')} (Extraordinaria viernes + Ordinaria sábado)\n"
                f"📅 Domingo {FECHA_DOMINGO.strftime('%d/%m/%y')} (Extraordinaria sábado + Ordinaria domingo)"
            )
        else:
            mensaje = (
                f"Buen día equipo, el día de hoy no se encontraron normas relevantes del sector.\n\n"
                f"📅 Extraordinaria {AYER.strftime('%d/%m/%y')}\n"
                f"📅 Ordinaria {HOY.strftime('%d/%m/%y')}"
            )
    
    # Enviar a Telegram
    print("\n💬 Enviando Telegram...")
    enviar_telegram(mensaje, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    
    # Resumen final
    print("\n" + "="*80)
    print("🎉 PROCESO COMPLETADO")
    print("="*80)
    print(f"✅ Normas procesadas: {len(aceptados)}")
    if aceptados and folder_id:
        print(f"📁 Carpeta Drive: {HOY.strftime('%Y-%m-%d')}")
    print("="*80)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
