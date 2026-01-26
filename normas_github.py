"""
=============================================================================
SISTEMA AUTOMATIZADO DE NORMAS DE HIDROCARBUROS
Versión GitHub Actions + Google Drive + Telegram
CON DETECCIÓN AUTOMÁTICA DE LUNES (fin de semana completo)
CON FILTRO ESPECIAL MINEM/OSINERGMIN
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

# Fechas y detección de lunes
HOY = date.today()
DIA_SEMANA = HOY.weekday()  # 0=Lun, 1=Mar, 2=Mié, 3=Jue, 4=Vie, 5=Sáb, 6=Dom

# Google Cloud (desde variables de entorno)
CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Determinar cuántos días revisar
if DIA_SEMANA == 0:  # Lunes
    DIAS_A_REVISAR = 3
    print("📅 MODO: LUNES - Revisando fin de semana completo (Sáb, Dom, Lun)")
else:
    DIAS_A_REVISAR = 1
    print("📅 MODO: DÍA NORMAL - Revisando solo hoy y ayer")

print(f"📅 HOY: {HOY.strftime('%d/%m/%Y')} ({['Lun','Mar','Mié','Jue','Vie','Sáb','Dom'][DIA_SEMANA]})")
print(f"🔍 Días a revisar: {DIAS_A_REVISAR}")
print("="*80)

# =============================================================================
# GOOGLE DRIVE CLIENT
# =============================================================================

class GoogleDriveClient:
    """Cliente para Google Drive y Sheets"""
    
    def __init__(self, credentials_json):
        # Decodificar credenciales desde base64
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

# *** SECTORES PRIORITARIOS - SIEMPRE SE DESCARGAN ***
SECTORES_PRIORITARIOS = set([normalizar_texto(x) for x in [
    'energia y minas',
    'energia minas',
    'minem',
    'ministerio de energia y minas',
    'ministerio de energia minas',
    'osinergmin',
    'organismo supervisor de la inversion en energia y mineria',
    'organismo supervisor inversion energia mineria',
    'supervision energia mineria'
]])

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
print(f"⭐ Sectores prioritarios: {len(SECTORES_PRIORITARIOS)}")

# =============================================================================
# EVALUACIÓN DE SECTOR PRIORITARIO
# =============================================================================

def es_sector_prioritario(sector):
    """Verifica si el sector es MINEM u OSINERGMIN"""
    sector_norm = normalizar_texto(sector)
    
    # Verificar coincidencia exacta
    for sector_prior in SECTORES_PRIORITARIOS:
        if sector_prior in sector_norm:
            return True, sector_prior
    
    return False, None

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
    """Extrae normas de una fecha específica"""
    
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
                
                # Sumilla y fecha
                p_tags = art.find_all("p")
                sumilla = ""
                fecha_pub = ""
                
                if p_tags:
                    b = p_tags[0].find("b")
                    if b:
                        fecha_pub = b.get_text(" ", strip=True).replace("Fecha:", "").strip()
                    
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
                    "fecha_busqueda": fecha_str,
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
    
    # Cargar corpus
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
    
    # Generar fechas a revisar
    print("\n📅 Generando fechas a revisar...")
    fechas_ordinarias = []
    fechas_extraordinarias = []
    
    for i in range(DIAS_A_REVISAR):
        fecha_ord = HOY - timedelta(days=i)
        fecha_ext = HOY - timedelta(days=i+1)
        
        fechas_ordinarias.append(fecha_ord)
        fechas_extraordinarias.append(fecha_ext)
        
        print(f"   {i+1}. Ordinaria: {fecha_ord.strftime('%d/%m/%Y')} | Extraordinaria: {fecha_ext.strftime('%d/%m/%Y')}")
    
    # Selenium
    print("\n🌐 Iniciando navegador...")
    driver = crear_driver()
    
    # Extraer normas
    print("\n📰 Extrayendo normas...")
    todos_candidatos = []
    
    # Extraer ordinarias
    for fecha in fechas_ordinarias:
        candidatos = extraer_normas(driver, fecha, es_extraordinaria=False)
        todos_candidatos.extend(candidatos)
        time.sleep(2)
    
    # Extraer extraordinarias
    for fecha in fechas_extraordinarias:
        candidatos = extraer_normas(driver, fecha, es_extraordinaria=True)
        todos_candidatos.extend(candidatos)
        time.sleep(2)
    
    driver.quit()
    print("\n✅ Navegador cerrado")
    
    # Deduplicar
    print("\n🔄 Deduplicando normas...")
    vistos = set()
    candidatos_unicos = []
    
    for c in todos_candidatos:
        key = (c['titulo'].strip().lower(), c.get('fecha_pub', ''))
        if key not in vistos and key[0]:
            vistos.add(key)
            candidatos_unicos.append(c)
    
    duplicados = len(todos_candidatos) - len(candidatos_unicos)
    print(f"   Total extraído: {len(todos_candidatos)}")
    print(f"   Duplicados eliminados: {duplicados}")
    print(f"   📊 Candidatos únicos: {len(candidatos_unicos)}")
    
    # *** DEBUG: Ver tipos extraídos ***
    print("\n🔍 DEBUG - Análisis de tipos extraídos:")
    tipos_count = {}
    for c in candidatos_unicos:
        tipo = c.get('tipo', 'SIN_TIPO')
        tipos_count[tipo] = tipos_count.get(tipo, 0) + 1
    
    for tipo, count in tipos_count.items():
        print(f"   {tipo}: {count} normas")
    
    # *** FILTRADO CON LÓGICA ESPECIAL PARA SECTORES PRIORITARIOS ***
    print("\n🔬 Filtrando relevancia...")
    aceptados = []
    prioritarios = []
    
    for c in candidatos_unicos:
        # 🔍 DEBUG: Ver cada norma extraordinaria antes del filtro
        if c.get('tipo') == "Extraordinaria":
            print(f"\n🔍 Procesando EXTRAORDINARIA:")
            print(f"   Título: {c['titulo'][:70]}")
            print(f"   Sector: {c['sector']}")
            print(f"   Tipo almacenado: '{c.get('tipo')}'")
        
        # VERIFICAR SI ES SECTOR PRIORITARIO
        es_prioritario, sector_match = es_sector_prioritario(c['sector'])
        
        if es_prioritario:
            # ⭐ SECTOR PRIORITARIO - SE ACEPTA AUTOMÁTICAMENTE
            aceptados.append(c)
            prioritarios.append(c)
            tipo_label = f" [{c.get('tipo', 'N/A')}]"
            print(f"   ⭐ PRIORITARIO{tipo_label}: {c['titulo'][:60]} ({sector_match})")
        else:
            # Aplicar filtro de relevancia normal
            relevante, razon = evaluar_relevancia(
                c['texto_completo'],
                vectorizador,
                X_base
            )
            
            if relevante:
                aceptados.append(c)
                tipo_label = f" [{c.get('tipo', 'N/A')}]"
                print(f"   ✅{tipo_label} {c['titulo'][:60]}")
            else:
                # 🔍 DEBUG: Ver por qué se rechazó una extraordinaria
                if c.get('tipo') == "Extraordinaria":
                    print(f"   ❌ RECHAZADA: {razon}")
    
    print(f"\n✅ Normas relevantes: {len(aceptados)}")
    print(f"   ⭐ De sectores prioritarios: {len(prioritarios)}")
    print(f"   🔍 Por filtro de relevancia: {len(aceptados) - len(prioritarios)}")
    
    # 🔍 DEBUG: Ver tipos en aceptados
    print("\n🔍 DEBUG - Tipos en normas aceptadas:")
    tipos_aceptados = {}
    for norma in aceptados:
        tipo = norma.get('tipo', 'SIN_TIPO')
        tipos_aceptados[tipo] = tipos_aceptados.get(tipo, 0) + 1
    
    for tipo, count in tipos_aceptados.items():
        print(f"   {tipo}: {count} normas")
    
    # Crear carpeta y descargar PDFs
    folder_id = None
    if aceptados:
        print("\n📥 Descargando PDFs...")
        
        folder_name = HOY.strftime("%Y-%m-%d")
        folder_id = drive_client.create_folder(DRIVE_FOLDER_ID, folder_name)
        
        if folder_id:
            for norma in aceptados:
                try:
                    response = requests.get(norma['pdf_url'], timeout=30)
                    if response.status_code == 200:
                        filename = re.sub(r'[^\w\s-]', '', norma['titulo'][:100])
                        filename = re.sub(r'\s+', '_', filename) + '.pdf'
                        
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
                norma.get('tipo', 'N/A')
            ])
        
        drive_client.append_to_sheet(SPREADSHEET_ID, 'A:F', rows)
        print(f"   ✅ {len(rows)} filas agregadas")
    
    # Actualizar corpus
    if aceptados:
        print("\n🧠 Actualizando corpus...")
        nuevo_contenido = "\n".join([n['texto_completo'] for n in aceptados])
        corpus_actualizado = texto_base + "\n" + nuevo_contenido
        drive_client.upload_text_file(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt', corpus_actualizado)
    
    # *** GENERAR MENSAJE TELEGRAM CON ETIQUETA (Extraordinaria) ***
    if aceptados:
        if DIA_SEMANA == 0:
            fecha_inicio = fechas_ordinarias[-1].strftime('%d/%m/%y')
            fecha_fin = HOY.strftime('%d/%m/%y')
            mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector del {fecha_inicio} al {fecha_fin}\n\n"
        else:
            mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector {HOY.strftime('%d/%m/%y')}\n\n"
        
        print("\n📱 Generando mensaje Telegram...")
        print("🔍 DEBUG - Verificando campo 'tipo' en cada norma:")
        
        for norma in aceptados:
            # 🔍 DEBUG: Verificar el campo tipo
            tipo_raw = norma.get('tipo')
            print(f"\n   Norma: {norma['titulo'][:50]}")
            print(f"   Campo 'tipo': '{tipo_raw}'")
            print(f"   Es None: {tipo_raw is None}")
            print(f"   Comparación == 'Extraordinaria': {tipo_raw == 'Extraordinaria'}")
            
            # Determinar etiqueta de tipo
            tipo_etiqueta = ""
            if tipo_raw and str(tipo_raw).strip().lower() == "extraordinaria":
                tipo_etiqueta = " (Extraordinaria)"
                print(f"   ✅ SE MARCARÁ COMO EXTRAORDINARIA")
            else:
                print(f"   ℹ️ No se marca (tipo='{tipo_raw}')")
            
            # Construir mensaje
            mensaje += f"<b>{norma['titulo']}{tipo_etiqueta}</b>\n"
            mensaje += f"{norma['sumilla']}\n\n"
    else:
        if DIA_SEMANA == 0:
            fecha_inicio = fechas_ordinarias[-1].strftime('%d/%m/%y')
            fecha_fin = HOY.strftime('%d/%m/%y')
            mensaje = (
                f"Buen día equipo, el día de hoy no se encontraron normas relevantes del sector.\n\n"
                f"📅 Periodo revisado: del {fecha_inicio} al {fecha_fin}\n"
                f"   • Sábado {fechas_extraordinarias[-1].strftime('%d/%m/%y')} (Extraordinaria)\n"
                f"   • Domingo {fechas_extraordinarias[-2].strftime('%d/%m/%y')} (Extraordinaria)\n"
                f"   • Lunes {HOY.strftime('%d/%m/%y')} (Ordinaria)"
            )
        else:
            ayer = HOY - timedelta(days=1)
            mensaje = (
                f"Buen día equipo, el día de hoy no se encontraron normas relevantes del sector.\n\n"
                f"📅 Extraordinaria {ayer.strftime('%d/%m/%y')}\n"
                f"📅 Ordinaria {HOY.strftime('%d/%m/%y')}"
            )
    
    # 🔍 DEBUG: Ver mensaje final
    print("\n🔍 DEBUG - Mensaje final a enviar:")
    print("="*80)
    print(mensaje)
    print("="*80)
    
    # Enviar Telegram
    print("\n💬 Enviando Telegram...")
enviar_telegram(mensaje, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

# Resumen final
print("\n" + "="*80)
print("🎉 PROCESO COMPLETADO")
print("="*80)
print(f"✅ Normas procesadas: {len(aceptados)}")
print(f"   ⭐ Prioritarias (MINEM/OSINERGMIN): {len(prioritarios)}")
print(f"   🔍 Por relevancia: {len(aceptados) - len(prioritarios)}")
print(f"📁 Carpeta Drive: {folder_name if aceptados else 'N/A'}")
if DIA_SEMANA == 0:
    print(f"📅 Modo: LUNES (revisó {DIAS_A_REVISAR} días)")
print("="*80)
