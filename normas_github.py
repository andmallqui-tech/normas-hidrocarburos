"""
=============================================================================
SISTEMA AUTOMATIZADO DE NORMAS DE HIDROCARBUROS
Versión GitHub Actions CORREGIDA con Debug Exhaustivo
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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

print("="*80)
print("🚀 SISTEMA DE NORMAS - GITHUB ACTIONS [VERSIÓN DEBUG]")
print("="*80)

# Fechas y detección de lunes
HOY = date.today()
DIA_SEMANA = HOY.weekday()

# Google Cloud
CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Determinar cuántos días revisar
if DIA_SEMANA == 0:
    DIAS_A_REVISAR = 3
    print("📅 MODO: LUNES - Revisando fin de semana completo")
else:
    DIAS_A_REVISAR = 1
    print("📅 MODO: DÍA NORMAL")

print(f"📅 HOY: {HOY.strftime('%d/%m/%Y')} ({['Lun','Mar','Mié','Jue','Vie','Sáb','Dom'][DIA_SEMANA]})")
print(f"🔍 Días a revisar: {DIAS_A_REVISAR}")
print("="*80)

# =============================================================================
# GOOGLE DRIVE CLIENT
# =============================================================================

class GoogleDriveClient:
    """Cliente para Google Drive y Sheets"""
    
    def __init__(self, credentials_json):
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
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = results.get('files', [])
            return files[0]['id'] if files else None
        except Exception as e:
            print(f"⚠️ Error buscando archivo: {e}")
            return None
    
    def download_text_file(self, file_id):
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            content = fh.getvalue().decode('utf-8')
            print(f"   📊 Corpus descargado: {len(content)} caracteres, {len(content.split())} palabras")
            print(f"   📋 Preview: {content[:200]}...")
            return content
        except Exception as e:
            print(f"⚠️ Error descargando: {e}")
            return ""
    
    def upload_text_file(self, folder_id, filename, content):
        try:
            file_metadata = {'name': filename, 'parents': [folder_id], 'mimeType': 'text/plain'}
            media = MediaIoBaseUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/plain', resumable=True)
            existing_id = self.get_file_by_name(folder_id, filename)
            
            if existing_id:
                self.drive_service.files().update(fileId=existing_id, media_body=media).execute()
                print(f"   ✅ Corpus actualizado: {len(content)} chars, {len(content.split())} palabras")
            else:
                self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                print(f"   ✅ Corpus creado: {len(content)} chars")
            return True
        except Exception as e:
            print(f"❌ Error subiendo {filename}: {e}")
            return False
    
    def upload_pdf(self, folder_id, filename, pdf_bytes):
        try:
            file_metadata = {'name': filename, 'parents': [folder_id], 'mimeType': 'application/pdf'}
            media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype='application/pdf', resumable=True)
            file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
            return file.get('webViewLink', '')
        except Exception as e:
            print(f"❌ Error subiendo PDF: {e}")
            return None
    
    def create_folder(self, parent_id, folder_name):
        try:
            existing_id = self.get_file_by_name(parent_id, folder_name)
            if existing_id:
                return existing_id
            file_metadata = {'name': folder_name, 'parents': [parent_id], 'mimeType': 'application/vnd.google-apps.folder'}
            folder = self.drive_service.files().create(body=file_metadata, fields='id').execute()
            return folder.get('id')
        except Exception as e:
            print(f"❌ Error creando carpeta: {e}")
            return None
    
    def append_to_sheet(self, spreadsheet_id, range_name, values):
        try:
            body = {'values': values}
            result = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body
            ).execute()
            return result
        except Exception as e:
            print(f"❌ Error actualizando Sheets: {e}")
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
        print(f"❌ Error Telegram: {e}")
        return False

# =============================================================================
# NORMALIZACIÓN Y KEYWORDS
# =============================================================================

def normalizar_texto(texto):
    if not isinstance(texto, str):
        return ""
    texto = texto.lower()
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    texto = re.sub(r'[^a-z0-9\s]', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

SECTORES_PRIORITARIOS = set([normalizar_texto(x) for x in [
    'energia y minas', 'energia minas', 'minem',
    'ministerio de energia y minas', 'osinergmin',
    'organismo supervisor de la inversion en energia y mineria'
]])

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

tokens_tecnicos = set()
for kw in KEYWORDS_MANUAL:
    for token in kw.split():
        if len(token) > 2:
            tokens_tecnicos.add(token)

print(f"🧠 Keywords: {len(KEYWORDS_MANUAL)}, Tokens: {len(tokens_tecnicos)}, Prioritarios: {len(SECTORES_PRIORITARIOS)}")

# =============================================================================
# FUNCIONES DE EVALUACIÓN
# =============================================================================

def es_sector_prioritario(sector):
    sector_norm = normalizar_texto(sector)
    for sector_prior in SECTORES_PRIORITARIOS:
        if sector_prior in sector_norm:
            return True, sector_prior
    return False, None

def evaluar_relevancia(texto_candidato, vectorizador, X_base):
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
    
    relevante = count_tokens >= 3 or (count_tokens >= 2 and tfidf_score >= 0.15)
    razon = f"✅ {count_tokens} términos, TF-IDF:{tfidf_score:.3f}, palabra:{palabra_encontrada}" if relevante else "❌ Insuficiente"
    return relevante, razon

# =============================================================================
# SELENIUM CON DEBUG MEJORADO
# =============================================================================

def crear_driver():
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
    tipo = "Extraordinaria" if es_extraordinaria else "Ordinaria"
    fecha_str = fecha_obj.strftime("%d/%m/%Y")
    
    print(f"\n{'='*80}")
    print(f"🔍 EXTRAYENDO {tipo} del {fecha_str}")
    print(f"{'='*80}")
    
    try:
        # Paso 1: Cargar página
        print("1️⃣ Cargando página...")
        driver.get("https://diariooficial.elperuano.pe/Normas")
        time.sleep(5)
        print(f"   ✅ URL actual: {driver.current_url}")
        
        # Paso 2: Configurar fechas
        print(f"2️⃣ Configurando fechas: {fecha_str}")
        driver.execute_script(f"""
            document.getElementById('cddesde').value = '{fecha_str}';
            document.getElementById('cdhasta').value = '{fecha_str}';
        """)
        time.sleep(1)
        
        # Verificar que se configuró
        desde = driver.execute_script("return document.getElementById('cddesde').value;")
        hasta = driver.execute_script("return document.getElementById('cdhasta').value;")
        print(f"   ✅ Desde: {desde}, Hasta: {hasta}")
        
        # Paso 3: Checkbox
        if es_extraordinaria:
            print("3️⃣ Marcando EXTRAORDINARIA...")
            driver.execute_script("document.getElementById('tipo').checked = true;")
        else:
            print("3️⃣ Desmarcando (ORDINARIA)...")
            driver.execute_script("document.getElementById('tipo').checked = false;")
        
        checked = driver.execute_script("return document.getElementById('tipo').checked;")
        print(f"   ✅ Checkbox extraordinaria: {checked}")
        
        # Paso 4: Hacer clic en buscar
        print("4️⃣ Haciendo clic en BUSCAR...")
        driver.execute_script("document.getElementById('btnBuscar').click();")
        time.sleep(10)
        print(f"   ✅ URL después de buscar: {driver.current_url}")
        
        # Paso 5: Scroll para cargar contenido dinámico
        print("5️⃣ Cargando contenido con scroll...")
        last_count = 0
        stable_count = 0
        
        for i in range(40):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            # SELECTORES ACTUALIZADOS - Buscar múltiples patrones
            articles = (
                soup.find_all("article", class_=lambda c: c and "articulo" in str(c).lower()) +
                soup.find_all("div", class_=lambda c: c and "articulo" in str(c).lower())
            )
            
            count = len(articles)
            print(f"   Scroll {i+1}/40: {count} artículos detectados")
            
            if count == last_count:
                stable_count += 1
            else:
                stable_count = 0
                last_count = count
            
            if stable_count >= 3:
                print("   ✅ Contenido estabilizado")
                break
        
        # Paso 6: Parsear con selectores mejorados
        print("6️⃣ Parseando artículos...")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # INTENTAR MÚLTIPLES SELECTORES
        print("   🔍 Intentando selector 1: article con 'articulo'...")
        articles = soup.find_all("article", class_=lambda c: c and "articulo" in str(c).lower())
        print(f"      → Encontrados: {len(articles)}")
        
        if not articles:
            print("   🔍 Intentando selector 2: div con 'item' o 'norma'...")
            articles = soup.find_all("div", class_=lambda c: c and ("item" in str(c).lower() or "norma" in str(c).lower()))
            print(f"      → Encontrados: {len(articles)}")
        
        if not articles:
            print("   🔍 Intentando selector 3: cualquier contenedor con h4 y h5...")
            all_containers = soup.find_all(["article", "div", "section"])
            articles = [c for c in all_containers if c.find("h4") and c.find("h5")]
            print(f"      → Encontrados: {len(articles)}")
        
        if not articles:
            print("   ⚠️ NO SE ENCONTRARON ARTÍCULOS")
            print(f"   📋 Guardando HTML para debug...")
            with open(f"debug_{tipo}_{fecha_str.replace('/', '-')}.html", 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print(f"   ✅ HTML guardado en: debug_{tipo}_{fecha_str.replace('/', '-')}.html")
            return []
        
        print(f"   ✅ Total artículos encontrados: {len(articles)}")
        
        # Paso 7: Extraer datos de cada artículo
        print("7️⃣ Extrayendo datos...")
        candidatos = []
        
        for idx, art in enumerate(articles, 1):
            try:
                # Sector (h4, h3, o span.sector)
                sector = ""
                sector_tag = art.find("h4") or art.find("h3") or art.find(class_=lambda c: c and "sector" in str(c).lower())
                if sector_tag:
                    sector = sector_tag.get_text(" ", strip=True)
                
                # Título (h5, h2, o enlace principal)
                titulo = ""
                titulo_tag = art.find("h5") or art.find("h2")
                if titulo_tag:
                    link = titulo_tag.find("a")
                    titulo = link.get_text(" ", strip=True) if link else titulo_tag.get_text(" ", strip=True)
                
                # Fecha y sumilla de los párrafos
                p_tags = art.find_all("p")
                fecha_pub = ""
                sumilla = ""
                
                for p in p_tags:
                    texto = p.get_text(" ", strip=True)
                    if "fecha:" in texto.lower():
                        fecha_pub = texto.replace("Fecha:", "").replace("fecha:", "").strip()
                    elif len(texto) > 30:  # Asumimos que la sumilla es el párrafo más largo
                        sumilla = texto
                
                # PDF URL - Buscar en inputs, enlaces o atributos data
                pdf_url = ""
                
                # Opción 1: Input con data-url
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
                
                # Opción 2: Enlace con href que contenga .pdf
                if not pdf_url:
                    for a in art.find_all("a", href=True):
                        if ".pdf" in a['href'].lower():
                            href = a['href']
                            if href.startswith("//"):
                                pdf_url = "https:" + href
                            elif href.startswith("/"):
                                pdf_url = "https://diariooficial.elperuano.pe" + href
                            elif href.startswith("http"):
                                pdf_url = href
                            break
                
                # Si no hay PDF, skip
                if not pdf_url:
                    print(f"   ⚠️ Artículo {idx} sin PDF - omitiendo")
                    continue
                
                # Debug del primer artículo
                if idx == 1:
                    print(f"\n   📋 DEBUG PRIMER ARTÍCULO:")
                    print(f"      Sector: {sector[:50]}")
                    print(f"      Título: {titulo[:50]}")
                    print(f"      Fecha: {fecha_pub}")
                    print(f"      Sumilla: {sumilla[:50]}")
                    print(f"      PDF: {pdf_url[:60]}")
                
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
                print(f"   ⚠️ Error en artículo {idx}: {e}")
                continue
        
        print(f"8️⃣ Candidatos extraídos: {len(candidatos)}")
        print(f"{'='*80}\n")
        return candidatos
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        return []

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "="*80)
    print("🚀 INICIANDO PROCESO")
    print("="*80)
    
    # Conectar Drive
    print("\n📁 Conectando a Google Drive...")
    drive_client = GoogleDriveClient(CREDENTIALS_JSON)
    
    # Cargar corpus
    print("\n🧠 Cargando corpus de aprendizaje...")
    corpus_file_id = drive_client.get_file_by_name(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt')
    
    if corpus_file_id:
        texto_base = drive_client.download_text_file(corpus_file_id)
    else:
        print("   📝 Corpus no existe, creando inicial...")
        texto_base = " ".join(KEYWORDS_MANUAL * 3)
        drive_client.upload_text_file(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt', texto_base)
    
    # Vectorizador
    print("\n🤖 Inicializando vectorizador TF-IDF...")
    vectorizador = TfidfVectorizer(lowercase=True, ngram_range=(1,2), max_features=2000)
    vectorizador.fit([texto_base])
    X_base = vectorizador.transform([texto_base])
    print(f"   ✅ Vocabulario: {len(vectorizador.vocabulary_)} términos")
    
    # Generar fechas
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
    print("\n🌐 Iniciando navegador Chrome...")
    driver = crear_driver()
    print("   ✅ Navegador iniciado")
    
    # Extraer normas
    print("\n📰 INICIANDO EXTRACCIÓN DE NORMAS...")
    todos_candidatos = []
    
    # Ordinarias
    for fecha in fechas_ordinarias:
        candidatos = extraer_normas(driver, fecha, es_extraordinaria=False)
        todos_candidatos.extend(candidatos)
        time.sleep(3)
    
    # Extraordinarias
    for fecha in fechas_extraordinarias:
        candidatos = extraer_normas(driver, fecha, es_extraordinaria=True)
        todos_candidatos.extend(candidatos)
        time.sleep(3)
    
    driver.quit()
    print("\n✅ Navegador cerrado")
    
    # Deduplicar
    print("\n🔄 Deduplicando...")
    vistos = set()
    candidatos_unicos = []
    
    for c in todos_candidatos:
        key = (c['titulo'].strip().lower(), c.get('fecha_pub', ''))
        if key not in vistos and key[0]:
            vistos.add(key)
            candidatos_unicos.append(c)
    
    print(f"   Total extraído: {len(todos_candidatos)}")
    print(f"   Duplicados: {len(todos_candidatos) - len(candidatos_unicos)}")
    print(f"   ✅ Únicos: {len(candidatos_unicos)}")
    
    # Filtrado
    print("\n🔬 Filtrando relevancia...")
    aceptados = []
    prioritarios = []
    
    for c in candidatos_unicos:
        es_prioritario, sector_match = es_sector_prioritario(c['sector'])
        
        if es_prioritario:
            aceptados.append(c)
            prioritarios.append(c)
            print(f"   ⭐ PRIORITARIO: {c['titulo'][:60]} (Sector: {sector_match})")
        else:
            relevante, razon = evaluar_relevancia(c['texto_completo'], vectorizador, X_base)
            if relevante:
                aceptados.append(c)
                print(f"   ✅ RELEVANTE: {c['titulo'][:60]}")
                print(f"      └─ {razon}")
    
    print(f"\n✅ Normas relevantes: {len(aceptados)}")
    print(f"   ⭐ Prioritarias: {len(prioritarios)}")
    print(f"   🔍 Por filtro: {len(aceptados) - len(prioritarios)}")
    
    # Descargar PDFs
    folder_id = None
    if aceptados:
        print("\n📥 Descargando PDFs...")
        folder_name = HOY.strftime("%Y-%m-%d")
        folder_id = drive_client.create_folder(DRIVE_FOLDER_ID, folder_name)
        
        if folder_id:
            for i, norma in enumerate(aceptados, 1):
                try:
                    print(f"   [{i}/{len(aceptados)}] Descargando: {norma['titulo'][:50]}...")
                    response = requests.get(norma['pdf_url'], timeout=30)
                    
                    if response.status_code == 200:
                        filename = re.sub(r'[^\w\s-]', '', norma['titulo'][:100])
                        filename = re.sub(r'\s+', '_', filename) + '.pdf'
                        link = drive_client.upload_pdf(folder_id, filename, response.content)
                        norma['drive_link'] = link if link else norma['pdf_url']
                        print(f"      ✅ Subido: {filename[:40]}")
                    else:
                        print(f"      ⚠️ HTTP {response.status_code}")
                        norma['drive_link'] = norma['pdf_url']
                        
                except Exception as e:
                    print(f"      ❌ Error: {e}")
                    norma['drive_link'] = norma['pdf_url']
    
    # Google Sheets
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
    
    # Telegram
    if aceptados:
        if DIA_SEMANA == 0:
            fecha_inicio = fechas_ordinarias[-1].strftime('%d/%m/%y')
            fecha_fin = HOY.strftime('%d/%m/%y')
            mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector del {fecha_inicio} al {fecha_fin}\n\n"
        else:
            mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector {HOY.strftime('%d/%m/%y')}\n\n"
        
        print("\n📱 Generando mensaje Telegram...")
        for norma in aceptados:
            tipo_etiqueta = ""
            if str(norma.get('tipo', '')).strip().lower() == "extraordinaria":
                tipo_etiqueta = " (Extraordinaria)"
            
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
    
    print("\n💬 Enviando Telegram...")
    enviar_telegram(mensaje, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    
    # Resumen final
    print("\n" + "="*80)
    print("🎉 PROCESO COMPLETADO")
    print("="*80)
    print(f"📊 Total candidatos extraídos: {len(todos_candidatos)}")
    print(f"📊 Candidatos únicos: {len(candidatos_unicos)}")
    print(f"✅ Normas procesadas: {len(aceptados)}")
    print(f"   ⭐ Prioritarias (MINEM/OSINERGMIN): {len(prioritarios)}")
    print(f"   🔍 Por relevancia: {len(aceptados) - len(prioritarios)}")
    if aceptados:
        print(f"📁 Carpeta Drive: {folder_name}")
        print(f"📊 Google Sheets: {len(aceptados)} filas agregadas")
        print(f"🧠 Corpus actualizado con {len(aceptados)} normas nuevas")
    if DIA_SEMANA == 0:
        print(f"📅 Modo: LUNES (revisó {DIAS_A_REVISAR} días)")
    print("="*80)
    
    # Mostrar detalle de normas aceptadas
    if aceptados:
        print(f"\n📋 NORMAS ACEPTADAS ({len(aceptados)}):")
        print("="*80)
        for i, norma in enumerate(aceptados, 1):
            tipo_label = "⭐ PRIORITARIA" if norma in prioritarios else "🔍 FILTRADA"
            print(f"{i}. [{tipo_label}] [{norma['tipo']}]")
            print(f"   {norma['titulo'][:70]}")
            print(f"   Sector: {norma['sector'][:50]}")
            print()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
