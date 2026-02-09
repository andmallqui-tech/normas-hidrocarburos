"""
=============================================================================
SISTEMA AUTOMATIZADO DE NORMAS - VERSIÓN CORREGIDA PARA GITHUB ACTIONS
=============================================================================
Basado en la lógica funcional del código de Visual Studio Code
Correcciones principales:
1. Scroll inteligente con detección de estabilidad
2. Selectores específicos de artículos
3. Búsqueda mejorada de PDF URL
4. Estructura de datos estandarizada
5. Lógica correcta de fechas para lunes
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
print("🚀 SISTEMA DE NORMAS - VERSIÓN CORREGIDA PARA GITHUB")
print("="*100)

HOY = date.today()
DIA_SEMANA = HOY.weekday()

CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# CORRECCIÓN 1: Lógica correcta de días a revisar
# Lunes (0): revisa Viernes (2 días), Sábado (1 día) y Domingo (0 días) = 3 ediciones ordinarias
# Otros días: revisa solo ayer (1 día) = 1 edición ordinaria
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
            scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
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
            
            palabras = len(content.split())
            lineas = len(content.split('\n'))
            
            print(f"   ✅ CORPUS DESCARGADO EXITOSAMENTE:")
            print(f"      📊 Tamaño: {len(content)} caracteres")
            print(f"      📊 Palabras: {palabras}")
            print(f"      📊 Líneas: {lineas}")
            print(f"      📋 PRIMEROS 300 CARACTERES:")
            print(f"      {content[:300]}...")
            
            return content
        except Exception as e:
            print(f"   ❌ Error descargando: {e}")
            return ""
    
    def upload_text_file(self, folder_id, filename, content):
        try:
            print(f"\n💾 SUBIENDO/ACTUALIZANDO: {filename}")
            print(f"   📊 Tamaño: {len(content)} chars, {len(content.split())} palabras")
            
            file_metadata = {'name': filename, 'parents': [folder_id], 'mimeType': 'text/plain'}
            media = MediaIoBaseUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/plain', resumable=True)
            existing_id = self.get_file_by_name(folder_id, filename)
            
            if existing_id:
                self.drive_service.files().update(fileId=existing_id, media_body=media).execute()
                print(f"   ✅ CORPUS ACTUALIZADO EN DRIVE (ID: {existing_id})")
            else:
                file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                print(f"   ✅ CORPUS CREADO EN DRIVE (ID: {file.get('id')})")
            
            return True
        except Exception as e:
            print(f"   ❌ Error subiendo: {e}")
            return False
    
    def upload_pdf(self, folder_id, filename, pdf_bytes):
        try:
            print(f"\n📤 SUBIENDO PDF: {filename}")
            print(f"   📊 Tamaño: {len(pdf_bytes) / 1024:.2f} KB")
            
            file_metadata = {'name': filename, 'parents': [folder_id], 'mimeType': 'application/pdf'}
            media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype='application/pdf', resumable=True)
            file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
            
            link = file.get('webViewLink', '')
            print(f"   ✅ PDF SUBIDO EXITOSAMENTE")
            print(f"   🔗 Link: {link}")
            
            return link
        except Exception as e:
            print(f"   ❌ ERROR SUBIENDO PDF: {e}")
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
            
            file_metadata = {'name': folder_name, 'parents': [parent_id], 'mimeType': 'application/vnd.google-apps.folder'}
            folder = self.drive_service.files().create(body=file_metadata, fields='id').execute()
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
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption='RAW', insertDataOption='INSERT_ROWS', body=body
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

print(f"\n🧠 CONFIGURACIÓN DE FILTRADO:")
print(f"   Keywords manuales: {len(KEYWORDS_MANUAL)}")
print(f"   Tokens técnicos: {len(tokens_tecnicos)}")
print(f"   Sectores prioritarios: {len(SECTORES_PRIORITARIOS)}")
print(f"   Palabras obligatorias: {len(PALABRAS_OBLIGATORIAS)}")

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
    
    for sector in SECTORES_EXCLUIR:
        if sector in texto_norm:
            return False, f"Sector excluido: {sector}"
    
    tiene_obligatoria = False
    palabra_encontrada = None
    for palabra in PALABRAS_OBLIGATORIAS:
        if palabra in texto_norm:
            tiene_obligatoria = True
            palabra_encontrada = palabra
            break
    
    if not tiene_obligatoria:
        return False, "Sin palabra obligatoria"
    
    count_tokens = sum(1 for token in tokens_tecnicos if token in texto_norm)
    
    try:
        Y = vectorizador.transform([texto_norm])
        tfidf_score = float(cosine_similarity(X_base, Y)[0][0])
    except:
        tfidf_score = 0.0
    
    relevante = count_tokens >= 3 or (count_tokens >= 2 and tfidf_score >= 0.15)
    razon = f"✅ {count_tokens} términos, TF-IDF:{tfidf_score:.3f}" if relevante else "❌ Insuficiente"
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
    """Completa URL relativa a absoluta - FUNCIÓN CRÍTICA"""
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
    """Limpia nombre para usar como archivo"""
    nombre = re.sub(r'[<>:"/\\|?*\n\r\t]', '', nombre)
    nombre = re.sub(r'\s+', '_', nombre.strip())
    return nombre[:150]

# =============================================================================
# SELENIUM - EXTRACCIÓN PRINCIPAL (VERSIÓN CORREGIDA)
# =============================================================================

def extraer_normas(driver, fecha_obj, es_extraordinaria=False):
    """
    FUNCIÓN CORREGIDA - Extrae normas usando la lógica exacta del código de VS Code
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
        
        print(f"3️⃣ Configurando checkbox extraordinaria: {es_extraordinaria}")
        if es_extraordinaria:
            driver.execute_script("document.getElementById('tipo').checked = true;")
        else:
            driver.execute_script("document.getElementById('tipo').checked = false;")
        
        time.sleep(1)
        
        print("4️⃣ Ejecutando búsqueda...")
        driver.execute_script("document.getElementById('btnBuscar').click();")
        time.sleep(10)
        
        # CORRECCIÓN: SCROLL CON DETECCIÓN DE ESTABILIDAD (igual que VS Code)
        print("5️⃣ Cargando contenido con scroll inteligente...")
        last_count = -1
        stable = 0
        max_scrolls = 40
        
        for i in range(max_scrolls):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)  # Mismo timing que VS Code
            
            # CORRECCIÓN: SELECTOR ESPECÍFICO DE ARTÍCULOS (igual que VS Code)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            articles = soup.find_all("article", class_=lambda c: c and "edicionesoficiales_articulos" in c)
            count = len(articles)
            
            print(f"   Scroll {i+1}/{max_scrolls}: {count} artículos")
            
            # Detectar estabilidad (igual que VS Code)
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
                # Extraer sector (igual que VS Code)
                sector = ""
                sector_tag = art.find("h4")
                if sector_tag:
                    sector = sector_tag.get_text(" ", strip=True)
                
                # Extraer título (igual que VS Code)
                titulo = ""
                titulo_tag = art.find("h5")
                if titulo_tag:
                    link = titulo_tag.find("a")
                    titulo = link.get_text(" ", strip=True) if link else titulo_tag.get_text(" ", strip=True)
                
                # Extraer fecha y sumilla (igual que VS Code)
                p_tags = art.find_all("p")
                fecha_pub = ""
                sumilla = ""
                
                for p in p_tags:
                    texto = p.get_text(" ", strip=True)
                    if "fecha:" in texto.lower():
                        fecha_pub = texto.replace("Fecha:", "").replace("fecha:", "").strip()
                    elif len(texto) > 30:
                        sumilla = texto
                
                # CORRECCIÓN: BÚSQUEDA MEJORADA DE PDF URL (igual que VS Code)
                pdf_url = ""
                for inp in art.find_all("input"):
                    if inp.has_attr("data-url"):
                        val = (inp.get("value", "") or "").lower()
                        if "descarga individual" in val or "descarga" in val:
                            pdf_url = complete_href(inp['data-url'])
                            break
                        if not pdf_url:
                            pdf_url = complete_href(inp['data-url'])
                
                # Fallback: buscar en enlaces
                if not pdf_url:
                    for a in art.find_all("a", href=True):
                        if ".pdf" in a['href'].lower():
                            pdf_url = complete_href(a['href'])
                            break
                
                if not pdf_url:
                    continue
                
                # CORRECCIÓN: ESTRUCTURA DE DATOS ESTANDARIZADA (igual que VS Code)
                texto_completo = f"{sector} {titulo} {sumilla}"
                nombre_archivo = sanitize_filename(titulo or sumilla[:60]) + ".pdf"
                
                candidatos.append({
                    "sector": sector,
                    "titulo": titulo,
                    "FechaPublicacion": fecha_pub,
                    "Sumilla": sumilla,
                    "pdf_url": pdf_url,
                    "NombreArchivo": nombre_archivo,
                    "TipoEdicion": tipo_edicion,
                    "texto_completo": texto_completo
                })
                
                # Debug primer artículo
                if idx == 1:
                    print(f"\n   📋 DEBUG PRIMER ARTÍCULO:")
                    print(f"      Sector: {sector[:60]}")
                    print(f"      Título: {titulo[:60]}")
                    print(f"      PDF URL: {pdf_url[:80]}")
                    print(f"      Tipo: {tipo_edicion}")
                
            except Exception as e:
                print(f"   ⚠️ Error en artículo {idx}: {e}")
                continue
        
        print(f"\n8️⃣ CANDIDATOS EXTRAÍDOS: {len(candidatos)}")
        print(f"{'='*100}\n")
        
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
    print("\n" + "="*100)
    print("🚀 INICIANDO PROCESO PRINCIPAL")
    print("="*100)
    
    # DRIVE
    print("\n📁 PASO 1: CONECTAR A GOOGLE DRIVE")
    drive_client = GoogleDriveClient(CREDENTIALS_JSON)
    
    # CORPUS
    print("\n🧠 PASO 2: CARGAR CORPUS DE APRENDIZAJE")
    corpus_file_id = drive_client.get_file_by_name(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt')
    
    if corpus_file_id:
        print(f"   ✅ Corpus encontrado, descargando...")
        texto_base = drive_client.download_text_file(corpus_file_id)
    else:
        print("   📝 Corpus NO existe, creando inicial...")
        texto_base = " ".join(KEYWORDS_MANUAL * 3)
        drive_client.upload_text_file(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt', texto_base)
    
    # VECTORIZADOR
    print("\n🤖 PASO 3: INICIALIZAR VECTORIZADOR TF-IDF")
    vectorizador = TfidfVectorizer(lowercase=True, ngram_range=(1,2), max_features=2000)
    vectorizador.fit([texto_base])
    X_base = vectorizador.transform([texto_base])
    print(f"   ✅ Vocabulario: {len(vectorizador.vocabulary_)} términos")
    
    # CORRECCIÓN: FECHAS CORRECTAS (igual que VS Code)
    print("\n📅 PASO 4: GENERAR FECHAS A REVISAR")
    fechas_a_procesar = []
    
    # Generar pares (fecha, es_extraordinaria)
    # Los lunes: revisa viernes, sábado, domingo (ordinarias) + jueves, viernes, sábado (extraordinarias)
    # Otros días: revisa hoy (ordinaria) + ayer (extraordinaria)
    
    if DIA_SEMANA == 0:  # Lunes
        print("   📅 ES LUNES - Revisando múltiples días:")
        # Ordinarias: viernes(-3), sábado(-2), domingo(-1)
        for dias_atras in range(3, 0, -1):
            fecha = HOY - timedelta(days=dias_atras)
            fechas_a_procesar.append((fecha, False))
            print(f"      • Ordinaria: {fecha.strftime('%d/%m/%Y')}")
        
        # Extraordinarias: jueves(-4), viernes(-3), sábado(-2)
        for dias_atras in range(4, 1, -1):
            fecha = HOY - timedelta(days=dias_atras)
            fechas_a_procesar.append((fecha, True))
            print(f"      • Extraordinaria: {fecha.strftime('%d/%m/%Y')}")
    else:  # Martes a domingo
        print("   📅 DÍA NORMAL - Revisando hoy y ayer:")
        # Ordinaria de hoy
        fechas_a_procesar.append((HOY, False))
        print(f"      • Ordinaria: {HOY.strftime('%d/%m/%Y')}")
        
        # Extraordinaria de ayer
        ayer = HOY - timedelta(days=1)
        fechas_a_procesar.append((ayer, True))
        print(f"      • Extraordinaria: {ayer.strftime('%d/%m/%Y')}")
    
    # SELENIUM
    print("\n🌐 PASO 5: INICIAR NAVEGADOR")
    driver = crear_driver()
    print("   ✅ Navegador iniciado")
    
    # EXTRAER
    print("\n📰 PASO 6: EXTRAER NORMAS")
    todos_candidatos = []
    
    for i, (fecha, es_ext) in enumerate(fechas_a_procesar, 1):
        tipo = "EXTRAORDINARIA" if es_ext else "ORDINARIA"
        print(f"\n📋 6.{i} - EXTRAYENDO {tipo} DEL {fecha.strftime('%d/%m/%Y')}:")
        candidatos = extraer_normas(driver, fecha, es_extraordinaria=es_ext)
        print(f"   ✅ Extraídos: {len(candidatos)} candidatos")
        todos_candidatos.extend(candidatos)
        time.sleep(3)
    
    driver.quit()
    print("\n✅ Navegador cerrado")
    
    # DEDUPLICAR
    print("\n🔄 PASO 7: DEDUPLICAR")
    vistos = set()
    candidatos_unicos = []
    
    for c in todos_candidatos:
        key = (c['titulo'].strip().lower(), c.get('FechaPublicacion', ''))
        if key not in vistos and key[0]:
            vistos.add(key)
            candidatos_unicos.append(c)
    
    print(f"   Total extraído: {len(todos_candidatos)}")
    print(f"   ✅ Únicos: {len(candidatos_unicos)}")
    
    # FILTRAR
    print("\n🔬 PASO 8: FILTRAR RELEVANCIA")
    aceptados = []
    prioritarios = []
    
    for i, c in enumerate(candidatos_unicos, 1):
        es_prioritario, sector_match = es_sector_prioritario(c['sector'])
        
        if es_prioritario:
            aceptados.append(c)
            prioritarios.append(c)
            print(f"   [{i}/{len(candidatos_unicos)}] ⭐ PRIORITARIO: {c['titulo'][:50]}")
        else:
            relevante, razon = evaluar_relevancia(c['texto_completo'], vectorizador, X_base)
            if relevante:
                aceptados.append(c)
                print(f"   [{i}/{len(candidatos_unicos)}] ✅ RELEVANTE: {c['titulo'][:50]}")
    
    print(f"\n✅ TOTAL ACEPTADOS: {len(aceptados)}")
    
    # DESCARGAR PDFs
    folder_id = None
    if aceptados:
        print("\n📥 PASO 9: DESCARGAR PDFs")
        folder_name = HOY.strftime("%Y-%m-%d")
        folder_id = drive_client.create_folder(DRIVE_FOLDER_ID, folder_name)
        
        if folder_id:
            print(f"   ✅ Carpeta lista: {folder_name}")
            
            for i, norma in enumerate(aceptados, 1):
                try:
                    print(f"\n   [{i}/{len(aceptados)}] Procesando: {norma['titulo'][:40]}...")
                    
                    # CORRECCIÓN: DESCARGA MEJORADA CON STREAM (igual que VS Code)
                    response = requests.get(norma['pdf_url'], timeout=30, stream=True)
                    print(f"      HTTP Status: {response.status_code}")
                    
                    if response.status_code == 200:
                        content_type = response.headers.get('content-type', '')
                        
                        # Verificar que es PDF
                        if 'pdf' in content_type.lower() or len(response.content) > 1000:
                            filename = norma['NombreArchivo']
                            
                            # Subir a Drive
                            link = drive_client.upload_pdf(folder_id, filename, response.content)
                            norma['drive_link'] = link if link else norma['pdf_url']
                        else:
                            print(f"      ⚠️ No es PDF válido")
                            norma['drive_link'] = norma['pdf_url']
                    else:
                        print(f"      ❌ HTTP {response.status_code}")
                        norma['drive_link'] = norma['pdf_url']
                        
                except Exception as e:
                    print(f"      ❌ Error: {e}")
                    norma['drive_link'] = norma['pdf_url']
    
    # Google Sheets - CORRECCIÓN: USAR NOMBRES CORRECTOS (igual que VS Code)
    if aceptados:
        print("\n📊 Actualizando Google Sheets...")
        rows = []
        for norma in aceptados:
            rows.append([
                HOY.strftime("%Y-%m-%d"),
                norma['titulo'],
                norma.get('FechaPublicacion', ''),
                norma.get('Sumilla', ''),
                norma.get('drive_link', ''),
                norma.get('TipoEdicion', '')
            ])
        drive_client.append_to_sheet(SPREADSHEET_ID, 'A:F', rows)
        print(f"   ✅ {len(rows)} filas agregadas")
    
    # Actualizar corpus
    if aceptados:
        print("\n🧠 Actualizando corpus...")
        nuevo_contenido = "\n".join([n['texto_completo'] for n in aceptados])
        corpus_actualizado = texto_base + "\n" + nuevo_contenido
        drive_client.upload_text_file(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt', corpus_actualizado)
    
    # Telegram - CORRECCIÓN: Lógica correcta de mensajes
    if aceptados:
        if DIA_SEMANA == 0:
            # Lunes: mostrar rango de fechas
            fecha_inicio = (HOY - timedelta(days=3)).strftime('%d/%m/%y')
            fecha_fin = HOY.strftime('%d/%m/%y')
            mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector del {fecha_inicio} al {fecha_fin}\n\n"
        else:
            # Otros días: mostrar fecha de hoy
            mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector {HOY.strftime('%d/%m/%y')}\n\n"
        
        for norma in aceptados:
            # Agregar etiqueta de extraordinaria si aplica
            tipo_etiqueta = ""
            if str(norma.get('TipoEdicion', '')).strip().lower() == "extraordinaria":
                tipo_etiqueta = " (Extraordinaria)"
            
            mensaje += f"<b>{norma['titulo']}{tipo_etiqueta}</b>\n"
            mensaje += f"{norma.get('Sumilla', '')}\n\n"
    else:
        if DIA_SEMANA == 0:
            fecha_inicio = (HOY - timedelta(days=3)).strftime('%d/%m/%y')
            fecha_fin = HOY.strftime('%d/%m/%y')
            mensaje = f"Buen día equipo, el día de hoy no se encontraron normas relevantes del sector.\n\n📅 Periodo revisado: del {fecha_inicio} al {fecha_fin}"
        else:
            ayer = HOY - timedelta(days=1)
            mensaje = f"Buen día equipo, el día de hoy no se encontraron normas relevantes del sector.\n\n📅 Extraordinaria {ayer.strftime('%d/%m/%y')}\n📅 Ordinaria {HOY.strftime('%d/%m/%y')}"
    
    print("\n💬 Enviando Telegram...")
    enviar_telegram(mensaje, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    
    # Resumen final
    print("\n" + "="*80)
    print("🎉 PROCESO COMPLETADO")
    print("="*80)
    print(f"✅ Normas procesadas: {len(aceptados)}")
    if aceptados:
        print(f"📁 Carpeta Drive: {folder_name}")
    print("="*80)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
