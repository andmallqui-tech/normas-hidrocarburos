"""
=============================================================================
SISTEMA AUTOMATIZADO DE NORMAS - GITHUB ACTIONS
Basado en el código de VS Code que SÍ FUNCIONA
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
print("🚀 SISTEMA DE NORMAS - GITHUB ACTIONS")
print("="*100)

HOY = date.today()
AYER = HOY - timedelta(days=1)
DIA_SEMANA = HOY.weekday()

# Variables de entorno
CREDENTIALS_JSON = os.getenv('GOOGLE_CREDENTIALS_JSON')
DRIVE_FOLDER_ID = os.getenv('DRIVE_FOLDER_ID')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# CARPETA LOCAL TEMPORAL PARA PDFs (como en VS Code)
CARPETA_DIA = f"/tmp/normas_{HOY.strftime('%Y-%m-%d')}"
os.makedirs(CARPETA_DIA, exist_ok=True)

print(f"📅 HOY: {HOY.strftime('%d/%m/%Y')} - DÍA: {['Lun','Mar','Mié','Jue','Vie','Sáb','Dom'][DIA_SEMANA]}")
print(f"📅 AYER: {AYER.strftime('%d/%m/%Y')}")
print(f"📁 Carpeta temporal PDFs: {CARPETA_DIA}")
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
            return files[0]['id'] if files else None
        except Exception as e:
            print(f"   ❌ Error buscando {filename}: {e}")
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
            print(f"   ✅ Corpus descargado: {len(content.split())} palabras")
            return content
        except Exception as e:
            print(f"   ❌ Error descargando: {e}")
            return ""
    
    def upload_text_file(self, folder_id, filename, content):
        try:
            file_metadata = {'name': filename, 'parents': [folder_id], 'mimeType': 'text/plain'}
            media = MediaIoBaseUpload(io.BytesIO(content.encode('utf-8')), mimetype='text/plain', resumable=True)
            existing_id = self.get_file_by_name(folder_id, filename)
            
            if existing_id:
                self.drive_service.files().update(fileId=existing_id, media_body=media).execute()
                print(f"   ✅ Corpus actualizado")
            else:
                self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                print(f"   ✅ Corpus creado")
            return True
        except Exception as e:
            print(f"   ❌ Error subiendo corpus: {e}")
            return False
    
    def upload_pdf(self, folder_id, filename, filepath):
        """Sube PDF desde archivo local (como VS Code)"""
        try:
            with open(filepath, 'rb') as f:
                pdf_bytes = f.read()
            
            file_metadata = {'name': filename, 'parents': [folder_id], 'mimeType': 'application/pdf'}
            media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype='application/pdf', resumable=True)
            file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink').execute()
            
            return file.get('webViewLink', '')
        except Exception as e:
            print(f"   ❌ Error subiendo PDF: {e}")
            return None
    
    def create_folder(self, parent_id, folder_name):
        try:
            existing_id = self.get_file_by_name(parent_id, folder_name)
            if existing_id:
                print(f"   ✅ Carpeta existe: {folder_name}")
                return existing_id
            
            file_metadata = {'name': folder_name, 'parents': [parent_id], 'mimeType': 'application/vnd.google-apps.folder'}
            folder = self.drive_service.files().create(body=file_metadata, fields='id').execute()
            print(f"   ✅ Carpeta creada: {folder_name}")
            return folder.get('id')
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
# NORMALIZACIÓN Y KEYWORDS (IGUAL QUE VS CODE)
# =============================================================================

def normalizar_texto(texto):
    if not isinstance(texto, str):
        return ""
    texto = texto.lower()
    texto = re.sub(r'[^a-z0-9\s]', '', texto)
    texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
    return texto

KEYWORDS_MANUAL = [normalizar_texto(x) for x in [
    'hidrocarburos','hidrocarburo','hidrocarburifero','hidrocarburifera',
    'petroleo','petrolero','petrolera','petrolífero',
    'gas natural','gas licuado','gnv','glp','gnl',
    'perupetro','osinergmin','minem','oefa','perúpetro',
    'ministerio de energia','energia y minas','dge','dgh',
    'organismo de evaluacion y fiscalizacion ambiental', 'produce',
    'oleoducto','gasoducto','poliducto','refineria','refinerias',
    'lote','lotes','pozo','pozos','yacimiento','yacimientos',
    'planta de gas','terminal','estacion de servicio',
    'planta de fraccionamiento','planta de procesamiento',
    'exploracion','explotacion','produccion petrolera','perforacion',
    'upstream','downstream','midstream','extraccion',
    'transporte de hidrocarburos','distribucion de gas', 'banda de precios',
    'combustible','combustibles','diesel','gasolina','kerosene', 'diesel b5',
    'turbo','residual','bunker','asfalto','nafta','gasohol','electromovilidad',
    'contrato de licencia','canon gasifero','canon petrolero',
    'regalia','concesion hidrocarburos','licencia de hidrocarburos',
    'designan','oefa', 'recargos',
    'reservas hidrocarburos','sismica','geofisica petrolera',
    'cuenca sedimentaria','barril','bep','barriles equivalentes',
    'estado de emergencia', 'lima', 'pcm','energia y minas','parque eolico','ductos',
    'electricidad','mineria','electricas','energeticos','fotovoltaica', 'distribución natural',
    'fijaron precios','energeticos renovables','recursos energeticos','electrica'
]]

PALABRAS_OBLIGATORIAS = set([normalizar_texto(x) for x in [
    'hidrocarburos','hidrocarburo','petroleo','gas natural',
    'perupetro','gnv','glp','oleoducto','gasoducto','refineria',
    'minem','osinergmin','ministerio de energia y minas',
    'organismo supervisor de la inversión en energía y minería','oefa'
]])

SECTORES_EXCLUIR = set([normalizar_texto(x) for x in [
    'educacion','salud','defensa','interior','mujer',
    'desarrollo social','trabajo','migraciones','comercio exterior','cultura',
    'vivienda','comunicaciones','justicia','relaciones exteriores','midis'
]])

# Crear tokens técnicos
tokens_kw = set()
for kw in KEYWORDS_MANUAL:
    for t in kw.split():
        if len(t) > 1:
            tokens_kw.add(t)

# Stemming simple
def simple_stem(w):
    sufijos = ['aciones','acion','amientos','amiento','mente','idad','idades',
               'iva','ivo','ivos','ivas','izar','izarse','cion','ciones','es','s']
    for s in sufijos:
        if w.endswith(s) and len(w) - len(s) > 3:
            return w[:-len(s)]
    return w

stems_kw = set(simple_stem(t) for t in tokens_kw)

print(f"\n🧠 CONFIGURACIÓN DE FILTRADO:")
print(f"   Keywords: {len(KEYWORDS_MANUAL)}")
print(f"   Tokens técnicos: {len(tokens_kw)}")
print(f"   Palabras obligatorias: {len(PALABRAS_OBLIGATORIAS)}")

# =============================================================================
# FUNCIONES DE EVALUACIÓN (IGUAL QUE VS CODE)
# =============================================================================

def similitud_tfidf(texto, vec_global, X_base):
    try:
        Y = vec_global.transform([normalizar_texto(texto)])
        from sklearn.metrics.pairwise import cosine_similarity
        return float(cosine_similarity(X_base, Y)[0][0])
    except:
        return 0.0

def fuzzy_score(texto, texto_base):
    from rapidfuzz import fuzz
    frags = []
    words = texto_base.split()
    for i in range(0, max(0, len(words)-40), 40):
        frags.append(" ".join(words[i:i+80]))
    
    cand = normalizar_texto(texto)[:400]
    best = 0
    for frag in frags[:200]:
        s = fuzz.partial_ratio(cand, frag[:300])
        if s > best:
            best = s
        if best >= 95:
            break
    return best / 100.0

def contar_tokens_tecnicos(texto):
    toks = [t for t in re.findall(r'\b[0-9a-z\-/]+\b', normalizar_texto(texto)) if len(t) > 2]
    matches = []
    for t in toks:
        st = simple_stem(t)
        if st in stems_kw:
            matches.append(t)
    return len(matches), list(dict.fromkeys(matches))[:8]

def evaluar_relevancia(texto_candidato, vec_global, X_base, texto_base):
    """IGUAL QUE VS CODE"""
    texto_norm = normalizar_texto(texto_candidato)
    
    # Verificar sector excluido
    for sector in SECTORES_EXCLUIR:
        if sector in texto_norm:
            return False, {"razon": f"Sector excluido: {sector}"}
    
    # Verificar palabra obligatoria
    tiene_obligatoria = False
    palabra_encontrada = None
    for palabra in PALABRAS_OBLIGATORIAS:
        if palabra in texto_norm:
            tiene_obligatoria = True
            palabra_encontrada = palabra
            break
    
    if not tiene_obligatoria:
        return False, {"razon": "Sin palabra obligatoria"}
    
    # Análisis técnico
    cnt, matched = contar_tokens_tecnicos(texto_candidato)
    tf = similitud_tfidf(texto_candidato, vec_global, X_base)
    fu = fuzzy_score(texto_candidato, texto_base)
    
    # Score combinado
    w_tok, w_tfidf, w_fuzz = 0.65, 0.20, 0.15
    token_score = min(1.0, cnt / max(2, 2))
    score = w_tok * token_score + w_tfidf * min(1.0, tf/0.2) + w_fuzz * min(1.0, fu/0.6)
    
    # Criterio de relevancia
    relevante = (cnt >= 3) or (cnt >= 2 and (tf >= 0.15 or score >= 0.50))
    
    info = {
        "count_tokens": cnt,
        "matched_terms": matched,
        "tfidf_sim": round(tf, 4),
        "fuzzy": round(fu, 4),
        "combined_score": round(score, 4),
        "relevant": bool(relevante),
        "razon": f"Palabra obligatoria: {palabra_encontrada}" if relevante else "No cumple umbrales"
    }
    
    return bool(relevante), info

# =============================================================================
# SELENIUM (IGUAL QUE VS CODE)
# =============================================================================

def crear_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(60)
    return driver

def complete_href(href):
    """IGUAL QUE VS CODE"""
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
    """IGUAL QUE VS CODE"""
    nombre = re.sub(r'[<>:"/\\|?*\n\r\t]', '', nombre)
    nombre = re.sub(r'\s+', '_', nombre.strip())
    return nombre[:150]

def extraer_normas_el_peruano(driver, fecha_obj, es_extraordinaria=False, max_scrolls=40):
    """
    FUNCIÓN EXACTAMENTE IGUAL QUE VS CODE
    """
    tipo = "Extraordinaria" if es_extraordinaria else "Ordinaria"
    fecha_str = fecha_obj.strftime("%d/%m/%Y")
    
    print(f"\n{'='*80}")
    print(f"🔍 EXTRAYENDO: {tipo} del {fecha_str}")
    print(f"{'='*80}")
    
    try:
        print("1️⃣ Accediendo a El Peruano...")
        driver.get("https://diariooficial.elperuano.pe/Normas")
        time.sleep(4)
        
        print(f"2️⃣ Configurando fecha: {fecha_str}")
        driver.execute_script(f"""
            document.getElementById('cddesde').value = '{fecha_str}';
            document.getElementById('cdhasta').value = '{fecha_str}';
        """)
        time.sleep(1)
        
        if es_extraordinaria:
            print("3️⃣ Marcando EXTRAORDINARIA...")
            driver.execute_script("document.getElementById('tipo').checked = true;")
        else:
            print("3️⃣ Modo ORDINARIA...")
            driver.execute_script("document.getElementById('tipo').checked = false;")
        
        time.sleep(1)
        
        print("4️⃣ Ejecutando búsqueda...")
        driver.execute_script("document.getElementById('btnBuscar').click();")
        time.sleep(8)
        
        # Scroll con detección de estabilidad
        print(f"5️⃣ Cargando contenido (máx {max_scrolls} scrolls)...")
        last_count = -1
        stable = 0
        
        for i in range(max_scrolls):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)
            
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            articles = soup.find_all("article", class_=lambda c: c and "edicionesoficiales_articulos" in c)
            count = len(articles)
            
            print(f"   Scroll {i+1}/{max_scrolls}: {count} artículos detectados")
            
            if count == last_count:
                stable += 1
            else:
                stable = 0
                last_count = count
            
            if stable >= 3:
                print("   ✅ Contenido estable, finalizando scroll")
                break
        
        print("6️⃣ Parseando artículos...")
        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all("article", class_=lambda c: c and "edicionesoficiales_articulos" in c)
        
        if not articles:
            print("   ⚠️ No se encontraron artículos")
            return []
        
        print(f"   📄 {len(articles)} artículos encontrados")
        
        # Procesar artículos
        candidatos = []
        for idx, art in enumerate(articles, 1):
            try:
                # Sector
                sector_tag = art.find("h4")
                sector = sector_tag.get_text(" ", strip=True) if sector_tag else ""
                
                # Título
                titulo_tag = art.find("h5")
                link_tag = titulo_tag.find("a") if titulo_tag else None
                titulo = link_tag.get_text(" ", strip=True) if link_tag else (
                    titulo_tag.get_text(" ", strip=True) if titulo_tag else ""
                )
                
                # Fecha y sumilla
                p_tags = art.find_all("p")
                fecha_pub = ""
                if p_tags:
                    b = p_tags[0].find("b")
                    if b:
                        fecha_pub = b.get_text(" ", strip=True).replace("Fecha:", "").strip()
                
                sumilla = p_tags[1].get_text(" ", strip=True) if len(p_tags) >= 2 else ""
                
                # PDF URL - EXACTAMENTE COMO VS CODE
                pdf_url = ""
                for inp in art.find_all("input"):
                    if inp.has_attr("data-url"):
                        val = (inp.get("value", "") or "").lower()
                        if "descarga individual" in val or "descarga" in val:
                            pdf_url = complete_href(inp['data-url'])
                            break
                        if not pdf_url:
                            pdf_url = complete_href(inp['data-url'])
                
                if not pdf_url:
                    continue
                
                # Crear registro - EXACTAMENTE COMO VS CODE
                texto_completo = " ".join([sector, titulo, sumilla])
                nombre_archivo = sanitize_filename(titulo or sumilla[:60]) + ".pdf"
                
                candidatos.append({
                    "sector": sector,
                    "titulo": titulo,
                    "FechaPublicacion": fecha_pub,
                    "Sumilla": sumilla,
                    "pdf_url": pdf_url,
                    "NombreArchivo": nombre_archivo,
                    "texto_completo": texto_completo,
                    "TipoEdicion": tipo  # ← CONSISTENTE
                })
                
            except Exception as e:
                print(f"   ⚠️ Error en artículo {idx}: {e}")
                continue
        
        print(f"7️⃣ Candidatos extraídos: {len(candidatos)}\n")
        return candidatos
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        import traceback
        traceback.print_exc()
        return []

def descargar_pdf(url, destino):
    """IGUAL QUE VS CODE - Descarga PDF local"""
    try:
        resp = requests.get(url, timeout=30, stream=True)
        resp.raise_for_status()
        with open(destino, 'wb') as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"   ❌ Error descargando: {e}")
        return False

# =============================================================================
# MAIN - EXACTAMENTE COMO VS CODE
# =============================================================================

def main():
    print("\n" + "="*80)
    print("🚀 INICIANDO PROCESO PRINCIPAL")
    print("="*80)
    
    # Conectar a Drive
    print("\n📁 PASO 1: CONECTAR A GOOGLE DRIVE")
    drive_client = GoogleDriveClient(CREDENTIALS_JSON)
    
    # Cargar corpus
    print("\n🧠 PASO 2: CARGAR CORPUS")
    corpus_file_id = drive_client.get_file_by_name(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt')
    
    if corpus_file_id:
        texto_base = drive_client.download_text_file(corpus_file_id)
    else:
        print("   📝 Creando corpus inicial...")
        texto_base = " ".join(KEYWORDS_MANUAL * 3)
        drive_client.upload_text_file(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt', texto_base)
    
    if not texto_base.strip():
        texto_base = " ".join(KEYWORDS_MANUAL * 3)
    
    # Vectorizador
    print("\n🤖 PASO 3: INICIALIZAR VECTORIZADOR")
    vec_global = TfidfVectorizer(lowercase=True, ngram_range=(1,2), max_features=5000)
    vec_global.fit([texto_base])
    X_base = vec_global.transform([texto_base])
    print(f"   ✅ Vocabulario: {len(vec_global.vocabulary_)} términos")
    
    # Crear driver
    print("\n🌐 PASO 4: INICIAR NAVEGADOR")
    driver = crear_driver()
    print("   ✅ Navegador listo")
    
    # EXTRACCIÓN - EXACTAMENTE COMO VS CODE
    print("\n📰 PASO 5: EXTRAER NORMAS")
    
    print("\n📋 5.A - EXTRAYENDO ORDINARIAS DE HOY:")
    candidatos_hoy = extraer_normas_el_peruano(driver, HOY, es_extraordinaria=False, max_scrolls=40)
    
    time.sleep(3)
    
    print("\n📋 5.B - EXTRAYENDO EXTRAORDINARIAS DE AYER:")
    candidatos_ayer = extraer_normas_el_peruano(driver, AYER, es_extraordinaria=True, max_scrolls=40)
    
    driver.quit()
    print("\n✅ Navegador cerrado")
    
    # Combinar todos los candidatos
    todos_candidatos = candidatos_hoy + candidatos_ayer
    
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN EXTRACCIÓN")
    print(f"{'='*80}")
    print(f"✅ Ordinarias (HOY): {len(candidatos_hoy)}")
    print(f"✅ Extraordinarias (AYER): {len(candidatos_ayer)}")
    print(f"✅ TOTAL: {len(todos_candidatos)}")
    print(f"{'='*80}\n")
    
    # FILTRAR
    print("🔬 PASO 6: FILTRAR RELEVANCIA")
    aceptados = []
    rechazados = 0
    
    for i, c in enumerate(todos_candidatos, 1):
        texto = c.get("texto_completo", "")
        relevante, info = evaluar_relevancia(texto, vec_global, X_base, texto_base)
        
        if relevante:
            aceptados.append(c)
            print(f"[{i}/{len(todos_candidatos)}] ✅ ACEPTA: {c.get('titulo', '')[:60]}")
        else:
            rechazados += 1
    
    print(f"\n✅ Aceptadas: {len(aceptados)}")
    print(f"⏭️ Rechazadas: {rechazados}\n")
    
    # DESCARGAR PDFs LOCALMENTE (como VS Code)
    if aceptados:
        print("📥 PASO 7: DESCARGAR PDFs LOCALMENTE")
        exitosos = 0
        fallidos = 0
        
        for i, norma in enumerate(aceptados, 1):
            try:
                nombre = norma.get("NombreArchivo", "")
                destino = os.path.join(CARPETA_DIA, nombre)
                
                if os.path.exists(destino):
                    print(f"[{i}/{len(aceptados)}] ⏭️ Ya existe: {nombre[:50]}")
                    exitosos += 1
                    continue
                
                url = norma.get("pdf_url", "")
                print(f"[{i}/{len(aceptados)}] ⬇️ Descargando: {nombre[:50]}...")
                
                if descargar_pdf(url, destino):
                    exitosos += 1
                    norma['pdf_local'] = destino  # Guardar ruta local
                    print(f"   ✅ Completado")
                else:
                    fallidos += 1
                
                time.sleep(1)
                
            except Exception as e:
                fallidos += 1
                print(f"   ❌ Error: {e}")
        
        print(f"\n✅ Exitosas: {exitosos}")
        print(f"❌ Fallidas: {fallidos}\n")
    
    # SUBIR PDFs A DRIVE (opcional)
    folder_id = None
    if aceptados:
        print("📤 PASO 8: SUBIR PDFs A DRIVE")
        folder_name = HOY.strftime("%Y-%m-%d")
        folder_id = drive_client.create_folder(DRIVE_FOLDER_ID, folder_name)
        
        if folder_id:
            for i, norma in enumerate(aceptados, 1):
                if 'pdf_local' in norma and os.path.exists(norma['pdf_local']):
                    print(f"[{i}/{len(aceptados)}] 📤 Subiendo: {norma['NombreArchivo'][:50]}...")
                    link = drive_client.upload_pdf(folder_id, norma['NombreArchivo'], norma['pdf_local'])
                    norma['EnlacePDF'] = link if link else norma['pdf_url']
                else:
                    norma['EnlacePDF'] = norma['pdf_url']
    
    # ACTUALIZAR SHEETS
    if aceptados:
        print("\n📊 PASO 9: ACTUALIZAR GOOGLE SHEETS")
        rows = []
        for norma in aceptados:
            rows.append([
                HOY.strftime("%Y-%m-%d"),
                norma.get('titulo', ''),
                norma.get('FechaPublicacion', ''),
                norma.get('Sumilla', ''),
                norma.get('EnlacePDF', norma.get('pdf_url', '')),
                norma.get('TipoEdicion', '')
            ])
        drive_client.append_to_sheet(SPREADSHEET_ID, 'A:F', rows)
    
    # ACTUALIZAR CORPUS
    if aceptados:
        print("\n🧠 PASO 10: ACTUALIZAR CORPUS")
        nuevo_contenido = "\n".join([n['texto_completo'] for n in aceptados])
        corpus_actualizado = texto_base + "\n" + nuevo_contenido
        drive_client.upload_text_file(DRIVE_FOLDER_ID, 'corpus_hidrocarburos.txt', corpus_actualizado)
    
    # TELEGRAM
    print("\n💬 PASO 11: ENVIAR TELEGRAM")
    if aceptados:
        mensaje = f"Buen día equipo, se envía la revisión de normas relevantes al sector {HOY.strftime('%d/%m/%y')}\n\n"
        
        for norma in aceptados:
            tipo_etiqueta = ""
            if str(norma.get('TipoEdicion', '')).strip() == "Extraordinaria":
                tipo_etiqueta = " (Extraordinaria)"
            
            mensaje += f"<b>{norma['titulo']}{tipo_etiqueta}</b>\n"
            mensaje += f"{norma.get('Sumilla', '')}\n\n"
    else:
        mensaje = f"Buen día equipo, el día de hoy no se encontraron normas relevantes del sector.\n\n📅 Extraordinaria {AYER.strftime('%d/%m/%y')}\n📅 Ordinaria {HOY.strftime('%d/%m/%y')}"
    
    enviar_telegram(mensaje, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    
    # RESUMEN FINAL
    print("\n" + "="*80)
    print("🎉 PROCESO COMPLETADO")
    print("="*80)
    print(f"✅ Normas procesadas: {len(aceptados)}")
    print(f"📁 PDFs locales: {CARPETA_DIA}")
    if folder_id:
        print(f"📁 Carpeta Drive: {folder_name}")
    print("="*80)
    
    # Mostrar normas
    if aceptados:
        print(f"\n📋 NORMAS ACEPTADAS ({len(aceptados)}):")
        for i, norma in enumerate(aceptados, 1):
            print(f"{i}. [{norma['TipoEdicion']}] {norma['titulo'][:70]}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
