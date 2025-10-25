#BOT_TOKEN  7554833400:AAEQzzpJESp_FNqd-nPLZh1QNlUoF9_bGMU #GITHUB TOKEN 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
import logging
import sqlite3
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from datetime import datetime, timedelta
import asyncio
import os
from flask import Flask
import threading
import requests
import time
import psutil
import base64
import json
import csv
from io import StringIO

# === CONFIGURAZIONE ===
DATABASE_NAME = 'interventi_vvf.db'
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_IDS = [1816045269, 653425963, 693843502, 6622015744]

# Configurazione backup GitHub
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GIST_ID = os.environ.get('GIST_ID')

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# === DATABASE ===
def init_db():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()

    # Tabella interventi
    c.execute('''CREATE TABLE IF NOT EXISTS interventi
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  rapporto_como TEXT,
                  progressivo_como TEXT,
                  numero_erba INTEGER,
                  data_uscita TIMESTAMP,
                  data_rientro TIMESTAMP,
                  mezzo_targa TEXT,
                  mezzo_tipo TEXT,
                  capopartenza TEXT,
                  autista TEXT,
                  indirizzo TEXT,
                  tipologia TEXT,
                  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    # Tabella partecipanti intervento
    c.execute('''CREATE TABLE IF NOT EXISTS partecipanti
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  intervento_id INTEGER,
                  vigile_id INTEGER,
                  FOREIGN KEY (intervento_id) REFERENCES interventi (id))''')

    # Tabella vigili
    c.execute('''CREATE TABLE IF NOT EXISTS vigili
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  nome TEXT,
                  cognome TEXT,
                  qualifica TEXT,
                  grado_patente_terrestre TEXT,
                  patente_nautica BOOLEAN DEFAULT 0,
                  saf BOOLEAN DEFAULT 0,
                  tpss BOOLEAN DEFAULT 0,
                  atp BOOLEAN DEFAULT 0,
                  attivo BOOLEAN DEFAULT 1)''')

    # Tabella mezzi
    c.execute('''CREATE TABLE IF NOT EXISTS mezzi
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  targa TEXT UNIQUE,
                  tipo TEXT,
                  attivo BOOLEAN DEFAULT 1)''')

    # Tabella utenti
    c.execute('''CREATE TABLE IF NOT EXISTS utenti
                 (user_id INTEGER PRIMARY KEY,
                  username TEXT,
                  nome TEXT,
                  ruolo TEXT DEFAULT 'in_attesa',
                  data_richiesta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  data_approvazione TIMESTAMP)''')

    # Inserisci admin
    for admin_id in ADMIN_IDS:
        c.execute('''INSERT OR IGNORE INTO utenti 
                     (user_id, nome, ruolo, data_approvazione) 
                     VALUES (?, 'Admin', 'admin', CURRENT_TIMESTAMP)''', (admin_id,))

    # Inserisci dati iniziali mezzi
    mezzi_iniziali = [
        ('AB123CD', 'APS'),
        ('EF456GH', 'ABP'),
        ('IL789JK', 'AS'),
        ('MN012PQ', 'AU')
    ]
    for targa, tipo in mezzi_iniziali:
        c.execute('''INSERT OR IGNORE INTO mezzi (targa, tipo) VALUES (?, ?)''', (targa, tipo))

    # Inserisci alcuni vigili di esempio
    vigili_iniziali = [
        ('Mario', 'Rossi', 'CSV', 'III', 1, 0, 1, 0),
        ('Luca', 'Bianchi', 'VV', 'II', 0, 1, 0, 1),
        ('Giuseppe', 'Verdi', 'CSV', 'IIIE', 1, 1, 0, 0),
        ('Andrea', 'Neri', 'VV', 'I', 0, 0, 1, 0),
        ('Paolo', 'Gialli', 'CSV', 'II', 1, 0, 0, 1)
    ]
    for nome, cognome, qualifica, grado, nautica, saf, tpss, atp in vigili_iniziali:
        c.execute('''INSERT OR IGNORE INTO vigili 
                    (nome, cognome, qualifica, grado_patente_terrestre, patente_nautica, saf, tpss, atp) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                    (nome, cognome, qualifica, grado, nautica, saf, tpss, atp))

    conn.commit()
    conn.close()

init_db()

# === SISTEMA DI EMERGENZA ===
def emergency_recreate_database():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    try:
        c.execute("SELECT 1 FROM interventi LIMIT 1")
        c.execute("SELECT 1 FROM vigili LIMIT 1")
        c.execute("SELECT 1 FROM utenti LIMIT 1")
        print("✅ Tabelle database verificate")
    except sqlite3.OperationalError:
        print("🚨 TABELLE NON TROVATE! Ricreo il database...")
        init_db()
        print("✅ Database ricreato con successo!")
    conn.close()

def check_database_integrity():
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        table_count = c.fetchone()[0]
        conn.close()
        return table_count >= 5
    except Exception as e:
        print(f"🚨 Errore verifica database: {e}")
        return False

# === FUNZIONI UTILITY ===
def is_admin(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT ruolo FROM utenti WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result and result[0] == 'admin'

def is_user_approved(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT ruolo FROM utenti WHERE user_id = ? AND ruolo IN ('admin', 'user')", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def get_richieste_in_attesa():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT user_id, username, nome, data_richiesta 
                 FROM utenti WHERE ruolo = 'in_attesa' ORDER BY data_richiesta''')
    result = c.fetchall()
    conn.close()
    return result

def approva_utente(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''UPDATE utenti SET ruolo = 'user', data_approvazione = CURRENT_TIMESTAMP 
                 WHERE user_id = ?''', (user_id,))
    conn.commit()
    conn.close()

# === FUNZIONI INTERVENTI ===
def get_prossimo_numero_erba():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("SELECT MAX(numero_erba) FROM interventi")
    result = c.fetchone()[0]
    conn.close()
    return (result or 0) + 1

def get_ultimi_interventi_attivi():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT id, rapporto_como, progressivo_como, numero_erba, data_uscita, indirizzo
                 FROM interventi 
                 WHERE data_rientro IS NULL 
                 ORDER BY data_uscita DESC LIMIT 5''')
    result = c.fetchall()
    conn.close()
    return result

def get_interventi_per_rapporto(rapporto, anno):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT * FROM interventi 
                 WHERE rapporto_como = ? AND strftime('%Y', data_uscita) = ?
                 ORDER BY data_uscita DESC''', (rapporto, anno))
    result = c.fetchall()
    conn.close()
    return result

def get_progressivo_per_rapporto(rapporto):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT progressivo_como FROM interventi 
                 WHERE rapporto_como = ? 
                 ORDER BY progressivo_como DESC LIMIT 1''', (rapporto,))
    result = c.fetchone()
    conn.close()
    
    if result:
        ultimo_prog = result[0]
        try:
            return str(int(ultimo_prog) + 1).zfill(2)
        except:
            return "02"
    return "01"

def inserisci_intervento(dati):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    try:
        c.execute('''INSERT INTO interventi 
                    (rapporto_como, progressivo_como, numero_erba, data_uscita, data_rientro,
                     mezzo_targa, mezzo_tipo, capopartenza, autista, indirizzo, tipologia)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (dati['rapporto_como'], dati['progressivo_como'], dati['numero_erba'],
                     dati['data_uscita'], dati['data_rientro'], dati['mezzo_targa'],
                     dati['mezzo_tipo'], dati['capopartenza'], dati['autista'],
                     dati['indirizzo'], dati.get('tipologia', '')))
        
        intervento_id = c.lastrowid
        
        for vigile_id in dati.get('partecipanti', []):
            c.execute('''INSERT INTO partecipanti (intervento_id, vigile_id) VALUES (?, ?)''',
                      (intervento_id, vigile_id))
        
        conn.commit()
        return intervento_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_ultimi_interventi(limite=10):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT i.*, 
                 GROUP_CONCAT(v.nome || ' ' || v.cognome) as partecipanti
                 FROM interventi i
                 LEFT JOIN partecipanti p ON i.id = p.intervento_id
                 LEFT JOIN vigili v ON p.vigile_id = v.id
                 GROUP BY i.id
                 ORDER BY i.data_uscita DESC LIMIT ?''', (limite,))
    result = c.fetchall()
    conn.close()
    return result

def get_statistiche_anno(corrente=True):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    anno = datetime.now().year if corrente else None
    
    if corrente:
        c.execute('''SELECT COUNT(*) FROM interventi 
                     WHERE strftime('%Y', data_uscita) = ?''', (str(anno),))
    else:
        c.execute('''SELECT COUNT(*) FROM interventi''')
    
    totale = c.fetchone()[0]
    
    if corrente:
        c.execute('''SELECT tipologia, COUNT(*) 
                     FROM interventi 
                     WHERE strftime('%Y', data_uscita) = ?
                     GROUP BY tipologia''', (str(anno),))
    else:
        c.execute('''SELECT tipologia, COUNT(*) FROM interventi GROUP BY tipologia''')
    
    tipologie = c.fetchall()
    
    if corrente:
        c.execute('''SELECT strftime('%m', data_uscita) as mese, COUNT(*)
                     FROM interventi 
                     WHERE strftime('%Y', data_uscita) = ?
                     GROUP BY mese''', (str(anno),))
    else:
        c.execute('''SELECT strftime('%m', data_uscita) as mese, COUNT(*)
                     FROM interventi 
                     GROUP BY mese''')
    
    mensili = c.fetchall()
    
    conn.close()
    return {
        'totale': totale,
        'tipologie': dict(tipologie),
        'mensili': dict(mensili)
    }

# === FUNZIONI VIGILI E MEZZI ===
def get_vigili_attivi():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT id, nome, cognome, qualifica FROM vigili WHERE attivo = 1 ORDER BY cognome, nome''')
    result = c.fetchall()
    conn.close()
    return result

def get_vigile_by_id(vigile_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT * FROM vigili WHERE id = ?''', (vigile_id,))
    result = c.fetchone()
    conn.close()
    return result

def get_mezzi_attivi():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT targa, tipo FROM mezzi WHERE attivo = 1 ORDER BY tipo''')
    result = c.fetchall()
    conn.close()
    return result

def get_tutti_vigili():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT * FROM vigili ORDER BY cognome, nome''')
    result = c.fetchall()
    conn.close()
    return result

def aggiorna_vigile(vigile_id, campo, valore):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute(f"UPDATE vigili SET {campo} = ? WHERE id = ?", (valore, vigile_id))
    conn.commit()
    conn.close()

# === SISTEMA BACKUP GITHUB ===
def backup_database_to_gist():
    if not GITHUB_TOKEN:
        print("❌ Token GitHub non configurato - backup disabilitato")
        return False
    
    try:
        with open(DATABASE_NAME, 'rb') as f:
            db_content = f.read()
        
        db_base64 = base64.b64encode(db_content).decode('utf-8')
        
        files = {
            'interventi_vvf_backup.json': {
                'content': json.dumps({
                    'timestamp': datetime.now().isoformat(),
                    'database_size': len(db_content),
                    'database_base64': db_base64,
                    'backup_type': 'automatic'
                })
            }
        }
        
        headers = {
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        if GIST_ID:
            url = f'https://api.github.com/gists/{GIST_ID}'
            data = {'files': files}
            response = requests.patch(url, headers=headers, json=data)
        else:
            url = 'https://api.github.com/gists'
            data = {
                'description': f'Backup Interventi VVF - {datetime.now().strftime("%Y-%m-%d %H:%M")}',
                'public': False,
                'files': files
            }
            response = requests.post(url, headers=headers, json=data)
        
        if response.status_code in [200, 201]:
            result = response.json()
            print(f"✅ Backup su Gist completato: {result['html_url']}")
            
            if not GIST_ID:
                with open('gist_id.txt', 'w') as f:
                    f.write(result['id'])
                print(f"📝 Nuovo Gist ID salvato: {result['id']}")
            
            return True
        else:
            print(f"❌ Errore backup Gist: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Errore durante backup: {str(e)}")
        return False

def restore_database_from_gist():
    if not GITHUB_TOKEN or not GIST_ID:
        print("❌ Token o Gist ID non configurati - restore disabilitato")
        return False
    
    try:
        headers = {
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        url = f'https://api.github.com/gists/{GIST_ID}'
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            gist_data = response.json()
            backup_file = gist_data['files'].get('interventi_vvf_backup.json')
            
            if backup_file:
                backup_content = json.loads(backup_file['content'])
                db_base64 = backup_content['database_base64']
                timestamp = backup_content['timestamp']
                
                db_content = base64.b64decode(db_base64)
                with open(DATABASE_NAME, 'wb') as f:
                    f.write(db_content)
                
                print(f"✅ Database ripristinato da backup: {timestamp}")
                return True
            else:
                print("❌ File di backup non trovato nel Gist")
                return False
        else:
            print(f"❌ Errore recupero Gist: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Errore durante restore: {str(e)}")
        return False

def backup_scheduler():
    print("🔄 Scheduler backup avviato (ogni 25 minuti)")
    time.sleep(10)
    backup_database_to_gist()
    
    while True:
        time.sleep(1500)
        backup_database_to_gist()

# === SISTEMA KEEP-ALIVE ULTRA-AGGRESSIVO ===
def keep_alive_aggressive():
    urls = [
        "https://tuo-bot-interventi.onrender.com/health",
        "https://tuo-bot-interventi.onrender.com/", 
        "https://tuo-bot-interventi.onrender.com/ping",
        "https://tuo-bot-interventi.onrender.com/status"
    ]
    
    print("🔄 Sistema keep-alive ULTRA-AGGRESSIVO avviato! Ping ogni 5 minuti...")
    
    while True:
        success_count = 0
        for url in urls:
            try:
                response = requests.get(url, timeout=15)
                if response.status_code == 200:
                    print(f"✅ Ping riuscito - {datetime.now().strftime('%H:%M:%S')} - {url}")
                    success_count += 1
                else:
                    print(f"⚠️  Ping {url} - Status: {response.status_code}")
            except Exception as e:
                print(f"❌ Errore ping {url}: {e}")
        
        print(f"📊 Ping completati: {success_count}/{len(urls)} successi")
        
        if success_count == 0:
            print("🚨 CRITICO: Tutti i ping fallitti! Riavvio in 30 secondi...")
            time.sleep(30)
            os._exit(1)
        
        time.sleep(300)

# === FUNZIONI SERVER STATUS ===
def get_system_metrics():
    try:
        process = psutil.Process(os.getpid())
        process_memory = process.memory_info().rss / 1024 / 1024
        
        system_memory = psutil.virtual_memory()
        total_memory_used = system_memory.used / 1024 / 1024
        total_memory_total = system_memory.total / 1024 / 1024
        memory_percent = system_memory.percent
        
        cpu_percent = psutil.cpu_percent(interval=1)
        
        boot_time = datetime.fromtimestamp(psutil.boot_time())
        uptime = datetime.now() - boot_time
        
        metrics_msg = "📊 **METRICHE DI SISTEMA:**\n"
        metrics_msg += f"• RAM Bot: {process_memory:.1f}MB\n"
        metrics_msg += f"• RAM Sistema: {total_memory_used:.1f}MB / {total_memory_total:.1f}MB ({memory_percent:.1f}%)\n"
        metrics_msg += f"• CPU: {cpu_percent:.1f}%\n"
        metrics_msg += f"• Uptime: {str(uptime).split('.')[0]}\n"
        
        return metrics_msg
        
    except Exception as e:
        return f"📊 Errore metriche: {str(e)}"

# === SERVER FLASK PER RENDER ===
app = Flask(__name__)

@app.route('/')
def home():
    return "🤖 Bot Interventi VVF - ONLINE 🟢 - Keep-alive attivo!"

@app.route('/health')
def health():
    return "OK"

@app.route('/ping')
def ping():
    return f"PONG - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

@app.route('/status')
def status():
    return "Bot Active | Keep-alive: ✅"

@app.route('/keep-alive')
def keep_alive_endpoint():
    return f"KEEP-ALIVE ACTIVE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

def run_flask():
    app.run(host='0.0.0.0', port=10000, debug=False)

# === TASTIERA FISICA ===
def crea_tastiera_fisica(user_id):
    if not is_user_approved(user_id):
        return ReplyKeyboardMarkup([[KeyboardButton("🚀 Richiedi Accesso")]], resize_keyboard=True)

    tastiera = [
        [KeyboardButton("➕ Nuovo Intervento"), KeyboardButton("📋 Ultimi Interventi")],
        [KeyboardButton("📊 Statistiche"), KeyboardButton("🔍 Cerca Rapporto")],
        [KeyboardButton("📤 Esporta Dati"), KeyboardButton("🆘 Help")]
    ]

    if is_admin(user_id):
        tastiera.append([KeyboardButton("👥 Gestisci Richieste"), KeyboardButton("⚙️ Gestione Vigili")])

    return ReplyKeyboardMarkup(tastiera, resize_keyboard=True, is_persistent=True)

# === HANDLER START ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''INSERT OR IGNORE INTO utenti (user_id, username, nome, ruolo) 
                 VALUES (?, ?, ?, 'in_attesa')''', 
                 (user_id, update.effective_user.username, user_name))
    conn.commit()
    conn.close()

    if not is_user_approved(user_id):
        richieste = get_richieste_in_attesa()
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"🆕 NUOVA RICHIESTA ACCESSO\n\nUser: {user_name}\nID: {user_id}\nRichieste in attesa: {len(richieste)}"
                )
            except:
                pass

        await update.message.reply_text(
            "✅ Richiesta inviata agli amministratori.\nAttendi l'approvazione!",
            reply_markup=crea_tastiera_fisica(user_id)
        )
        return

    welcome_text = f"👨‍💻 BENVENUTO ADMIN {user_name}!" if is_admin(user_id) else f"👤 BENVENUTO {user_name}!"
    await update.message.reply_text(welcome_text, reply_markup=crea_tastiera_fisica(user_id))

# === GESTIONE RICHIESTE ACCESSO ===
async def gestisci_richieste(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        return

    richieste = get_richieste_in_attesa()
    if not richieste:
        await update.message.reply_text("✅ Nessuna richiesta di accesso in sospeso.")
        return

    prima_richiesta = richieste[0]
    user_id_rich, username, nome, data_richiesta = prima_richiesta
    data = data_richiesta.split()[0] if data_richiesta else "N/A"
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Approva", callback_data=f"approva_{user_id_rich}"),
            InlineKeyboardButton("❌ Rifiuta", callback_data=f"rifiuta_{user_id_rich}")
        ]
    ]

    reply_markup = InlineKeyboardMarkup(keyboard)
    
    richieste_rimanenti = len(richieste) - 1
    info_rimanenti = f"\n\n📋 Richieste rimanenti: {richieste_rimanenti}" if richieste_rimanenti > 0 else ""
    
    await update.message.reply_text(
        f"👤 **RICHIESTA ACCESSO**\n\n"
        f"🆔 **ID:** {user_id_rich}\n"
        f"👤 **Nome:** {nome}\n"
        f"📱 **Username:** @{username}\n"
        f"📅 **Data:** {data}{info_rimanenti}",
        reply_markup=reply_markup
    )

# === NUOVO INTERVENTO - FLUSSO COMPLETO ===
async def avvia_nuovo_intervento(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return

    context.user_data['nuovo_intervento'] = {}
    context.user_data['fase'] = 'scelta_tipo'
    
    keyboard = [
        [
            InlineKeyboardButton("🆕 Nuovo Rapporto", callback_data="tipo_nuovo"),
            InlineKeyboardButton("🔗 Collegato a Esistente", callback_data="tipo_collegato")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "🔰 **NUOVO INTERVENTO**\n\n"
        "Seleziona il tipo di intervento:",
        reply_markup=reply_markup
    )

async def gestisci_scelta_tipo(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    await query.answer()
    
    if callback_data == "tipo_nuovo":
        context.user_data['fase'] = 'inserisci_rapporto'
        await query.edit_message_text(
            "📝 **INSERISCI RAPPORTO COMO**\n\n"
            "Inserisci il numero del rapporto Como (solo numeri):"
        )
    else:
        interventi_attivi = get_ultimi_interventi_attivi()
        if not interventi_attivi:
            await query.edit_message_text("❌ Nessun intervento attivo trovato. Crea un nuovo rapporto.")
            return
        
        keyboard = []
        for intervento in interventi_attivi:
            id_int, rapporto, progressivo, num_erba, data_uscita, indirizzo = intervento
            data = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M')
            keyboard.append([
                InlineKeyboardButton(
                    f"#{num_erba} - R{rapporto}/{progressivo} - {data}",
                    callback_data=f"collega_{id_int}"
                )
            ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            "🔗 **SELEZIONA INTERVENTO ATTIVO**\n\n"
            "Scegli l'intervento a cui collegarti:",
            reply_markup=reply_markup
        )

async def gestisci_collega_intervento(update: Update, context: ContextTypes.DEFAULT_TYPE, intervento_id: int):
    query = update.callback_query
    await query.answer()
    
    # Recupera dati intervento esistente
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT rapporto_como, numero_erba FROM interventi WHERE id = ?''', (intervento_id,))
    intervento = c.fetchone()
    conn.close()
    
    if intervento:
        rapporto_como, numero_erba = intervento
        progressivo_como = get_progressivo_per_rapporto(rapporto_como)
        
        context.user_data['nuovo_intervento']['rapporto_como'] = rapporto_como
        context.user_data['nuovo_intervento']['progressivo_como'] = progressivo_como
        context.user_data['nuovo_intervento']['numero_erba'] = numero_erba
        
        context.user_data['fase'] = 'data_uscita'
        
        oggi = datetime.now().strftime('%d/%m/%Y')
        ieri = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
        
        keyboard = [
            [
                InlineKeyboardButton(f"🟢 OGGI ({oggi})", callback_data="data_oggi"),
                InlineKeyboardButton(f"🟡 IERI ({ieri})", callback_data="data_ieri")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            f"🔗 **COLLEGATO A R{rapporto_como}**\n"
            f"Progressivo: {progressivo_como}\n\n"
            "📅 **DATA USCITA**\n"
            "Seleziona la data di uscita:",
            reply_markup=reply_markup
        )

async def gestisci_rapporto_como(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rapporto = update.message.text.strip()
    
    if not rapporto.isdigit():
        await update.message.reply_text("❌ Inserisci solo numeri! Riprova:")
        return
    
    context.user_data['nuovo_intervento']['rapporto_como'] = rapporto
    context.user_data['nuovo_intervento']['progressivo_como'] = "01"
    context.user_data['nuovo_intervento']['numero_erba'] = get_prossimo_numero_erba()
    context.user_data['fase'] = 'data_uscita'
    
    oggi = datetime.now().strftime('%d/%m/%Y')
    ieri = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
    
    keyboard = [
        [
            InlineKeyboardButton(f"🟢 OGGI ({oggi})", callback_data="data_oggi"),
            InlineKeyboardButton(f"🟡 IERI ({ieri})", callback_data="data_ieri")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📅 **DATA USCITA**\n\n"
        "Seleziona la data di uscita:",
        reply_markup=reply_markup
    )

async def gestisci_data_uscita(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    await query.answer()
    
    if callback_data == "data_oggi":
        data_uscita = datetime.now()
    else:
        data_uscita = datetime.now() - timedelta(days=1)
    
    context.user_data['nuovo_intervento']['data_uscita'] = data_uscita.strftime('%Y-%m-%d')
    context.user_data['fase'] = 'ora_uscita'
    
    await query.edit_message_text(
        "⏰ **ORA USCITA**\n\n"
        "Inserisci l'ora di uscita (formato 24h, es: 1423 per le 14:23):"
    )

async def gestisci_ora_uscita(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        ora_str = update.message.text.strip()
        if len(ora_str) != 4 or not ora_str.isdigit():
            raise ValueError("Formato non valido")
        
        ore = int(ora_str[:2])
        minuti = int(ora_str[2:])
        
        if not (0 <= ore <= 23 and 0 <= minuti <= 59):
            raise ValueError("Ora non valida")
        
        data_uscita = datetime.strptime(context.user_data['nuovo_intervento']['data_uscita'], '%Y-%m-%d')
        data_uscita = data_uscita.replace(hour=ore, minute=minuti)
        context.user_data['nuovo_intervento']['data_uscita_completa'] = data_uscita.strftime('%Y-%m-%d %H:%M:%S')
        context.user_data['fase'] = 'selezione_mezzo'
        
        mezzi = get_mezzi_attivi()
        keyboard = []
        for targa, tipo in mezzi:
            keyboard.append([InlineKeyboardButton(f"🚒 {targa} - {tipo}", callback_data=f"mezzo_{targa}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "🚒 **SELEZIONE MEZZO**\n\n"
            "Scegli il mezzo utilizzato:",
            reply_markup=reply_markup
        )
        
    except ValueError as e:
        await update.message.reply_text("❌ Formato ora non valido! Inserisci 4 cifre (es: 1423 per 14:23):")

async def gestisci_selezione_mezzo(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    await query.answer()
    
    targa = callback_data.replace('mezzo_', '')
    mezzi = get_mezzi_attivi()
    tipo_mezzo = next((tipo for targa_m, tipo in mezzi if targa_m == targa), "")
    
    context.user_data['nuovo_intervento']['mezzo_targa'] = targa
    context.user_data['nuovo_intervento']['mezzo_tipo'] = tipo_mezzo
    context.user_data['fase'] = 'selezione_capopartenza'
    
    vigili = get_vigili_attivi()
    keyboard = []
    for vigile_id, nome, cognome, qualifica in vigili:
        keyboard.append([InlineKeyboardButton(f"👨‍🚒 {cognome} {nome} ({qualifica})", callback_data=f"capo_{vigile_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        "👨‍🚒 **CAPOPARTENZA**\n\n"
        "Seleziona il capopartenza:",
        reply_markup=reply_markup
    )

async def gestisci_selezione_capopartenza(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    await query.answer()
    
    vigile_id = int(callback_data.replace('capo_', ''))
    vigile = get_vigile_by_id(vigile_id)
    
    context.user_data['nuovo_intervento']['capopartenza_id'] = vigile_id
    context.user_data['nuovo_intervento']['capopartenza'] = f"{vigile[1]} {vigile[2]}"
    context.user_data['fase'] = 'selezione_autista'
    
    vigili = get_vigili_attivi()
    keyboard = []
    for vigile_id, nome, cognome, qualifica in vigili:
        if vigile_id != context.user_data['nuovo_intervento']['capopartenza_id']:
            keyboard.append([InlineKeyboardButton(f"🚗 {cognome} {nome} ({qualifica})", callback_data=f"autista_{vigile_id}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        "🚗 **AUTISTA**\n\n"
        "Seleziona l'autista:",
        reply_markup=reply_markup
    )

async def gestisci_selezione_autista(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    await query.answer()
    
    vigile_id = int(callback_data.replace('autista_', ''))
    vigile = get_vigile_by_id(vigile_id)
    
    context.user_data['nuovo_intervento']['autista_id'] = vigile_id
    context.user_data['nuovo_intervento']['autista'] = f"{vigile[1]} {vigile[2]}"
    context.user_data['nuovo_intervento']['partecipanti'] = [
        context.user_data['nuovo_intervento']['capopartenza_id'],
        vigile_id
    ]
    context.user_data['fase'] = 'selezione_vigili'
    
    context.user_data['vigili_da_selezionare'] = [
        vigile for vigile in get_vigili_attivi() 
        if vigile[0] not in context.user_data['nuovo_intervento']['partecipanti']
    ]
    context.user_data['vigili_selezionati'] = []
    
    await mostra_selezione_vigili(query, context)

async def mostra_selezione_vigili(query, context):
    vigili_da_selezionare = context.user_data['vigili_da_selezionare']
    
    if not vigili_da_selezionare:
        context.user_data['fase'] = 'inserisci_indirizzo'
        await query.edit_message_text(
            "📝 **INDIRIZZO INTERVENTO**\n\n"
            "Inserisci l'indirizzo dell'intervento:"
        )
        return
    
    vigile_corrente = vigili_da_selezionare[0]
    vigile_id, nome, cognome, qualifica = vigile_corrente
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Sì", callback_data=f"vigile_si_{vigile_id}"),
            InlineKeyboardButton("❌ No", callback_data=f"vigile_no_{vigile_id}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"👨‍🚒 **PARTECIPANTI**\n\n"
        f"**{cognome} {nome}** ({qualifica})\n"
        f"Ha partecipato all'intervento?",
        reply_markup=reply_markup
    )

async def gestisci_selezione_vigile(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    await query.answer()
    
    parts = callback_data.split('_')
    scelta = parts[1]
    vigile_id = int(parts[2])
    
    if scelta == 'si':
        context.user_data['vigili_selezionati'].append(vigile_id)
        context.user_data['nuovo_intervento']['partecipanti'].append(vigile_id)
    
    context.user_data['vigili_da_selezionare'] = context.user_data['vigili_da_selezionare'][1:]
    
    await mostra_selezione_vigili(query, context)

async def gestisci_indirizzo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    indirizzo = update.message.text.strip()
    context.user_data['nuovo_intervento']['indirizzo'] = indirizzo
    context.user_data['fase'] = 'conferma'
    
    await mostra_riepilogo(update, context)

async def mostra_riepilogo(update, context):
    dati = context.user_data['nuovo_intervento']
    
    partecipanti_nomi = []
    for vigile_id in dati['partecipanti']:
        vigile = get_vigile_by_id(vigile_id)
        if vigile:
            partecipanti_nomi.append(f"{vigile[1]} {vigile[2]}")
    
    riepilogo = f"""
📋 **RIEPILOGO INTERVENTO**

🔢 **Progressivo Erba:** #{dati['numero_erba']}
📄 **Rapporto Como:** {dati['rapporto_como']}/{dati['progressivo_como']}
📅 **Uscita:** {datetime.strptime(dati['data_uscita_completa'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')}
🚒 **Mezzo:** {dati['mezzo_targa']} - {dati['mezzo_tipo']}
👨‍🚒 **Capopartenza:** {dati['capopartenza']}
🚗 **Autista:** {dati['autista']}
👥 **Partecipanti:** {', '.join(partecipanti_nomi)}
📍 **Indirizzo:** {dati['indirizzo']}
"""

    keyboard = [
        [
            InlineKeyboardButton("✅ Conferma", callback_data="conferma_si"),
            InlineKeyboardButton("❌ Annulla", callback_data="conferma_no")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if hasattr(update, 'message'):
        await update.message.reply_text(riepilogo, reply_markup=reply_markup)
    else:
        await update.edit_message_text(riepilogo, reply_markup=reply_markup)

async def conferma_intervento(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    await query.answer()
    
    if callback_data == "conferma_si":
        try:
            dati = context.user_data['nuovo_intervento']
            dati['data_rientro'] = None
            
            intervento_id = inserisci_intervento(dati)
            
            await query.edit_message_text(
                f"✅ **INTERVENTO REGISTRATO!**\n\n"
                f"Progressivo Erba: #{dati['numero_erba']}\n"
                f"Rapporto Como: {dati['rapporto_como']}/{dati['progressivo_como']}\n\n"
                f"L'intervento è stato salvato correttamente."
            )
            
        except Exception as e:
            await query.edit_message_text(f"❌ Errore durante il salvataggio: {str(e)}")
    else:
        await query.edit_message_text("❌ Intervento annullato.")
    
    for key in ['nuovo_intervento', 'fase', 'vigili_da_selezionare', 'vigili_selezionati']:
        if key in context.user_data:
            del context.user_data[key]

# === GESTIONE VIGILI (ADMIN) ===
async def gestione_vigili(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("👥 Lista Vigili", callback_data="lista_vigili")],
        [InlineKeyboardButton("✏️ Modifica Vigile", callback_data="modifica_vigile")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "⚙️ **GESTIONE VIGILI**\n\n"
        "Seleziona un'operazione:",
        reply_markup=reply_markup
    )

async def mostra_lista_vigili(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    vigili = get_tutti_vigili()
    if not vigili:
        await query.edit_message_text("❌ Nessun vigile trovato nel database.")
        return
    
    messaggio = "👥 **ELENCO VIGILI**\n\n"
    for vigile in vigili:
        id_v, nome, cognome, qualifica, grado, nautica, saf, tpss, atp, attivo = vigile
        status = "🟢" if attivo else "🔴"
        specialita = []
        if nautica: specialita.append("🛥️")
        if saf: specialita.append("🔥")
        if tpss: specialita.append("🚧")
        if atp: specialita.append("✈️")
        
        messaggio += f"{status} **{cognome} {nome}**\n"
        messaggio += f"   {qualifica} | Patente: {grado} {''.join(specialita)}\n\n"
    
    await query.edit_message_text(messaggio)

# === ESPORTAZIONE DATI ===
async def esporta_dati(update: Update, context: ContextTypes.DEFAULT_TYPE):
    interventi = get_ultimi_interventi(1000)
    
    if not interventi:
        await update.message.reply_text("❌ Nessun dato da esportare.")
        return
    
    output = StringIO()
    writer = csv.writer(output)
    
    writer.writerow([
        'Numero Erba', 'Rapporto Como', 'Progressivo', 'Data Uscita', 'Data Rientro',
        'Mezzo', 'Capopartenza', 'Autista', 'Partecipanti', 'Indirizzo', 'Tipologia'
    ])
    
    for intervento in interventi:
        id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, indirizzo, tipologia, created_at, partecipanti = intervento
        
        data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')
        data_rientro_fmt = datetime.strptime(data_rientro, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M') if data_rientro else ''
        mezzo = f"{mezzo_targa} - {mezzo_tipo}"
        
        writer.writerow([
            num_erba, rapporto, progressivo, data_uscita_fmt, data_rientro_fmt,
            mezzo, capo, autista, partecipanti or '', indirizzo, tipologia or ''
        ])
    
    csv_data = output.getvalue()
    output.close()
    
    await update.message.reply_document(
        document=StringIO(csv_data),
        filename=f"interventi_export_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        caption="📤 **Esportazione dati completata**"
    )

# === HELP ===
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
🆘 **GUIDA BOT INTERVENTI VVF**

🎯 **FUNZIONALITÀ PRINCIPALI:**

👤 **UTENTE:**
• ➕ Nuovo Intervento - Registra un nuovo intervento
• 📋 Ultimi Interventi - Visualizza gli ultimi 10 interventi
• 📊 Statistiche - Statistiche annuali
• 🔍 Cerca Rapporto - Cerca interventi per rapporto Como
• 📤 Esporta Dati - Esporta dati in CSV (solo admin)

👨‍💻 **ADMIN:**
• 👥 Gestisci Richieste - Approva nuovi utenti
• ⚙️ Gestione Vigili - Gestisci anagrafica vigili

🔧 **SISTEMA:**
• ✅ Always online con keep-alive
• 💾 Backup automatico ogni 25 minuti
• 🔒 Accesso controllato
• 📱 Interfaccia ottimizzata per mobile
"""

    await update.message.reply_text(help_text, reply_markup=crea_tastiera_fisica(update.effective_user.id))

# === GESTIONE MESSAGGI PRINCIPALE ===
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if not is_user_approved(user_id):
        if text == "🚀 Richiedi Accesso":
            await start(update, context)
        return

    fase = context.user_data.get('fase')
    if fase == 'inserisci_rapporto':
        await gestisci_rapporto_como(update, context)
    elif fase == 'ora_uscita':
        await gestisci_ora_uscita(update, context)
    elif fase == 'inserisci_indirizzo':
        await gestisci_indirizzo(update, context)
    
    elif text == "➕ Nuovo Intervento":
        await avvia_nuovo_intervento(update, context)
    
    elif text == "📋 Ultimi Interventi":
        interventi = get_ultimi_interventi(10)
        if not interventi:
            await update.message.reply_text("📭 Nessun intervento registrato.")
            return
        
        messaggio = "📋 **ULTIMI 10 INTERVENTI**\n\n"
        for intervento in interventi:
            id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, indirizzo, tipologia, created_at, partecipanti = intervento
            
            data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M')
            messaggio += f"🔢 **#{num_erba}** - R{rapporto}/{progressivo}\n"
            messaggio += f"📅 {data_uscita_fmt} - 🚒 {mezzo_targa}\n"
            messaggio += f"👨‍🚒 Capo: {capo}\n"
            messaggio += f"🚗 Autista: {autista}\n"
            messaggio += f"👥 Partecipanti: {partecipanti or 'Nessuno'}\n"
            messaggio += f"📍 {indirizzo}\n"
            messaggio += "─" * 30 + "\n"
        
        await update.message.reply_text(messaggio)
    
    elif text == "📊 Statistiche":
        stats = get_statistiche_anno(corrente=True)
        
        messaggio = f"📊 **STATISTICHE {datetime.now().year}**\n\n"
        messaggio += f"📈 **Totale interventi:** {stats['totale']}\n\n"
        
        if stats['tipologie']:
            messaggio += "📋 **Per tipologia:**\n"
            for tipologia, count in stats['tipologie'].items():
                if tipologia:
                    messaggio += f"• {tipologia}: {count}\n"
            messaggio += "\n"
        
        if stats['mensili']:
            messaggio += "📅 **Andamento mensile:**\n"
            for mese in sorted(stats['mensili'].keys()):
                count = stats['mensili'][mese]
                nome_mese = datetime.strptime(mese, '%m').strftime('%B')
                messaggio += f"• {nome_mese}: {count}\n"
        
        await update.message.reply_text(messaggio)
    
    elif text == "🔍 Cerca Rapporto":
        context.user_data['fase_ricerca'] = 'anno'
        await update.message.reply_text("🔍 **RICERCA RAPPORTO**\n\nInserisci l'anno del rapporto:")
    
    elif text == "📤 Esporta Dati":
        if is_admin(user_id):
            await esporta_dati(update, context)
        else:
            await update.message.reply_text("❌ Solo gli amministratori possono esportare i dati.")
    
    elif text == "👥 Gestisci Richieste" and is_admin(user_id):
        await gestisci_richieste(update, context)
    
    elif text == "⚙️ Gestione Vigili" and is_admin(user_id):
        await gestione_vigili(update, context)
    
    elif text == "🆘 Help":
        await help_command(update, context)
    
    elif context.user_data.get('fase_ricerca') == 'anno':
        anno = text.strip()
        if anno.isdigit() and len(anno) == 4:
            context.user_data['anno_ricerca'] = anno
            context.user_data['fase_ricerca'] = 'rapporto'
            await update.message.reply_text("Inserisci il numero del rapporto Como:")
        else:
            await update.message.reply_text("❌ Anno non valido! Inserisci 4 cifre (es: 2024):")
    
    elif context.user_data.get('fase_ricerca') == 'rapporto':
        rapporto = text.strip()
        anno = context.user_data.get('anno_ricerca')
        
        if rapporto.isdigit():
            interventi = get_interventi_per_rapporto(rapporto, anno)
            if not interventi:
                await update.message.reply_text(f"❌ Nessun intervento trovato per il rapporto {rapporto}/{anno}")
            else:
                messaggio = f"🔍 **RISULTATI RICERCA R{rapporto}/{anno}**\n\n"
                for intervento in interventi:
                    id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, indirizzo, tipologia, created_at = intervento
                    data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M')
                    messaggio += f"🔢 **#{num_erba}** - Prog: {progressivo}\n"
                    messaggio += f"📅 {data_uscita_fmt} - 🚒 {mezzo_targa}\n"
                    messaggio += f"👨‍🚒 Capo: {capo}\n"
                    messaggio += f"📍 {indirizzo}\n"
                    messaggio += "─" * 30 + "\n"
                
                await update.message.reply_text(messaggio)
        else:
            await update.message.reply_text("❌ Numero rapporto non valido!")
        
        for key in ['fase_ricerca', 'anno_ricerca']:
            if key in context.user_data:
                del context.user_data[key]
    
    else:
        await update.message.reply_text("ℹ️ Usa i pulsanti per navigare.", reply_markup=crea_tastiera_fisica(user_id))

# === GESTIONE BOTTONI INLINE ===
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    # Gestione nuovo intervento
    if data.startswith("tipo_"):
        await gestisci_scelta_tipo(update, context, data)
    
    elif data.startswith("collega_"):
        intervento_id = int(data.replace('collega_', ''))
        await gestisci_collega_intervento(update, context, intervento_id)
    
    elif data.startswith("data_"):
        await gestisci_data_uscita(update, context, data)
    
    elif data.startswith("mezzo_"):
        await gestisci_selezione_mezzo(update, context, data)
    
    elif data.startswith("capo_"):
        await gestisci_selezione_capopartenza(update, context, data)
    
    elif data.startswith("autista_"):
        await gestisci_selezione_autista(update, context, data)
    
    elif data.startswith("vigile_"):
        await gestisci_selezione_vigile(update, context, data)
    
    elif data.startswith("conferma_"):
        await conferma_intervento(update, context, data)
    
    # Gestione richieste accesso
    elif data.startswith("approva_"):
        if not is_admin(user_id):
            return
            
        user_id_approvare = int(data[8:])
        approva_utente(user_id_approvare)
        
        try:
            await context.bot.send_message(
                user_id_approvare,
                "✅ ACCESSO APPROVATO! Ora puoi usare tutte le funzioni del bot.\nUsa /start per iniziare."
            )
        except:
            pass
            
        richieste_rimanenti = get_richieste_in_attesa()
        if richieste_rimanenti:
            messaggio = f"✅ Utente approvato! 📋 Richieste rimanenti: {len(richieste_rimanenti)}"
        else:
            messaggio = "✅ Utente approvato! 🎉 Tutte le richieste gestite."
            
        await query.edit_message_text(messaggio)

    elif data.startswith("rifiuta_"):
        if not is_admin(user_id):
            return
            
        user_id_rifiutare = int(data[8:])
        conn = sqlite3.connect(DATABASE_NAME)
        c = conn.cursor()
        c.execute("DELETE FROM utenti WHERE user_id = ?", (user_id_rifiutare,))
        conn.commit()
        conn.close()
        
        richieste_rimanenti = get_richieste_in_attesa()
        if richieste_rimanenti:
            messaggio = f"❌ Utente rifiutato! 📋 Richieste rimanenti: {len(richieste_rimanenti)}"
        else:
            messaggio = "❌ Utente rifiutato! 🎉 Tutte le richieste gestite."
            
        await query.edit_message_text(messaggio)
    
    # Gestione vigili
    elif data == "lista_vigili":
        await mostra_lista_vigili(update, context)
    
    elif data == "modifica_vigile":
        await query.edit_message_text("✏️ Modifica vigile - da implementare")

# === MAIN ===
def main():
    print("🚀 Avvio Bot Interventi VVF...")
    
    print("🔄 Tentativo di ripristino database da backup...")
    if not restore_database_from_gist():
        print("🔄 Inizializzazione database nuovo...")
        init_db()
    
    print("🔍 Verifica integrità database...")
    if not check_database_integrity():
        print("🔄 Ricreazione database di emergenza...")
        emergency_recreate_database()
    
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print("✅ Flask server started on port 10000")
    
    keep_alive_thread = threading.Thread(target=keep_alive_aggressive, daemon=True)
    keep_alive_thread.start()
    print("✅ Sistema keep-alive ULTRA-AGGRESSIVO attivato! Ping ogni 5 minuti")
    
    backup_thread = threading.Thread(target=backup_scheduler, daemon=True)
    backup_thread.start()
    print("✅ Scheduler backup attivato! Backup ogni 25 minuti")
    
    application = Application.builder().token(BOT_TOKEN).build()
    
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("🤖 Bot Interventi VVF Avviato!")
    print("📍 Server: Render.com")
    print("🟢 Status: ONLINE con keep-alive ultra-aggressivo")
    print("💾 Database: SQLite3 con backup automatico")
    print("👥 Admin configurati:", len(ADMIN_IDS))
    print("⏰ Ping automatici ogni 5 minuti - Zero spin down! 🚀")
    print("💾 Backup automatici ogni 25 minuti - Dati al sicuro! 🛡️")
    
    application.run_polling()

if __name__ == '__main__':
    main()
