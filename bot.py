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
from io import StringIO, BytesIO
from telegram.error import BadRequest, NetworkError

# === CONFIGURAZIONE ===
DATABASE_NAME = 'interventi_vvf.db'
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_IDS = [1816045269, 653425963, 693843502, 6622015744]

# Configurazione backup GitHub
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN')
GIST_ID = os.environ.get('GIST_ID')

# === MAPPING TIPOLOGIE CON CODICI BREVI ===
TIPOLOGIE_MAPPING = {
    "27": ("27", "tip_01"),
    "Apertura porte e finestre": ("Apertura porte/finestre", "tip_02"),
    "Ascensore bloccato": ("Ascensore bloccato", "tip_03"),
    "Assistenza attività di Protezione Civile e Sanitarie": ("Ass. Protezione Civile", "tip_04"),
    "Assistenza TSO": ("Assistenza TSO", "tip_05"),
    "Bonifica insetti": ("Bonifica insetti", "tip_06"),
    "Crollo parziale di elementi strutturali": ("Crollo parziale", "tip_07"),
    "Danni d'acqua in genere": ("Danni d'acqua", "tip_08"),
    "Fuoriuscita di acqua per rottura di tubazioni, canali e simili": ("Fuoriuscita acqua", "tip_09"),
    "Esplosione": ("Esplosione", "tip_10"),
    "Frane": ("Frane", "tip_11"),
    "Fuga Gas": ("Fuga Gas", "tip_12"),
    "Guasto elettrico": ("Guasto elettrico", "tip_13"),
    "Incendio/fuoco controllato": ("Incendio controllato", "tip_14"),
    "Incendio abitazione": ("Incendio abitazione", "tip_15"),
    "Incendio Autovettura": ("Incendio autovettura", "tip_16"),
    "Incendio Boschivo": ("Incendio boschivo", "tip_17"),
    "Incendio Canna Fumaria": ("Incendio canna fumaria", "tip_18"),
    "Incendio Capannone": ("Incendio capannone", "tip_19"),
    "Incendio Cascina": ("Incendio cascina", "tip_20"),
    "Incendio generico": ("Incendio generico", "tip_21"),
    "Incendio sterpaglie": ("Incendio sterpaglie", "tip_22"),
    "Incendio Tetto": ("Incendio tetto", "tip_23"),
    "Incidente Aereo": ("Incidente aereo", "tip_24"),
    "Incidente stradale": ("Incidente stradale", "tip_25"),
    "Infortunio sul lavoro": ("Infortunio lavoro", "tip_26"),
    "Palo pericolante": ("Palo pericolante", "tip_27"),
    "Recupero animali morti": ("Recupero animali morti", "tip_28"),
    "Recupero / assistenza veicoli": ("Recupero veicoli", "tip_29"),
    "Recupero merci e beni": ("Recupero merci", "tip_30"),
    "Recupero Salma": ("Recupero salma", "tip_31"),
    "Ricerca Persona (SAR)": ("Ricerca persona", "tip_32"),
    "Rimozione ostacoli non dovuti al traffico": ("Rimozione ostacoli", "tip_33"),
    "Salvataggio animali": ("Salvataggio animali", "tip_34"),
    "Servizio Assistenza Generico": ("Servizio assistenza", "tip_35"),
    "Smontaggio controllato di elementi costruttivi": ("Smontaggio controllato", "tip_36"),
    "Soccorso Persona": ("Soccorso persona", "tip_37"),
    "Sopralluoghi e verifiche di stabilità edifici e manufatti": ("Sopralluogo stabilità", "tip_38"),
    "Sopralluogo per incendio": ("Sopralluogo incendio", "tip_39"),
    "Sversamenti": ("Sversamenti", "tip_40"),
    "Taglio Pianta": ("Taglio pianta", "tip_41"),
    "Tentato suicidio": ("Tentato suicidio", "tip_42")
}

# Lista delle tipologie complete per riferimento
TIPOLOGIE_INTERVENTO = list(TIPOLOGIE_MAPPING.keys())

# Funzioni di utilità per il mapping
def get_tipologia_by_callback(callback_data):
    """Restituisce la tipologia completa dato il callback breve"""
    for tipologia, (_, callback_breve) in TIPOLOGIE_MAPPING.items():
        if callback_breve == callback_data:
            return tipologia
    return None

def get_callback_by_tipologia(tipologia):
    """Restituisce il callback breve dato la tipologia completa"""
    if tipologia in TIPOLOGIE_MAPPING:
        return TIPOLOGIE_MAPPING[tipologia][1]
    return None

def get_display_name(tipologia):
    """Restituisce il nome da visualizzare per la tipologia"""
    if tipologia in TIPOLOGIE_MAPPING:
        return TIPOLOGIE_MAPPING[tipologia][0]
    return tipologia

# Gradi patente
GRADI_PATENTE = ["I", "II", "III", "IIIE"]

# Tipi mezzi predefiniti
TIPI_MEZZO_PREDEFINITI = ["APS TLF3", "ABP Daf", "A/TRID ML120E", "CA/PU Defender 110", "CA/PU Ranger Bosch.", "RI Motopompa Humbaur", "AF Polisoccorso", "FB Arimar", "AV E-Doblò", "Mezzo sostitutivo"]

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
                  comune TEXT,
                  via TEXT,
                  indirizzo TEXT,
                  tipologia TEXT,
                  cambio_personale BOOLEAN DEFAULT 0,
                  km_finali INTEGER,
                  litri_riforniti INTEGER,
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
                  telefono TEXT,
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
        ('26613', 'APS TLF3'),
        ('24674', 'ABP Daf'),
        ('26690', 'A/TRID ML120E'),
        ('23377', 'CA/PU Defender 110'),
        ('29471', 'CA/PU Ranger Bosch.'),
        ('04901', 'RI Motopompa Humbaur'),
        ('4020', 'FB Arimar'),
        ('28946', 'AF Polisoccorso'),
        ('35682', 'AV E-Doblò'),
        ('90117', 'Mezzo sostitutivo')
    ]
    for targa, tipo in mezzi_iniziali:
        c.execute('''INSERT OR IGNORE INTO mezzi (targa, tipo) VALUES (?, ?)''', (targa, tipo))

    # Inserisce vigili di base
    vigili_iniziali = [
        ('Rudi', 'Caverio', 'VV', 'IIIE', 0, 1, 0, 0),
        ('Simone', 'Maxenti', 'VV', 'IIIE', 1, 0, 1, 1),
        ('Gabriele', 'Redaelli', 'CSV', 'IIIE', 0, 1, 1, 1),
        ('Mauro', 'Zappa', 'VV', 'II', 0, 0, 1, 0),
        ('Giuseppe Felice', 'Baruffini', 'CSV', 'IIIE', 0, 0, 1, 0)
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
    c.execute('''SELECT user_id, username, nome, telefono, data_richiesta 
                 FROM utenti WHERE ruolo = 'in_attesa' ORDER BY data_richiesta''')
    result = c.fetchall()
    conn.close()
    return result

def get_utenti_approvati():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT user_id, username, nome, telefono, ruolo, data_approvazione 
                 FROM utenti WHERE ruolo IN ('admin', 'user') ORDER BY nome''')
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

def rimuovi_utente(user_id):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute("DELETE FROM utenti WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def aggiorna_telefono_utente(user_id, telefono):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''UPDATE utenti SET telefono = ? WHERE user_id = ?''', (telefono, user_id))
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

def get_ultimi_15_interventi():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT id, rapporto_como, progressivo_como, numero_erba, data_uscita, indirizzo
                 FROM interventi 
                 ORDER BY data_uscita DESC LIMIT 15''')
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

def get_interventi_per_anno(anno):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT * FROM interventi 
                 WHERE strftime('%Y', data_uscita) = ?
                 ORDER BY data_uscita DESC''', (anno,))
    result = c.fetchall()
    conn.close()
    return result

def get_intervento_by_rapporto(rapporto, progressivo):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT * FROM interventi 
                 WHERE rapporto_como = ? AND progressivo_como = ?''', (rapporto, progressivo))
    result = c.fetchone()
    conn.close()
    return result

def get_ultimi_km_mezzo(targa):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT km_finali FROM interventi 
                 WHERE mezzo_targa = ? AND km_finali IS NOT NULL 
                 ORDER BY data_uscita DESC LIMIT 1''', (targa,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else 0

def aggiorna_intervento(rapporto, progressivo, campo, valore):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute(f"UPDATE interventi SET {campo} = ? WHERE rapporto_como = ? AND progressivo_como = ?", 
              (valore, rapporto, progressivo))
    conn.commit()
    conn.close()

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

def get_ultimo_indirizzo_per_rapporto(rapporto):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT indirizzo FROM interventi 
                 WHERE rapporto_como = ? 
                 ORDER BY data_uscita DESC LIMIT 1''', (rapporto,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else ""

def get_ultima_tipologia_per_rapporto(rapporto):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT tipologia FROM interventi 
                 WHERE rapporto_como = ? 
                 ORDER BY data_uscita DESC LIMIT 1''', (rapporto,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else ""

def normalizza_comune(comune):
    """Normalizza il nome del comune con prima lettera maiuscola e resto minuscolo"""
    if not comune:
        return ""
    return ' '.join(word.capitalize() for word in comune.split())

def inserisci_intervento(dati):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    try:
        c.execute('''INSERT INTO interventi 
                    (rapporto_como, progressivo_como, numero_erba, data_uscita, data_rientro,
                     mezzo_targa, mezzo_tipo, capopartenza, autista, comune, via, indirizzo, tipologia, 
                     cambio_personale, km_finali, litri_riforniti)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (dati['rapporto_como'], dati['progressivo_como'], dati['numero_erba'],
                     dati['data_uscita_completa'], dati.get('data_rientro_completa'),
                     dati['mezzo_targa'], dati['mezzo_tipo'], dati['capopartenza'], 
                     dati['autista'], dati.get('comune', ''), dati.get('via', ''), 
                     dati.get('indirizzo', ''), dati.get('tipologia', ''), 
                     dati.get('cambio_personale', False), dati.get('km_finali'), 
                     dati.get('litri_riforniti')))
        
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

def calcola_durata_intervento(data_uscita, data_rientro):
    """Calcola la durata dell'intervento in ore e minuti"""
    try:
        if not data_rientro:
            return "In corso"
        
        uscita = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S')
        rientro = datetime.strptime(data_rientro, '%Y-%m-%d %H:%M:%S')
        
        durata = rientro - uscita
        ore = durata.seconds // 3600
        minuti = (durata.seconds % 3600) // 60
        
        return f"{ore:02d}:{minuti:02d}"
    except:
        return "N/A"

def get_statistiche_anno(anno=None):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    
    if anno:
        c.execute('''SELECT COUNT(DISTINCT rapporto_como) 
                     FROM interventi 
                     WHERE strftime('%Y', data_uscita) = ?''', (str(anno),))
        totale_interventi = c.fetchone()[0]
        
        c.execute('''SELECT COUNT(*) 
                     FROM interventi 
                     WHERE strftime('%Y', data_uscita) = ?''', (str(anno),))
        totale_partenze = c.fetchone()[0]
        
        c.execute('''SELECT tipologia, COUNT(*) 
                     FROM interventi 
                     WHERE strftime('%Y', data_uscita) = ?
                     GROUP BY tipologia''', (str(anno),))
        tipologie = c.fetchall()
        
        c.execute('''SELECT mezzo_tipo, COUNT(*) 
                     FROM interventi 
                     WHERE strftime('%Y', data_uscita) = ?
                     GROUP BY mezzo_tipo''', (str(anno),))
        mezzi = c.fetchall()
        
        c.execute('''SELECT strftime('%m', data_uscita) as mese, COUNT(*)
                     FROM interventi 
                     WHERE strftime('%Y', data_uscita) = ?
                     GROUP BY mese''', (str(anno),))
        mensili = c.fetchall()
    else:
        c.execute('''SELECT COUNT(DISTINCT rapporto_como) FROM interventi''')
        totale_interventi = c.fetchone()[0]
        
        c.execute('''SELECT COUNT(*) FROM interventi''')
        totale_partenze = c.fetchone()[0]
        
        c.execute('''SELECT tipologia, COUNT(*) FROM interventi GROUP BY tipologia''')
        tipologie = c.fetchall()
        
        c.execute('''SELECT mezzo_tipo, COUNT(*) FROM interventi GROUP BY mezzo_tipo''')
        mezzi = c.fetchall()
        
        c.execute('''SELECT strftime('%m', data_uscita) as mese, COUNT(*)
                     FROM interventi 
                     GROUP BY mese''')
        mensili = c.fetchall()
    
    conn.close()
    
    return {
        'totale_interventi': totale_interventi,
        'totale_partenze': totale_partenze,
        'tipologie': dict(tipologie),
        'mezzi': dict(mezzi),
        'mensili': dict(mensili)
    }

def get_anni_disponibili():
    """Restituisce la lista degli anni per cui ci sono interventi"""
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT DISTINCT strftime('%Y', data_uscita) as anno 
                 FROM interventi 
                 ORDER BY anno DESC''')
    anni = [row[0] for row in c.fetchall()]
    conn.close()
    return anni

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

def get_tutti_mezzi():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT * FROM mezzi ORDER BY tipo, targa''')
    result = c.fetchall()
    conn.close()
    return result

def get_tipi_mezzo():
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT DISTINCT tipo FROM mezzi ORDER BY tipo''')
    result = [row[0] for row in c.fetchall()]
    conn.close()
    
    for tipo in TIPI_MEZZO_PREDEFINITI:
        if tipo not in result:
            result.append(tipo)
    
    return sorted(result)

def aggiorna_vigile(vigile_id, campo, valore):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute(f"UPDATE vigili SET {campo} = ? WHERE id = ?", (valore, vigile_id))
    conn.commit()
    conn.close()

def aggiungi_vigile(nome, cognome, qualifica, grado_patente, patente_nautica=False, saf=False, tpss=False, atp=False):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''INSERT INTO vigili 
                (nome, cognome, qualifica, grado_patente_terrestre, patente_nautica, saf, tpss, atp) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                (nome, cognome, qualifica, grado_patente, patente_nautica, saf, tpss, atp))
    conn.commit()
    vigile_id = c.lastrowid
    conn.close()
    return vigile_id

def aggiungi_mezzo(targa, tipo):
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO mezzi (targa, tipo) VALUES (?, ?)''', (targa, tipo))
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
        
        current_gist_id = os.environ.get('GIST_ID')
        
        if current_gist_id:
            url = f'https://api.github.com/gists/{current_gist_id}'
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
            
            if not current_gist_id:
                new_gist_id = result['id']
                print(f"📝 Nuovo Gist ID creato: {new_gist_id}")
                print(f"⚠️  COPIA QUESTO GIST_ID NELLE VARIABILI AMBIENTE SU RENDER: {new_gist_id}")
                print(f"🔗 Gist URL: {result['html_url']}")
            
            return True
        else:
            print(f"❌ Errore backup Gist: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Errore durante backup: {str(e)}")
        return False

def restore_database_from_gist():
    current_gist_id = os.environ.get('GIST_ID')
    if not GITHUB_TOKEN or not current_gist_id:
        print("❌ Token o Gist ID non configurati - restore disabilitato")
        return False
    
    try:
        headers = {
            'Authorization': f'token {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }
        
        url = f'https://api.github.com/gists/{current_gist_id}'
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
    print("🔄 Backup iniziale in corso...")
    backup_database_to_gist()
    
    while True:
        time.sleep(1500)
        print("🔄 Backup automatico in corso...")
        backup_database_to_gist()

# === SISTEMA KEEP-ALIVE ULTRA-AGGRESSIVO ===
def keep_alive_aggressive():
    service_url = "https://bot-erba-interventi-2-0.onrender.com"
    urls = [
        f"{service_url}/health",
        f"{service_url}/", 
        f"{service_url}/ping",
        f"{service_url}/status"
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
        [KeyboardButton("📤 Estrazione Dati"), KeyboardButton("/start 🔄"), KeyboardButton("🆘 Help")]
    ]

    if is_admin(user_id):
        tastiera.append([KeyboardButton("👥 Gestisci Richieste"), KeyboardButton("⚙️ Gestione")])

    return ReplyKeyboardMarkup(tastiera, resize_keyboard=True, is_persistent=True)

# === SISTEMA DI SELEZIONE TIPOLOGIA PAGINATO CON MAPPING ===
def crea_tastiera_tipologie_paginata(page=0, items_per_page=8):
    """Crea una tastiera paginata per le tipologie usando il mapping"""
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    
    # Usa le chiavi del mapping (tipologie complete)
    tipologie_lista = list(TIPOLOGIE_MAPPING.keys())
    tipologie_pagina = tipologie_lista[start_idx:end_idx]
    
    keyboard = []
    
    # Aggiungi le tipologie della pagina corrente con nomi brevi
    for tipologia in tipologie_pagina:
        display_name = TIPOLOGIE_MAPPING[tipologia][0]
        callback_breve = TIPOLOGIE_MAPPING[tipologia][1]
        keyboard.append([InlineKeyboardButton(display_name, callback_data=callback_breve)])
    
    # Aggiungi bottoni di navigazione
    nav_buttons = []
    
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Precedente", callback_data=f"tipopage_{page-1}"))
    
    if end_idx < len(tipologie_lista):
        nav_buttons.append(InlineKeyboardButton("Successivo ➡️", callback_data=f"tipopage_{page+1}"))
    
    if nav_buttons:
        keyboard.append(nav_buttons)
    
    # Aggiungi sempre il bottone "Altro" per tipologie personalizzate
    keyboard.append([InlineKeyboardButton("✏️ Altra Tipologia (Personalizzata)", callback_data="tipologia_altro")])
    
    return InlineKeyboardMarkup(keyboard)

async def mostra_selezione_tipologia_paginata(update, context, page=0):
    """Mostra la selezione tipologie con paginazione"""
    reply_markup = crea_tastiera_tipologie_paginata(page)
    
    totale_tipologie = len(TIPOLOGIE_MAPPING)
    totale_pagine = (totale_tipologie + 7) // 8  # 8 items per pagina
    messaggio_paginazione = f" - Pagina {page+1} di {totale_pagine}" if totale_pagine > 1 else ""
    
    messaggio = f"🚨 **TIPOLOGIA INTERVENTO**{messaggio_paginazione}\n\n"
    messaggio += "Seleziona una tipologia dalla lista:\n\n"
    messaggio += "⚠️ **NOTA BENE:**\n"
    messaggio += "• Usa 'Altra Tipologia' SOLO se l'intervento non rientra in nessuna categoria sopra\n"
    messaggio += "• Le categorie sono quelle ufficiali del comando\n"
    messaggio += "• Scegli sempre la categoria più specifica possibile"
    
    if hasattr(update, 'message'):
        await update.message.reply_text(messaggio, reply_markup=reply_markup)
    else:
        await update.edit_message_text(messaggio, reply_markup=reply_markup)

# === IMPORT/EXPORT CSV ===
async def gestisci_file_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Solo gli amministratori possono importare dati.")
        return
    
    document = update.message.document
    if not document.file_name.endswith('.csv'):
        await update.message.reply_text("❌ Il file deve essere in formato CSV.")
        return
    
    try:
        file = await context.bot.get_file(document.file_id)
        file_content = await file.download_as_bytearray()
        
        encodings = ['utf-8', 'latin-1', 'windows-1252', 'iso-8859-1', 'cp1252']
        csv_content = None
        
        for encoding in encodings:
            try:
                csv_content = file_content.decode(encoding).splitlines()
                print(f"✅ File decodificato con encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if csv_content is None:
            await update.message.reply_text("❌ Impossibile decodificare il file. Usa un encoding UTF-8 valido.")
            return
        
        reader = csv.reader(csv_content)
        headers = next(reader)
        
        if 'user_id' in headers[0].lower() or 'telefono' in headers[0].lower():
            await gestisci_import_utenti(update, context, reader)
        else:
            await gestisci_import_interventi(update, context, reader)
        
    except Exception as e:
        await update.message.reply_text(f"❌ Errore durante l'importazione: {str(e)}")
        print(f"Errore dettagliato: {e}")

async def gestisci_import_interventi(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    skipped_count = 0
    error_count = 0
    
    for row in reader:
        try:
            if len(row) < 16:
                error_count += 1
                continue
            
            num_erba = int(row[0]) if row[0] and row[0].isdigit() else get_prossimo_numero_erba()
            rapporto_como = row[1]
            progressivo_como = row[2]
            
            existing = get_intervento_by_rapporto(rapporto_como, progressivo_como)
            if existing:
                skipped_count += 1
                continue
            
            try:
                data_uscita = datetime.strptime(row[3], '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')
            except:
                try:
                    data_uscita = datetime.strptime(row[3], '%d/%m/%Y').strftime('%Y-%m-%d %H:%M:%S')
                except:
                    data_uscita = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            data_rientro = None
            if row[4]:
                try:
                    data_rientro = datetime.strptime(row[4], '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')
                except:
                    try:
                        data_rientro = datetime.strptime(row[4], '%d/%m/%Y').strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        data_rientro = None
            
            dati = {
                'numero_erba': num_erba,
                'rapporto_como': rapporto_como,
                'progressivo_como': progressivo_como,
                'data_uscita_completa': data_uscita,
                'data_rientro_completa': data_rientro,
                'mezzo_targa': row[5],
                'mezzo_tipo': row[6],
                'capopartenza': row[7],
                'autista': row[8],
                'comune': row[10] if len(row) > 10 else '',
                'via': row[11] if len(row) > 11 else '',
                'indirizzo': row[12] if len(row) > 12 else '',
                'tipologia': row[13] if len(row) > 13 else '',
                'cambio_personale': row[14].lower() in ['sì', 'si', '1', 'true', 'vero'] if len(row) > 14 else False,
                'km_finali': int(row[15]) if len(row) > 15 and row[15] and row[15].isdigit() else None,
                'litri_riforniti': int(row[16]) if len(row) > 16 and row[16] and row[16].isdigit() else None,
                'partecipanti': []
            }
            
            inserisci_intervento(dati)
            imported_count += 1
            
        except Exception as e:
            error_count += 1
            print(f"Errore nell'importazione riga: {e}")
            continue
    
    await update.message.reply_text(
        f"✅ **IMPORTAZIONE INTERVENTI COMPLETATA**\n\n"
        f"📊 **Risultati:**\n"
        f"• ✅ Record importati: {imported_count}\n"
        f"• ⏭️ Record saltati (già presenti): {skipped_count}\n"
        f"• ❌ Errori: {error_count}\n\n"
        f"I dati sono stati aggiunti al database."
    )

async def gestisci_import_utenti(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    updated_count = 0
    error_count = 0
    
    for row in reader:
        try:
            if len(row) < 4:
                error_count += 1
                continue
            
            user_id = int(row[0])
            username = row[1] if len(row) > 1 else ''
            nome = row[2] if len(row) > 2 else ''
            telefono = row[3] if len(row) > 3 else ''
            ruolo = row[4] if len(row) > 4 else 'user'
            
            conn = sqlite3.connect(DATABASE_NAME)
            c = conn.cursor()
            c.execute("SELECT * FROM utenti WHERE user_id = ?", (user_id,))
            existing_user = c.fetchone()
            
            if existing_user:
                c.execute('''UPDATE utenti 
                            SET username = ?, nome = ?, telefono = ?, ruolo = ?
                            WHERE user_id = ?''', 
                         (username, nome, telefono, ruolo, user_id))
                updated_count += 1
            else:
                c.execute('''INSERT INTO utenti 
                            (user_id, username, nome, telefono, ruolo, data_approvazione) 
                            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''', 
                         (user_id, username, nome, telefono, ruolo))
                imported_count += 1
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            error_count += 1
            print(f"Errore nell'importazione utente: {e}")
            continue
    
    await update.message.reply_text(
        f"✅ **IMPORTAZIONE UTENTI COMPLETATA**\n\n"
        f"📊 **Risultati:**\n"
        f"• ✅ Utenti importati: {imported_count}\n"
        f"• 🔄 Utenti aggiornati: {updated_count}\n"
        f"• ❌ Errori: {error_count}\n\n"
        f"I dati utenti sono stati aggiornati nel database."
    )

# === ESTRAZIONE DATI ===
async def estrazione_dati(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    keyboard = [
        [InlineKeyboardButton("📋 Dati Interventi Completi", callback_data="export_tutto")],
        [InlineKeyboardButton("📅 Dati Interventi per Anno", callback_data="export_anno")]
    ]
    
    if is_admin(user_id):
        keyboard.extend([
            [InlineKeyboardButton("🏠 Status Caserma (Vigili+Mezzi)", callback_data="export_status")],
            [InlineKeyboardButton("👥 Utenti Approvati", callback_data="export_utenti")]
        ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📤 **ESTRAZIONE DATI**\n\n"
        "Seleziona il tipo di estrazione:",
        reply_markup=reply_markup
    )

async def esegui_export_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    try:
        vigili = get_tutti_vigili()
        mezzi = get_tutti_mezzi()
        
        output = StringIO()
        writer = csv.writer(output)
        
        writer.writerow(["_ VIGILI _"])
        writer.writerow(['Nome', 'Cognome', 'Qualifica', 'Grado Patente', 'Patente Nautica', 'SAF', 'TPSS', 'ATP', 'Stato'])
        
        for vigile in vigili:
            id_v, nome, cognome, qualifica, grado, nautica, saf, tpss, atp, attivo = vigile
            writer.writerow([
                nome, cognome, qualifica, grado,
                1 if nautica else 0,
                1 if saf else 0,
                1 if tpss else 0,
                1 if atp else 0,
                1 if attivo else 0
            ])
        
        writer.writerow([])
        writer.writerow(["_ MEZZI _"])
        writer.writerow(['Targa', 'Tipo', 'Stato'])
        
        for mezzo in mezzi:
            id_m, targa, tipo, attivo = mezzo
            writer.writerow([
                targa, tipo,
                1 if attivo else 0
            ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"status_caserma_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Status Caserma in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="🏠 **STATUS CASERMA**\n\nFile CSV contenente l'elenco completo di vigili e mezzi."
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione: {str(e)}")

async def esegui_export_utenti(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    try:
        utenti = get_utenti_approvati()
        
        output = StringIO()
        writer = csv.writer(output)
        
        writer.writerow(['user_id', 'username', 'nome', 'telefono', 'ruolo', 'data_approvazione'])
        
        for utente in utenti:
            user_id, username, nome, telefono, ruolo, data_approvazione = utente
            writer.writerow([
                user_id,
                username or '',
                nome or '',
                telefono or '',
                ruolo,
                data_approvazione or ''
            ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"utenti_approvati_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Utenti Approvati in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="👥 **UTENTI APPROVATI**\n\nFile CSV contenente l'elenco degli utenti approvati.\n\n"
                   "📝 **Formato per importazione:**\n"
                   "user_id,username,nome,telefono,ruolo,data_approvazione"
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione utenti: {str(e)}")

# === GESTIONE RICHIESTE ACCESSO ===
async def gestisci_richieste(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Solo gli amministratori possono gestire le richieste.")
        return

    richieste = get_richieste_in_attesa()
    utenti = get_utenti_approvati()
    utenti_normali = [u for u in utenti if u[0] not in ADMIN_IDS]
    
    keyboard = [
        [InlineKeyboardButton("📋 Richieste in attesa", callback_data="richieste_attesa")],
        [InlineKeyboardButton("👥 Utenti approvati", callback_data="utenti_approvati")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    messaggio = "👥 **GESTIONE RICHIESTE**\n\n"
    messaggio += f"📋 Richieste in attesa: {len(richieste)}\n"
    messaggio += f"👥 Utenti approvati: {len(utenti_normali)}\n\n"
    messaggio += "Seleziona un'operazione:"
    
    await update.message.reply_text(messaggio, reply_markup=reply_markup)

async def mostra_richieste_attesa(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    richieste = get_richieste_in_attesa()
    if not richieste:
        await query.edit_message_text("✅ Nessuna richiesta di accesso in sospeso.")
        return

    prima_richiesta = richieste[0]
    user_id_rich, username, nome, telefono, data_richiesta = prima_richiesta
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
    
    await query.edit_message_text(
        f"👤 **RICHIESTA ACCESSO**\n\n"
        f"🆔 **ID:** {user_id_rich}\n"
        f"👤 **Nome:** {nome}\n"
        f"📱 **Username:** @{username}\n"
        f"📞 **Telefono:** {telefono or 'Non fornito'}\n"
        f"📅 **Data:** {data}{info_rimanenti}",
        reply_markup=reply_markup
    )

async def mostra_utenti_approvati(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    utenti = get_utenti_approvati()
    if not utenti:
        await query.edit_message_text("❌ Nessun utente approvato trovato.")
        return
    
    utenti_normali = [u for u in utenti if u[0] not in ADMIN_IDS]
    
    if not utenti_normali:
        await query.edit_message_text("✅ Solo amministratori nel sistema. Nessun utente normale da rimuovere.")
        return
    
    keyboard = []
    for user_id_u, username, nome, telefono, ruolo, data_approvazione in utenti_normali:
        emoji = "👤"
        keyboard.append([
            InlineKeyboardButton(f"{emoji} {nome} (@{username})", callback_data=f"rimuovi_{user_id_u}")
        ])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "👥 **UTENTI APPROVATI**\n\n"
        "Seleziona un utente da rimuovere:",
        reply_markup=reply_markup
    )

async def conferma_rimozione_utente(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id_rimuovere: int):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    utenti = get_utenti_approvati()
    utente = next((u for u in utenti if u[0] == user_id_rimuovere), None)
    
    if not utente:
        await query.edit_message_text("❌ Utente non trovato.")
        return
    
    user_id_u, username, nome, telefono, ruolo, data_approvazione = utente
    
    keyboard = [
        [
            InlineKeyboardButton("✅ CONFERMA RIMOZIONE", callback_data=f"conferma_rimozione_{user_id_rimuovere}"),
            InlineKeyboardButton("❌ ANNULLA", callback_data="annulla_rimozione")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🚨 **CONFERMA RIMOZIONE UTENTE**\n\n"
        f"Stai per rimuovere l'accesso a:\n"
        f"👤 **Nome:** {nome}\n"
        f"📱 **Username:** @{username}\n"
        f"📞 **Telefono:** {telefono or 'Non fornito'}\n"
        f"🆔 **ID:** {user_id_rimuovere}\n\n"
        f"⚠️ **Questa azione è irreversibile!**\n"
        f"L'utente non potrà più accedere al bot.\n\n"
        f"Confermi la rimozione?",
        reply_markup=reply_markup
    )

async def esegui_rimozione_utente(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id_rimuovere: int):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    utenti = get_utenti_approvati()
    utente = next((u for u in utenti if u[0] == user_id_rimuovere), None)
    
    if utente:
        user_id_u, username, nome, telefono, ruolo, data_approvazione = utente
        rimuovi_utente(user_id_rimuovere)
        
        await query.edit_message_text(
            f"✅ **UTENTE RIMOSSO**\n\n"
            f"👤 **Nome:** {nome}\n"
            f"📱 **Username:** @{username}\n"
            f"📞 **Telefono:** {telefono or 'Non fornito'}\n"
            f"🆔 **ID:** {user_id_rimuovere}\n\n"
            f"L'utente non ha più accesso al bot."
        )
    else:
        await query.edit_message_text("❌ Utente non trovato.")

# === HANDLER START ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    for key in list(context.user_data.keys()):
        del context.user_data[key]
    
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

# === NUOVO INTERVENTO - FLUSSO COMPLETO ===
async def avvia_nuovo_intervento(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return

    for key in ['nuovo_intervento', 'fase', 'vigili_da_selezionare', 'vigili_selezionati']:
        if key in context.user_data:
            del context.user_data[key]
    
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
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    if callback_data == "tipo_nuovo":
        context.user_data['fase'] = 'inserisci_rapporto'
        await query.edit_message_text(
            "📝 **INSERISCI RAPPORTO COMO**\n\n"
            "Inserisci il numero del rapporto Como (solo numeri):"
        )
    else:
        interventi_recenti = get_ultimi_15_interventi()
        
        if not interventi_recenti:
            await query.edit_message_text("❌ Nessun intervento trovato nel database.")
            return
        
        keyboard = []
        for intervento in interventi_recenti:
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
            "🔗 **SELEZIONA INTERVENTO ESISTENTE**\n\n"
            "Scegli l'intervento a cui collegarti (ultimi 15):",
            reply_markup=reply_markup
        )

async def gestisci_collega_intervento(update: Update, context: ContextTypes.DEFAULT_TYPE, intervento_id: int):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT rapporto_como, numero_erba, indirizzo, tipologia FROM interventi WHERE id = ?''', (intervento_id,))
    intervento = c.fetchone()
    conn.close()
    
    if intervento:
        rapporto_como, numero_erba, indirizzo, tipologia = intervento
        progressivo_como = get_progressivo_per_rapporto(rapporto_como)
        
        context.user_data['nuovo_intervento']['rapporto_como'] = rapporto_como
        context.user_data['nuovo_intervento']['progressivo_como'] = progressivo_como
        context.user_data['nuovo_intervento']['numero_erba'] = numero_erba
        
        if progressivo_como != "01":
            if indirizzo:
                context.user_data['nuovo_intervento']['indirizzo'] = indirizzo
            if tipologia:
                context.user_data['nuovo_intervento']['tipologia'] = tipologia
        
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
        
        messaggio = f"🔗 **COLLEGATO A R{rapporto_como}**\n"
        messaggio += f"Progressivo: {progressivo_como}\n"
        
        if progressivo_como != "01":
            if indirizzo:
                messaggio += f"📍 Indirizzo: {indirizzo}\n"
            if tipologia:
                messaggio += f"🚨 Tipologia: {tipologia}\n"
        
        messaggio += "\n📅 **DATA USCITA**\nSeleziona la data di uscita:"
        
        await query.edit_message_text(messaggio, reply_markup=reply_markup)

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
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
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
        context.user_data['fase'] = 'data_rientro'
        
        oggi = datetime.now().strftime('%d/%m/%Y')
        ieri = (datetime.now() - timedelta(days=1)).strftime('%d/%m/%Y')
        
        keyboard = [
            [
                InlineKeyboardButton(f"🟢 OGGI ({oggi})", callback_data="rientro_oggi"),
                InlineKeyboardButton(f"🟡 IERI ({ieri})", callback_data="rientro_ieri")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "📅 **DATA RIENTRO**\n\n"
            "Seleziona la data di rientro:",
            reply_markup=reply_markup
        )
        
    except ValueError as e:
        await update.message.reply_text("❌ Formato ora non valido! Inserisci 4 cifre (es: 1423 per 14:23):")

async def gestisci_data_rientro(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    if callback_data == "rientro_oggi":
        data_rientro = datetime.now()
    else:
        data_rientro = datetime.now() - timedelta(days=1)
    
    context.user_data['nuovo_intervento']['data_rientro'] = data_rientro.strftime('%Y-%m-%d')
    context.user_data['fase'] = 'ora_rientro'
    
    await query.edit_message_text(
        "⏰ **ORA RIENTRO**\n\n"
        "Inserisci l'ora di rientro (formato 24h, es: 1630 per le 16:30):"
    )

async def gestisci_ora_rientro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        ora_str = update.message.text.strip()
        if len(ora_str) != 4 or not ora_str.isdigit():
            raise ValueError("Formato non valido")
        
        ore = int(ora_str[:2])
        minuti = int(ora_str[2:])
        
        if not (0 <= ore <= 23 and 0 <= minuti <= 59):
            raise ValueError("Ora non valida")
        
        data_rientro = datetime.strptime(context.user_data['nuovo_intervento']['data_rientro'], '%Y-%m-%d')
        data_rientro = data_rientro.replace(hour=ore, minute=minuti)
        
        data_uscita = datetime.strptime(context.user_data['nuovo_intervento']['data_uscita_completa'], '%Y-%m-%d %H:%M:%S')
        if data_rientro <= data_uscita:
            await update.message.reply_text(
                "❌ **ERRORE: L'ora di rientro deve essere successiva all'ora di uscita!**\n\n"
                f"Uscita: {data_uscita.strftime('%d/%m/%Y %H:%M')}\n"
                f"Rientro: {data_rientro.strftime('%d/%m/%Y %H:%M')}\n\n"
                "Inserisci nuovamente l'ora di rientro:"
            )
            return
        
        context.user_data['nuovo_intervento']['data_rientro_completa'] = data_rientro.strftime('%Y-%m-%d %H:%M:%S')
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
        await update.message.reply_text("❌ Formato ora non valido! Inserisci 4 cifres (es: 1630 per 16:30):")

async def gestisci_selezione_mezzo(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    targa = callback_data.replace('mezzo_', '')
    mezzi = get_mezzi_attivi()
    tipo_mezzo = next((tipo for targa_m, tipo in mezzi if targa_m == targa), "")
    
    context.user_data['nuovo_intervento']['mezzo_targa'] = targa
    context.user_data['nuovo_intervento']['mezzo_tipo'] = tipo_mezzo
    
    progressivo = context.user_data['nuovo_intervento'].get('progressivo_como', '01')
    
    if progressivo in ['02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
        context.user_data['fase'] = 'cambio_personale'
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Sì", callback_data="cambio_si"),
                InlineKeyboardButton("❌ No", callback_data="cambio_no")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            "🔄 **CAMBIO PERSONALE**\n\n"
            "Il mezzo è uscito per cambio personale?",
            reply_markup=reply_markup
        )
    else:
        context.user_data['nuovo_intervento']['cambio_personale'] = False
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

async def gestisci_cambio_personale(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    context.user_data['nuovo_intervento']['cambio_personale'] = (callback_data == "cambio_si")
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
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
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
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    vigile_id = int(callback_data.replace('autista_', ''))
    vigile = get_vigile_by_id(vigile_id)
    
    context.user_data['nuovo_intervento']['autista_id'] = vigile_id
    context.user_data['nuovo_intervento']['autista'] = f"{vigile[1]} {vigile[2]}"
    
    partecipanti_attuali = context.user_data['nuovo_intervento'].get('partecipanti', [])
    if context.user_data['nuovo_intervento']['capopartenza_id'] not in partecipanti_attuali:
        partecipanti_attuali.append(context.user_data['nuovo_intervento']['capopartenza_id'])
    if vigile_id not in partecipanti_attuali:
        partecipanti_attuali.append(vigile_id)
    
    context.user_data['nuovo_intervento']['partecipanti'] = partecipanti_attuali
    context.user_data['fase'] = 'selezione_vigili'
    
    tutti_vigili = get_vigili_attivi()
    context.user_data['vigili_da_selezionare'] = [
        vigile for vigile in tutti_vigili 
        if vigile[0] not in context.user_data['nuovo_intervento']['partecipanti']
    ]
    context.user_data['vigili_selezionati'] = []
    
    await mostra_selezione_vigili(query, context)

async def mostra_selezione_vigili(query, context):
    vigili_da_selezionare = context.user_data['vigili_da_selezionare']
    
    if not vigili_da_selezionare:
        context.user_data['fase'] = 'inserisci_comune'
        await query.edit_message_text(
            "🏘️ **COMUNE**\n\n"
            "Inserisci il comune dell'intervento:"
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
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    parts = callback_data.split('_')
    scelta = parts[1]
    vigile_id = int(parts[2])
    
    if scelta == 'si':
        if vigile_id not in context.user_data['nuovo_intervento']['partecipanti']:
            context.user_data['nuovo_intervento']['partecipanti'].append(vigile_id)
    
    context.user_data['vigili_da_selezionare'] = context.user_data['vigili_da_selezionare'][1:]
    
    await mostra_selezione_vigili(query, context)

async def gestisci_comune(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comune = update.message.text.strip()
    comune_normalizzato = normalizza_comune(comune)
    context.user_data['nuovo_intervento']['comune'] = comune_normalizzato
    context.user_data['fase'] = 'inserisci_via'
    
    await update.message.reply_text(
        "📍 **VIA**\n\n"
        "Inserisci la via dell'intervento:"
    )

async def gestisci_via(update: Update, context: ContextTypes.DEFAULT_TYPE):
    via = update.message.text.strip()
    context.user_data['nuovo_intervento']['via'] = via
    
    comune = context.user_data['nuovo_intervento'].get('comune', '')
    indirizzo_completo = f"{comune}, {via}" if comune else via
    context.user_data['nuovo_intervento']['indirizzo'] = indirizzo_completo
    
    context.user_data['fase'] = 'tipologia_intervento'
    await mostra_selezione_tipologia_paginata(update, context)

async def gestisci_tipologia_intervento(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    if callback_data.startswith("tipopage_"):
        page = int(callback_data.replace('tipopage_', ''))
        await mostra_selezione_tipologia_paginata(query, context, page)
    
    elif callback_data == "tipologia_altro":
        context.user_data['fase'] = 'inserisci_tipologia_personalizzata'
        await query.edit_message_text(
            "✏️ **TIPOLOGIA PERSONALIZZATA**\n\n"
            "Inserisci la tipologia di intervento:"
        )
    else:
        # Usa il mapping per ottenere la tipologia completa dal callback breve
        tipologia_completa = get_tipologia_by_callback(callback_data)
        if tipologia_completa:
            context.user_data['nuovo_intervento']['tipologia'] = tipologia_completa
            context.user_data['fase'] = 'km_finali'
            
            await query.edit_message_text(
                f"✅ Tipologia selezionata: **{tipologia_completa}**\n\n"
                "🛣️ **KM FINALI**\n\n"
                "Inserisci i km finali del mezzo (solo numeri):"
            )
        else:
            await query.edit_message_text("❌ Errore nella selezione della tipologia. Riprova.")

async def gestisci_tipologia_personalizzata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tipologia = update.message.text.strip()
    context.user_data['nuovo_intervento']['tipologia'] = tipologia
    context.user_data['fase'] = 'km_finali'
    
    await update.message.reply_text(
        f"✅ Tipologia personalizzata salvata: **{tipologia}**\n\n"
        "🛣️ **KM FINALI**\n\n"
        "Inserisci i km finali del mezzo (solo numeri):"
    )

async def gestisci_km_finali(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        km_finali = int(update.message.text.strip())
        targa = context.user_data['nuovo_intervento']['mezzo_targa']
        ultimi_km = get_ultimi_km_mezzo(targa)
        
        if km_finali < ultimi_km:
            await update.message.reply_text(
                f"❌ **ATTENZIONE: Km finali inferiori ai precedenti!**\n\n"
                f"Ultimi km registrati: {ultimi_km}\n"
                f"Km inseriti: {km_finali}\n\n"
                f"Controlla i dati e inserisci nuovamente i km finali:"
            )
            return
        
        context.user_data['nuovo_intervento']['km_finali'] = km_finali
        context.user_data['fase'] = 'litri_riforniti'
        
        await update.message.reply_text(
            "⛽ **LITRI RIFORNITI**\n\n"
            "Inserisci i litri riforniti nel mezzo (solo numeri):"
        )
        
    except ValueError:
        await update.message.reply_text("❌ Valore non valido! Inserisci solo numeri interi:")

async def gestisci_litri_riforniti(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        litri_riforniti = int(update.message.text.strip())
        if litri_riforniti < 0:
            raise ValueError("Valore negativo")
        
        context.user_data['nuovo_intervento']['litri_riforniti'] = litri_riforniti
        context.user_data['fase'] = 'conferma'
        
        await mostra_riepilogo(update, context)
        
    except ValueError:
        await update.message.reply_text("❌ Valore non valido! Inserisci solo numeri interi:")

async def mostra_riepilogo(update, context):
    dati = context.user_data['nuovo_intervento']
    
    partecipanti_nomi = []
    for vigile_id in dati['partecipanti']:
        vigile = get_vigile_by_id(vigile_id)
        if vigile:
            partecipanti_nomi.append(f"{vigile[1]} {vigile[2]}")
    
    partecipanti_nomi = list(dict.fromkeys(partecipanti_nomi))
    
    cambio_personale = "✅ Sì" if dati.get('cambio_personale', False) else "❌ No"
    km_finali = dati.get('km_finali', 'Non specificato')
    litri_riforniti = dati.get('litri_riforniti', 'Non specificato')
    
    durata = calcola_durata_intervento(dati['data_uscita_completa'], dati.get('data_rientro_completa'))
    
    riepilogo = f"""
📋 **RIEPILOGO INTERVENTO**

🔢 **Progressivo Erba:** #{dati['numero_erba']}
📄 **Rapporto Como:** {dati['rapporto_como']}/{dati['progressivo_como']}
📅 **Uscita:** {datetime.strptime(dati['data_uscita_completa'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')}
📅 **Rientro:** {datetime.strptime(dati['data_rientro_completa'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')}
⏱️ **Durata:** {durata}
🚒 **Mezzo:** {dati['mezzo_targa']} - {dati['mezzo_tipo']}
🔄 **Cambio personale:** {cambio_personale}
🛣️ **Km finali:** {km_finali}
⛽ **Litri riforniti:** {litri_riforniti}
👨‍🚒 **Capopartenza:** {dati['capopartenza']}
🚗 **Autista:** {dati['autista']}
🚨 **Tipologia:** {dati.get('tipologia', 'Non specificata')}
👥 **Partecipanti:** {', '.join(partecipanti_nomi)}
🏘️ **Comune:** {dati.get('comune', 'Non specificato')}
📍 **Via:** {dati.get('via', 'Non specificata')}
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
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    if callback_data == "conferma_si":
        try:
            dati = context.user_data['nuovo_intervento']
            
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

# === GESTIONE AMMINISTRATIVA ===
async def gestione_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Solo gli amministratori possono accedere a questa funzione.")
        return
        
    keyboard = [
        [InlineKeyboardButton("👥 Gestione Vigili", callback_data="admin_vigili")],
        [InlineKeyboardButton("🚒 Gestione Mezzi", callback_data="admin_mezzi")],
        [InlineKeyboardButton("✏️ Modifica Intervento", callback_data="modifica_intervento")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "⚙️ **GESTIONE AMMINISTRATIVA**\n\n"
        "Seleziona un'operazione:",
        reply_markup=reply_markup
    )

async def gestione_vigili_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    keyboard = [
        [InlineKeyboardButton("👥 Lista Vigili", callback_data="lista_vigili")],
        [InlineKeyboardButton("📥 Importa Vigili .csv", callback_data="importa_vigili")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "👥 **GESTIONE VIGILI**\n\n"
        "Seleziona un'operazione:",
        reply_markup=reply_markup
    )

async def gestione_mezzi_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    keyboard = [
        [InlineKeyboardButton("🚒 Lista Mezzi", callback_data="lista_mezzi")],
        [InlineKeyboardButton("📥 Importa Mezzi .csv", callback_data="importa_mezzi_info")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "🚒 **GESTIONE MEZZI**\n\n"
        "Seleziona un'operazione:",
        reply_markup=reply_markup
    )

async def importa_mezzi_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    await query.edit_message_text(
        "📥 **IMPORTA MEZZI DA CSV**\n\n"
        "Per aggiungere nuovi mezzi, invia un file CSV con questa formattazione:\n\n"
        "Targa,Tipo\n"
        "AB123CD,APS\n"
        "EF456GH,ABP\n\n"
        "I mezzi verranno aggiunti automaticamente al database."
    )

async def mostra_lista_vigili(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    vigili = get_tutti_vigili()
    if not vigili:
        await query.edit_message_text("❌ Nessun vigile trovato nel database.")
        return
    
    messaggio = "👥 **ELENCO COMPLETO VIGILI**\n\n"
    for vigile in vigili:
        id_v, nome, cognome, qualifica, grado, nautica, saf, tpss, atp, attivo = vigile
        status = "🟢" if attivo else "🔴"
        specialita = []
        if nautica: specialita.append("🛥️")
        if saf: specialita.append("🔗")
        if tpss: specialita.append("🚑")
        if atp: specialita.append("🤿")
        
        messaggio += f"{status} **{cognome} {nome}** (ID: {id_v})\n"
        messaggio += f"   {qualifica} | Patente: {grado} {''.join(specialita)}\n\n"
    
    await query.edit_message_text(messaggio)

async def mostra_lista_mezzi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    mezzi = get_tutti_mezzi()
    if not mezzi:
        await query.edit_message_text("❌ Nessun mezzo trovato nel database.")
        return
    
    messaggio = "🚒 **ELENCO MEZZI**\n\n"
    for mezzo in mezzi:
        id_m, targa, tipo, attivo = mezzo
        status = "🟢" if attivo else "🔴"
        messaggio += f"{status} **{targa}** - {tipo}\n"
    
    await query.edit_message_text(messaggio)

async def importa_vigili_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    await query.edit_message_text(
        "📥 **IMPORTA VIGILI DA CSV**\n\n"
        "Invia un file CSV con l'elenco dei vigili.\n\n"
        "Formattazione richiesta:\n"
        "Nome,Cognome,Qualifica,Grado Patente,Patente Nautica (1/0),SAF (1/0),TPSS (1/0),ATP (1/0)\n\n"
        "Esempio:\n"
        "Mario,Rossi,CSV,III,1,0,1,0"
    )

# === MODIFICA INTERVENTO ===
async def avvia_modifica_intervento(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    for key in ['modifica_intervento', 'fase_modifica']:
        if key in context.user_data:
            del context.user_data[key]
    
    context.user_data['modifica_intervento'] = {}
    context.user_data['fase_modifica'] = 'anno'
    
    await query.edit_message_text(
        "✏️ **MODIFICA INTERVENTO**\n\n"
        "Inserisci l'ANNO del rapporto Como da modificare (es: 2024):"
    )

async def gestisci_anno_modifica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    anno = update.message.text.strip()
    
    if not anno.isdigit() or len(anno) != 4:
        await update.message.reply_text("❌ Anno non valido! Inserisci 4 cifre (es: 2024):")
        return
    
    context.user_data['modifica_intervento']['anno'] = anno
    context.user_data['fase_modifica'] = 'rapporto'
    
    await update.message.reply_text(
        f"📅 Anno selezionato: {anno}\n\n"
        "Inserisci il numero del rapporto Como da modificare:"
    )

async def gestisci_rapporto_modifica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rapporto = update.message.text.strip()
    
    if not rapporto.isdigit():
        await update.message.reply_text("❌ Inserisci solo numeri! Riprova:")
        return
    
    anno = context.user_data['modifica_intervento']['anno']
    context.user_data['modifica_intervento']['rapporto'] = rapporto
    context.user_data['fase_modifica'] = 'progressivo'
    
    await update.message.reply_text(
        f"📄 Rapporto: {rapporto}\n"
        f"📅 Anno: {anno}\n\n"
        "Inserisci il progressivo dell'intervento da modificare (es: 01, 02):"
    )

async def gestisci_progressivo_modifica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    progressivo = update.message.text.strip().zfill(2)
    
    if not progressivo.isdigit() or len(progressivo) != 2:
        await update.message.reply_text("❌ Progressivo non valido! Inserisci 2 cifre (es: 01):")
        return
    
    rapporto = context.user_data['modifica_intervento']['rapporto']
    anno = context.user_data['modifica_intervento']['anno']
    
    interventi_anno = get_interventi_per_anno(anno)
    intervento_trovato = None
    
    for intervento in interventi_anno:
        if len(intervento) >= 4:
            rapporto_db = intervento[1]
            progressivo_db = intervento[2]
            
            if rapporto_db == rapporto and progressivo_db == progressivo:
                intervento_trovato = intervento
                break
    
    if not intervento_trovato:
        await update.message.reply_text(
            f"❌ Intervento R{rapporto}/{progressivo} per l'anno {anno} non trovato.\n"
            f"Verifica i dati e riprova."
        )
        for key in ['modifica_intervento', 'fase_modifica']:
            if key in context.user_data:
                del context.user_data[key]
        return
    
    context.user_data['modifica_intervento']['progressivo'] = progressivo
    context.user_data['modifica_intervento']['dati'] = intervento_trovato
    
    await mostra_campi_modificabili(update, context)

async def mostra_campi_modificabili(update, context):
    intervento = context.user_data['modifica_intervento']['dati']
    rapporto = context.user_data['modifica_intervento']['rapporto']
    progressivo = context.user_data['modifica_intervento']['progressivo']
    anno = context.user_data['modifica_intervento']['anno']
    
    if len(intervento) >= 16:
        id_int, rapporto, prog, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at = intervento[:16]
        
        keyboard = [
            [InlineKeyboardButton("📅 Data/Ora Uscita", callback_data="campo_data_uscita")],
            [InlineKeyboardButton("📅 Data/Ora Rientro", callback_data="campo_data_rientro")],
            [InlineKeyboardButton("🚒 Mezzo", callback_data="campo_mezzo")],
            [InlineKeyboardButton("👨‍🚒 Capopartenza", callback_data="campo_capopartenza")],
            [InlineKeyboardButton("🚗 Autista", callback_data="campo_autista")],
            [InlineKeyboardButton("📍 Indirizzo", callback_data="campo_indirizzo")],
            [InlineKeyboardButton("🚨 Tipologia", callback_data="campo_tipologia")],
            [InlineKeyboardButton("🛣️ Km Finali", callback_data="campo_km_finali")],
            [InlineKeyboardButton("⛽ Litri Riforniti", callback_data="campo_litri_riforniti")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"✏️ **MODIFICA INTERVENTO R{rapporto}/{progressivo} - {anno}**\n\n"
            f"Seleziona il campo da modificare:\n\n"
            f"📅 Uscita: {datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')}\n"
            f"📅 Rientro: {datetime.strptime(data_rientro, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M') if data_rientro else 'Non specificato'}\n"
            f"🚒 Mezzo: {mezzo_targa} - {mezzo_tipo}\n"
            f"👨‍🚒 Capo: {capo}\n"
            f"🚗 Autista: {autista}\n"
            f"📍 Indirizzo: {indirizzo}\n"
            f"🚨 Tipologia: {tipologia or 'Non specificata'}\n"
            f"🛣️ Km finali: {km_finali or 'Non specificato'}\n"
            f"⛽ Litri riforniti: {litri_riforniti or 'Non specificato'}",
            reply_markup=reply_markup
        )

async def gestisci_selezione_campo(update: Update, context: ContextTypes.DEFAULT_TYPE, campo: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    context.user_data['modifica_intervento']['campo_selezionato'] = campo
    
    if campo in ['mezzo', 'capopartenza', 'autista']:
        if campo == 'mezzo':
            mezzi = get_mezzi_attivi()
            keyboard = []
            for targa, tipo in mezzi:
                keyboard.append([InlineKeyboardButton(f"{targa} - {tipo}", callback_data=f"modmezzo_{targa}")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Seleziona il nuovo mezzo:", reply_markup=reply_markup)
        
        elif campo == 'capopartenza':
            vigili = get_vigili_attivi()
            keyboard = []
            for vigile_id, nome, cognome, qualifica in vigili:
                keyboard.append([InlineKeyboardButton(f"{cognome} {nome} ({qualifica})", callback_data=f"modcapo_{vigile_id}")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Seleziona il nuovo capopartenza:", reply_markup=reply_markup)
        
        elif campo == 'autista':
            vigili = get_vigili_attivi()
            keyboard = []
            for vigile_id, nome, cognome, qualifica in vigili:
                keyboard.append([InlineKeyboardButton(f"{cognome} {nome} ({qualifica})", callback_data=f"modautista_{vigile_id}")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("Seleziona il nuovo autista:", reply_markup=reply_markup)
    
    elif campo == 'tipologia':
        context.user_data['fase_modifica'] = 'modifica_tipologia'
        await mostra_selezione_tipologia_paginata(query, context, 0)
    
    elif campo == 'indirizzo':
        context.user_data['fase_modifica'] = 'modifica_indirizzo'
        context.user_data['sottofase_indirizzo'] = 'comune'
        await query.edit_message_text(
            "🏘️ **MODIFICA INDIRIZZO**\n\n"
            "Inserisci il nuovo comune dell'intervento:"
        )
    
    elif campo in ['data_uscita', 'data_rientro']:
        context.user_data['fase_modifica'] = 'modifica_orari'
        context.user_data['tipo_orario'] = campo
        
        if campo == 'data_uscita':
            await query.edit_message_text(
                "⏰ **MODIFICA DATA/ORA USCITA**\n\n"
                "Inserisci la nuova data e ora di uscita nel formato:\n"
                "GG/MM/AAAA HH:MM\n\n"
                "Esempio: 25/12/2024 14:30"
            )
        else:
            await query.edit_message_text(
                "⏰ **MODIFICA DATA/ORA RIENTRO**\n\n"
                "Inserisci la nuova data e ora di rientro nel formato:\n"
                "GG/MM/AAAA HH:MM\n\n"
                "Esempio: 25/12/2024 16:45"
            )
    
    else:
        context.user_data['fase_modifica'] = 'inserisci_valore'
        messaggi_campi = {
            'km_finali': "Inserisci i nuovi km finali:",
            'litri_riforniti': "Inserisci i nuovi litri riforniti:"
        }
        
        await query.edit_message_text(messaggi_campi.get(campo, "Inserisci il nuovo valore:"))

async def gestisci_modifica_indirizzo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    testo = update.message.text.strip()
    sottofase = context.user_data['sottofase_indirizzo']
    
    if sottofase == 'comune':
        comune_normalizzato = normalizza_comune(testo)
        context.user_data['modifica_intervento']['nuovo_comune'] = comune_normalizzato
        context.user_data['sottofase_indirizzo'] = 'via'
        
        await update.message.reply_text(
            "📍 **MODIFICA VIA**\n\n"
            "Inserisci la nuova via dell'intervento:"
        )
    
    elif sottofase == 'via':
        via = testo.strip()
        context.user_data['modifica_intervento']['nuova_via'] = via
        
        comune = context.user_data['modifica_intervento'].get('nuovo_comune', '')
        indirizzo_completo = f"{comune}, {via}" if comune else via
        
        rapporto = context.user_data['modifica_intervento']['rapporto']
        progressivo = context.user_data['modifica_intervento']['progressivo']
        
        try:
            aggiorna_intervento(rapporto, progressivo, 'comune', comune)
            aggiorna_intervento(rapporto, progressivo, 'via', via)
            aggiorna_intervento(rapporto, progressivo, 'indirizzo', indirizzo_completo)
            
            await update.message.reply_text(
                f"✅ **INDIRIZZO AGGIORNATO!**\n\n"
                f"Rapporto: R{rapporto}/{progressivo}\n"
                f"Nuovo comune: {comune}\n"
                f"Nuova via: {via}\n"
                f"Indirizzo completo: {indirizzo_completo}"
            )
            
        except Exception as e:
            await update.message.reply_text(f"❌ Errore durante l'aggiornamento: {str(e)}")
        
        for key in ['modifica_intervento', 'fase_modifica', 'sottofase_indirizzo', 'nuovo_comune', 'nuova_via']:
            if key in context.user_data:
                del context.user_data[key]

async def gestisci_modifica_orari(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        nuovo_valore = update.message.text.strip()
        data_ora = datetime.strptime(nuovo_valore, '%d/%m/%Y %H:%M')
        nuovo_valore_db = data_ora.strftime('%Y-%m-%d %H:%M:%S')
        
        campo = context.user_data['tipo_orario']
        rapporto = context.user_data['modifica_intervento']['rapporto']
        progressivo = context.user_data['modifica_intervento']['progressivo']
        
        if campo == 'data_rientro':
            intervento = get_intervento_by_rapporto(rapporto, progressivo)
            if intervento:
                data_uscita_db = intervento[4]
                data_uscita = datetime.strptime(data_uscita_db, '%Y-%m-%d %H:%M:%S')
                
                if data_ora <= data_uscita:
                    await update.message.reply_text(
                        "❌ **ERRORE: L'ora di rientro deve essere successiva all'ora di uscita!**\n\n"
                        f"Uscita: {data_uscita.strftime('%d/%m/%Y %H:%M')}\n"
                        f"Rientro inserito: {data_ora.strftime('%d/%m/%Y %H:%M')}\n\n"
                        "Inserisci nuovamente la data/ora di rientro:"
                    )
                    return
        
        campo_db = 'data_uscita' if campo == 'data_uscita' else 'data_rientro'
        aggiorna_intervento(rapporto, progressivo, campo_db, nuovo_valore_db)
        
        await update.message.reply_text(
            f"✅ **INTERVENTO MODIFICATO!**\n\n"
            f"Rapporto: R{rapporto}/{progressivo}\n"
            f"Campo aggiornato: {campo}\n"
            f"Nuovo valore: {data_ora.strftime('%d/%m/%Y %H:%M')}"
        )
        
    except ValueError as e:
        await update.message.reply_text(
            "❌ Formato data/ora non valido!\n\n"
            "Inserisci nel formato: GG/MM/AAAA HH:MM\n"
            "Esempio: 25/12/2024 14:30\n\n"
            "Riprova:"
        )
        return
    except Exception as e:
        await update.message.reply_text(f"❌ Errore durante la modifica: {str(e)}")
    
    for key in ['modifica_intervento', 'fase_modifica', 'tipo_orario']:
        if key in context.user_data:
            del context.user_data[key]

async def gestisci_valore_modifica_bottoni(update: Update, context: ContextTypes.DEFAULT_TYPE, campo: str, valore: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    rapporto = context.user_data['modifica_intervento']['rapporto']
    progressivo = context.user_data['modifica_intervento']['progressivo']
    campo_selezionato = context.user_data['modifica_intervento']['campo_selezionato']
    
    try:
        campi_db = {
            'mezzo': 'mezzo_targa',
            'capopartenza': 'capopartenza',
            'autista': 'autista',
            'tipologia': 'tipologia'
        }
        
        campo_db = campi_db.get(campo_selezionato)
        
        if campo_selezionato == 'mezzo':
            mezzi = get_mezzi_attivi()
            tipo_mezzo = next((tipo for targa_m, tipo in mezzi if targa_m == valore), "")
            aggiorna_intervento(rapporto, progressivo, 'mezzo_targa', valore)
            aggiorna_intervento(rapporto, progressivo, 'mezzo_tipo', tipo_mezzo)
            valore_mostrato = f"{valore} - {tipo_mezzo}"
        else:
            aggiorna_intervento(rapporto, progressivo, campo_db, valore)
            valore_mostrato = valore
        
        await query.edit_message_text(
            f"✅ **INTERVENTO MODIFICATO!**\n\n"
            f"Rapporto: R{rapporto}/{progressivo}\n"
            f"Campo aggiornato: {campo_selezionato}\n"
            f"Nuovo valore: {valore_mostrato}"
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante la modifica: {str(e)}")
    
    for key in ['modifica_intervento', 'fase_modifica']:
        if key in context.user_data:
            del context.user_data[key]

async def gestisci_valore_modifica(update: Update, context: ContextTypes.DEFAULT_TYPE):
    nuovo_valore = update.message.text.strip()
    campo = context.user_data['modifica_intervento']['campo_selezionato']
    rapporto = context.user_data['modifica_intervento']['rapporto']
    progressivo = context.user_data['modifica_intervento']['progressivo']
    
    try:
        if campo == 'km_finali':
            nuovo_valore = int(nuovo_valore)
        elif campo == 'litri_riforniti':
            nuovo_valore = int(nuovo_valore)
        
        campi_db = {
            'km_finali': 'km_finali',
            'litri_riforniti': 'litri_riforniti'
        }
        
        aggiorna_intervento(rapporto, progressivo, campi_db[campo], nuovo_valore)
        
        await update.message.reply_text(
            f"✅ **INTERVENTO MODIFICATO!**\n\n"
            f"Rapporto: R{rapporto}/{progressivo}\n"
            f"Campo aggiornato: {campo}\n"
            f"Nuovo valore: {nuovo_valore}"
        )
        
    except ValueError as e:
        await update.message.reply_text(f"❌ Errore nel formato del dato: {str(e)}")
    except Exception as e:
        await update.message.reply_text(f"❌ Errore durante la modifica: {str(e)}")
    
    for key in ['modifica_intervento', 'fase_modifica']:
        if key in context.user_data:
            del context.user_data[key]

# === STATISTICHE ===
async def mostra_statistiche(update: Update, context: ContextTypes.DEFAULT_TYPE):
    anni = get_anni_disponibili()
    
    if not anni:
        await update.message.reply_text("📊 **STATISTICHE**\n\nNessun dato disponibile per le statistiche.")
        return
    
    keyboard = []
    for anno in anni:
        keyboard.append([InlineKeyboardButton(f"📊 {anno}", callback_data=f"stats_{anno}")])
    
    keyboard.append([InlineKeyboardButton("📊 Tutti gli anni", callback_data="stats_tutti")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📊 **STATISTICHE**\n\n"
        "Seleziona l'anno per visualizzare le statistiche:",
        reply_markup=reply_markup
    )

async def gestisci_statistiche(update: Update, context: ContextTypes.DEFAULT_TYPE, anno: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    if anno == 'tutti':
        stats = get_statistiche_anno()
        titolo = "TUTTI GLI ANNI"
    else:
        stats = get_statistiche_anno(anno)
        titolo = anno
    
    messaggio = f"📊 **STATISTICHE {titolo}**\n\n"
    messaggio += f"📈 **Totale interventi:** {stats['totale_interventi']}\n"
    messaggio += f"🚒 **Totale partenze:** {stats['totale_partenze']}\n\n"
    
    if stats['tipologie']:
        messaggio += "📋 **Per tipologia:**\n"
        for tipologia, count in stats['tipologie'].items():
            if tipologia:
                messaggio += f"• {tipologia}: {count}\n"
        messaggio += "\n"
    
    if stats['mezzi']:
        messaggio += "🚒 **Per mezzo:**\n"
        for mezzo, count in stats['mezzi'].items():
            if mezzo:
                messaggio += f"• {mezzo}: {count}\n"
        messaggio += "\n"
    
    if stats['mensili']:
        messaggio += "📅 **Andamento mensile:**\n"
        for mese in sorted(stats['mensili'].keys()):
            count = stats['mensili'][mese]
            nome_mese = datetime.strptime(mese, '%m').strftime('%B')
            messaggio += f"• {nome_mese}: {count}\n"
    
    await query.edit_message_text(messaggio)

# === ESPORTAZIONE DATI ===
async def esporta_dati(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Solo gli amministratori possono esportare i dati.")
        return
    
    keyboard = [
        [InlineKeyboardButton("📅 Esporta per anno", callback_data="export_anno")],
        [InlineKeyboardButton("📋 Esporta tutto", callback_data="export_tutto")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📤 **ESPORTAZIONE DATI**\n\n"
        "Seleziona il tipo di esportazione:",
        reply_markup=reply_markup
    )

async def gestisci_export_anno(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    anni = get_anni_disponibili()
    
    if not anni:
        await query.edit_message_text("❌ Nessun dato da esportare.")
        return
    
    keyboard = []
    for anno in anni:
        keyboard.append([InlineKeyboardButton(anno, callback_data=f"export_anno_{anno}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "📅 **ESPORTA PER ANNO**\n\n"
        "Seleziona l'anno:",
        reply_markup=reply_markup
    )

async def esegui_export(update: Update, context: ContextTypes.DEFAULT_TYPE, tipo: str, anno: str = None):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    try:
        if tipo == 'anno':
            interventi = get_interventi_per_anno(anno)
            filename_suffix = f"anno_{anno}"
            caption = f"Esportazione dati per l'anno {anno}"
        else:
            interventi = get_ultimi_interventi(10000)
            filename_suffix = "completo"
            caption = "Esportazione completa di tutti i dati"
        
        if not interventi:
            await query.edit_message_text("❌ Nessun dato da esportare per i criteri selezionati.")
            return
        
        output = StringIO()
        writer = csv.writer(output)
        
        writer.writerow([
            'Numero_Erba', 'Rapporto_Como', 'Progressivo', 'Data_Uscita', 'Data_Rientro', 'Durata',
            'Mezzo_Targa', 'Mezzo_Tipo', 'Capopartenza', 'Autista', 'Partecipanti', 
            'Comune', 'Via', 'Indirizzo', 'Tipologia', 'Cambio_Personale', 'Km_Finali', 'Litri_Riforniti', 'Data_Creazione'
        ])
        
        for intervento in interventi:
            if len(intervento) >= 18:
                id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, comune, via, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at = intervento[:18]
                
                conn = sqlite3.connect(DATABASE_NAME)
                c = conn.cursor()
                c.execute('''SELECT GROUP_CONCAT(v.nome || ' ' || v.cognome) 
                             FROM partecipanti p 
                             JOIN vigili v ON p.vigile_id = v.id 
                             WHERE p.intervento_id = ?''', (id_int,))
                partecipanti_result = c.fetchone()
                partecipanti = partecipanti_result[0] if partecipanti_result and partecipanti_result[0] else ''
                conn.close()
                
                durata = calcola_durata_intervento(data_uscita, data_rientro)
                
                try:
                    data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')
                except:
                    data_uscita_fmt = data_uscita
                
                try:
                    data_rientro_fmt = datetime.strptime(data_rientro, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M') if data_rientro else ''
                except:
                    data_rientro_fmt = data_rientro or ''
                
                try:
                    created_fmt = datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M') if created_at else ''
                except:
                    created_fmt = created_at or ''
                
                writer.writerow([
                    num_erba, rapporto, progressivo, data_uscita_fmt, data_rientro_fmt, durata,
                    mezzo_targa, mezzo_tipo, capo, autista, partecipanti, 
                    comune, via, indirizzo, tipologia or '', 'Sì' if cambio_personale else 'No', 
                    km_finali or '', litri_riforniti or '', created_fmt
                ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"interventi_export_{filename_suffix}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption=f"📤 **{caption}**\n\nFile CSV contenente gli interventi."
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione: {str(e)}")
    
    for key in ['export_tipo', 'fase_export', 'export_anno']:
        if key in context.user_data:
            del context.user_data[key]

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
• 📤 Estrazione Dati - Estrai dati in formato CSV

👨‍💻 **ADMIN:**
• 👥 Gestisci Richieste - Approva nuovi utenti e gestisci utenti
• ⚙️ Gestione - Gestisci vigili, mezzi e modifica interventi
• 📤 Esporta Dati - Scarica dati completi in CSV
• 📥 Importa Dati - Invia file CSV per importare dati

🔧 **SISTEMA:**
• ✅ Always online con keep-alive
• 💾 Backup automatico ogni 25 minuti
• 🔒 Accesso controllato
• 📱 Interfaccia ottimizzata per mobile

📁 **IMPORTAZIONE:**
Gli admin possono importare dati inviando un file CSV con la stessa formattazione dell'esportazione.
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
    fase_modifica = context.user_data.get('fase_modifica')
    
    if fase == 'inserisci_rapporto':
        await gestisci_rapporto_como(update, context)
        return
    elif fase == 'ora_uscita':
        await gestisci_ora_uscita(update, context)
        return
    elif fase == 'ora_rientro':
        await gestisci_ora_rientro(update, context)
        return
    elif fase == 'inserisci_comune':
        await gestisci_comune(update, context)
        return
    elif fase == 'inserisci_via':
        await gestisci_via(update, context)
        return
    elif fase == 'inserisci_tipologia_personalizzata':
        await gestisci_tipologia_personalizzata(update, context)
        return
    elif fase == 'km_finali':
        await gestisci_km_finali(update, context)
        return
    elif fase == 'litri_riforniti':
        await gestisci_litri_riforniti(update, context)
        return
    elif fase_modifica == 'anno':
        await gestisci_anno_modifica(update, context)
        return
    elif fase_modifica == 'rapporto':
        await gestisci_rapporto_modifica(update, context)
        return
    elif fase_modifica == 'progressivo':
        await gestisci_progressivo_modifica(update, context)
        return
    elif fase_modifica == 'inserisci_valore':
        await gestisci_valore_modifica(update, context)
        return
    elif fase_modifica == 'modifica_orari':
        await gestisci_modifica_orari(update, context)
        return
    elif fase_modifica == 'modifica_indirizzo':
        await gestisci_modifica_indirizzo(update, context)
        return
    elif fase_modifica == 'modifica_tipologia':
        if text.startswith("/"):
            pass
        else:
            tipologia = text.strip()
            rapporto = context.user_data['modifica_intervento']['rapporto']
            progressivo = context.user_data['modifica_intervento']['progressivo']
            aggiorna_intervento(rapporto, progressivo, 'tipologia', tipologia)
            await update.message.reply_text(
                f"✅ **TIPOLOGIA AGGIORNATA!**\n\n"
                f"Rapporto: R{rapporto}/{progressivo}\n"
                f"Nuova tipologia: {tipologia}"
            )
            for key in ['modifica_intervento', 'fase_modifica']:
                if key in context.user_data:
                    del context.user_data[key]
            return
    
    if text == "➕ Nuovo Intervento":
        for key in ['nuovo_intervento', 'fase', 'vigili_da_selezionare', 'vigili_selezionati']:
            if key in context.user_data:
                del context.user_data[key]
        await avvia_nuovo_intervento(update, context)
    
    elif text == "📋 Ultimi Interventi":
        interventi = get_ultimi_interventi(10)
        if not interventi:
            await update.message.reply_text("📭 Nessun intervento registrato.")
            return
        
        messaggio = "📋 **ULTIMI 10 INTERVENTI**\n\n"
        for intervento in interventi:
            if len(intervento) >= 18:
                id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, comune, via, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at, partecipanti = intervento
                
                try:
                    data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M')
                    data_rientro_fmt = datetime.strptime(data_rientro, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M') if data_rientro else 'In corso'
                    durata = calcola_durata_intervento(data_uscita, data_rientro)
                except:
                    data_uscita_fmt = data_uscita
                    data_rientro_fmt = data_rientro or 'In corso'
                    durata = "N/A"
                    
                cambio = "🔄" if cambio_personale else ""
                km_info = f" | 🛣️{km_finali}km" if km_finali else ""
                litri_info = f" | ⛽{litri_riforniti}L" if litri_riforniti else ""
                    
                messaggio += f"🔢 **#{num_erba}** - R{rapporto}/{progressivo} {cambio}\n"
                messaggio += f"📅 {data_uscita_fmt} - {data_rientro_fmt} ({durata})\n"
                messaggio += f"🚒 {mezzo_targa} - {mezzo_tipo}{km_info}{litri_info}\n"
                messaggio += f"👨‍🚒 Capo: {capo}\n"
                messaggio += f"🚗 Autista: {autista}\n"
                messaggio += f"👥 Partecipanti: {partecipanti or 'Nessuno'}\n"
                messaggio += f"🚨 {tipologia or 'Non specificata'}\n"
                messaggio += f"📍 {comune}, {via}\n"
                messaggio += "─" * 30 + "\n"
        
        await update.message.reply_text(messaggio)
    
    elif text == "📊 Statistiche":
        await mostra_statistiche(update, context)
    
    elif text == "📤 Estrazione Dati":
        await estrazione_dati(update, context)
    
    elif text == "/start 🔄":
        await start(update, context)
    
    elif text == "🔍 Cerca Rapporto":
        for key in ['fase_ricerca', 'anno_ricerca']:
            if key in context.user_data:
                del context.user_data[key]
                
        context.user_data['fase_ricerca'] = 'anno'
        await update.message.reply_text("🔍 **RICERCA RAPPORTO**\n\nInserisci l'anno del rapporto:")
    
    elif text == "👥 Gestisci Richieste":
        await gestisci_richieste(update, context)
    
    elif text == "⚙️ Gestione":
        await gestione_admin(update, context)
    
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
                    if len(intervento) >= 18:
                        id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, comune, via, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at = intervento
                        try:
                            data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M')
                            data_rientro_fmt = datetime.strptime(data_rientro, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M') if data_rientro else 'In corso'
                            durata = calcola_durata_intervento(data_uscita, data_rientro)
                        except:
                            data_uscita_fmt = data_uscita
                            data_rientro_fmt = data_rientro or 'In corso'
                            durata = "N/A"
                            
                        cambio = "🔄" if cambio_personale else ""
                        km_info = f" | 🛣️{km_finali}km" if km_finali else ""
                        litri_info = f" | ⛽{litri_riforniti}L" if litri_riforniti else ""
                            
                        messaggio += f"🔢 **#{num_erba}** - Prog: {progressivo} {cambio}\n"
                        messaggio += f"📅 {data_uscita_fmt} - {data_rientro_fmt} ({durata})\n"
                        messaggio += f"🚒 {mezzo_targa}{km_info}{litri_info}\n"
                        messaggio += f"👨‍🚒 Capo: {capo}\n"
                        messaggio += f"🚨 {tipologia or 'Non specificata'}\n"
                        messaggio += f"📍 {comune}, {via}\n"
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
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
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
    
    elif data.startswith("cambio_"):
        await gestisci_cambio_personale(update, context, data)
    
    elif data.startswith("capo_"):
        await gestisci_selezione_capopartenza(update, context, data)
    
    elif data.startswith("autista_"):
        await gestisci_selezione_autista(update, context, data)
    
    elif data.startswith("vigile_"):
        await gestisci_selezione_vigile(update, context, data)
    
    elif data.startswith("tipologia_"):
        await gestisci_tipologia_intervento(update, context, data)
    
    elif data.startswith("tipopage_"):
        page = int(data.replace('tipopage_', ''))
        await mostra_selezione_tipologia_paginata(query, context, page)
    
    elif data.startswith("rientro_"):
        await gestisci_data_rientro(update, context, data)
    
    elif data.startswith("conferma_"):
        await conferma_intervento(update, context, data)
    
    # Gestione richieste accesso
    elif data == "richieste_attesa":
        await mostra_richieste_attesa(update, context)
    
    elif data == "utenti_approvati":
        await mostra_utenti_approvati(update, context)
    
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
        rimuovi_utente(user_id_rifiutare)
        
        richieste_rimanenti = get_richieste_in_attesa()
        if richieste_rimanenti:
            messaggio = f"❌ Utente rifiutato! 📋 Richieste rimanenti: {len(richieste_rimanenti)}"
        else:
            messaggio = "❌ Utente rifiutato! 🎉 Tutte le richieste gestite."
            
        await query.edit_message_text(messaggio)
    
    # Gestione rimozione utenti
    elif data.startswith("rimuovi_"):
        user_id_rimuovere = int(data.replace('rimuovi_', ''))
        await conferma_rimozione_utente(update, context, user_id_rimuovere)
    
    elif data.startswith("conferma_rimozione_"):
        user_id_rimuovere = int(data.replace('conferma_rimozione_', ''))
        await esegui_rimozione_utente(update, context, user_id_rimuovere)
    
    elif data == "annulla_rimozione":
        await query.edit_message_text("❌ Rimozione utente annullata.")
    
    # Gestione admin
    elif data == "admin_vigili":
        await gestione_vigili_admin(update, context)
    
    elif data == "admin_mezzi":
        await gestione_mezzi_admin(update, context)
    
    elif data == "modifica_intervento":
        await avvia_modifica_intervento(update, context)
    
    elif data == "export_menu":
        await esporta_dati(update, context)
    
    elif data == "lista_vigili":
        await mostra_lista_vigili(update, context)
    
    elif data == "lista_mezzi":
        await mostra_lista_mezzi(update, context)
    
    elif data == "importa_vigili":
        await importa_vigili_csv(update, context)
    
    elif data == "importa_mezzi_info":
        await importa_mezzi_info(update, context)
    
    # Gestione modifica intervento
    elif data.startswith("campo_"):
        campo = data.replace('campo_', '')
        await gestisci_selezione_campo(update, context, campo)
    
    elif data.startswith("modmezzo_"):
        targa = data.replace('modmezzo_', '')
        await gestisci_valore_modifica_bottoni(update, context, 'mezzo', targa)
    
    elif data.startswith("modcapo_"):
        vigile_id = int(data.replace('modcapo_', ''))
        vigile = get_vigile_by_id(vigile_id)
        if vigile:
            nome_completo = f"{vigile[1]} {vigile[2]}"
            await gestisci_valore_modifica_bottoni(update, context, 'capopartenza', nome_completo)
    
    elif data.startswith("modautista_"):
        vigile_id = int(data.replace('modautista_', ''))
        vigile = get_vigile_by_id(vigile_id)
        if vigile:
            nome_completo = f"{vigile[1]} {vigile[2]}"
            await gestisci_valore_modifica_bottoni(update, context, 'autista', nome_completo)
    
    elif data.startswith("modtipologia_"):
        if data == "modtipologia_altro":
            context.user_data['fase_modifica'] = 'inserisci_tipologia'
            await query.edit_message_text("Inserisci la nuova tipologia:")
        else:
            tipologia = data.replace('modtipologia_', '')
            await gestisci_valore_modifica_bottoni(update, context, 'tipologia', tipologia)
    
    # Gestione statistiche
    elif data.startswith("stats_"):
        anno = data.replace('stats_', '')
        await gestisci_statistiche(update, context, anno)
    
    # Gestione esportazione
    elif data == "export_anno":
        await gestisci_export_anno(update, context)
    
    elif data == "export_tutto":
        await esegui_export(update, context, 'tutto')
    
    elif data.startswith("export_anno_"):
        anno = data.replace('export_anno_', '')
        await esegui_export(update, context, 'anno', anno)
    
    elif data == "export_status":
        await esegui_export_status(update, context)
    
    elif data == "export_utenti":
        await esegui_export_utenti(update, context)

# === ERROR HANDLER ===
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, BadRequest) and "Query is too old" in str(context.error):
        return
    print(f"❌ Errore: {context.error}")

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
    application.add_handler(MessageHandler(filters.Document.ALL, gestisci_file_csv))
    application.add_error_handler(error_handler)

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
