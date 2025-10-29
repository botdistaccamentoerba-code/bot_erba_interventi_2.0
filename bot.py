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

# === MAPPING TIPOLOGIE CON CODICI BREVI - VERSIONE CORRETTA ===
TIPOLOGIE_MAPPING = {
    "tip_01": ("27", "27"),
    "tip_02": ("Apertura porte/finestre", "Apertura porte e finestre"),
    "tip_03": ("Ascensore bloccato", "Ascensore bloccato"),
    "tip_04": ("Ass. Protezione Civile", "Assistenza attività di Protezione Civile e Sanitarie"),
    "tip_05": ("Assistenza TSO", "Assistenza TSO"),
    "tip_06": ("Bonifica insetti", "Bonifica insetti"),
    "tip_07": ("Crollo parziale", "Crollo parziale di elementi strutturali"),
    "tip_08": ("Danni d'acqua", "Danni d'acqua in genere"),
    "tip_09": ("Fuoriuscita acqua", "Fuoriuscita di acqua per rottura di tubazioni, canali e simili"),
    "tip_10": ("Esplosione", "Esplosione"),
    "tip_11": ("Frane", "Frane"),
    "tip_12": ("Fuga Gas", "Fuga Gas"),
    "tip_13": ("Guasto elettrico", "Guasto elettrico"),
    "tip_14": ("Incendio controllato", "Incendio/fuoco controllato"),
    "tip_15": ("Incendio abitazione", "Incendio abitazione"),
    "tip_16": ("Incendio autovettura", "Incendio Autovettura"),
    "tip_17": ("Incendio boschivo", "Incendio Boschivo"),
    "tip_18": ("Incendio canna fumaria", "Incendio Canna Fumaria"),
    "tip_19": ("Incendio capannone", "Incendio Capannone"),
    "tip_20": ("Incendio cascina", "Incendio Cascina"),
    "tip_21": ("Incendio generico", "Incendio generico"),
    "tip_22": ("Incendio sterpaglie", "Incendio sterpaglie"),
    "tip_23": ("Incendio tetto", "Incendio Tetto"),
    "tip_24": ("Incidente aereo", "Incidente Aereo"),
    "tip_25": ("Incidente stradale", "Incidente stradale"),
    "tip_26": ("Infortunio lavoro", "Infortunio sul lavoro"),
    "tip_27": ("Palo pericolante", "Palo pericolante"),
    "tip_28": ("Recupero animali morti", "Recupero animali morti"),
    "tip_29": ("Recupero veicoli", "Recupero / assistenza veicoli"),
    "tip_30": ("Recupero merci", "Recupero merci e beni"),
    "tip_31": ("Recupero salma", "Recupero Salma"),
    "tip_32": ("Ricerca persona", "Ricerca Persona (SAR)"),
    "tip_33": ("Rimozione ostacoli", "Rimozione ostacoli non dovuti al traffico"),
    "tip_34": ("Salvataggio animali", "Salvataggio animali"),
    "tip_35": ("Servizio assistenza", "Servizio Assistenza Generico"),
    "tip_36": ("Smontaggio controllato", "Smontaggio controllato di elementi costruttivi"),
    "tip_37": ("Soccorso persona", "Soccorso Persona"),
    "tip_38": ("Sopralluogo stabilità", "Sopralluoghi e verifiche di stabilità edifici e manufatti"),
    "tip_39": ("Sopralluogo incendio", "Sopralluogo per incendio"),
    "tip_40": ("Sversamenti", "Sversamenti"),
    "tip_41": ("Taglio pianta", "Taglio Pianta"),
    "tip_42": ("Tentato suicidio", "Tentato suicidio")
}

# Lista delle tipologie complete per riferimento
TIPOLOGIE_INTERVENTO = [tipologia_completa for _, tipologia_completa in TIPOLOGIE_MAPPING.values()]

# Funzioni di utilità per il mapping - VERSIONE SEMPLIFICATA E CORRETTA
def get_tipologia_by_callback(callback_data):
    """Restituisce la tipologia completa dato il callback breve"""
    if callback_data in TIPOLOGIE_MAPPING:
        return TIPOLOGIE_MAPPING[callback_data][1]  # Restituisce la tipologia completa
    return None

def get_display_name_by_callback(callback_data):
    """Restituisce il nome da visualizzare dato il callback breve"""
    if callback_data in TIPOLOGIE_MAPPING:
        return TIPOLOGIE_MAPPING[callback_data][0]  # Restituisce il nome breve per display
    return None

def get_callback_by_tipologia(tipologia_completa):
    """Restituisce il callback breve dato la tipologia completa"""
    for callback_breve, (_, tipologia) in TIPOLOGIE_MAPPING.items():
        if tipologia == tipologia_completa:
            return callback_breve
    return None

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

# === SISTEMA INVIO AUTOMATICO CSV ===
async def invio_csv_automatico_interventi(context: ContextTypes.DEFAULT_TYPE, anno_corrente=None):
    """Funzione per inviare automaticamente CSV degli interventi"""
    try:
        if anno_corrente is None:
            anno_corrente = datetime.now().year
        
        print(f"🔄 Invio automatico CSV interventi per l'anno {anno_corrente}")
        
        # Ottieni gli interventi dell'anno corrente
        interventi = get_interventi_per_anno(str(anno_corrente))
        
        if not interventi:
            print("❌ Nessun intervento trovato per l'anno corrente")
            return
        
        # Crea il file CSV
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
        csv_file.name = f"interventi_backup_{anno_corrente}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        # Invia al super user ogni giorno
        super_user_id = 1816045269
        await context.bot.send_document(
            chat_id=super_user_id,
            document=csv_file,
            filename=csv_file.name,
            caption=f"📊 **BACKUP AUTOMATICO INTERVENTI {anno_corrente}**\n\n"
                   f"File CSV contenente tutti gli interventi dell'anno {anno_corrente}.\n"
                   f"Data generazione: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        )
        print(f"✅ CSV interventi inviato al super user {super_user_id}")
        
        # Reinizializza il file per inviarlo agli altri admin
        csv_file.seek(0)
        
        # Invia agli altri admin ogni domenica
        oggi = datetime.now()
        if oggi.weekday() == 6:  # 6 = Domenica
            altri_admin = [admin_id for admin_id in ADMIN_IDS if admin_id != super_user_id]
            for admin_id in altri_admin:
                try:
                    await context.bot.send_document(
                        chat_id=admin_id,
                        document=csv_file,
                        filename=csv_file.name,
                        caption=f"📊 **BACKUP SETTIMANALE INTERVENTI {anno_corrente}**\n\n"
                               f"File CSV contenente tutti gli interventi dell'anno {anno_corrente}.\n"
                               f"Data generazione: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
                    )
                    print(f"✅ CSV interventi inviato all'admin {admin_id}")
                    csv_file.seek(0)  # Riposiziona all'inizio per il prossimo invio
                except Exception as e:
                    print(f"❌ Errore nell'invio a admin {admin_id}: {e}")
        
    except Exception as e:
        print(f"❌ Errore durante l'invio automatico CSV interventi: {e}")

async def invio_csv_automatico_status(context: ContextTypes.DEFAULT_TYPE):
    """Funzione per inviare automaticamente CSV dello status caserma"""
    try:
        print("🔄 Invio automatico CSV status caserma")
        
        vigili = get_tutti_vigili()
        mezzi = get_tutti_mezzi()
        
        # Crea il file CSV
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
        csv_file.name = f"status_caserma_backup_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        # Invia a tutti gli admin
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_document(
                    chat_id=admin_id,
                    document=csv_file,
                    filename=csv_file.name,
                    caption="🏠 **BACKUP STATUS CASERMA**\n\n"
                           "File CSV contenente l'elenco completo di vigili e mezzi.\n"
                           f"Data generazione: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
                )
                print(f"✅ CSV status caserma inviato all'admin {admin_id}")
                csv_file.seek(0)  # Riposiziona all'inizio per il prossimo invio
            except Exception as e:
                print(f"❌ Errore nell'invio status a admin {admin_id}: {e}")
        
    except Exception as e:
        print(f"❌ Errore durante l'invio automatico CSV status: {e}")

async def scheduler_invio_automatico(context: ContextTypes.DEFAULT_TYPE):
    """Scheduler per gli invii automatici"""
    print("🔄 Scheduler invio automatico CSV avviato")
    
    while True:
        try:
            now = datetime.now()
            
            # Controlla se è l'ora programmata (23:55)
            if now.hour == 23 and now.minute == 55:
                print("⏰ Ora di invio automatico CSV raggiunta")
                
                # Invio interventi al super user (ogni giorno)
                await invio_csv_automatico_interventi(context)
                
                # Invio status caserma (ogni 4 mesi a fine mese)
                if now.month in [3, 6, 9, 12] and now.day >= 25:  # Fine marzo, giugno, settembre, dicembre
                    await invio_csv_automatico_status(context)
                
                # Aspetta 2 minuti per evitare esecuzioni multiple
                await asyncio.sleep(120)
            else:
                # Controlla ogni minuto
                await asyncio.sleep(60)
                
        except Exception as e:
            print(f"❌ Errore nello scheduler: {e}")
            await asyncio.sleep(60)

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

# === SISTEMA DI SELEZIONE TIPOLOGIA PAGINATO - VERSIONE CORRETTA ===
def crea_tastiera_tipologie_paginata(page=0, items_per_page=8):
    """Crea una tastiera paginata per le tipologie usando il mapping"""
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    
    # Prendi tutti i callback brevi ordinati
    callback_lista = list(TIPOLOGIE_MAPPING.keys())
    callback_pagina = callback_lista[start_idx:end_idx]
    
    keyboard = []
    
    # Aggiungi le tipologie della pagina corrente
    for callback_breve in callback_pagina:
        display_name = TIPOLOGIE_MAPPING[callback_breve][0]  # Nome breve per display
        keyboard.append([InlineKeyboardButton(display_name, callback_data=callback_breve)])
    
    # Aggiungi bottoni di navigazione
    nav_buttons = []
    
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Precedente", callback_data=f"tipopage_{page-1}"))
    
    if end_idx < len(callback_lista):
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

# === NUOVO SISTEMA IMPORT/EXPORT CSV SEPARATI ===
async def gestisci_file_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Solo gli amministratori possono importare dati.")
        return
    
    document = update.message.document
    filename = document.file_name.lower()
    
    if not filename.endswith('.csv'):
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
        
        # Riconoscimento del tipo di CSV in base al nome del file
        if 'db_interventi.csv' in filename:
            await gestisci_import_interventi(update, context, reader)
        elif 'db_vigili.csv' in filename:
            await gestisci_import_vigili(update, context, reader)
        elif 'db_mezzi.csv' in filename:
            await gestisci_import_mezzi(update, context, reader)
        elif 'db_user.csv' in filename:
            await gestisci_import_utenti(update, context, reader)
        else:
            await update.message.reply_text(
                "❌ Formato file non riconosciuto.\n\n"
                "📁 **Formati supportati:**\n"
                "• db_interventi.csv - Dati interventi\n"
                "• db_vigili.csv - Anagrafica vigili\n" 
                "• db_mezzi.csv - Anagrafica mezzi\n"
                "• db_user.csv - Utenti del bot\n\n"
                "Usa i pulsanti di esportazione per ottenere il formato corretto."
            )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Errore durante l'importazione: {str(e)}")
        print(f"Errore dettagliato: {e}")

async def gestisci_import_interventi(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    skipped_count = 0
    error_count = 0
    error_details = []
    
    # Leggi l'header
    try:
        headers = next(reader)
        expected_headers = ['Numero_Erba', 'Rapporto_Como', 'Progressivo', 'Data_Uscita', 'Data_Rientro', 
                           'Mezzo_Targa', 'Mezzo_Tipo', 'Capopartenza', 'Autista', 'Comune', 'Via', 
                           'Indirizzo', 'Tipologia', 'Cambio_Personale', 'Km_Finali', 'Litri_Riforniti']
        
        if len(headers) != len(expected_headers):
            await update.message.reply_text(
                f"❌ Formato CSV non valido per interventi.\n"
                f"Attese {len(expected_headers)} colonne, trovate {len(headers)}.\n"
                f"Header trovato: {headers}"
            )
            return
    except StopIteration:
        await update.message.reply_text("❌ File CSV vuoto.")
        return
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 16:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero colonne insufficiente ({len(row)})")
                continue
            
            # Estrai i dati dalla riga
            num_erba = int(row[0]) if row[0] and row[0].isdigit() else get_prossimo_numero_erba()
            rapporto_como = row[1]
            progressivo_como = row[2]
            
            # Controlla se l'intervento esiste già (stesso rapporto, progressivo e anno)
            existing = get_intervento_by_rapporto(rapporto_como, progressivo_como)
            if existing:
                skipped_count += 1
                continue
            
            # Parsing date
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
            
            # Preparazione dati
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
                'comune': row[9] if len(row) > 9 else '',
                'via': row[10] if len(row) > 10 else '',
                'indirizzo': row[11] if len(row) > 11 else '',
                'tipologia': row[12] if len(row) > 12 else '',
                'cambio_personale': row[13].lower() in ['sì', 'si', '1', 'true', 'vero', 'yes'] if len(row) > 13 else False,
                'km_finali': int(row[14]) if len(row) > 14 and row[14] and row[14].isdigit() else None,
                'litri_riforniti': int(row[15]) if len(row) > 15 and row[15] and row[15].isdigit() else None,
                'partecipanti': []
            }
            
            inserisci_intervento(dati)
            imported_count += 1
            
        except Exception as e:
            error_count += 1
            error_details.append(f"Riga {row_num}: {str(e)}")
            print(f"Errore nell'importazione riga {row_num}: {e}")
            continue
    
    # Invia report
    report = f"✅ **IMPORTAZIONE INTERVENTI COMPLETATA**\n\n"
    report += f"📊 **Risultati:**\n"
    report += f"• ✅ Record importati: {imported_count}\n"
    report += f"• ⏭️ Record saltati (già presenti): {skipped_count}\n"
    report += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        report += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            report += f"• {detail}\n"
        if len(error_details) > 5:
            report += f"• ... e altri {len(error_details) - 5} errori\n"
    
    await update.message.reply_text(report)

async def gestisci_import_vigili(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    updated_count = 0
    error_count = 0
    error_details = []
    
    # Leggi l'header
    try:
        headers = next(reader)
        expected_headers = ['Nome', 'Cognome', 'Qualifica', 'Grado Patente', 'Patente Nautica', 'SAF', 'TPSS', 'ATP', 'Attivo']
        
        if len(headers) != len(expected_headers):
            await update.message.reply_text(
                f"❌ Formato CSV non valido per vigili.\n"
                f"Attese {len(expected_headers)} colonne, trovate {len(headers)}.\n"
                f"Header trovato: {headers}"
            )
            return
    except StopIteration:
        await update.message.reply_text("❌ File CSV vuoto.")
        return
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 9:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero colonne insufficiente ({len(row)})")
                continue
            
            nome = row[0]
            cognome = row[1]
            qualifica = row[2]
            grado_patente = row[3]
            patente_nautica = row[4].lower() in ['1', 'true', 'si', 'sì', 'yes']
            saf = row[5].lower() in ['1', 'true', 'si', 'sì', 'yes']
            tpss = row[6].lower() in ['1', 'true', 'si', 'sì', 'yes']
            atp = row[7].lower() in ['1', 'true', 'si', 'sì', 'yes']
            attivo = row[8].lower() in ['1', 'true', 'si', 'sì', 'yes']
            
            # Cerca se il vigile esiste già
            conn = sqlite3.connect(DATABASE_NAME)
            c = conn.cursor()
            c.execute('''SELECT id FROM vigili WHERE nome = ? AND cognome = ?''', (nome, cognome))
            existing_vigile = c.fetchone()
            
            if existing_vigile:
                # Aggiorna vigile esistente
                vigile_id = existing_vigile[0]
                c.execute('''UPDATE vigili SET 
                            qualifica = ?, grado_patente_terrestre = ?, patente_nautica = ?, 
                            saf = ?, tpss = ?, atp = ?, attivo = ?
                            WHERE id = ?''',
                         (qualifica, grado_patente, patente_nautica, saf, tpss, atp, attivo, vigile_id))
                updated_count += 1
            else:
                # Inserisci nuovo vigile
                c.execute('''INSERT INTO vigili 
                            (nome, cognome, qualifica, grado_patente_terrestre, patente_nautica, saf, tpss, atp, attivo) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                         (nome, cognome, qualifica, grado_patente, patente_nautica, saf, tpss, atp, attivo))
                imported_count += 1
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            error_count += 1
            error_details.append(f"Riga {row_num}: {str(e)}")
            print(f"Errore nell'importazione vigile {row_num}: {e}")
            continue
    
    # Invia report
    report = f"✅ **IMPORTAZIONE VIGILI COMPLETATA**\n\n"
    report += f"📊 **Risultati:**\n"
    report += f"• ✅ Vigili importati: {imported_count}\n"
    report += f"• 🔄 Vigili aggiornati: {updated_count}\n"
    report += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        report += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            report += f"• {detail}\n"
        if len(error_details) > 5:
            report += f"• ... e altri {len(error_details) - 5} errori\n"
    
    await update.message.reply_text(report)

async def gestisci_import_mezzi(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    updated_count = 0
    error_count = 0
    error_details = []
    
    # Leggi l'header
    try:
        headers = next(reader)
        expected_headers = ['Targa', 'Tipo', 'Attivo']
        
        if len(headers) != len(expected_headers):
            await update.message.reply_text(
                f"❌ Formato CSV non valido per mezzi.\n"
                f"Attese {len(expected_headers)} colonne, trovate {len(headers)}.\n"
                f"Header trovato: {headers}"
            )
            return
    except StopIteration:
        await update.message.reply_text("❌ File CSV vuoto.")
        return
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 3:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero colonne insufficiente ({len(row)})")
                continue
            
            targa = row[0]
            tipo = row[1]
            attivo = row[2].lower() in ['1', 'true', 'si', 'sì', 'yes']
            
            # Inserisci o aggiorna mezzo
            conn = sqlite3.connect(DATABASE_NAME)
            c = conn.cursor()
            
            # Controlla se il mezzo esiste già
            c.execute('''SELECT id FROM mezzi WHERE targa = ?''', (targa,))
            existing_mezzo = c.fetchone()
            
            if existing_mezzo:
                # Aggiorna mezzo esistente
                c.execute('''UPDATE mezzi SET tipo = ?, attivo = ? WHERE targa = ?''',
                         (tipo, attivo, targa))
                updated_count += 1
            else:
                # Inserisci nuovo mezzo
                c.execute('''INSERT INTO mezzi (targa, tipo, attivo) VALUES (?, ?, ?)''',
                         (targa, tipo, attivo))
                imported_count += 1
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            error_count += 1
            error_details.append(f"Riga {row_num}: {str(e)}")
            print(f"Errore nell'importazione mezzo {row_num}: {e}")
            continue
    
    # Invia report
    report = f"✅ **IMPORTAZIONE MEZZI COMPLETATA**\n\n"
    report += f"📊 **Risultati:**\n"
    report += f"• ✅ Mezzi importati: {imported_count}\n"
    report += f"• 🔄 Mezzi aggiornati: {updated_count}\n"
    report += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        report += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            report += f"• {detail}\n"
        if len(error_details) > 5:
            report += f"• ... e altri {len(error_details) - 5} errori\n"
    
    await update.message.reply_text(report)

async def gestisci_import_utenti(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    updated_count = 0
    error_count = 0
    error_details = []
    
    # Leggi l'header
    try:
        headers = next(reader)
        expected_headers = ['user_id', 'username', 'nome', 'telefono', 'ruolo', 'data_approvazione']
        
        if len(headers) != len(expected_headers):
            await update.message.reply_text(
                f"❌ Formato CSV non valido per utenti.\n"
                f"Attese {len(expected_headers)} colonne, trovate {len(headers)}.\n"
                f"Header trovato: {headers}"
            )
            return
    except StopIteration:
        await update.message.reply_text("❌ File CSV vuoto.")
        return
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 6:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero colonne insufficiente ({len(row)})")
                continue
            
            user_id = int(row[0])
            username = row[1] if len(row) > 1 else ''
            nome = row[2] if len(row) > 2 else ''
            telefono = row[3] if len(row) > 3 else ''
            ruolo = row[4] if len(row) > 4 else 'user'
            data_approvazione = row[5] if len(row) > 5 else None
            
            conn = sqlite3.connect(DATABASE_NAME)
            c = conn.cursor()
            c.execute("SELECT * FROM utenti WHERE user_id = ?", (user_id,))
            existing_user = c.fetchone()
            
            if existing_user:
                c.execute('''UPDATE utenti 
                            SET username = ?, nome = ?, telefono = ?, ruolo = ?, data_approvazione = ?
                            WHERE user_id = ?''', 
                         (username, nome, telefono, ruolo, data_approvazione, user_id))
                updated_count += 1
            else:
                c.execute('''INSERT INTO utenti 
                            (user_id, username, nome, telefono, ruolo, data_approvazione) 
                            VALUES (?, ?, ?, ?, ?, ?)''', 
                         (user_id, username, nome, telefono, ruolo, data_approvazione))
                imported_count += 1
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            error_count += 1
            error_details.append(f"Riga {row_num}: {str(e)}")
            print(f"Errore nell'importazione utente {row_num}: {e}")
            continue
    
    # Invia report
    report = f"✅ **IMPORTAZIONE UTENTI COMPLETATA**\n\n"
    report += f"📊 **Risultati:**\n"
    report += f"• ✅ Utenti importati: {imported_count}\n"
    report += f"• 🔄 Utenti aggiornati: {updated_count}\n"
    report += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        report += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            report += f"• {detail}\n"
        if len(error_details) > 5:
            report += f"• ... e altri {len(error_details) - 5} errori\n"
    
    await update.message.reply_text(report)

# === ESTRAZIONE DATI AGGIORNATA ===
async def estrazione_dati(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    keyboard = [
        [InlineKeyboardButton("📋 Dati Interventi Completi", callback_data="export_tutto")],
        [InlineKeyboardButton("📅 Dati Interventi per Anno", callback_data="export_anno")],
        [InlineKeyboardButton("👥 Elenco Vigili", callback_data="export_vigili")],
        [InlineKeyboardButton("🚒 Elenco Mezzi", callback_data="export_mezzi")]
    ]
    
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("👤 Utenti Approvati", callback_data="export_utenti")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📤 **ESTRAZIONE DATI**\n\n"
        "Seleziona il tipo di estrazione:",
        reply_markup=reply_markup
    )

async def esegui_export_vigili(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    try:
        vigili = get_tutti_vigili()
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Header per db_vigili.csv
        writer.writerow(['Nome', 'Cognome', 'Qualifica', 'Grado Patente', 'Patente Nautica', 'SAF', 'TPSS', 'ATP', 'Attivo'])
        
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
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"db_vigili_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Vigili in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="👥 **ELENCO VIGILI**\n\n"
                   "File CSV contenente l'elenco completo dei vigili.\n\n"
                   "📝 **Formato per importazione:**\n"
                   "• Nome, Cognome, Qualifica, Grado Patente, Patente Nautica (0/1), SAF (0/1), TPSS (0/1), ATP (0/1), Attivo (0/1)"
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione vigili: {str(e)}")

async def esegui_export_mezzi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    try:
        mezzi = get_tutti_mezzi()
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Header per db_mezzi.csv
        writer.writerow(['Targa', 'Tipo', 'Attivo'])
        
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
        csv_file.name = f"db_mezzi_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Mezzi in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="🚒 **ELENCO MEZZI**\n\n"
                   "File CSV contenente l'elenco completo dei mezzi.\n\n"
                   "📝 **Formato per importazione:**\n"
                   "• Targa, Tipo, Attivo (0/1)"
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione mezzi: {str(e)}")

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
        
        # Header per db_user.csv
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
        csv_file.name = f"db_user_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Utenti in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="👤 **UTENTI APPROVATI**\n\n"
                   "File CSV contenente l'elenco degli utenti approvati.\n\n"
                   "📝 **Formato per importazione:**\n"
                   "user_id,username,nome,telefono,ruolo,data_approvazione"
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione utenti: {str(e)}")

# Le funzioni esegui_export e gestisci_export_anno rimangono come prima per gli interventi
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
            export_filename = f"db_interventi_{anno}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        else:
            interventi = get_ultimi_interventi(10000)
            filename_suffix = "completo"
            caption = "Esportazione completa di tutti i dati"
            export_filename = f"db_interventi_completo_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        if not interventi:
            await query.edit_message_text("❌ Nessun dato da esportare per i criteri selezionati.")
            return
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Header per db_interventi.csv (16 colonne)
        writer.writerow([
            'Numero_Erba', 'Rapporto_Como', 'Progressivo', 'Data_Uscita', 'Data_Rientro',
            'Mezzo_Targa', 'Mezzo_Tipo', 'Capopartenza', 'Autista', 'Comune', 'Via', 
            'Indirizzo', 'Tipologia', 'Cambio_Personale', 'Km_Finali', 'Litri_Riforniti'
        ])
        
        for intervento in interventi:
            if len(intervento) >= 18:
                id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, comune, via, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at = intervento[:18]
                
                # Formatta le date per l'esportazione
                try:
                    data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M')
                except:
                    data_uscita_fmt = data_uscita
                
                try:
                    data_rientro_fmt = datetime.strptime(data_rientro, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M') if data_rientro else ''
                except:
                    data_rientro_fmt = data_rientro or ''
                
                writer.writerow([
                    num_erba, rapporto, progressivo, data_uscita_fmt, data_rientro_fmt,
                    mezzo_targa, mezzo_tipo, capo, autista, comune, via, 
                    indirizzo, tipologia or '', '1' if cambio_personale else '0', 
                    km_finali or '', litri_riforniti or ''
                ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = export_filename
        
        await query.edit_message_text("📤 Generazione file in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption=f"📤 **{caption}**\n\n"
                   f"File CSV contenente gli interventi.\n\n"
                   f"📝 **Formato per importazione:**\n"
                   f"• 16 colonne specifiche per interventi\n"
                   f"• Formato date: GG/MM/AAAA HH:MM\n"
                   f"• Cambio_Personale: 0/1"
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione: {str(e)}")

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
        "📅 **ESPORTA INTERVENTI PER ANNO**\n\n"
        "Seleziona l'anno:",
        reply_markup=reply_markup
    )

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

# === GESTIONE TIPOLOGIA NEL FLUSSO MODIFICA INTERVENTO ===
async def gestisci_tipologia_modifica(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    if callback_data.startswith("tipopage_"):
        page = int(callback_data.replace('tipopage_', ''))
        await mostra_selezione_tipologia_paginata(query, context, page)
        return
    
    elif callback_data == "tipologia_altro":
        context.user_data['fase_modifica'] = 'inserisci_tipologia_modifica'
        await query.edit_message_text(
            "✏️ **MODIFICA TIPOLOGIA**\n\n"
            "Inserisci la nuova tipologia di intervento:"
        )
        return
    
    else:
        if callback_data in TIPOLOGIE_MAPPING:
            tipologia_completa = TIPOLOGIE_MAPPING[callback_data][1]
            display_name = TIPOLOGIE_MAPPING[callback_data][0]
            
            rapporto = context.user_data['modifica_intervento']['rapporto']
            progressivo = context.user_data['modifica_intervento']['progressivo']
            
            aggiorna_intervento(rapporto, progressivo, 'tipologia', tipologia_completa)
            
            await query.edit_message_text(
                f"✅ **TIPOLOGIA AGGIORNATA!**\n\n"
                f"Rapporto: R{rapporto}/{progressivo}\n"
                f"Nuova tipologia: {display_name}"
            )
            
            for key in ['modifica_intervento', 'fase_modifica']:
                if key in context.user_data:
                    del context.user_data[key]
        else:
            await query.edit_message_text(
                "❌ Errore nella selezione della tipologia. Riprova.",
                reply_markup=crea_tastiera_tipologie_paginata(0)
            )

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
    elif fase_modifica == 'inserisci_tipologia_modifica':
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
    
    # Gestione tipologia
    elif data.startswith("tip_") or data.startswith("tipopage_") or data == "tipologia_altro":
        fase = context.user_data.get('fase')
        fase_modifica = context.user_data.get('fase_modifica')
        
        if fase == 'tipologia_intervento':
            await gestisci_tipologia_intervento(update, context, data)
        elif fase_modifica == 'modifica_tipologia':
            await gestisci_tipologia_modifica(update, context, data)
        else:
            await gestisci_tipologia_intervento(update, context, data)
    
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
    
    elif data == "export_vigili":
        await esegui_export_vigili(update, context)
    
    elif data == "export_mezzi":
        await esegui_export_mezzi(update, context)
    
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
    
    # Aggiungi lo scheduler per gli invii automatici CSV
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(
            lambda context: asyncio.create_task(scheduler_invio_automatico(context)),
            interval=60,  # Controlla ogni minuto
            first=10
        )
        print("✅ Scheduler invio automatico CSV attivato! Controllo ogni minuto")
    
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
    print("📤 Invio automatico CSV: Super User ogni giorno 23:55, Admin domenica 23:55, Status ogni 4 mesi")
    
    application.run_polling()

if __name__ == '__main__':
    main()
