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
SUPER_ADMIN_ID = 1816045269  # ID del super admin per l'invio automatico

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
    """Restituisce gli ultimi interventi (sia attivi che completati)"""
    conn = sqlite3.connect(DATABASE_NAME)
    c = conn.cursor()
    c.execute('''SELECT id, rapporto_como, progressivo_como, numero_erba, data_uscita, indirizzo, data_rientro
                 FROM interventi 
                 ORDER BY data_uscita DESC LIMIT 10''')
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

# === INVIO AUTOMATICO CSV AGLI ADMIN ===
async def invia_csv_automatico_admin(context):
    """Funzione per inviare automaticamente i CSV agli admin"""
    try:
        # Crea i file CSV
        files_to_send = []
        
        # 1. Interventi con partecipanti
        interventi = get_ultimi_interventi(10000)
        if interventi:
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['Numero_Erba', 'Rapporto_Como', 'Progressivo', 'Data_Uscita', 'Data_Rientro', 
                           'Mezzo_Targa', 'Mezzo_Tipo', 'Capopartenza', 'Autista', 'Partecipanti', 'Comune', 'Via', 
                           'Indirizzo', 'Tipologia', 'Cambio_Personale', 'Km_Finali', 'Litri_Riforniti'])
            
            for intervento in interventi:
                if len(intervento) >= 18:
                    id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, comune, via, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at = intervento[:18]
                    
                    # Recupera i partecipanti
                    partecipanti_nomi = []
                    conn = sqlite3.connect(DATABASE_NAME)
                    c = conn.cursor()
                    c.execute('''SELECT v.nome, v.cognome 
                                 FROM partecipanti p 
                                 JOIN vigili v ON p.vigile_id = v.id 
                                 WHERE p.intervento_id = ?''', (id_int,))
                    partecipanti = c.fetchall()
                    conn.close()
                    
                    for nome, cognome in partecipanti:
                        partecipanti_nomi.append(f"{cognome} {nome}")
                    
                    partecipanti_str = "; ".join(partecipanti_nomi)
                    
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
                        mezzo_targa, mezzo_tipo, capo, autista, partecipanti_str, comune, via, 
                        indirizzo, tipologia or '', 'Sì' if cambio_personale else 'No', 
                        km_finali or '', litri_riforniti or ''
                    ])
            
            csv_data = output.getvalue()
            output.close()
            csv_bytes = csv_data.encode('utf-8')
            files_to_send.append(('db_interventi.csv', csv_bytes))
        
        # ... resto del codice per vigili, mezzi e utenti rimane uguale ...
        
        # 2. Vigili
        vigili = get_tutti_vigili()
        if vigili:
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(['Nome', 'Cognome', 'Qualifica', 'Grado_Patente', 'Patente_Nautica', 'SAF', 'TPSS', 'ATP', 'Stato'])
            
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
            files_to_send.append(('db_vigili.csv', csv_bytes))
        
        # 3. Mezzi
        mezzi = get_tutti_mezzi()
        if mezzi:
            output = StringIO()
            writer = csv.writer(output)
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
            files_to_send.append(('db_mezzi.csv', csv_bytes))
        
        # 4. Utenti
        utenti = get_utenti_approvati()
        if utenti:
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
            files_to_send.append(('db_user.csv', csv_bytes))
        
        # Invia i file a tutti gli admin
        for admin_id in ADMIN_IDS:
            try:
                for filename, csv_bytes in files_to_send:
                    csv_file = BytesIO(csv_bytes)
                    csv_file.name = filename
                    
                    await context.bot.send_document(
                        chat_id=admin_id,
                        document=csv_file,
                        filename=filename,
                        caption=f"📊 Backup automatico - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
                    )
                    time.sleep(1)  # Piccola pausa tra i file
                
                print(f"✅ CSV inviati automaticamente all'admin {admin_id}")
                
            except Exception as e:
                print(f"❌ Errore nell'invio a admin {admin_id}: {e}")
        
    except Exception as e:
        print(f"❌ Errore generale nell'invio automatico CSV: {e}")

def scheduler_invio_csv(context):
    """Scheduler per l'invio automatico dei CSV"""
    asyncio.create_task(invia_csv_automatico_admin(context))

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

# === IMPORT/EXPORT CSV - VERSIONE SEMPLIFICATA ===
async def gestisci_file_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Solo gli amministratori possono importare dati.")
        return
    
    document = update.message.document
    file_name = document.file_name.lower()
    
    if not file_name.endswith('.csv'):
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
        
        # Determina il tipo di CSV in base al nome del file
        if 'db_interventi' in file_name:
            await gestisci_import_interventi(update, context, reader)
        elif 'db_vigili' in file_name:
            await gestisci_import_vigili(update, context, reader)
        elif 'db_mezzi' in file_name:
            await gestisci_import_mezzi(update, context, reader)
        elif 'db_user' in file_name:
            await gestisci_import_utenti(update, context, reader)
        else:
            await update.message.reply_text(
                "❌ Impossibile determinare il tipo di CSV.\n\n"
                "I nomi dei file devono contenere:\n"
                "• 'db_interventi' per gli interventi\n"
                "• 'db_vigili' per i vigili\n"
                "• 'db_mezzi' per i mezzi\n"
                "• 'db_user' per gli utenti"
            )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Errore durante l'importazione: {str(e)}")
        print(f"Errore dettagliato: {e}")

async def gestisci_import_interventi(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    skipped_count = 0
    error_count = 0
    error_details = []
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 17:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero di colonne insufficiente ({len(row)}/17)")
                continue
            
            # Estrai i dati dalla riga
            num_erba = int(row[0]) if row[0] and row[0].isdigit() else get_prossimo_numero_erba()
            rapporto_como = row[1]
            progressivo_como = row[2]
            
            # Verifica se l'intervento esiste già
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
            
            # Gestione partecipanti
            partecipanti_ids = []
            if len(row) > 9 and row[9]:  # Colonna Partecipanti
                partecipanti_str = row[9]
                # I partecipanti sono separati da punto e virgola: "Cognome Nome; Cognome2 Nome2"
                partecipanti_lista = [p.strip() for p in partecipanti_str.split(';')]
                
                for partecipante in partecipanti_lista:
                    if partecipante:
                        # Cerca il vigile per nome e cognome
                        nome_cognome = partecipante.split()
                        if len(nome_cognome) >= 2:
                            cognome = nome_cognome[0]
                            nome = ' '.join(nome_cognome[1:])
                            
                            conn = sqlite3.connect(DATABASE_NAME)
                            c = conn.cursor()
                            c.execute("SELECT id FROM vigili WHERE cognome = ? AND nome = ?", (cognome, nome))
                            vigile = c.fetchone()
                            conn.close()
                            
                            if vigile:
                                partecipanti_ids.append(vigile[0])
                            else:
                                print(f"⚠️ Vigile non trovato: {partecipante}")
            
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
                'partecipanti': partecipanti_ids,
                'comune': row[10] if len(row) > 10 else '',
                'via': row[11] if len(row) > 11 else '',
                'indirizzo': row[12] if len(row) > 12 else '',
                'tipologia': row[13] if len(row) > 13 else '',
                'cambio_personale': row[14].lower() in ['sì', 'si', '1', 'true', 'vero'] if len(row) > 14 else False,
                'km_finali': int(row[15]) if len(row) > 15 and row[15] and row[15].isdigit() else None,
                'litri_riforniti': int(row[16]) if len(row) > 16 and row[16] and row[16].isdigit() else None
            }
            
            inserisci_intervento(dati)
            imported_count += 1
            
        except Exception as e:
            error_count += 1
            error_details.append(f"Riga {row_num}: {str(e)}")
            print(f"Errore nell'importazione riga {row_num}: {e}")
            continue
    
    # Invia il report
    messaggio = f"✅ **IMPORTAZIONE INTERVENTI COMPLETATA**\n\n"
    messaggio += f"📊 **Risultati:**\n"
    messaggio += f"• ✅ Record importati: {imported_count}\n"
    messaggio += f"• ⏭️ Record saltati (già presenti): {skipped_count}\n"
    messaggio += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        messaggio += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            messaggio += f"• {detail}\n"
        if len(error_details) > 5:
            messaggio += f"• ... e altri {len(error_details) - 5} errori\n"
    
    await update.message.reply_text(messaggio)

async def gestisci_import_vigili(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    updated_count = 0
    error_count = 0
    error_details = []
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 9:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero di colonne insufficiente ({len(row)}/9)")
                continue
            
            nome = row[0]
            cognome = row[1]
            qualifica = row[2]
            grado_patente = row[3]
            patente_nautica = bool(int(row[4])) if row[4] and row[4].isdigit() else False
            saf = bool(int(row[5])) if row[5] and row[5].isdigit() else False
            tpss = bool(int(row[6])) if row[6] and row[6].isdigit() else False
            atp = bool(int(row[7])) if row[7] and row[7].isdigit() else False
            attivo = bool(int(row[8])) if len(row) > 8 and row[8] and row[8].isdigit() else True
            
            # Cerca se il vigile esiste già
            conn = sqlite3.connect(DATABASE_NAME)
            c = conn.cursor()
            c.execute("SELECT id FROM vigili WHERE nome = ? AND cognome = ?", (nome, cognome))
            existing_vigile = c.fetchone()
            
            if existing_vigile:
                # Aggiorna il vigile esistente
                vigile_id = existing_vigile[0]
                c.execute('''UPDATE vigili SET 
                            qualifica = ?, grado_patente_terrestre = ?, patente_nautica = ?, 
                            saf = ?, tpss = ?, atp = ?, attivo = ?
                            WHERE id = ?''',
                         (qualifica, grado_patente, patente_nautica, saf, tpss, atp, attivo, vigile_id))
                updated_count += 1
            else:
                # Inserisce nuovo vigile
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
            continue
    
    messaggio = f"✅ **IMPORTAZIONE VIGILI COMPLETATA**\n\n"
    messaggio += f"📊 **Risultati:**\n"
    messaggio += f"• ✅ Vigili importati: {imported_count}\n"
    messaggio += f"• 🔄 Vigili aggiornati: {updated_count}\n"
    messaggio += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        messaggio += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            messaggio += f"• {detail}\n"
    
    await update.message.reply_text(messaggio)

async def gestisci_import_mezzi(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    updated_count = 0
    error_count = 0
    error_details = []
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 3:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero di colonne insufficiente ({len(row)}/3)")
                continue
            
            targa = row[0]
            tipo = row[1]
            attivo = bool(int(row[2])) if row[2] and row[2].isdigit() else True
            
            # Usa la funzione esistente per aggiungere/aggiornare il mezzo
            aggiungi_mezzo(targa, tipo)
            
            # Aggiorna lo stato attivo se necessario
            if not attivo:
                conn = sqlite3.connect(DATABASE_NAME)
                c = conn.cursor()
                c.execute("UPDATE mezzi SET attivo = ? WHERE targa = ?", (attivo, targa))
                conn.commit()
                conn.close()
            
            updated_count += 1  # aggiungi_mezzo fa INSERT OR REPLACE, quindi è sempre un aggiornamento
            
        except Exception as e:
            error_count += 1
            error_details.append(f"Riga {row_num}: {str(e)}")
            continue
    
    messaggio = f"✅ **IMPORTAZIONE MEZZI COMPLETATA**\n\n"
    messaggio += f"📊 **Risultati:**\n"
    messaggio += f"• 🔄 Mezzi importati/aggiornati: {updated_count}\n"
    messaggio += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        messaggio += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            messaggio += f"• {detail}\n"
    
    await update.message.reply_text(messaggio)

async def gestisci_import_utenti(update: Update, context: ContextTypes.DEFAULT_TYPE, reader):
    imported_count = 0
    updated_count = 0
    error_count = 0
    error_details = []
    
    for row_num, row in enumerate(reader, start=2):
        try:
            if len(row) < 6:
                error_count += 1
                error_details.append(f"Riga {row_num}: Numero di colonne insufficiente ({len(row)}/6)")
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
            continue
    
    messaggio = f"✅ **IMPORTAZIONE UTENTI COMPLETATA**\n\n"
    messaggio += f"📊 **Risultati:**\n"
    messaggio += f"• ✅ Utenti importati: {imported_count}\n"
    messaggio += f"• 🔄 Utenti aggiornati: {updated_count}\n"
    messaggio += f"• ❌ Errori: {error_count}\n\n"
    
    if error_details:
        messaggio += "📋 **Dettagli errori (prime 5):**\n"
        for detail in error_details[:5]:
            messaggio += f"• {detail}\n"
    
    await update.message.reply_text(messaggio)

# === ESTRAZIONE DATI - VERSIONE SEMPLIFICATA ===
async def estrazione_dati(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_user_approved(user_id):
        return
    
    keyboard = [
        [InlineKeyboardButton("📋 Interventi Completi", callback_data="export_interventi")],
        [InlineKeyboardButton("📅 Interventi per Anno", callback_data="export_anno_scelta")],  # NUOVO BOTTONE
        [InlineKeyboardButton("👥 Vigili", callback_data="export_vigili")],
        [InlineKeyboardButton("🚒 Mezzi", callback_data="export_mezzi")]
    ]
    
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("👤 Utenti", callback_data="export_utenti")])
        keyboard.append([InlineKeyboardButton("📤 Invia CSV a Admin", callback_data="invia_csv_admin")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📤 **ESTRAZIONE DATI**\n\n"
        "Seleziona il tipo di estrazione:",
        reply_markup=reply_markup
    )

async def esegui_export_interventi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    try:
        interventi = get_ultimi_interventi(10000)
        
        if not interventi:
            await query.edit_message_text("❌ Nessun intervento da esportare.")
            return
        
        output = StringIO()
        writer = csv.writer(output)
        
        writer.writerow([
            'Numero_Erba', 'Rapporto_Como', 'Progressivo', 'Data_Uscita', 'Data_Rientro',
            'Mezzo_Targa', 'Mezzo_Tipo', 'Capopartenza', 'Autista', 'Partecipanti', 'Comune', 'Via', 
            'Indirizzo', 'Tipologia', 'Cambio_Personale', 'Km_Finali', 'Litri_Riforniti'
        ])
        
        for intervento in interventi:
            if len(intervento) >= 18:
                id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, comune, via, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at = intervento[:18]
                
                # Recupera i partecipanti per questo intervento
                partecipanti_nomi = []
                conn = sqlite3.connect(DATABASE_NAME)
                c = conn.cursor()
                c.execute('''SELECT v.nome, v.cognome 
                             FROM partecipanti p 
                             JOIN vigili v ON p.vigile_id = v.id 
                             WHERE p.intervento_id = ?''', (id_int,))
                partecipanti = c.fetchall()
                conn.close()
                
                for nome, cognome in partecipanti:
                    partecipanti_nomi.append(f"{cognome} {nome}")
                
                partecipanti_str = "; ".join(partecipanti_nomi)
                
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
                    mezzo_targa, mezzo_tipo, capo, autista, partecipanti_str, comune, via, 
                    indirizzo, tipologia or '', 'Sì' if cambio_personale else 'No', 
                    km_finali or '', litri_riforniti or ''
                ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"db_interventi_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="📋 **INTERVENTI**\n\nFile CSV contenente tutti gli interventi con partecipanti."
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione: {str(e)}")

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
        
        writer.writerow(['Nome', 'Cognome', 'Qualifica', 'Grado_Patente', 'Patente_Nautica', 'SAF', 'TPSS', 'ATP', 'Stato'])
        
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
            caption="👥 **VIGILI**\n\nFile CSV contenente l'elenco completo dei vigili."
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
        csv_file.name = f"db_mezzi_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file Mezzi in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption="🚒 **MEZZI**\n\nFile CSV contenente l'elenco completo dei mezzi."
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
            caption="👤 **UTENTI**\n\nFile CSV contenente l'elenco degli utenti approvati."
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione utenti: {str(e)}")
async def mostra_scelta_anno_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra la selezione degli anni per l'esportazione"""
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    anni = get_anni_disponibili()
    
    if not anni:
        await query.edit_message_text("❌ Nessun dato disponibile per l'esportazione.")
        return
    
    keyboard = []
    for anno in anni:
        keyboard.append([InlineKeyboardButton(f"📅 {anno}", callback_data=f"export_anno_{anno}")])
    
    # Aggiungi opzione per tutti gli anni
    keyboard.append([InlineKeyboardButton("📅 Tutti gli anni", callback_data="export_anno_tutti")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "📅 **ESPORTA INTERVENTI PER ANNO**\n\n"
        "Seleziona l'anno da esportare:",
        reply_markup=reply_markup
    )
async def esegui_export_interventi_anno(update: Update, context: ContextTypes.DEFAULT_TYPE, anno: str = None):
    """Esporta gli interventi per un anno specifico"""
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    try:
        if anno == "tutti":
            interventi = get_ultimi_interventi(10000)
            filename_suffix = "interventi_completo"
            caption = "Esportazione completa di tutti gli interventi"
        else:
            interventi = get_interventi_per_anno(anno)
            filename_suffix = f"interventi_anno_{anno}"
            caption = f"Esportazione interventi per l'anno {anno}"
        
        if not interventi:
            await query.edit_message_text("❌ Nessun intervento da esportare per i criteri selezionati.")
            return
        
        output = StringIO()
        writer = csv.writer(output)
        
        writer.writerow([
            'Numero_Erba', 'Rapporto_Como', 'Progressivo', 'Data_Uscita', 'Data_Rientro',
            'Mezzo_Targa', 'Mezzo_Tipo', 'Capopartenza', 'Autista', 'Partecipanti', 'Comune', 'Via', 
            'Indirizzo', 'Tipologia', 'Cambio_Personale', 'Km_Finali', 'Litri_Riforniti'
        ])
        
        for intervento in interventi:
            if len(intervento) >= 18:
                id_int, rapporto, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, comune, via, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at = intervento[:18]
                
                # Recupera i partecipanti per questo intervento
                partecipanti_nomi = []
                conn = sqlite3.connect(DATABASE_NAME)
                c = conn.cursor()
                c.execute('''SELECT v.nome, v.cognome 
                             FROM partecipanti p 
                             JOIN vigili v ON p.vigile_id = v.id 
                             WHERE p.intervento_id = ?''', (id_int,))
                partecipanti = c.fetchall()
                conn.close()
                
                for nome, cognome in partecipanti:
                    partecipanti_nomi.append(f"{cognome} {nome}")
                
                partecipanti_str = "; ".join(partecipanti_nomi)
                
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
                    mezzo_targa, mezzo_tipo, capo, autista, partecipanti_str, comune, via,  # CORRETTO: mezzo_targa
                    indirizzo, tipologia or '', 'Sì' if cambio_personale else 'No', 
                    km_finali or '', litri_riforniti or ''
                ])
        
        csv_data = output.getvalue()
        output.close()
        
        csv_bytes = csv_data.encode('utf-8')
        csv_file = BytesIO(csv_bytes)
        csv_file.name = f"{filename_suffix}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        await query.edit_message_text("📤 Generazione file in corso...")
        await context.bot.send_document(
            chat_id=query.message.chat_id,
            document=csv_file,
            filename=csv_file.name,
            caption=f"📤 **{caption}**\n\nFile CSV contenente gli interventi."
        )
        
    except Exception as e:
        await query.edit_message_text(f"❌ Errore durante l'esportazione: {str(e)}")
async def invia_csv_admin_manual(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Invio manuale dei CSV agli admin"""
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    await query.edit_message_text("📤 Invio CSV a tutti gli admin in corso...")
    await invia_csv_automatico_admin(context)
    await query.edit_message_text("✅ CSV inviati a tutti gli admin!")

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

# === GESTIONE TIPOLOGIA NEL FLUSSO NUOVO INTERVENTO - VERSIONE CORRETTA ===
async def gestisci_tipologia_intervento(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    print(f"DEBUG: Callback ricevuto: {callback_data}")  # Debug
    
    if callback_data.startswith("tipopage_"):
        # Navigazione pagine
        page = int(callback_data.replace('tipopage_', ''))
        await mostra_selezione_tipologia_paginata(query, context, page)
        return
    
    elif callback_data == "tipologia_altro":
        # Tipologia personalizzata
        context.user_data['fase'] = 'inserisci_tipologia_personalizzata'
        await query.edit_message_text(
            "✏️ **TIPOLOGIA PERSONALIZZATA**\n\n"
            "Inserisci la tipologia di intervento:"
        )
        return
    
    else:
        # Selezione tipologia dalla lista
        print(f"DEBUG: Cerco tipologia per callback: {callback_data}")  # Debug
        
        # Verifica se il callback esiste nel mapping
        if callback_data in TIPOLOGIE_MAPPING:
            tipologia_completa = TIPOLOGIE_MAPPING[callback_data][1]
            display_name = TIPOLOGIE_MAPPING[callback_data][0]
            
            print(f"DEBUG: Tipologia trovata - Completa: {tipologia_completa}, Display: {display_name}")  # Debug
            
            context.user_data['nuovo_intervento']['tipologia'] = tipologia_completa
            context.user_data['fase'] = 'km_finali'
            
            await query.edit_message_text(
                f"✅ Tipologia selezionata: **{display_name}**\n\n"
                "🛣️ **KM FINALI**\n\n"
                "Inserisci i km finali del mezzo (solo numeri):"
            )
        else:
            print(f"DEBUG: Callback NON trovato nel mapping: {callback_data}")  # Debug
            await query.edit_message_text(
                "❌ Errore nella selezione della tipologia. Riprova.",
                reply_markup=crea_tastiera_tipologie_paginata(0)
            )

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
📅 **Rientro:** {datetime.strptime(dati['data_rientro_completa'], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y %H:%M') if dati.get('data_rientro_completa') else 'Non specificato'}
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
        [InlineKeyboardButton("✏️ Modifica Intervento", callback_data="modifica_intervento")],
        [InlineKeyboardButton("📤 Invia CSV Admin", callback_data="invia_csv_admin")]
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
        "Targa,Tipo,Stato\n"
        "AB123CD,APS,1\n"
        "EF456GH,ABP,0\n\n"
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
        "Nome,Cognome,Qualifica,Grado Patente,Patente Nautica (1/0),SAF (1/0),TPSS (1/0),ATP (1/0),Stato (1/0)\n\n"
        "Esempio:\n"
        "Mario,Rossi,CSV,III,1,0,1,0,1"
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

# === GESTIONE TIPOLOGIA NEL FLUSSO MODIFICA INTERVENTO - VERSIONE CORRETTA ===
async def gestisci_tipologia_modifica(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    print(f"DEBUG MODIFICA: Callback ricevuto: {callback_data}")  # Debug
    
    if callback_data.startswith("tipopage_"):
        # Navigazione pagine
        page = int(callback_data.replace('tipopage_', ''))
        await mostra_selezione_tipologia_paginata(query, context, page)
        return
    
    elif callback_data == "tipologia_altro":
        # Tipologia personalizzata
        context.user_data['fase_modifica'] = 'inserisci_tipologia_modifica'
        await query.edit_message_text(
            "✏️ **MODIFICA TIPOLOGIA**\n\n"
            "Inserisci la nuova tipologia di intervento:"
        )
        return
    
    else:
        # Selezione tipologia dalla lista
        print(f"DEBUG MODIFICA: Cerco tipologia per callback: {callback_data}")  # Debug
        
        if callback_data in TIPOLOGIE_MAPPING:
            tipologia_completa = TIPOLOGIE_MAPPING[callback_data][1]
            display_name = TIPOLOGIE_MAPPING[callback_data][0]
            
            print(f"DEBUG MODIFICA: Tipologia trovata - Completa: {tipologia_completa}, Display: {display_name}")  # Debug
            
            rapporto = context.user_data['modifica_intervento']['rapporto']
            progressivo = context.user_data['modifica_intervento']['progressivo']
            
            aggiorna_intervento(rapporto, progressivo, 'tipologia', tipologia_completa)
            
            await query.edit_message_text(
                f"✅ **TIPOLOGIA AGGIORNATA!**\n\n"
                f"Rapporto: R{rapporto}/{progressivo}\n"
                f"Nuova tipologia: {display_name}"
            )
            
            # Pulisci lo stato
            for key in ['modifica_intervento', 'fase_modifica']:
                if key in context.user_data:
                    del context.user_data[key]
        else:
            print(f"DEBUG MODIFICA: Callback NON trovato nel mapping: {callback_data}")  # Debug
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
        "Seleziona l'anno per le statistiche:",
        reply_markup=reply_markup
    )

async def gestisci_statistiche(update: Update, context: ContextTypes.DEFAULT_TYPE, callback_data: str):
    query = update.callback_query
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    if callback_data == "stats_tutti":
        stats = get_statistiche_anno()
        titolo = "TUTTI GLI ANNI"
    else:
        anno = callback_data.replace('stats_', '')
        stats = get_statistiche_anno(anno)
        titolo = anno
    
    if not stats['totale_interventi']:
        await query.edit_message_text(f"📊 **STATISTICHE {titolo}**\n\nNessun dato disponibile.")
        return
    
    messaggio = f"📊 **STATISTICHE {titolo}**\n\n"
    messaggio += f"🔢 **Interventi totali:** {stats['totale_interventi']}\n"
    messaggio += f"🚒 **Partenze totali:** {stats['totale_partenze']}\n\n"
    
    # Top 10 tipologie (invece di 5)
    if stats['tipologie']:
        messaggio += "🚨 **TOP 10 TIPOLOGIE:**\n"
        tipologie_ordinate = sorted(stats['tipologie'].items(), key=lambda x: x[1], reverse=True)[:10]  # Cambiato da 5 a 10
        for i, (tipologia, count) in enumerate(tipologie_ordinate, 1):
            nome_breve = next((display for callback, (display, full) in TIPOLOGIE_MAPPING.items() if full == tipologia), tipologia)
            messaggio += f"{i}. {nome_breve}: {count}\n"
        messaggio += "\n"
    
    # Top 5 mezzi (manteniamo 5 per i mezzi)
    if stats['mezzi']:
        messaggio += "🚒 **TOP 5 MEZZI:**\n"
        mezzi_ordinati = sorted(stats['mezzi'].items(), key=lambda x: x[1], reverse=True)[:5]
        for i, (mezzo, count) in enumerate(mezzi_ordinati, 1):
            messaggio += f"{i}. {mezzo}: {count}\n"
    
    await query.edit_message_text(messaggio)
# === ULTIMI INTERVENTI ===
async def ultimi_interventi(update: Update, context: ContextTypes.DEFAULT_TYPE):
    interventi = get_ultimi_interventi_attivi()
    
    if not interventi:
        await update.message.reply_text("📋 **ULTIMI INTERVENTI**\n\nNessun intervento trovato nel database.")
        return
    
    messaggio = "📋 **ULTIMI 10 INTERVENTI**\n\n"
    for intervento in interventi:
        id_int, rapporto, progressivo, num_erba, data_uscita, indirizzo, data_rientro = intervento
        
        # Formatta le date
        data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M')
        data_rientro_fmt = datetime.strptime(data_rientro, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M') if data_rientro else "In corso"
        
        # Recupera altri dettagli dell'intervento
        conn = sqlite3.connect(DATABASE_NAME)
        c = conn.cursor()
        c.execute('''SELECT mezzo_targa, mezzo_tipo, capopartenza, autista, tipologia 
                     FROM interventi WHERE id = ?''', (id_int,))
        dettagli = c.fetchone()
        conn.close()
        
        if dettagli:
            mezzo_targa, mezzo_tipo, capopartenza, autista, tipologia = dettagli
            
            # Trova il nome breve della tipologia
            nome_breve_tipologia = next((display for callback, (display, full) in TIPOLOGIE_MAPPING.items() if full == tipologia), tipologia)
            
            messaggio += f"🔢 **#{num_erba}** - R{rapporto}/{progressivo}\n"
            messaggio += f"📅 **Uscita:** {data_uscita_fmt}\n"
            messaggio += f"📅 **Rientro:** {data_rientro_fmt}\n"
            messaggio += f"🚒 **Mezzo:** {mezzo_targa} - {mezzo_tipo}\n"
            messaggio += f"👨‍🚒 **Capo:** {capopartenza}\n"
            messaggio += f"🚗 **Autista:** {autista}\n"
            messaggio += f"📍 **Indirizzo:** {indirizzo}\n"
            messaggio += f"🚨 **Tipologia:** {nome_breve_tipologia}\n"
            messaggio += "─" * 30 + "\n\n"
    
    await update.message.reply_text(messaggio)

# === CERCA RAPPORTO ===
async def cerca_rapporto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['cerca_rapporto'] = {}
    context.user_data['fase_cerca'] = 'anno'
    
    await update.message.reply_text(
        "🔍 **CERCA RAPPORTO**\n\n"
        "Inserisci l'ANNO del rapporto Como da cercare (es: 2024):"
    )

async def gestisci_anno_cerca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    anno = update.message.text.strip()
    
    if not anno.isdigit() or len(anno) != 4:
        await update.message.reply_text("❌ Anno non valido! Inserisci 4 cifre (es: 2024):")
        return
    
    context.user_data['cerca_rapporto']['anno'] = anno
    context.user_data['fase_cerca'] = 'rapporto'
    
    await update.message.reply_text(
        f"📅 Anno selezionato: {anno}\n\n"
        "Inserisci il numero del rapporto Como da cercare:"
    )

async def gestisci_rapporto_cerca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rapporto = update.message.text.strip()
    
    if not rapporto.isdigit():
        await update.message.reply_text("❌ Inserisci solo numeri! Riprova:")
        return
    
    anno = context.user_data['cerca_rapporto']['anno']
    interventi = get_interventi_per_rapporto(rapporto, anno)
    
    if not interventi:
        await update.message.reply_text(
            f"❌ Nessun intervento trovato per il rapporto R{rapporto} nell'anno {anno}."
        )
        for key in ['cerca_rapporto', 'fase_cerca']:
            if key in context.user_data:
                del context.user_data[key]
        return
    
    messaggio = f"🔍 **INTERVENTI R{rapporto} - {anno}**\n\n"
    for intervento in interventi:
        if len(intervento) >= 16:
            id_int, rapporto_db, progressivo, num_erba, data_uscita, data_rientro, mezzo_targa, mezzo_tipo, capo, autista, indirizzo, tipologia, cambio_personale, km_finali, litri_riforniti, created_at = intervento[:16]
            
            data_uscita_fmt = datetime.strptime(data_uscita, '%Y-%m-%d %H:%M:%S').strftime('%d/%m %H:%M')
            durata = calcola_durata_intervento(data_uscita, data_rientro)
            
            messaggio += f"🔢 **#{num_erba}** - Progressivo: {progressivo}\n"
            messaggio += f"📅 {data_uscita_fmt} - ⏱️ {durata}\n"
            messaggio += f"🚒 {mezzo_targa} - {mezzo_tipo}\n"
            messaggio += f"👨‍🚒 Capo: {capo}\n"
            messaggio += f"📍 {indirizzo}\n"
            if tipologia:
                nome_breve = next((display for callback, (display, full) in TIPOLOGIE_MAPPING.items() if full == tipologia), tipologia)
                messaggio += f"🚨 {nome_breve}\n"
            messaggio += "\n"
    
    await update.message.reply_text(messaggio)
    
    for key in ['cerca_rapporto', 'fase_cerca']:
        if key in context.user_data:
            del context.user_data[key]

# === HELP ===
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_admin_user = is_admin(user_id)
    
    messaggio = "🆘 **GUIDA ALL'USO DEL BOT**\n\n"
    messaggio += "📋 **COMANDI PRINCIPALI:**\n"
    messaggio += "• /start - Riavvia il bot\n"
    messaggio += "• ➕ Nuovo Intervento - Registra un nuovo intervento\n"
    messaggio += "• 📋 Ultimi Interventi - Mostra gli interventi recenti\n"
    messaggio += "• 📊 Statistiche - Visualizza le statistiche\n"
    messaggio += "• 🔍 Cerca Rapporto - Cerca interventi per rapporto\n"
    messaggio += "• 📤 Estrazione Dati - Esporta dati in CSV\n\n"
    
    if is_admin_user:
        messaggio += "⚙️ **COMANDI AMMINISTRATORE:**\n"
        messaggio += "• 👥 Gestisci Richieste - Gestisci richieste accesso\n"
        messaggio += "• ⚙️ Gestione - Menu amministrativo\n"
        messaggio += "• 📥 Importa CSV - Carica dati da file CSV\n\n"
    
    messaggio += "💡 **SUGGERIMENTI:**\n"
    messaggio += "• Usa la tastiera fisica per navigare velocemente\n"
    messaggio += "• Segui il flusso guidato per nuovi interventi\n"
    messaggio += "• Controlla sempre i dati prima di confermare"
    
    await update.message.reply_text(messaggio)

# === GESTIONE MESSAGGI DI TESTO ===
async def gestisci_messaggio_testo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    testo = update.message.text
    
    if not is_user_approved(user_id):
        if testo == "🚀 Richiedi Accesso":
            await start(update, context)
        return
    
    # Controlla se siamo in un flusso attivo
    if 'fase' in context.user_data:
        fase = context.user_data['fase']
        
        if fase == 'inserisci_rapporto':
            await gestisci_rapporto_como(update, context)
        elif fase == 'ora_uscita':
            await gestisci_ora_uscita(update, context)
        elif fase == 'ora_rientro':
            await gestisci_ora_rientro(update, context)
        elif fase == 'inserisci_comune':
            await gestisci_comune(update, context)
        elif fase == 'inserisci_via':
            await gestisci_via(update, context)
        elif fase == 'inserisci_tipologia_personalizzata':
            await gestisci_tipologia_personalizzata(update, context)
        elif fase == 'km_finali':
            await gestisci_km_finali(update, context)
        elif fase == 'litri_riforniti':
            await gestisci_litri_riforniti(update, context)
    
    elif 'fase_modifica' in context.user_data:
        fase_modifica = context.user_data['fase_modifica']
        
        if fase_modifica == 'anno':
            await gestisci_anno_modifica(update, context)
        elif fase_modifica == 'rapporto':
            await gestisci_rapporto_modifica(update, context)
        elif fase_modifica == 'progressivo':
            await gestisci_progressivo_modifica(update, context)
        elif fase_modifica == 'inserisci_tipologia_modifica':
            # Gestione tipologia personalizzata in modifica
            tipologia = update.message.text.strip()
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
        
        elif fase_modifica == 'modifica_indirizzo':
            await gestisci_modifica_indirizzo(update, context)
        elif fase_modifica == 'modifica_orari':
            await gestisci_modifica_orari(update, context)
        elif fase_modifica == 'inserisci_valore':
            await gestisci_valore_modifica(update, context)
    
    elif 'fase_cerca' in context.user_data:
        fase_cerca = context.user_data['fase_cerca']
        
        if fase_cerca == 'anno':
            await gestisci_anno_cerca(update, context)
        elif fase_cerca == 'rapporto':
            await gestisci_rapporto_cerca(update, context)
    
    else:
        # Gestione comandi dalla tastiera fisica
        if testo == "➕ Nuovo Intervento":
            await avvia_nuovo_intervento(update, context)
        elif testo == "📋 Ultimi Interventi":
            await ultimi_interventi(update, context)
        elif testo == "📊 Statistiche":
            await mostra_statistiche(update, context)
        elif testo == "🔍 Cerca Rapporto":
            await cerca_rapporto(update, context)
        elif testo == "📤 Estrazione Dati":
            await estrazione_dati(update, context)
        elif testo == "👥 Gestisci Richieste" and is_admin(user_id):
            await gestisci_richieste(update, context)
        elif testo == "⚙️ Gestione" and is_admin(user_id):
            await gestione_admin(update, context)
        elif testo == "/start 🔄":
            await start(update, context)
        elif testo == "🆘 Help":
            await help_command(update, context)

# === GESTIONE CALLBACK QUERY ===
async def gestisci_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    callback_data = query.data
    
    try:
        await query.answer()
    except BadRequest as e:
        if "Query is too old" in str(e):
            return
    
    # Gestione tipologie (pagine)
    if callback_data.startswith("tipopage_"):
        if 'fase_modifica' in context.user_data and context.user_data['fase_modifica'] == 'modifica_tipologia':
            await gestisci_tipologia_modifica(update, context, callback_data)
        else:
            await gestisci_tipologia_intervento(update, context, callback_data)
    
    # Gestione nuovo intervento
    elif callback_data in ["tipo_nuovo", "tipo_collegato"]:
        await gestisci_scelta_tipo(update, context, callback_data)
    elif callback_data.startswith("collega_"):
        intervento_id = int(callback_data.replace('collega_', ''))
        await gestisci_collega_intervento(update, context, intervento_id)
    elif callback_data in ["data_oggi", "data_ieri"]:
        await gestisci_data_uscita(update, context, callback_data)
    elif callback_data in ["rientro_oggi", "rientro_ieri"]:
        await gestisci_data_rientro(update, context, callback_data)
    elif callback_data.startswith("mezzo_"):
        await gestisci_selezione_mezzo(update, context, callback_data)
    elif callback_data in ["cambio_si", "cambio_no"]:
        await gestisci_cambio_personale(update, context, callback_data)
    elif callback_data.startswith("capo_"):
        await gestisci_selezione_capopartenza(update, context, callback_data)
    elif callback_data.startswith("autista_"):
        await gestisci_selezione_autista(update, context, callback_data)
    elif callback_data.startswith("vigile_si_") or callback_data.startswith("vigile_no_"):
        await gestisci_selezione_vigile(update, context, callback_data)
    elif callback_data == "tipologia_altro":
        if 'fase_modifica' in context.user_data and context.user_data['fase_modifica'] == 'modifica_tipologia':
            await gestisci_tipologia_modifica(update, context, callback_data)
        else:
            await gestisci_tipologia_intervento(update, context, callback_data)
    elif callback_data in TIPOLOGIE_MAPPING:
        if 'fase_modifica' in context.user_data and context.user_data['fase_modifica'] == 'modifica_tipologia':
            await gestisci_tipologia_modifica(update, context, callback_data)
        else:
            await gestisci_tipologia_intervento(update, context, callback_data)
    elif callback_data in ["conferma_si", "conferma_no"]:
        await conferma_intervento(update, context, callback_data)
    
    # Gestione richieste accesso
    elif callback_data == "richieste_attesa":
        await mostra_richieste_attesa(update, context)
    elif callback_data == "utenti_approvati":
        await mostra_utenti_approvati(update, context)
    elif callback_data.startswith("approva_"):
        user_id_approvare = int(callback_data.replace('approva_', ''))
        approva_utente(user_id_approvare)
        await query.edit_message_text(f"✅ Utente {user_id_approvare} approvato!")
    elif callback_data.startswith("rifiuta_"):
        user_id_rifiutare = int(callback_data.replace('rifiuta_', ''))
        rimuovi_utente(user_id_rifiutare)
        await query.edit_message_text(f"❌ Richiesta di {user_id_rifiutare} rifiutata!")
    elif callback_data.startswith("rimuovi_"):
        user_id_rimuovere = int(callback_data.replace('rimuovi_', ''))
        await conferma_rimozione_utente(update, context, user_id_rimuovere)
    elif callback_data.startswith("conferma_rimozione_"):
        user_id_rimuovere = int(callback_data.replace('conferma_rimozione_', ''))
        await esegui_rimozione_utente(update, context, user_id_rimuovere)
    elif callback_data == "annulla_rimozione":
        await query.edit_message_text("❌ Rimozione annullata.")
    
    # Gestione amministrativa
    elif callback_data == "admin_vigili":
        await gestione_vigili_admin(update, context)
    elif callback_data == "admin_mezzi":
        await gestione_mezzi_admin(update, context)
    elif callback_data == "modifica_intervento":
        await avvia_modifica_intervento(update, context)
    elif callback_data == "invia_csv_admin":
        await invia_csv_admin_manual(update, context)
    elif callback_data == "lista_vigili":
        await mostra_lista_vigili(update, context)
    elif callback_data == "lista_mezzi":
        await mostra_lista_mezzi(update, context)
    elif callback_data == "importa_vigili":
        await importa_vigili_csv(update, context)
    elif callback_data == "importa_mezzi_info":
        await importa_mezzi_info(update, context)
    
    # Gestione modifica intervento
    elif callback_data.startswith("campo_"):
        campo = callback_data.replace('campo_', '')
        await gestisci_selezione_campo(update, context, campo)
    elif callback_data.startswith("modmezzo_"):
        targa = callback_data.replace('modmezzo_', '')
        await gestisci_valore_modifica_bottoni(update, context, 'mezzo', targa)
    elif callback_data.startswith("modcapo_"):
        vigile_id = callback_data.replace('modcapo_', '')
        vigile = get_vigile_by_id(int(vigile_id))
        if vigile:
            nome_completo = f"{vigile[1]} {vigile[2]}"
            await gestisci_valore_modifica_bottoni(update, context, 'capopartenza', nome_completo)
    elif callback_data.startswith("modautista_"):
        vigile_id = callback_data.replace('modautista_', '')
        vigile = get_vigile_by_id(int(vigile_id))
        if vigile:
            nome_completo = f"{vigile[1]} {vigile[2]}"
            await gestisci_valore_modifica_bottoni(update, context, 'autista', nome_completo)
    
    # Gestione statistiche
    elif callback_data.startswith("stats_"):
        await gestisci_statistiche(update, context, callback_data)
    
    # Gestione esportazione dati
    elif callback_data == "export_interventi":
        await esegui_export_interventi(update, context)
    elif callback_data == "export_anno_scelta":
        await mostra_scelta_anno_export(update, context)
    elif callback_data.startswith("export_anno_"):
        anno = callback_data.replace('export_anno_', '')
        await esegui_export_interventi_anno(update, context, anno)
    elif callback_data == "export_vigili":
        await esegui_export_vigili(update, context)
    elif callback_data == "export_mezzi":
        await esegui_export_mezzi(update, context)
    elif callback_data == "export_utenti":
        await esegui_export_utenti(update, context)

# === SCHEDULER INVIO CSV AUTOMATICO - VERSIONE CORRETTA ===
def avvia_scheduler_csv_manual():
    """Avvia lo scheduler manuale per l'invio automatico dei CSV"""
    print("⏰ Scheduler CSV manuale avviato. Controllo ogni ora...")
    
    while True:
        now = datetime.now()
        
        # Controlla se è l'ora programmata (23:55)
        if now.hour == 23 and now.minute == 55:
            print("🕐 Ora di inviare i CSV automatici agli admin...")
            try:
                # Crea un contesto fittizio per l'invio
                class ContextFittizio:
                    def __init__(self):
                        # Creiamo un bot fittizio per l'invio
                        from telegram import Bot
                        self.bot = Bot(token=BOT_TOKEN)
                
                context_fittizio = ContextFittizio()
                asyncio.run(invia_csv_automatico_admin(context_fittizio))
                print("✅ CSV automatici inviati con successo!")
            except Exception as e:
                print(f"❌ Errore nell'invio automatico CSV: {e}")
            
            # Aspetta 65 minuti per evitare esecuzioni multiple
            time.sleep(3900)
        else:
            # Controlla ogni minuto
            time.sleep(60)

# === MAIN CORRETTO ===
def main():
    # Verifica integrità database
    if not check_database_integrity():
        print("🚨 Database corrotto o incompleto! Ricreazione...")
        emergency_recreate_database()
    
    # Ripristino da backup se disponibile
    print("🔄 Verifica backup...")
    restore_database_from_gist()
    
    # Avvia server Flask in thread separato
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Avvia keep-alive aggressivo in thread separato
    keep_alive_thread = threading.Thread(target=keep_alive_aggressive, daemon=True)
    keep_alive_thread.start()
    
    # Avvia backup scheduler in thread separato
    backup_thread = threading.Thread(target=backup_scheduler, daemon=True)
    backup_thread.start()
    
    # Avvia scheduler CSV manuale in thread separato
    scheduler_thread = threading.Thread(target=avvia_scheduler_csv_manual, daemon=True)
    scheduler_thread.start()
    
    # Crea application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Aggiungi handler
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, gestisci_messaggio_testo))
    application.add_handler(MessageHandler(filters.Document.ALL, gestisci_file_csv))
    application.add_handler(CallbackQueryHandler(gestisci_callback))
    
    # Avvia bot
    print("🤖 Bot avviato!")
    application.run_polling()

if __name__ == '__main__':
    main()
