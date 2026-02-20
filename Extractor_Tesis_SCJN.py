import flet as ft
import threading
import time
import os
from datetime import datetime
import sqlite3
from typing import List, Dict, Optional, Tuple
import hashlib
import logging
import requests
import json
import pandas as pd
import subprocess
import platform
import re
import asyncio
import sys
import html
from pathlib import Path
import uuid
import tempfile
import inspect
import shutil

# Variable global para la instancia de base de datos
# Variable global para la instancia de base de datos y su ruta
GLOBAL_DB = None
GLOBAL_DB_PATH = None
extractor_thread = None
descargador_thread = None
stop_extraction = False
stop_download = False

def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def get_icon_path():
    icon_path = resource_path("icono.ico")
    if hasattr(sys, '_MEIPASS') and os.path.exists(icon_path):
        return icon_path
    dev_icon_path = "icono.ico"
    if os.path.exists(dev_icon_path):
        return dev_icon_path
    return None

def get_data_folder():
    """Retorna la ruta a la carpeta 'data' junto al ejecutable/script."""
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def ensure_db_in_data(data_dir):
    db_path = os.path.join(data_dir, "tesis_scjn.db")
    if not os.path.exists(db_path):
        src = resource_path("tesis_scjn.db")
        if os.path.exists(src):
            shutil.copy2(src, db_path)
            print(f"Base de datos copiada a {db_path}")
        else:
            print("Archivo de base no encontrado en paquete, se creará nueva.")
    return db_path



class SCJNTesisDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
        self.migrar_datos_existentes()

    def connect(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA journal_mode=WAL")
        self.cursor.execute("PRAGMA cache_size = -20000")

    def create_tables(self):
        try:
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tesis (
                    ius TEXT PRIMARY KEY,
                    id TEXT,
                    rubro TEXT,
                    clave_tesis TEXT,
                    localizacion TEXT,
                    sala TEXT,
                    epoca TEXT,
                    instancia TEXT,
                    fuente TEXT,
                    tipo_tesis INTEGER,
                    tipo_jurisprudencia INTEGER,
                    tipo_jurisprudencia_texto TEXT,
                    precedentes TEXT,
                    ejecutorias TEXT,
                    votos TEXT,
                    volumen TEXT,
                    tomo TEXT,
                    pagina TEXT,
                    mes TEXT,
                    anio TEXT,
                    epoca_config TEXT,
                    tipo_tesis_config TEXT,
                    fecha_extraccion TEXT,
                    fecha_actualizacion TEXT,
                    descargado TEXT DEFAULT 'No',
                    ubicacion TEXT,
                    UNIQUE(ius)
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS materia (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT UNIQUE NOT NULL
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tesis_materia (
                    tesis_ius TEXT NOT NULL,
                    materia_id INTEGER NOT NULL,
                    PRIMARY KEY (tesis_ius, materia_id),
                    FOREIGN KEY (tesis_ius) REFERENCES tesis(ius) ON DELETE CASCADE,
                    FOREIGN KEY (materia_id) REFERENCES materia(id) ON DELETE CASCADE
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS control_extracciones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    epoca TEXT,
                    tipo_tesis TEXT,
                    pagina INTEGER,
                    total_tesis INTEGER DEFAULT 0,
                    fecha_inicio TEXT,
                    fecha_fin TEXT,
                    estado TEXT DEFAULT 'pendiente',
                    hash_config TEXT,
                    UNIQUE(epoca, tipo_tesis, pagina)
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS resumen_epoca (
                    epoca TEXT PRIMARY KEY,
                    cantidad INTEGER DEFAULT 0,
                    fecha_actualizacion TEXT
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS resumen_tipo_tesis (
                    tipo_tesis TEXT PRIMARY KEY,
                    cantidad INTEGER DEFAULT 0,
                    fecha_actualizacion TEXT
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS resumen_sala (
                    sala TEXT PRIMARY KEY,
                    cantidad INTEGER DEFAULT 0,
                    fecha_actualizacion TEXT
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS resumen_tipo_jurisprudencia (
                    tipo_jurisprudencia TEXT PRIMARY KEY,
                    cantidad INTEGER DEFAULT 0,
                    fecha_actualizacion TEXT
                )
            ''')
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS resumen_materia (
                    materia TEXT PRIMARY KEY,
                    cantidad INTEGER DEFAULT 0,
                    fecha_actualizacion TEXT
                )
            ''')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tesis_ius ON tesis(ius)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tesis_descargado ON tesis(descargado)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tesis_epoca_config ON tesis(epoca_config)')
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_tesis_fecha_ius 
                ON tesis(fecha_actualizacion DESC, ius DESC)
            ''')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tesis_rubro ON tesis(rubro)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tesis_clave_tesis ON tesis(clave_tesis)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tesis_sala ON tesis(sala)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tesis_materia_tesis_ius ON tesis_materia(tesis_ius)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_tesis_materia_materia_id ON tesis_materia(materia_id)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_materia_nombre ON materia(nombre)')
            self.cursor.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS tesis_fts USING fts5(
                    ius,
                    rubro,
                    clave_tesis,
                    epoca_config,
                    content=tesis,
                    content_rowid=rowid,
                    tokenize = 'unicode61 remove_diacritics 2'
                )
            ''')
            self.cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS tesis_ai AFTER INSERT ON tesis BEGIN
                    INSERT INTO tesis_fts(rowid, ius, rubro, clave_tesis, epoca_config)
                    VALUES (new.rowid, new.ius, new.rubro, new.clave_tesis, new.epoca_config);
                END;
            ''')
            self.cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS tesis_au AFTER UPDATE ON tesis BEGIN
                    INSERT INTO tesis_fts(tesis_fts, rowid, ius, rubro, clave_tesis, epoca_config)
                    VALUES('delete', old.rowid, old.ius, old.rubro, old.clave_tesis, old.epoca_config);
                    INSERT INTO tesis_fts(rowid, ius, rubro, clave_tesis, epoca_config)
                    VALUES (new.rowid, new.ius, new.rubro, new.clave_tesis, new.epoca_config);
                END;
            ''')
            self.cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS tesis_ad AFTER DELETE ON tesis BEGIN
                    INSERT INTO tesis_fts(tesis_fts, rowid, ius, rubro, clave_tesis, epoca_config)
                    VALUES('delete', old.rowid, old.ius, old.rubro, old.clave_tesis, old.epoca_config);
                END;
            ''')
            self.conn.commit()
            self._populate_fts_if_needed()
        except Exception as e:
            logging.error(f"Error al crear tablas: {e}")
            raise

    def _populate_fts_if_needed(self):
        self.cursor.execute("SELECT COUNT(*) FROM tesis_fts")
        if self.cursor.fetchone()[0] == 0:
            self.cursor.execute('''
                INSERT INTO tesis_fts(rowid, ius, rubro, clave_tesis, epoca_config)
                SELECT rowid, ius, rubro, clave_tesis, epoca_config FROM tesis
            ''')
            self.conn.commit()

    def migrar_datos_existentes(self):
        self.cursor.execute("PRAGMA table_info(tesis)")
        columnas = [col[1] for col in self.cursor.fetchall()]
        tiene_columna_materias = 'materias' in columnas
        self.cursor.execute("SELECT COUNT(*) FROM tesis")
        total_tesis = self.cursor.fetchone()[0]
        if total_tesis == 0:
            return
        self.cursor.execute("SELECT COUNT(*) FROM resumen_epoca")
        resumenes_existentes = self.cursor.fetchone()[0] > 0
        if tiene_columna_materias and not resumenes_existentes:
            logging.info("Migrando datos existentes al nuevo esquema...")
            self.cursor.execute("SELECT ius, materias FROM tesis WHERE materias IS NOT NULL AND materias != ''")
            tesis_con_materias = self.cursor.fetchall()
            for ius, materias_str in tesis_con_materias:
                self._asignar_materias_a_tesis(ius, materias_str)
            self.actualizar_resumenes()
            logging.info("Migración completada.")
            return
        if not tiene_columna_materias and not resumenes_existentes:
            logging.info("Actualizando tablas de resumen...")
            self.actualizar_resumenes()
            return

    def _get_or_create_materia(self, nombre: str) -> Optional[int]:
        nombre = nombre.strip()
        if not nombre:
            return None
        self.cursor.execute("SELECT id FROM materia WHERE nombre = ?", (nombre,))
        row = self.cursor.fetchone()
        if row:
            return row[0]
        self.cursor.execute("INSERT INTO materia (nombre) VALUES (?)", (nombre,))
        self.conn.commit()
        return self.cursor.lastrowid

    def _asignar_materias_a_tesis(self, ius: str, materias_str: str):
        if not materias_str:
            return
        self.cursor.execute("DELETE FROM tesis_materia WHERE tesis_ius = ?", (ius,))
        materias = [m.strip() for m in materias_str.split(',') if m.strip()]
        contador = 0
        for materia in materias:
            materia_id = self._get_or_create_materia(materia)
            if materia_id:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO tesis_materia (tesis_ius, materia_id)
                    VALUES (?, ?)
                ''', (ius, materia_id))
                contador += 1
        self.conn.commit()

    def tesis_exists(self, ius: str) -> bool:
        self.cursor.execute("SELECT 1 FROM tesis WHERE ius = ?", (ius,))
        return self.cursor.fetchone() is not None

    def insert_tesis(self, tesis_data: Dict) -> bool:
        try:
            if self.tesis_exists(tesis_data['IUS']):
                self.cursor.execute('''
                    UPDATE tesis SET
                        rubro = ?,
                        clave_tesis = ?,
                        localizacion = ?,
                        sala = ?,
                        epoca = ?,
                        instancia = ?,
                        fuente = ?,
                        tipo_tesis = ?,
                        tipo_jurisprudencia = ?,
                        tipo_jurisprudencia_texto = ?,
                        epoca_config = ?,
                        tipo_tesis_config = ?,
                        tomo = ?,
                        pagina = ?,
                        mes = ?,
                        anio = ?,
                        fecha_actualizacion = ?,
                        descargado = COALESCE(descargado, 'No'),
                        ubicacion = COALESCE(ubicacion, '')
                    WHERE ius = ?
                ''', (
                    tesis_data['Rubro'],
                    tesis_data['Clave_Tesis'],
                    tesis_data['Localizacion'],
                    tesis_data['Sala'],
                    tesis_data['Epoca'],
                    tesis_data['Instancia'],
                    tesis_data['Fuente'],
                    tesis_data['Tipo_Tesis'],
                    tesis_data['Tipo_Jurisprudencia'],
                    tesis_data['Tipo_Jurisprudencia_Texto'],
                    tesis_data['Epoca_Config'],
                    tesis_data['Tipo_Tesis_Config'],
                    tesis_data.get('Tomo', ''),
                    tesis_data.get('Pagina', ''),
                    tesis_data.get('Mes', ''),
                    tesis_data.get('Anio', ''),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    tesis_data['IUS']
                ))
                return False
            else:
                self.cursor.execute('''
                    INSERT INTO tesis (
                        ius, id, rubro, clave_tesis, localizacion, sala, epoca,
                        instancia, fuente, tipo_tesis, tipo_jurisprudencia,
                        tipo_jurisprudencia_texto,
                        epoca_config, tipo_tesis_config,
                        tomo, pagina, mes, anio, fecha_extraccion, fecha_actualizacion,
                        descargado, ubicacion
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'No', '')
                ''', (
                    tesis_data['IUS'],
                    tesis_data['ID'],
                    tesis_data['Rubro'],
                    tesis_data['Clave_Tesis'],
                    tesis_data['Localizacion'],
                    tesis_data['Sala'],
                    tesis_data['Epoca'],
                    tesis_data['Instancia'],
                    tesis_data['Fuente'],
                    tesis_data['Tipo_Tesis'],
                    tesis_data['Tipo_Jurisprudencia'],
                    tesis_data['Tipo_Jurisprudencia_Texto'],
                    tesis_data['Epoca_Config'],
                    tesis_data['Tipo_Tesis_Config'],
                    tesis_data.get('Tomo', ''),
                    tesis_data.get('Pagina', ''),
                    tesis_data.get('Mes', ''),
                    tesis_data.get('Anio', ''),
                    tesis_data['Fecha_Extraccion'],
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
                return True
        except sqlite3.IntegrityError as e:
            logging.error(f"Error de integridad al insertar tesis {tesis_data['IUS']}: {e}")
            return False

    def actualizar_tesis_detalles(self, tesis_data: Dict) -> bool:
        try:
            self.cursor.execute('''
                UPDATE tesis SET
                    precedentes = COALESCE(?, precedentes),
                    ejecutorias = COALESCE(?, ejecutorias),
                    votos = COALESCE(?, votos),
                    volumen = COALESCE(?, volumen),
                    fecha_actualizacion = ?
                WHERE ius = ?
            ''', (
                tesis_data.get('Precedentes'),
                tesis_data.get('Ejecutorias'),
                tesis_data.get('Votos'),
                tesis_data.get('Volumen'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                tesis_data['IUS']
            ))
            if 'Materias' in tesis_data and tesis_data['Materias']:
                self._asignar_materias_a_tesis(tesis_data['IUS'], tesis_data['Materias'])
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error al actualizar detalles de tesis {tesis_data.get('IUS', 'DESCONOCIDO')}: {e}")
            return False

    def _build_fts_query(self, texto: str) -> str:
        if not texto or not texto.strip():
            return ""
        partes = re.findall(r'"[^"]+"|\S+', texto.strip())
        tokens = []
        for parte in partes:
            if parte.startswith('"') and parte.endswith('"'):
                escapado = re.sub(r'([+\-&|!(){}[\]^"~*?:\\])', r'\\\1', parte)
                tokens.append(escapado)
            else:
                escapado = re.sub(r'([+\-&|!(){}[\]^"~*?:\\])', r'\\\1', parte)
                tokens.append(escapado + "*")
        return ' '.join(tokens)

    def contar_tesis_filtradas(self, materia: str = None, epoca: str = None, texto: str = "",
                               ultimo_ius: str = None, ultima_fecha: str = None) -> int:
        query = "SELECT COUNT(*) FROM tesis t"
        params = []
        condiciones = []

        if epoca and epoca != "Todas":
            condiciones.append("t.epoca_config = ?")
            params.append(epoca)

        if materia and materia != "Todas":
            condiciones.append(
                "EXISTS (SELECT 1 FROM tesis_materia tm JOIN materia m ON tm.materia_id = m.id "
                "WHERE tm.tesis_ius = t.ius AND m.nombre = ?)"
            )
            params.append(materia)

        if texto and texto.strip():
            fts_query = self._build_fts_query(texto)
            query = "SELECT COUNT(*) FROM tesis t INNER JOIN tesis_fts ON t.rowid = tesis_fts.rowid"
            condiciones = []
            params = []
            if epoca and epoca != "Todas":
                condiciones.append("t.epoca_config = ?")
                params.append(epoca)
            if materia and materia != "Todas":
                condiciones.append(
                    "EXISTS (SELECT 1 FROM tesis_materia tm JOIN materia m ON tm.materia_id = m.id "
                    "WHERE tm.tesis_ius = t.ius AND m.nombre = ?)"
                )
                params.append(materia)
            condiciones.append("tesis_fts MATCH ?")
            params.append(fts_query)

        if ultimo_ius and ultima_fecha:
            condiciones.append("(t.fecha_actualizacion, t.ius) < (?, ?)")
            params.extend([ultima_fecha, ultimo_ius])

        if condiciones:
            query += " WHERE " + " AND ".join(condiciones)

        self.cursor.execute(query, params)
        return self.cursor.fetchone()[0]

    def obtener_tesis_paginadas_keyset(self, materia: str = None, epoca: str = None,
                                       texto: str = "", limite: int = 50,
                                       ultimo_ius: str = None, ultima_fecha: str = None) -> List[Dict]:
        query = '''
            SELECT t.ius, t.epoca_config, t.rubro, t.clave_tesis, t.descargado,
                   t.fecha_actualizacion,
                   (SELECT GROUP_CONCAT(m.nombre, ', ') 
                    FROM tesis_materia tm 
                    JOIN materia m ON tm.materia_id = m.id 
                    WHERE tm.tesis_ius = t.ius) as materias
            FROM tesis t
        '''
        params = []
        condiciones = []

        if epoca and epoca != "Todas":
            condiciones.append("t.epoca_config = ?")
            params.append(epoca)

        if materia and materia != "Todas":
            condiciones.append(
                "EXISTS (SELECT 1 FROM tesis_materia tm JOIN materia m ON tm.materia_id = m.id "
                "WHERE tm.tesis_ius = t.ius AND m.nombre = ?)"
            )
            params.append(materia)

        if texto and texto.strip():
            fts_query = self._build_fts_query(texto)
            query += " INNER JOIN tesis_fts ON t.rowid = tesis_fts.rowid"
            condiciones.append("tesis_fts MATCH ?")
            params.append(fts_query)

        if ultimo_ius and ultima_fecha:
            condiciones.append("(t.fecha_actualizacion, t.ius) < (?, ?)")
            params.extend([ultima_fecha, ultimo_ius])

        if condiciones:
            query += " WHERE " + " AND ".join(condiciones)

        query += " ORDER BY t.fecha_actualizacion DESC, t.ius DESC LIMIT ?"
        params.append(limite)

        self.cursor.execute(query, params)
        return [dict(row) for row in self.cursor.fetchall()]

    def obtener_tesis_por_ius(self, ius: str) -> Dict:
        query = '''
            SELECT t.ius, t.epoca_config, t.rubro, t.clave_tesis, t.descargado, t.ubicacion,
                   (SELECT GROUP_CONCAT(m.nombre, ', ') 
                    FROM tesis_materia tm 
                    JOIN materia m ON tm.materia_id = m.id 
                    WHERE tm.tesis_ius = t.ius) as materias
            FROM tesis t
            WHERE t.ius = ?
        '''
        self.cursor.execute(query, (ius,))
        row = self.cursor.fetchone()
        return dict(row) if row else {}

    def obtener_tesis_por_descargar(self, limite: int = None, incluir_fallidas: bool = False) -> List[Dict]:
        if incluir_fallidas:
            query = '''
                SELECT ius, epoca_config, rubro, clave_tesis, epoca 
                FROM tesis 
                WHERE descargado = 'No' OR descargado IS NULL
                ORDER BY ius
            '''
        else:
            query = '''
                SELECT ius, epoca_config, rubro, clave_tesis, epoca 
                FROM tesis 
                WHERE descargado = 'No' 
                ORDER BY ius
            '''
        if limite:
            query += " LIMIT ?"
            self.cursor.execute(query, (limite,))
        else:
            self.cursor.execute(query)
        return [dict(t) for t in self.cursor.fetchall()]

    def marcar_como_descargado(self, ius: str, ubicacion: str) -> bool:
        try:
            self.cursor.execute('''
                UPDATE tesis 
                SET descargado = 'Sí', ubicacion = ?, fecha_actualizacion = ?
                WHERE ius = ?
            ''', (ubicacion, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ius))
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error al marcar tesis {ius} como descargada: {e}")
            return False

    def verificar_estado_descarga(self, ius: str) -> Tuple[bool, str]:
        self.cursor.execute('''
            SELECT descargado, ubicacion FROM tesis WHERE ius = ?
        ''', (ius,))
        row = self.cursor.fetchone()
        if row:
            return row[0] == 'Sí', row[1] or ''
        return False, ''

    def exportar_a_csv(self, output_file: str = None) -> str:
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"tesis_export_{timestamp}.csv"
        query = '''
            SELECT t.*, 
                   (SELECT GROUP_CONCAT(m.nombre, ', ') 
                    FROM tesis_materia tm 
                    JOIN materia m ON tm.materia_id = m.id 
                    WHERE tm.tesis_ius = t.ius) as materias
            FROM tesis t
        '''
        df = pd.read_sql_query(query, self.conn)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        return output_file

    def exportar_resumenes(self, output_file: str = None) -> str:
        if not output_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"resumenes_tesis_{timestamp}.xlsx"
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            tablas = ['resumen_epoca', 'resumen_tipo_tesis', 'resumen_sala',
                     'resumen_tipo_jurisprudencia', 'resumen_materia']
            for tabla in tablas:
                df = pd.read_sql_query(f"SELECT * FROM {tabla}", self.conn)
                df.to_excel(writer, sheet_name=tabla, index=False)
        return output_file

    def registrar_extraccion(self, epoca: str, tipo_tesis: str, pagina: int,
                             total_tesis: int = 0, estado: str = 'completada'):
        hash_config = hashlib.md5(f"{epoca}_{tipo_tesis}_{pagina}".encode()).hexdigest()
        try:
            self.cursor.execute('''
                INSERT OR REPLACE INTO control_extracciones 
                (epoca, tipo_tesis, pagina, total_tesis, fecha_inicio, fecha_fin, estado, hash_config)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                epoca, tipo_tesis, pagina, total_tesis,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                estado, hash_config
            ))
            self.conn.commit()
        except Exception as e:
            logging.error(f"Error al registrar extracción: {e}")

    def pagina_procesada(self, epoca: str, tipo_tesis: str, pagina: int) -> bool:
        self.cursor.execute('''
            SELECT 1 FROM control_extracciones 
            WHERE epoca = ? AND tipo_tesis = ? AND pagina = ? AND estado = 'completada'
        ''', (epoca, tipo_tesis, pagina))
        return self.cursor.fetchone() is not None

    def limpiar_control_extracciones(self, epoca: str = None, tipo_tesis: str = None):
        try:
            if epoca and tipo_tesis:
                self.cursor.execute('DELETE FROM control_extracciones WHERE epoca = ? AND tipo_tesis = ?',
                                    (epoca, tipo_tesis))
            elif epoca:
                self.cursor.execute('DELETE FROM control_extracciones WHERE epoca = ?', (epoca,))
            else:
                self.cursor.execute('DELETE FROM control_extracciones')
            self.conn.commit()
            return True
        except Exception as e:
            logging.error(f"Error al limpiar control de extracciones: {e}")
            return False

    def actualizar_resumenes(self):
        temp_conn = None
        try:
            temp_conn = sqlite3.connect(self.db_path, timeout=10)
            temp_conn.row_factory = sqlite3.Row
            temp_conn.execute("PRAGMA journal_mode=WAL")
            temp_conn.execute("PRAGMA cache_size = 0")
            temp_conn.execute("BEGIN IMMEDIATE")
            temp_cursor = temp_conn.cursor()
            fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            temp_cursor.execute("DELETE FROM resumen_epoca")
            temp_cursor.execute('''
                INSERT INTO resumen_epoca (epoca, cantidad, fecha_actualizacion)
                SELECT epoca_config, COUNT(*), ? FROM tesis GROUP BY epoca_config
            ''', (fecha_actual,))
            temp_cursor.execute("DELETE FROM resumen_tipo_tesis")
            temp_cursor.execute('''
                INSERT INTO resumen_tipo_tesis (tipo_tesis, cantidad, fecha_actualizacion)
                SELECT tipo_tesis_config, COUNT(*), ? FROM tesis GROUP BY tipo_tesis_config
            ''', (fecha_actual,))
            temp_cursor.execute("DELETE FROM resumen_sala")
            temp_cursor.execute('''
                INSERT INTO resumen_sala (sala, cantidad, fecha_actualizacion)
                SELECT sala, COUNT(*), ? FROM tesis WHERE sala IS NOT NULL AND sala != '' GROUP BY sala
            ''', (fecha_actual,))
            temp_cursor.execute("DELETE FROM resumen_tipo_jurisprudencia")
            temp_cursor.execute('''
                INSERT INTO resumen_tipo_jurisprudencia (tipo_jurisprudencia, cantidad, fecha_actualizacion)
                SELECT tipo_jurisprudencia_texto, COUNT(*), ? FROM tesis WHERE tipo_jurisprudencia_texto IS NOT NULL GROUP BY tipo_jurisprudencia_texto
            ''', (fecha_actual,))
            temp_cursor.execute("DELETE FROM resumen_materia")
            temp_cursor.execute('''
                INSERT INTO resumen_materia (materia, cantidad, fecha_actualizacion)
                SELECT m.nombre, COUNT(DISTINCT tm.tesis_ius), ?
                FROM materia m
                JOIN tesis_materia tm ON m.id = tm.materia_id
                GROUP BY m.nombre
            ''', (fecha_actual,))
            temp_conn.commit()
        except Exception as e:
            logging.error(f"Error en actualizar_resumenes: {e}")
            if temp_conn:
                temp_conn.rollback()
        finally:
            if temp_conn:
                temp_conn.close()

    def obtener_epocas_unicas(self) -> List[str]:
        self.cursor.execute("""
            SELECT DISTINCT epoca_config 
            FROM tesis 
            WHERE epoca_config IS NOT NULL AND epoca_config != ''
            ORDER BY epoca_config
        """)
        epocas = [row[0] for row in self.cursor.fetchall()]
        return ["Todas"] + epocas

    def obtener_materias_unicas(self) -> List[str]:
        self.cursor.execute("""
            SELECT nombre 
            FROM materia 
            ORDER BY nombre
        """)
        materias = [row[0] for row in self.cursor.fetchall()]
        return ["Todas"] + materias

    def obtener_estadisticas(self) -> Dict:
        stats = {}
        self.cursor.execute("SELECT COUNT(*) FROM tesis")
        stats['total_tesis'] = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT COUNT(*) FROM tesis WHERE descargado = 'Sí'")
        stats['tesis_descargadas'] = self.cursor.fetchone()[0]
        self.cursor.execute("""
            SELECT epoca_config, COUNT(*) 
            FROM tesis 
            WHERE epoca_config IS NOT NULL 
            GROUP BY epoca_config 
            ORDER BY epoca_config
        """)
        stats['por_epoca'] = dict(self.cursor.fetchall())
        self.cursor.execute("""
            SELECT tipo_tesis_config, COUNT(*) 
            FROM tesis 
            GROUP BY tipo_tesis_config
        """)
        stats['por_tipo_tesis'] = dict(self.cursor.fetchall())
        self.cursor.execute("""
            SELECT m.nombre, COUNT(DISTINCT tm.tesis_ius) as cantidad
            FROM materia m
            JOIN tesis_materia tm ON m.id = tm.materia_id
            GROUP BY m.nombre
            ORDER BY cantidad DESC
            LIMIT 10
        """)
        stats['materias_comunes'] = dict(self.cursor.fetchall())
        self.cursor.execute("SELECT MAX(fecha_actualizacion) FROM tesis")
        stats['ultima_actualizacion'] = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT COUNT(*) FROM control_extracciones WHERE estado = 'completada'")
        stats['paginas_procesadas'] = self.cursor.fetchone()[0]
        return stats

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None


class SCJNTesisExtractor:
    def __init__(self, db_path: str):
        self.base_url = "https://sjf2.scjn.gob.mx/services/sjftesismicroservice/api/public/tesis"
        self.detalle_url = "https://sjf2.scjn.gob.mx/services/sjftesismicroservice/api/public/tesis"
        self.headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.db_path = db_path
        self.db = None
        self.configuraciones_epocas = {
            "9na_epoca": {
                "idEpoca": ["5"],
                "numInstancia": ["6", "1", "2", "7"],
                "lbSearch": ["9a. Época - Todas las Instancias"]
            },
            "10ma_epoca": {
                "idEpoca": ["100"],
                "numInstancia": ["6", "1", "2", "50", "7"],
                "lbSearch": ["10a. Época - Todas las Instancias"]
            },
            "11va_epoca": {
                "idEpoca": ["200"],
                "numInstancia": ["6", "1", "2", "60", "50", "7"],
                "lbSearch": ["11a. Época - Todas las Instancias"]
            },
            "12va_epoca": {
                "idEpoca": ["210"],
                "numInstancia": ["6", "0", "60", "7", "70", "80"],
                "lbSearch": ["12a. Época - Todas las Instancias"]
            }
        }
        self.tipos_tesis = {
            "jurisprudencia": ["1"],
            "aislada": ["0"]
        }
        self.init_db()

    def init_db(self):
        if self.db is None:
            self.db = SCJNTesisDatabase(self.db_path)

    def construir_payload(self, epoca: str, tipo_tesis: str):
        config = self.configuraciones_epocas[epoca]
        payload = {
            "classifiers": [
                {"name": "idEpoca", "value": config["idEpoca"], "allSelected": False, "visible": False, "isMatrix": False},
                {"name": "numInstancia", "value": config["numInstancia"], "allSelected": False, "visible": False, "isMatrix": False},
                {"name": "idTipoTesis", "value": self.tipos_tesis[tipo_tesis], "allSelected": False, "visible": False, "isMatrix": False},
                {"name": "tipoDocumento", "value": ["1"], "allSelected": False, "visible": False, "isMatrix": False}
            ],
            "searchTerms": [],
            "bFacet": True,
            "ius": [],
            "idApp": "SJFAPP2020",
            "lbSearch": config["lbSearch"],
            "filterExpression": ""
        }
        return payload

    def procesar_tesis(self, tesis_data: Dict, epoca: str, tipo_tesis: str) -> Dict:
        detalles = self.extraer_detalles_localizacion(tesis_data.get('localizacion', ''))
        tesis_procesada = {
            'IUS': tesis_data.get('ius'),
            'ID': tesis_data.get('id'),
            'Rubro': self.limpiar_html(tesis_data.get('rubro', '')).strip(),
            'Clave_Tesis': tesis_data.get('claveTesis', '').strip(),
            'Localizacion': tesis_data.get('localizacion', '').strip(),
            'Sala': tesis_data.get('sala', '').strip(),
            'Epoca': tesis_data.get('epocaAbr', '').strip(),
            'Instancia': tesis_data.get('instanciaAbr', '').strip(),
            'Fuente': tesis_data.get('fuente', '').strip(),
            'Tipo_Tesis': tesis_data.get('tipoTesis'),
            'Tipo_Jurisprudencia': tesis_data.get('tipoJurisprudencia'),
            'Tipo_Jurisprudencia_Texto': self.mapear_tipo_jurisprudencia(tesis_data.get('tipoJurisprudencia')),
            'Epoca_Config': epoca.replace('_', ' ').title(),
            'Tipo_Tesis_Config': tipo_tesis.title(),
            'Tomo': detalles.get('Tomo', ''),
            'Pagina': detalles.get('Pagina', ''),
            'Mes': detalles.get('Mes', ''),
            'Anio': detalles.get('Anio', ''),
            'Fecha_Extraccion': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Precedentes': '',
            'Materias': '',
            'Ejecutorias': '',
            'Votos': '',
            'Volumen': ''
        }
        return tesis_procesada

    def obtener_detalles_tesis(self, ius: str) -> Optional[Dict]:
        url_sin_semanal = f"{self.detalle_url}/{ius}?hostName=https://sjf2.scjn.gob.mx"
        try:
            response = requests.get(url_sin_semanal, headers=self.headers, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                url_con_semanal = f"{self.detalle_url}/{ius}?isSemanal=true&hostName=https://sjf2.scjn.gob.mx"
                response2 = requests.get(url_con_semanal, headers=self.headers, timeout=30)
                if response2.status_code == 200:
                    return response2.json()
            return None
        except Exception as e:
            logging.debug(f"Error al obtener detalles para IUS {ius}: {e}")
            return None

    def procesar_detalles_tesis(self, detalles: Dict) -> Dict:
        if not detalles:
            return {
                'Precedentes': '', 'Materias': '', 'Ejecutorias': '',
                'Votos': '', 'Volumen': '', 'Tomo': '', 'Pagina': ''
            }
        materias = detalles.get('materias', '')
        if isinstance(materias, list):
            materias = ', '.join(materias)
        elif materias is None:
            materias = ''
        ejecutorias = detalles.get('ejecutorias', [])
        ejecutorias_str = ', '.join(str(e) for e in ejecutorias) if ejecutorias else ''
        votos = detalles.get('votos', [])
        votos_str = ', '.join(str(v) for v in votos) if votos else ''
        volumen = detalles.get('volumen', '')
        tomo = detalles.get('tomo', '')
        pagina = detalles.get('pagina', '')
        if not tomo and volumen and 'Libro' in volumen:
            libro_match = re.search(r'Libro\s+(\d+)', volumen)
            if libro_match:
                tomo = libro_match.group(1)
        return {
            'IUS': str(detalles.get('ius', '')),
            'Precedentes': self.limpiar_html(detalles.get('precedentes', '')),
            'Materias': materias,
            'Ejecutorias': ejecutorias_str,
            'Votos': votos_str,
            'Volumen': volumen,
            'Tomo': tomo if tomo else '',
            'Pagina': str(pagina) if pagina else ''
        }

    def completar_con_datos_principales(self, tesis_procesada: Dict, tesis_original: Dict):
        materias = tesis_original.get('materias', '')
        if isinstance(materias, list):
            materias = ', '.join(materias)
        tesis_procesada['Materias'] = materias if materias else ''
        precedentes = tesis_original.get('precedentes', '')
        tesis_procesada['Precedentes'] = self.limpiar_html(precedentes) if precedentes else ''
        ejecutorias = tesis_original.get('ejecutorias', [])
        tesis_procesada['Ejecutorias'] = ', '.join(str(e) for e in ejecutorias) if ejecutorias else ''
        votos = tesis_original.get('votos', [])
        tesis_procesada['Votos'] = ', '.join(str(v) for v in votos) if votos else ''

    def extraer_detalles_localizacion(self, localizacion: str) -> Dict:
        detalles = {'Tomo': '', 'Pagina': '', 'Mes': '', 'Anio': ''}
        if not localizacion:
            return detalles
        try:
            tomo_match = re.search(r'Tomo\s+([^,;]+)', localizacion, re.IGNORECASE)
            if tomo_match:
                detalles['Tomo'] = tomo_match.group(1).strip()
            pagina_match = re.search(r'[Pp]ág\.?\s*(\d+)', localizacion)
            if pagina_match:
                detalles['Pagina'] = pagina_match.group(1).strip()
            fecha_match = re.search(r'([A-Za-z]+)\s+de\s+(\d{4})', localizacion)
            if fecha_match:
                detalles['Mes'] = fecha_match.group(1).strip()
                detalles['Anio'] = fecha_match.group(2).strip()
            else:
                fecha_match2 = re.search(r'(\d{4})', localizacion)
                if fecha_match2:
                    detalles['Anio'] = fecha_match2.group(1).strip()
        except Exception:
            pass
        return detalles

    def limpiar_html(self, texto: str) -> str:
        if not texto:
            return texto
        texto = re.sub(r'<br\s*/?>', '\n', texto)
        texto = re.sub(r'<p>', '', texto)
        texto = re.sub(r'</p>', '\n\n', texto)
        texto = re.sub(r'<[^>]+>', '', texto)
        texto = html.unescape(texto)
        texto = re.sub(r'\n\s*\n', '\n\n', texto)
        return texto.strip()

    def mapear_tipo_jurisprudencia(self, tipo: int) -> str:
        mapeo = {
            1: "Por reiteración",
            2: "Por contradicción",
            3: "Por sustitución",
            4: "Por acción de inconstitucionalidad",
            5: "Por controversia constitucional",
            6: "Aislada"
        }
        return mapeo.get(tipo, f"Desconocido ({tipo})")

    def obtener_pagina(self, epoca: str, tipo_tesis: str, pagina: int, size: int = 50) -> Optional[Dict]:
        try:
            payload = self.construir_payload(epoca, tipo_tesis)
            params = {'page': pagina, 'size': size}
            response = requests.post(self.base_url, headers=self.headers, params=params, json=payload, timeout=30)
            if response.status_code == 200:
                datos = response.json()
                if datos.get('documents'):
                    return datos
            return None
        except Exception as e:
            logging.error(f"Error al obtener página {pagina+1}: {e}")
            return None

    def procesar_epoca_tipo(self, epoca: str, tipo_tesis: str, size: int = 50, max_paginas: int = 1000,
                           combinacion_actual: int = 0, total_combinaciones: int = 0,
                           callback_progreso=None) -> Dict:
        if self.db is None:
            self.init_db()
        estadisticas = {
            'total_paginas': 0, 'tesis_nuevas': 0, 'tesis_existentes': 0,
            'detalles_actualizados': 0, 'detalles_fallidos': 0,
            'paginas_procesadas': 0, 'paginas_omitidas': 0, 'error': False
        }
        pagina = 0
        contador_commit = 0
        try:
            while True:
                global stop_extraction
                if stop_extraction:
                    estadisticas['error'] = True
                    break
                if max_paginas and pagina >= max_paginas:
                    break
                if self.db.pagina_procesada(epoca, tipo_tesis, pagina):
                    pagina += 1
                    estadisticas['paginas_omitidas'] += 1
                    continue
                if callback_progreso:
                    resultado = callback_progreso(pagina, max_paginas,
                                                f"{epoca.replace('_', ' ')} - {tipo_tesis} - Página {pagina+1} - ({combinacion_actual}/{total_combinaciones})")
                    if resultado is None or resultado is False:
                        break
                datos = self.obtener_pagina(epoca, tipo_tesis, pagina, size)
                if not datos:
                    break
                documentos = datos.get('documents', [])
                if not documentos:
                    break
                for tesis in documentos:
                    try:
                        if stop_extraction:
                            break
                        tesis_procesada = self.procesar_tesis(tesis, epoca, tipo_tesis)
                        if self.db.insert_tesis(tesis_procesada):
                            estadisticas['tesis_nuevas'] += 1
                        else:
                            estadisticas['tesis_existentes'] += 1
                        ius = tesis_procesada['IUS']
                        if ius:
                            detalles = self.obtener_detalles_tesis(ius)
                            if detalles:
                                detalles_procesados = self.procesar_detalles_tesis(detalles)
                                detalles_procesados['IUS'] = ius
                            else:
                                self.completar_con_datos_principales(tesis_procesada, tesis)
                                detalles_procesados = {
                                    'IUS': ius,
                                    'Precedentes': tesis_procesada.get('Precedentes', ''),
                                    'Materias': tesis_procesada.get('Materias', ''),
                                    'Ejecutorias': tesis_procesada.get('Ejecutorias', ''),
                                    'Votos': tesis_procesada.get('Votos', ''),
                                    'Volumen': tesis_procesada.get('Volumen', '')
                                }
                            if self.db.actualizar_tesis_detalles(detalles_procesados):
                                estadisticas['detalles_actualizados'] += 1
                            else:
                                estadisticas['detalles_fallidos'] += 1
                        time.sleep(0.3)
                    except Exception as e:
                        logging.error(f"Error procesando tesis individual: {e}")
                        continue
                if stop_extraction:
                    break
                self.db.registrar_extraccion(epoca, tipo_tesis, pagina, len(documentos), 'completada')
                estadisticas['paginas_procesadas'] += 1
                contador_commit += 1
                if contador_commit % 5 == 0:
                    self.db.conn.commit()
                total_paginas_api = datos.get('totalPage', 0)
                if total_paginas_api > 0:
                    estadisticas['total_paginas'] = total_paginas_api
                if pagina >= total_paginas_api - 1:
                    break
                pagina += 1
                time.sleep(0.5)
        except Exception as e:
            logging.error(f"Error durante el procesamiento: {e}")
            estadisticas['error'] = True
        if contador_commit % 5 != 0:
            self.db.conn.commit()
        return estadisticas

    def extraer_todas_epocas_y_tipos(self, size: int = 50, max_paginas_por_consulta: int = 1000,
                                      forzar_reextraccion: bool = False, callback_progreso=None):
        if self.db is None:
            self.init_db()
        if forzar_reextraccion:
            self.db.limpiar_control_extracciones()
        total_consultas = len(self.configuraciones_epocas) * len(self.tipos_tesis)
        consulta_actual = 0
        estadisticas_totales = {
            'total_tesis_nuevas': 0, 'total_tesis_existentes': 0,
            'total_detalles_actualizados': 0, 'total_detalles_fallidos': 0,
            'total_paginas_procesadas': 0, 'total_paginas_omitidas': 0,
            'consultas_completadas': 0, 'consultas_con_error': 0
        }
        for epoca_nombre in self.configuraciones_epocas.keys():
            for tipo_nombre in self.tipos_tesis.keys():
                consulta_actual += 1
                if callback_progreso:
                    callback_progreso(consulta_actual, total_consultas,
                                     f"Extrayendo {epoca_nombre.replace('_', ' ')} - {tipo_nombre}... ({consulta_actual}/{total_consultas})",
                                     consulta_actual / total_consultas)
                stats = self.procesar_epoca_tipo(
                    epoca=epoca_nombre, tipo_tesis=tipo_nombre, size=size,
                    max_paginas=max_paginas_por_consulta,
                    combinacion_actual=consulta_actual, total_combinaciones=total_consultas,
                    callback_progreso=callback_progreso
                )
                estadisticas_totales['total_tesis_nuevas'] += stats['tesis_nuevas']
                estadisticas_totales['total_tesis_existentes'] += stats['tesis_existentes']
                estadisticas_totales['total_detalles_actualizados'] += stats['detalles_actualizados']
                estadisticas_totales['total_detalles_fallidos'] += stats['detalles_fallidos']
                estadisticas_totales['total_paginas_procesadas'] += stats['paginas_procesadas']
                estadisticas_totales['total_paginas_omitidas'] += stats['paginas_omitidas']
                if stats['error']:
                    estadisticas_totales['consultas_con_error'] += 1
                else:
                    estadisticas_totales['consultas_completadas'] += 1
                global stop_extraction
                if stop_extraction:
                    break
                time.sleep(1)
            if stop_extraction:
                break
        self.db.actualizar_resumenes()
        return estadisticas_totales

    def cerrar(self):
        if self.db:
            self.db.close()
            self.db = None


class DescargadorTesis:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.base_carpeta = "tesis_descargadas"

    def crear_estructura_carpetas(self):
        if not os.path.exists(self.base_carpeta):
            os.makedirs(self.base_carpeta)
        epocas = ["9na Epoca", "10ma Epoca", "11va Epoca", "12va Epoca"]
        for epoca in epocas:
            carpeta_epoca = os.path.join(self.base_carpeta, epoca)
            if not os.path.exists(carpeta_epoca):
                os.makedirs(carpeta_epoca)

    def obtener_carpeta_epoca(self, epoca_config: str) -> str:
        epocas_mapping = {
            "9na Epoca": "9na Epoca",
            "10ma Epoca": "10ma Epoca",
            "11va Epoca": "11va Epoca",
            "12va Epoca": "12va Epoca"
        }
        epoca_normalizada = epoca_config
        for key in epocas_mapping:
            if key in epoca_config:
                epoca_normalizada = epocas_mapping[key]
                break
        return os.path.join(self.base_carpeta, epoca_normalizada)

    def descargar_tesis(self, ius: str, epoca_config: str) -> Tuple[bool, str]:
        url_base = f"https://sjf2.scjn.gob.mx/services/sjftesismicroservice/api/public/tesis/reporte/{ius}"
        params = {
            "nameDocto": "Tesis",
            "hostName": "https://sjf2.scjn.gob.mx",
            "soloParrafos": "false",
            "appSource": "SJFAPP2020"
        }
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                'Referer': 'https://sjf2.scjn.gob.mx/'
            }
            response = requests.get(url_base, params=params, headers=headers, timeout=30)
            if response.status_code == 200:
                if not os.path.exists(self.base_carpeta):
                    self.crear_estructura_carpetas()
                carpeta_epoca = self.obtener_carpeta_epoca(epoca_config)
                nombre_archivo = f"tesis_{ius}.pdf"
                ruta_completa = os.path.join(carpeta_epoca, nombre_archivo)
                with open(ruta_completa, 'wb') as f:
                    f.write(response.content)
                return True, ruta_completa
            else:
                return False, ""
        except Exception as e:
            logging.error(f"Error al descargar tesis {ius}: {e}")
            return False, ""

    def descargar_tesis_individual(self, ius: str, epoca_config: str) -> Tuple[bool, str]:
        db = SCJNTesisDatabase(self.db_path)
        descargado, ubicacion_bd = db.verificar_estado_descarga(ius)
        carpeta_epoca = self.obtener_carpeta_epoca(epoca_config)
        ruta_esperada = os.path.join(carpeta_epoca, f"tesis_{ius}.pdf")
        if descargado and ubicacion_bd and os.path.exists(ubicacion_bd):
            db.close()
            return True, ubicacion_bd
        elif descargado and os.path.exists(ruta_esperada):
            db.marcar_como_descargado(ius, ruta_esperada)
            db.close()
            return True, ruta_esperada
        elif os.path.exists(ruta_esperada):
            db.marcar_como_descargado(ius, ruta_esperada)
            db.close()
            return True, ruta_esperada
        exito, ruta = self.descargar_tesis(ius, epoca_config)
        if exito:
            db.marcar_como_descargado(ius, ruta)
        db.close()
        return exito, ruta

    def descargar_todas_pendientes(self, limite: int = None, delay: float = 1.0, reintentos: int = 3,
                                   incluir_fallidas: bool = False, callback_progreso = None) -> Tuple[int, int, int, int]:
        db = SCJNTesisDatabase(self.db_path)
        tesis_pendientes = db.obtener_tesis_por_descargar(limite, incluir_fallidas)
        total = len(tesis_pendientes)
        exitos = 0
        fallos = 0
        omitidos = 0
        for i, tesis in enumerate(tesis_pendientes):
            global stop_download
            if stop_download:
                break
            ius = tesis['ius']
            epoca_config = tesis['epoca_config']
            if callback_progreso:
                callback_progreso(i, total, f"Descargando {ius}... ({i+1}/{total})")
            exito = False
            ruta = ""
            for intento in range(reintentos):
                try:
                    if stop_download:
                        break
                    exito, ruta = self.descargar_tesis_individual(ius, epoca_config)
                    if exito:
                        break
                    elif intento < reintentos - 1:
                        time.sleep(delay * 2)
                except Exception as e:
                    if intento < reintentos - 1:
                        time.sleep(delay)
            if exito:
                exitos += 1
            else:
                fallos += 1
            time.sleep(delay)
        db.close()
        return exitos, fallos, omitidos, total

    def cerrar(self):
        pass

def abrir_archivo_con_aplicacion_predeterminada(ruta_archivo: str) -> bool:
    try:
        if os.path.exists(ruta_archivo):
            sistema_operativo = platform.system()
            if sistema_operativo == "Windows":
                os.startfile(ruta_archivo)
            elif sistema_operativo == "Darwin":
                subprocess.call(('open', ruta_archivo))
            else:
                subprocess.call(('xdg-open', ruta_archivo))
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"Error al abrir archivo {ruta_archivo}: {e}")
        return False


class ListasManager:
    def __init__(self, data_dir: str):
        self.filename = "listas.json"
        self.filepath = os.path.join(data_dir, self.filename)
        self.data = self._load()
        if not os.path.exists(self.filepath):
            self._save({"lists": []})

    def _determine_filepath(self) -> str:
        if getattr(sys, 'frozen', False):
            project_folder = os.path.dirname(sys.executable)
        else:
            current_frame = inspect.currentframe()
            archivo_actual = inspect.getfile(current_frame.f_code)
            project_folder = os.path.dirname(os.path.abspath(archivo_actual))
        if os.access(project_folder, os.W_OK):
            return os.path.join(project_folder, self.filename)
        home = os.path.expanduser("~")
        if platform.system() == "Windows":
            folder = os.path.join(os.environ.get("APPDATA", os.path.join(home, "AppData", "Roaming")),
                                  "Extractor_Tesis_SCJN")
        elif platform.system() == "Darwin":
            folder = os.path.join(home, "Library", "Application Support", "Extractor_Tesis_SCJN")
        else:
            folder = os.path.join(home, ".local", "share", "Extractor_Tesis_SCJN")
        try:
            os.makedirs(folder, exist_ok=True)
        except Exception:
            folder = project_folder
        return os.path.join(folder, self.filename)

    def _check_writable(self):
        folder = os.path.dirname(self.filepath)
        if not os.access(folder, os.W_OK):
            temp_dir = tempfile.gettempdir()
            fallback = os.path.join(temp_dir, "Extractor_Tesis_SCJN", self.filename)
            os.makedirs(os.path.dirname(fallback), exist_ok=True)
            self.filepath = fallback

    def _load(self) -> Dict:
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if not isinstance(data, dict) or "lists" not in data:
                    data = {"lists": []}
            except Exception:
                data = {"lists": []}
                self._save(data)
        else:
            data = {"lists": []}
            self._save(data)
        return data

    def _save(self, data=None) -> bool:
        if data is None:
            data = self.data
        try:
            with open(self.filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logging.error(f"Error al guardar {self.filepath}: {e}")
            return False

    def create_list(self, name: str) -> Optional[str]:
        list_id = str(uuid.uuid4())
        new_list = {"id": list_id, "name": name, "theses": []}
        self.data["lists"].append(new_list)
        if self._save():
            return list_id
        self.data["lists"].pop()
        return None

    def rename_list(self, list_id: str, new_name: str) -> bool:
        for lst in self.data["lists"]:
            if lst["id"] == list_id:
                lst["name"] = new_name
                return self._save()
        return False

    def delete_list(self, list_id: str) -> bool:
        for i, lst in enumerate(self.data["lists"]):
            if lst["id"] == list_id:
                del self.data["lists"][i]
                return self._save()
        return False

    def add_thesis_to_list(self, list_id: str, ius: str) -> bool:
        for lst in self.data["lists"]:
            if lst["id"] == list_id:
                if ius not in lst["theses"]:
                    lst["theses"].append(ius)
                    return self._save()
                return True
        return False

    def remove_thesis_from_list(self, list_id: str, ius: str) -> bool:
        for lst in self.data["lists"]:
            if lst["id"] == list_id:
                if ius in lst["theses"]:
                    lst["theses"].remove(ius)
                    return self._save()
                return True
        return False

    def get_list(self, list_id: str) -> Optional[Dict]:
        for lst in self.data["lists"]:
            if lst["id"] == list_id:
                return lst
        return None

    def get_all_lists(self) -> List[Dict]:
        return sorted(self.data["lists"], key=lambda x: x["name"])


def main(page: ft.Page):

    global GLOBAL_DB, GLOBAL_DB_PATH
    data_dir = get_data_folder()
    db_path = ensure_db_in_data(data_dir)
    GLOBAL_DB_PATH = db_path
    db = SCJNTesisDatabase(db_path)
    GLOBAL_DB = db 
    listas_manager = ListasManager(data_dir)

    page.window.maximized = True
    page.title = "Sistema de Gestión de Tesis SCJN"
    page.theme_mode = ft.ThemeMode.LIGHT
    icono_path = get_icon_path()
    if icono_path:
        page.window.icon = icono_path

    page.theme = ft.Theme(
        scrollbar_theme=ft.ScrollbarTheme(
            thumb_color=ft.Colors.TEAL,
            track_color=ft.Colors.GREY_200,
            track_visibility=True,
            thickness=6,
            radius=4,
        )
    )


    is_processing_extraccion = False
    is_processing_descarga = False
    current_view = "tabla"
    current_materia_filter = "Todas"
    current_epoca_filter = "Todas"

    current_page_tesis = []
    last_ius = None
    last_fecha = None
    has_more = False

    search_debounce_timer = None
    DEBOUNCE_DELAY = 0.3

    bd_version = 0
    last_bd_version = -1
    last_filtros = {"materia": "Todas", "epoca": "Todas", "texto": ""}

    def on_search_change():
        nonlocal search_debounce_timer
        if search_debounce_timer:
            search_debounce_timer.cancel()
        async def debounce():
            await asyncio.sleep(DEBOUNCE_DELAY)
            buscar_tesis_con_filtros(reset_pagination=True)
        search_debounce_timer = asyncio.create_task(debounce())

    def on_filtro_change(e=None):
        nonlocal current_materia_filter, current_epoca_filter
        current_materia_filter = materia_dropdown.value or "Todas"
        current_epoca_filter = epoca_dropdown.value or "Todas"
        buscar_tesis_con_filtros(reset_pagination=True)

    def buscar_tesis_con_filtros(reset_pagination: bool = True):
        nonlocal last_ius, last_fecha, has_more, current_page_tesis
        nonlocal last_filtros, last_bd_version
        texto = search_field.value.strip()
        materia = materia_dropdown.value
        epoca = epoca_dropdown.value

        if reset_pagination:
            last_ius = None
            last_fecha = None
            current_page_tesis = []

        try:
            resultados = GLOBAL_DB.obtener_tesis_paginadas_keyset(
                materia=materia,
                epoca=epoca,
                texto=texto,
                limite=50,
                ultimo_ius=last_ius,
                ultima_fecha=last_fecha
            )
            if reset_pagination:
                current_page_tesis = resultados
                actualizar_tabla(current_page_tesis, append=False)
            else:
                current_page_tesis.extend(resultados)
                actualizar_tabla(resultados, append=True)

            if resultados:
                last_ius = resultados[-1]['ius']
                last_fecha = resultados[-1]['fecha_actualizacion']
                total_restantes = GLOBAL_DB.contar_tesis_filtradas(materia, epoca, texto, last_ius, last_fecha)
                has_more = total_restantes > 0
            else:
                last_ius = None
                last_fecha = None
                has_more = False

            total_encontradas = GLOBAL_DB.contar_tesis_filtradas(materia, epoca, texto)
            titulo = f"Tesis encontradas: {total_encontradas}"
            if has_more:
                titulo += " (cargadas primeras 50)"
            tabla_container.content.controls[0].content.value = titulo
            cargar_mas_btn.visible = has_more
            estado = f"Mostrando {len(current_page_tesis)} tesis"
            detalle = f"Materia: {materia} | Época: {epoca}"
            actualizar_estado(estado, detalle)

            last_filtros = {"materia": materia, "epoca": epoca, "texto": texto}
            last_bd_version = bd_version
            page.update()
        except Exception as e:
            logging.error(f"Error en búsqueda: {e}")
            actualizar_estado("Error en búsqueda", str(e))

    def cargar_mas_tesis():
        buscar_tesis_con_filtros(reset_pagination=False)

    def cargar_ultimas_tesis():
        materia_dropdown.value = "Todas"
        epoca_dropdown.value = "Todas"
        search_field.value = ""
        buscar_tesis_con_filtros(reset_pagination=True)
        tabla_container.content.controls[0].content.value = "Últimas Tesis Agregadas"
        page.update()

    def crear_fila_tesis(tesis, index=None, es_lista=False, list_id=None):
        ius = tesis.get('ius', '')
        rubro = tesis.get('rubro', '')
        rubro_tooltip = rubro if rubro else None
        if rubro and len(rubro) > 150:
            rubro = rubro[:147] + "..."

        materia = tesis.get('materias', '') or "(Sin materia)"
        if len(materia) > 50:
            materia = materia[:47] + "..."

        ius_button = ft.TextButton(
            content=ft.Text(ius, size=11, color=ft.Colors.BLUE_800, weight=ft.FontWeight.BOLD),
            tooltip=f"Abrir PDF de tesis {ius}",
            on_click=lambda e, ius=ius, epoca=tesis.get('epoca_config', ''), rubro=tesis.get('rubro', ''): on_ius_click(ius, epoca, rubro),
            style=ft.ButtonStyle(padding=ft.Padding(5, 3, 5, 3), overlay_color=ft.Colors.TRANSPARENT)
        )

        if es_lista:
            action_button = ft.IconButton(
                icon=ft.Icons.REMOVE_CIRCLE_OUTLINE,
                icon_size=18,
                icon_color=ft.Colors.RED,
                tooltip="Quitar de esta lista",
                on_click=lambda e, lid=list_id, ius=ius: (
                    listas_manager.remove_thesis_from_list(lid, ius),
                    actualizar_estado(f"Tesis {ius} eliminada de la lista", ""),
                    mostrar_lista_detalle(lid)
                )
            )
        else:
            action_button = ft.IconButton(
                icon=ft.Icons.ADD_CIRCLE_OUTLINE,
                icon_size=18,
                icon_color=ft.Colors.TEAL,
                tooltip="Agregar a lista",
                on_click=lambda e, ius=ius: mostrar_seleccion_lista(ius)
            )

        estado_container = ft.Container(
            content=ft.Text(tesis.get('descargado', 'No'), size=11, color=ft.Colors.WHITE, weight=ft.FontWeight.BOLD),
            bgcolor=ft.Colors.GREEN if tesis.get('descargado') == 'Sí' else ft.Colors.RED,
            padding=ft.Padding.symmetric(horizontal=5, vertical=2),
            border_radius=4,
            alignment=ft.Alignment.CENTER,
            width=50,
        )

        numero = ft.Text(str(index) if index is not None else "", size=11, color=ft.Colors.BLUE_GREY_700, text_align=ft.TextAlign.CENTER)

        fila = ft.Container(
            content=ft.Row([
                ft.Container(numero, width=40),
                ft.Container(ius_button, width=80),
                ft.Container(
                    ft.Text(rubro, size=12, text_align=ft.TextAlign.JUSTIFY, max_lines=3, overflow=ft.TextOverflow.ELLIPSIS),
                    tooltip=rubro_tooltip,
                    expand=True,
                ),
                ft.Container(
                    ft.Text(materia, size=11, max_lines=3, overflow=ft.TextOverflow.ELLIPSIS),
                    width=90,
                ),
                ft.Container(estado_container, width=70),
                ft.Container(action_button, width=50),
            ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER),
            padding=10,
            border=ft.Border.only(bottom=ft.BorderSide(1, ft.Colors.BLUE_GREY_100)),
        )
        return fila

    columna_tabla = ft.Column(scroll=ft.ScrollMode.ADAPTIVE, expand=True, spacing=0)

    def actualizar_tabla(tesis_list: List[Dict], append: bool = False):
        if not append:
            columna_tabla.controls.clear()
            start_index = 1
            for i, tesis in enumerate(tesis_list):
                columna_tabla.controls.append(
                    crear_fila_tesis(tesis, index=start_index + i, es_lista=False)
                )
        else:
            start_index = len(columna_tabla.controls) + 1
            for i, tesis in enumerate(tesis_list):
                columna_tabla.controls.append(
                    crear_fila_tesis(tesis, index=start_index + i, es_lista=False)
                )
        page.update()


    listas_manager = ListasManager(data_dir)
    current_list_id = None
    current_list_name = ""
    tesis_pendiente_seleccion = None
    crear_lista_origen = None



    def create_button(text, icon, on_click, color=ft.Colors.BLUE, width=140):
        return ft.FilledButton(
            content=ft.Row([
                ft.Icon(icon, size=18),
                ft.Text(text, size=12)
            ], alignment=ft.MainAxisAlignment.CENTER, spacing=8),
            height=36,
            width=width,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=8),
                bgcolor=color
            ),
            on_click=on_click
        )

    search_field = ft.TextField(
        label="Buscar tesis",
        hint_text="Busca ingresando el Registro, rubro o clave",
        expand=True,
        height=40,
        border_radius=10,
        bgcolor=ft.Colors.WHITE,
        border_color=ft.Colors.BLUE_GREY_200,
        visible=True,
        on_change=lambda e: on_search_change()
    )

    materia_dropdown = ft.DropdownM2(
        label="Materia",
        hint_text="Selecciona una materia",
        options=[ft.dropdownm2.Option("Todas")],
        value="Todas",
        width=200,
        height=40,
        border_radius=10,
        bgcolor=ft.Colors.WHITE,
        border_color=ft.Colors.BLUE_GREY_200,
        visible=True,
        on_change=on_filtro_change
    )

    epoca_dropdown = ft.DropdownM2(
        label="Época",
        hint_text="Selecciona una época",
        options=[ft.dropdownm2.Option("Todas")],
        value="Todas",
        width=200,
        height=40,
        border_radius=10,
        bgcolor=ft.Colors.WHITE,
        border_color=ft.Colors.BLUE_GREY_200,
        visible=True,
        on_change=on_filtro_change
    )

    exportar_btn = create_button("Exportar a CSV", ft.Icons.IMPORT_EXPORT, lambda e: exportar_datos(), ft.Colors.GREEN)
    estadisticas_btn = create_button("Estadísticas", ft.Icons.ANALYTICS, lambda e: mostrar_estadisticas(), ft.Colors.PURPLE)
    listas_btn = create_button("Listas", ft.Icons.LIST, lambda e: mostrar_listas(), ft.Colors.TEAL)
    volver_tabla_btn = create_button("Volver", ft.Icons.ARROW_BACK, lambda e: mostrar_tabla(), ft.Colors.PURPLE)
    volver_tabla_btn.visible = False
    volver_tabla_btn_green = create_button("Volver", ft.Icons.ARROW_BACK, lambda e: mostrar_tabla(), ft.Colors.TEAL)
    volver_tabla_btn_green.visible = False
    volver_listas_desde_detalle_btn = create_button("Volver a Listas", ft.Icons.ARROW_BACK, lambda e: mostrar_listas(), ft.Colors.TEAL)
    volver_listas_desde_detalle_btn.visible = False

    detener_extraccion_btn = create_button("Detener Extracción", ft.Icons.STOP, lambda e: detener_extraccion(), ft.Colors.RED, width=160)
    detener_extraccion_btn.visible = False
    detener_descarga_btn = create_button("Detener Descarga", ft.Icons.STOP, lambda e: detener_descarga(), ft.Colors.RED, width=160)
    detener_descarga_btn.visible = False

    status_text = ft.Text("Listo", size=12, color=ft.Colors.BLACK)
    detail_text = ft.Text("", size=10, color=ft.Colors.BLACK)
    message_area = ft.Column(controls=[status_text, detail_text], spacing=2, expand=True)

    cargar_mas_btn = ft.Button(
        "Cargar más tesis",
        icon=ft.Icons.ARROW_DOWNWARD,
        color=ft.Colors.WHITE,
        bgcolor=ft.Colors.GREY_500,
        on_click=lambda e: cargar_mas_tesis(),
        visible=False
    )

    tabla_container = ft.Container(
        content=ft.Column([
            ft.Container(
                content=ft.Text("Últimas Tesis Agregadas", size=16, weight=ft.FontWeight.BOLD, color=ft.Colors.BLUE_GREY_800),
                padding=ft.Padding(0, 0, 0, 10),
            ),
            ft.Container(
                content=ft.Column([
                    ft.Container(
                        content=ft.Row([
                            ft.Container(ft.Text("#", size=12, weight=ft.FontWeight.BOLD), width=40),
                            ft.Container(ft.Text("Registro", size=12, weight=ft.FontWeight.BOLD), width=80),
                            ft.Container(ft.Text("Rubro", size=12, weight=ft.FontWeight.BOLD), expand=True),
                            ft.Container(ft.Text("Materia", size=12, weight=ft.FontWeight.BOLD), width=90),
                            ft.Container(ft.Text("Descargado", size=12, weight=ft.FontWeight.BOLD), width=70),
                            ft.Container(ft.Text("", width=50)),
                        ], spacing=10, vertical_alignment=ft.CrossAxisAlignment.CENTER),
                        padding=ft.Padding(10, 5, 10, 5),
                        bgcolor=ft.Colors.BLUE_GREY_50,
                        border_radius=ft.BorderRadius.only(top_left=8, top_right=8),
                    ),
                    columna_tabla,
                    ft.Container(
                        content=cargar_mas_btn,
                        alignment=ft.Alignment.CENTER,
                        padding=ft.Padding(0, 10, 0, 0),
                    ),
                ], spacing=0, expand=True),
                expand=True,
                border_radius=12,
                bgcolor=ft.Colors.WHITE,
                shadow=ft.BoxShadow(spread_radius=0, blur_radius=10, color=ft.Colors.BLACK12),
                padding=0,
            )
        ], expand=True, spacing=0),
        expand=True
    )

    proceso_detail_text = ft.Text("", size=12, color=ft.Colors.BLACK, weight=ft.FontWeight.BOLD)
    proceso_status_text = ft.Text("", size=10, color=ft.Colors.BLACK)

    estadisticas_container = ft.Container(expand=True, visible=False)

    list_view_lista_detalle = ft.ListView(expand=True, spacing=0, padding=10, auto_scroll=False)

    listas_container = ft.Container(
        expand=True,
        visible=False,
        content=ft.Column([
            ft.Container(
                content=ft.Text("Mis Listas de Tesis", size=18, weight=ft.FontWeight.BOLD, color=ft.Colors.TEAL_800),
                padding=ft.Padding(0, 0, 0, 10),
                alignment=ft.Alignment.CENTER,
            ),
            ft.Container(
                content=ft.Row([], wrap=True, spacing=15, run_spacing=15),
                expand=True,
                padding=10,
            ),
            ft.Container(
                content=volver_tabla_btn,
                alignment=ft.Alignment.CENTER,
                padding=20
            )
        ], spacing=10, expand=True)
    )

    lista_detalle_container = ft.Container(
        expand=True,
        visible=False,
        content=ft.Column([
            ft.Container(
                content=ft.Text("", size=18, weight=ft.FontWeight.BOLD, color=ft.Colors.TEAL_800),
                padding=ft.Padding(0, 0, 0, 10),
                alignment=ft.Alignment.CENTER,
            ),
            ft.Container(
                content=list_view_lista_detalle,
                expand=True,
                border_radius=12,
                bgcolor=ft.Colors.WHITE,
                shadow=ft.BoxShadow(spread_radius=0, blur_radius=10, color=ft.Colors.BLACK12),
                padding=12,
            ),
            ft.Container(
                content=volver_listas_desde_detalle_btn,
                alignment=ft.Alignment.CENTER,
                padding=20
            )
        ], spacing=10, expand=True)
    )

    seleccionar_lista_container = ft.Container(
        expand=True,
        visible=False,
        content=ft.Column([], horizontal_alignment=ft.CrossAxisAlignment.CENTER, expand=True)
    )

    renombrar_lista_container = ft.Container(
        expand=True,
        visible=False,
        content=ft.Column([], horizontal_alignment=ft.CrossAxisAlignment.CENTER, expand=True)
    )

    eliminar_lista_container = ft.Container(
        expand=True,
        visible=False,
        content=ft.Column([], horizontal_alignment=ft.CrossAxisAlignment.CENTER, expand=True)
    )

    crear_lista_container = ft.Container(
        expand=True,
        visible=False,
        content=ft.Column([], horizontal_alignment=ft.CrossAxisAlignment.CENTER, expand=True)
    )

    extraer_todas_btn_estadisticas = create_button("Extraer Todas", ft.Icons.DOWNLOAD,
                                                   lambda e: iniciar_extraccion_completa(), ft.Colors.BLUE)
    descargar_pdfs_btn_estadisticas = create_button("Descargar PDFs", ft.Icons.PICTURE_AS_PDF,
                                                     lambda e: iniciar_descarga_pendientes(), ft.Colors.ORANGE)

    def actualizar_estado(mensaje: str, detalle: str = ""):
        status_text.value = mensaje
        detail_text.value = detalle
        page.update()

    def actualizar_progreso_estadisticas(mensaje: str, estado: str = ""):
        proceso_detail_text.value = mensaje
        proceso_status_text.value = estado
        page.update()

    def mostrar_tabla():
        nonlocal current_view, last_filtros, last_bd_version
        current_view = "tabla"

        filtros_actuales = {
            "materia": materia_dropdown.value or "Todas",
            "epoca": epoca_dropdown.value or "Todas",
            "texto": search_field.value.strip()
        }
        if filtros_actuales == last_filtros and bd_version == last_bd_version:
            header.visible = True
            tabla_container.visible = True
            estadisticas_container.visible = False
            listas_container.visible = False
            lista_detalle_container.visible = False
            seleccionar_lista_container.visible = False
            renombrar_lista_container.visible = False
            eliminar_lista_container.visible = False
            crear_lista_container.visible = False
            volver_tabla_btn.visible = False
            volver_tabla_btn_green.visible = False
            volver_listas_desde_detalle_btn.visible = False
            estadisticas_btn.visible = True
            listas_btn.visible = True
            search_field.visible = True
            materia_dropdown.visible = True
            epoca_dropdown.visible = True
            detener_extraccion_btn.visible = False
            detener_descarga_btn.visible = False
            if current_page_tesis:
                tabla_container.content.controls[0].content.value = "Últimas Tesis Agregadas" if filtros_actuales == {"materia":"Todas","epoca":"Todas","texto":""} else f"Tesis encontradas: {len(current_page_tesis)}"
            page.update()
            return


        header.visible = True
        tabla_container.visible = True
        estadisticas_container.visible = False
        listas_container.visible = False
        lista_detalle_container.visible = False
        seleccionar_lista_container.visible = False
        renombrar_lista_container.visible = False
        eliminar_lista_container.visible = False
        crear_lista_container.visible = False
        volver_tabla_btn.visible = False
        volver_tabla_btn_green.visible = False
        volver_listas_desde_detalle_btn.visible = False
        estadisticas_btn.visible = True
        listas_btn.visible = True
        search_field.visible = True
        materia_dropdown.visible = True
        epoca_dropdown.visible = True
        detener_extraccion_btn.visible = False
        detener_descarga_btn.visible = False


        if filtros_actuales == {"materia":"Todas","epoca":"Todas","texto":""}:
            cargar_ultimas_tesis()
        else:
            buscar_tesis_con_filtros(reset_pagination=True)
        page.update()

    def mostrar_estadisticas():
        nonlocal current_view
        current_view = "estadisticas"
        header.visible = True
        tabla_container.visible = False
        estadisticas_container.visible = True
        listas_container.visible = False
        lista_detalle_container.visible = False
        seleccionar_lista_container.visible = False
        renombrar_lista_container.visible = False
        eliminar_lista_container.visible = False
        crear_lista_container.visible = False
        volver_tabla_btn.visible = True
        volver_tabla_btn_green.visible = False
        volver_listas_desde_detalle_btn.visible = False
        estadisticas_btn.visible = False
        listas_btn.visible = False
        search_field.visible = False
        materia_dropdown.visible = False
        epoca_dropdown.visible = False
        detener_extraccion_btn.visible = is_processing_extraccion
        detener_descarga_btn.visible = is_processing_descarga

        stats = GLOBAL_DB.obtener_estadisticas()
        contenido = ft.Column(scroll=ft.ScrollMode.ADAPTIVE, spacing=15)

        if stats['total_tesis'] == 0:
            contenido.controls.append(
                ft.Container(
                    content=ft.Column([
                        ft.Icon(ft.Icons.INFO, size=50, color=ft.Colors.BLUE_GREY_400),
                        ft.Text("No hay tesis en la base de datos.", size=16, color=ft.Colors.BLUE_GREY_700),
                        ft.Text("Ejecute la Extracción.", size=14, color=ft.Colors.BLUE_GREY_600),
                    ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    padding=40,
                    alignment=ft.Alignment.CENTER
                )
            )
        else:
            cards = ft.Row(wrap=True, spacing=15, alignment=ft.MainAxisAlignment.CENTER, vertical_alignment=ft.CrossAxisAlignment.CENTER)
            metricas = [
                ("Total", str(stats['total_tesis']), ft.Colors.BLUE_700),
                ("Descargadas", str(stats.get('tesis_descargadas', 0)), ft.Colors.GREEN_700),
                ("Pendientes", str(stats['total_tesis'] - stats.get('tesis_descargadas', 0)), ft.Colors.ORANGE_700),
                ("Última Actualización", stats['ultima_actualizacion'][:10] if stats['ultima_actualizacion'] else "N/A", ft.Colors.PURPLE_700),
            ]
            for titulo, valor, color in metricas:
                cards.controls.append(
                    ft.Container(
                        content=ft.Column([
                            ft.Text(titulo, size=12, color=ft.Colors.BLUE_GREY_600, weight=ft.FontWeight.BOLD),
                            ft.Text(valor, size=20, weight=ft.FontWeight.BOLD, color=color)
                        ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                        padding=15,
                        width=300,
                        bgcolor=ft.Colors.WHITE,
                        border_radius=10,
                        shadow=ft.BoxShadow(spread_radius=0, blur_radius=5, color=ft.Colors.BLACK12)
                    )
                )
            contenido.controls.append(cards)

            stats_containers = ft.Row(spacing=15, alignment=ft.MainAxisAlignment.CENTER, vertical_alignment=ft.CrossAxisAlignment.START, expand=True)

            if stats.get('materias_comunes'):
                materias_list = ft.Column(spacing=5)
                for materia, cantidad in stats['materias_comunes'].items():
                    if materia and materia.strip():
                        porcentaje = (cantidad / stats['total_tesis'] * 100) if stats['total_tesis'] > 0 else 0
                        materias_list.controls.append(
                            ft.Row([
                                ft.Text(materia[:50] + "..." if len(materia) > 50 else materia, size=11, expand=True),
                                ft.Text(f"{cantidad} ({porcentaje:.1f}%)", size=11, weight=ft.FontWeight.BOLD)
                            ])
                        )
                if len(materias_list.controls) > 0:
                    materias_container = ft.Container(
                        content=ft.Column([
                            ft.Text("Materias Más Comunes", size=14, weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER),
                            ft.Container(height=10),
                            ft.Container(content=materias_list, expand=True)
                        ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                        padding=20,
                        bgcolor=ft.Colors.WHITE,
                        border_radius=10,
                        shadow=ft.BoxShadow(spread_radius=0, blur_radius=5, color=ft.Colors.BLACK12),
                        expand=True,
                    )
                    stats_containers.controls.append(materias_container)

            if stats.get('por_epoca'):
                epocas_list = ft.Column(spacing=5)
                for epoca, cantidad in stats['por_epoca'].items():
                    porcentaje = (cantidad / stats['total_tesis'] * 100) if stats['total_tesis'] > 0 else 0
                    epocas_list.controls.append(
                        ft.Row([
                            ft.Text(epoca or "Sin época", size=11, expand=True),
                            ft.Text(f"{cantidad} ({porcentaje:.1f}%)", size=11, weight=ft.FontWeight.BOLD)
                        ])
                    )
                epocas_container = ft.Container(
                    content=ft.Column([
                        ft.Text("Distribución por Época", size=14, weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER),
                        ft.Container(height=10),
                        ft.Container(content=epocas_list, expand=True)
                    ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    padding=20,
                    bgcolor=ft.Colors.WHITE,
                    border_radius=10,
                    shadow=ft.BoxShadow(spread_radius=0, blur_radius=5, color=ft.Colors.BLACK12),
                    expand=True,
                )
                stats_containers.controls.append(epocas_container)

            if stats.get('por_tipo_tesis'):
                tipos_list = ft.Column(spacing=5)
                for tipo, cantidad in stats['por_tipo_tesis'].items():
                    porcentaje = (cantidad / stats['total_tesis'] * 100) if stats['total_tesis'] > 0 else 0
                    tipos_list.controls.append(
                        ft.Row([
                            ft.Text(tipo or "Sin tipo", size=11, expand=True),
                            ft.Text(f"{cantidad} ({porcentaje:.1f}%)", size=11, weight=ft.FontWeight.BOLD)
                        ])
                    )
                tipos_container = ft.Container(
                    content=ft.Column([
                        ft.Text("Distribución por Tipo de Tesis", size=14, weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER),
                        ft.Container(height=10),
                        ft.Container(content=tipos_list, expand=True)
                    ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    padding=20,
                    bgcolor=ft.Colors.WHITE,
                    border_radius=10,
                    shadow=ft.BoxShadow(spread_radius=0, blur_radius=5, color=ft.Colors.BLACK12),
                    expand=True,
                )
                stats_containers.controls.append(tipos_container)

            if stats_containers.controls:
                contenido.controls.append(
                    ft.Container(content=stats_containers, margin=ft.Margin(0, 10, 0, 0))
                )

        progreso_container = ft.Container(
            content=ft.Column([
                proceso_detail_text,
                proceso_status_text,
            ], spacing=5, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
            padding=10,
            bgcolor=ft.Colors.TRANSPARENT,
            border_radius=10,
            expand=True,
            alignment=ft.Alignment.CENTER
        )
        contenido.controls.append(progreso_container)

        botones_container = ft.Container(
            content=ft.Column([
                ft.Row([
                    extraer_todas_btn_estadisticas,
                    descargar_pdfs_btn_estadisticas,
                    exportar_btn,
                    detener_extraccion_btn,
                    detener_descarga_btn,
                ], alignment=ft.MainAxisAlignment.CENTER, spacing=15),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
            padding=20,
            margin=ft.Margin(0, 10, 0, 0)
        )
        contenido.controls.append(botones_container)
        
        text_containter = ft.Container(content=ft.Text("Creado y desarrollado por Enrique Hernandez Roque", size=12, color="black"))
        contenido.controls.append(text_containter)

        estadisticas_container.content = ft.Column([
            ft.Text("Estadísticas de la Base de Datos", size=18, weight=ft.FontWeight.BOLD, color=ft.Colors.BLUE_GREY_800, text_align=ft.TextAlign.CENTER),
            contenido
        ], spacing=20, scroll=ft.ScrollMode.ADAPTIVE, horizontal_alignment=ft.CrossAxisAlignment.CENTER)

        actualizar_estado("Estadísticas cargadas")
        page.update()


    def mostrar_listas():
        nonlocal current_view
        current_view = "listas"
        header.visible = True
        tabla_container.visible = False
        estadisticas_container.visible = False
        listas_container.visible = True
        lista_detalle_container.visible = False
        seleccionar_lista_container.visible = False
        renombrar_lista_container.visible = False
        eliminar_lista_container.visible = False
        crear_lista_container.visible = False
        volver_tabla_btn.visible = False
        volver_tabla_btn_green.visible = True
        volver_listas_desde_detalle_btn.visible = False
        estadisticas_btn.visible = False
        listas_btn.visible = False
        search_field.visible = False
        materia_dropdown.visible = False
        epoca_dropdown.visible = False
        detener_extraccion_btn.visible = False
        detener_descarga_btn.visible = False

        grid = listas_container.content.controls[1].content
        grid.controls.clear()

        crear_tarjeta = ft.Container(
            content=ft.Column([
                ft.Icon(ft.Icons.ADD_CIRCLE_OUTLINE, size=50, color=ft.Colors.TEAL),
                ft.Text("Crear nueva lista", size=14, weight=ft.FontWeight.BOLD, color=ft.Colors.TEAL),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.CENTER, spacing=10),
            width=200,
            height=180,
            bgcolor=ft.Colors.WHITE,
            border_radius=12,
            shadow=ft.BoxShadow(spread_radius=0, blur_radius=8, color=ft.Colors.BLACK12),
            padding=15,
            alignment=ft.Alignment.CENTER,
            ink=True,
            on_click=lambda e: mostrar_crear_lista(desde="listas")
        )
        grid.controls.append(crear_tarjeta)

        lists = listas_manager.get_all_lists()
        for lst in lists:
            count = len(lst["theses"])
            tarjeta = ft.Container(
                content=ft.Column([
                    ft.Row([
                        ft.Text(lst["name"], size=16, weight=ft.FontWeight.BOLD, expand=True),
                        ft.IconButton(
                            icon=ft.Icons.EDIT,
                            icon_size=18,
                            tooltip="Renombrar",
                            on_click=lambda e, lid=lst["id"], name=lst["name"]: mostrar_renombrar_lista(lid, name)
                        ),
                        ft.IconButton(
                            icon=ft.Icons.DELETE,
                            icon_size=18,
                            tooltip="Eliminar",
                            on_click=lambda e, lid=lst["id"], name=lst["name"]: mostrar_eliminar_lista(lid, name)
                        ),
                    ], spacing=5),
                    ft.Container(height=10),
                    ft.Icon(ft.Icons.FOLDER, size=40, color=ft.Colors.TEAL),
                    ft.Container(height=10),
                    ft.Text(f"{count} tesis", size=14, color=ft.Colors.BLUE_GREY_700),
                ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                width=200,
                height=180,
                bgcolor=ft.Colors.WHITE,
                border_radius=12,
                shadow=ft.BoxShadow(spread_radius=0, blur_radius=8, color=ft.Colors.BLACK12),
                padding=15,
                alignment=ft.Alignment.CENTER,
                ink=True,
                on_click=lambda e, lid=lst["id"]: mostrar_lista_detalle(lid)
            )
            grid.controls.append(tarjeta)

        page.update()

    def mostrar_seleccion_lista(ius: str):
        nonlocal current_view, tesis_pendiente_seleccion
        tesis_pendiente_seleccion = ius
        current_view = "seleccionar_lista"

        header.visible = True
        tabla_container.visible = False
        estadisticas_container.visible = False
        listas_container.visible = False
        lista_detalle_container.visible = False
        seleccionar_lista_container.visible = True
        renombrar_lista_container.visible = False
        eliminar_lista_container.visible = False
        crear_lista_container.visible = False

        volver_tabla_btn.visible = False
        volver_tabla_btn_green.visible = True
        volver_listas_desde_detalle_btn.visible = False

        search_field.visible = False
        materia_dropdown.visible = False
        epoca_dropdown.visible = False
        estadisticas_btn.visible = False
        listas_btn.visible = False
        detener_extraccion_btn.visible = False
        detener_descarga_btn.visible = False

        lists = listas_manager.get_all_lists()
        content_column = ft.Column(spacing=20, horizontal_alignment=ft.CrossAxisAlignment.CENTER)

        content_column.controls.append(
            ft.Text("Selecciona una lista", size=20, weight=ft.FontWeight.BOLD, color=ft.Colors.TEAL_800)
        )

        if lists:
            lista_botones = ft.Column(spacing=10, horizontal_alignment=ft.CrossAxisAlignment.CENTER)
            for lst in lists:
                count = len(lst["theses"])
                btn = ft.Button(
                    content=ft.Row([
                        ft.Icon(ft.Icons.FOLDER, color=ft.Colors.TEAL),
                        ft.Text(f"{lst['name']} ({count})", size=16, weight=ft.FontWeight.W_500),
                    ], alignment=ft.MainAxisAlignment.CENTER),
                    style=ft.ButtonStyle(
                        shape=ft.RoundedRectangleBorder(radius=8),
                        bgcolor=ft.Colors.WHITE,
                        color=ft.Colors.BLACK,
                        elevation=2,
                        padding=15,
                    ),
                    width=400,
                    on_click=lambda e, lid=lst["id"], name=lst["name"]: (
                        listas_manager.add_thesis_to_list(lid, tesis_pendiente_seleccion),
                        actualizar_estado(f"Tesis agregada a la lista '{name}'", ""),
                        mostrar_lista_detalle(lid)
                    )
                )
                lista_botones.controls.append(btn)
            content_column.controls.append(lista_botones)
        else:
            content_column.controls.append(
                ft.Container(
                    content=ft.Column([
                        ft.Icon(ft.Icons.INFO, size=40, color=ft.Colors.BLUE_GREY_400),
                        ft.Text("No hay listas creadas.", size=16, color=ft.Colors.BLUE_GREY_600),
                    ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    padding=30,
                )
            )

        btn_crear = ft.Button(
            content=ft.Row([
                ft.Icon(ft.Icons.ADD, color=ft.Colors.TEAL),
                ft.Text("Crear nueva lista", size=16, weight=ft.FontWeight.W_500),
            ], alignment=ft.MainAxisAlignment.CENTER),
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=8),
                bgcolor=ft.Colors.TEAL_50,
                color=ft.Colors.TEAL_800,
                elevation=2,
                padding=15,
            ),
            width=400,
            on_click=lambda e: mostrar_crear_lista(desde="seleccion", tesis_ius=tesis_pendiente_seleccion)
        )
        content_column.controls.append(btn_crear)

        seleccionar_lista_container.content = ft.Container(
            content=content_column,
            alignment=ft.Alignment.CENTER,
            expand=True,
        )
        page.update()

    def mostrar_lista_detalle(list_id: str):
        nonlocal current_view, current_list_id, current_list_name
        current_view = "lista_detalle"
        current_list_id = list_id
        lst = listas_manager.get_list(list_id)
        if not lst:
            mostrar_listas()
            return
        current_list_name = lst["name"]

        header.visible = True
        tabla_container.visible = False
        estadisticas_container.visible = False
        listas_container.visible = False
        lista_detalle_container.visible = True
        seleccionar_lista_container.visible = False
        renombrar_lista_container.visible = False
        eliminar_lista_container.visible = False
        crear_lista_container.visible = False

        volver_tabla_btn.visible = False
        volver_tabla_btn_green.visible = False
        volver_listas_desde_detalle_btn.visible = True

        estadisticas_btn.visible = False
        listas_btn.visible = False
        search_field.visible = False
        materia_dropdown.visible = False
        epoca_dropdown.visible = False
        detener_extraccion_btn.visible = False
        detener_descarga_btn.visible = False

        lista_detalle_container.content.controls[0].content.value = f"{lst['name']} ({len(lst['theses'])} tesis)"

        list_view_lista_detalle.controls.clear()

        if not lst["theses"]:
            list_view_lista_detalle.controls.append(
                ft.Container(
                    content=ft.Column([
                        ft.Icon(ft.Icons.INFO, size=40, color=ft.Colors.BLUE_GREY_400),
                        ft.Text("Esta lista no tiene tesis agregadas.", size=16, color=ft.Colors.BLUE_GREY_600),
                        ft.Text("Puedes agregarlas desde el menú principal usando el botón + en cada tesis.",
                            size=14, color=ft.Colors.BLUE_GREY_500, text_align=ft.TextAlign.CENTER)
                    ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                    padding=40,
                    alignment=ft.Alignment.CENTER
                )
            )
        else:
            for idx, ius in enumerate(lst["theses"], start=1):
                tesis = GLOBAL_DB.obtener_tesis_por_ius(ius)
                if tesis:
                    list_view_lista_detalle.controls.append(
                        crear_fila_tesis(tesis, index=idx, es_lista=True, list_id=list_id)
                    )
        page.update()

    def mostrar_crear_lista(desde="listas", tesis_ius=None):
        nonlocal current_view, crear_lista_origen, tesis_pendiente_seleccion
        current_view = "crear_lista"
        crear_lista_origen = desde
        if tesis_ius:
            tesis_pendiente_seleccion = tesis_ius

        header.visible = True
        tabla_container.visible = False
        estadisticas_container.visible = False
        listas_container.visible = False
        lista_detalle_container.visible = False
        seleccionar_lista_container.visible = False
        crear_lista_container.visible = True
        renombrar_lista_container.visible = False
        eliminar_lista_container.visible = False

        volver_tabla_btn.visible = False
        volver_tabla_btn_green.visible = False
        volver_listas_desde_detalle_btn.visible = False

        estadisticas_btn.visible = False
        listas_btn.visible = False
        search_field.visible = False
        materia_dropdown.visible = False
        epoca_dropdown.visible = False
        detener_extraccion_btn.visible = False
        detener_descarga_btn.visible = False

        nombre_field = ft.TextField(
            label="Nombre de la lista",
            autofocus=True,
            width=400,
            border_radius=8,
        )
        error_text = ft.Text("", color=ft.Colors.RED, size=12, visible=False)
        mensaje_exito = ft.Text("", color=ft.Colors.GREEN, size=12, visible=False)

        def on_cancelar(e):
            if desde == "listas":
                mostrar_listas()
            else:
                mostrar_seleccion_lista(tesis_pendiente_seleccion)

        def on_crear(e):
            nombre = nombre_field.value.strip()
            if not nombre:
                error_text.value = "El nombre no puede estar vacío"
                error_text.visible = True
                mensaje_exito.visible = False
                page.update()
                return
            list_id = listas_manager.create_list(nombre)
            if list_id is None:
                error_text.value = f"Error al guardar la lista. Verifica permisos en:\n{listas_manager.filepath}"
                error_text.visible = True
                mensaje_exito.visible = False
                page.update()
                return
            error_text.visible = False
            mensaje_exito.value = f"Lista '{nombre}' creada exitosamente"
            mensaje_exito.visible = True
            page.update()
            time.sleep(0.3)
            if desde == "listas":
                mostrar_lista_detalle(list_id)
            else:
                listas_manager.add_thesis_to_list(list_id, tesis_pendiente_seleccion)
                mostrar_lista_detalle(list_id)
                actualizar_estado(f"Tesis agregada a la nueva lista '{nombre}'", "")

        btn_crear = ft.FilledButton(
            content=ft.Text("Crear lista", size=14),
            on_click=on_crear,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=8),
                bgcolor=ft.Colors.TEAL,
                color=ft.Colors.WHITE,
                padding=15,
            ),
            width=200,
        )
        btn_cancelar = ft.OutlinedButton(
            content=ft.Text("Cancelar", size=14),
            on_click=on_cancelar,
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=8),
                padding=15,
            ),
            width=200,
        )

        content = ft.Column(
            [
                ft.Container(height=20),
                ft.Text("Crear nueva lista", size=24, weight=ft.FontWeight.BOLD, color=ft.Colors.TEAL_800),
                ft.Container(height=30),
                nombre_field,
                ft.Container(height=10),
                error_text,
                mensaje_exito,
                ft.Container(height=20),
                ft.Row([btn_crear, btn_cancelar], alignment=ft.MainAxisAlignment.CENTER, spacing=20),
                ft.Container(height=20),
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            scroll=ft.ScrollMode.ADAPTIVE,
        )

        crear_lista_container.content = ft.Container(
            content=content,
            alignment=ft.Alignment.CENTER,
            expand=True,
            padding=20,
        )
        page.update()

    def mostrar_renombrar_lista(list_id: str, nombre_actual: str):
        nonlocal current_view
        current_view = "renombrar_lista"

        header.visible = True
        tabla_container.visible = False
        estadisticas_container.visible = False
        listas_container.visible = False
        lista_detalle_container.visible = False
        seleccionar_lista_container.visible = False
        renombrar_lista_container.visible = True
        eliminar_lista_container.visible = False
        crear_lista_container.visible = False

        search_field.visible = False
        materia_dropdown.visible = False
        epoca_dropdown.visible = False
        estadisticas_btn.visible = False
        listas_btn.visible = False
        volver_tabla_btn.visible = False
        volver_tabla_btn_green.visible = False
        volver_listas_desde_detalle_btn.visible = False
        detener_extraccion_btn.visible = False
        detener_descarga_btn.visible = False

        nombre_field = ft.TextField(
            label="Nuevo nombre",
            value=nombre_actual,
            autofocus=True,
            width=400,
            border_radius=10,
        )

        def on_guardar(e):
            nuevo_nombre = nombre_field.value.strip()
            if not nuevo_nombre:
                nombre_field.error_text = "El nombre no puede estar vacío"
                page.update()
                return
            if listas_manager.rename_list(list_id, nuevo_nombre):
                actualizar_estado(f"Lista renombrada a '{nuevo_nombre}'", "")
                if current_list_id == list_id:
                    mostrar_lista_detalle(list_id)
                else:
                    mostrar_listas()
            else:
                actualizar_estado("Error al renombrar la lista", "error")

        content_column = ft.Column(
            spacing=20,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                ft.Text("Renombrar lista", size=20, weight=ft.FontWeight.BOLD, color=ft.Colors.TEAL_800),
                ft.Container(height=10),
                ft.Text(f"Nombre actual: {nombre_actual}", size=14, color=ft.Colors.BLUE_GREY_700),
                nombre_field,
                ft.Row(
                    [
                        ft.Button(
                            "Cancelar",
                            on_click=lambda e: mostrar_listas() if current_list_id is None else mostrar_lista_detalle(current_list_id),
                            style=ft.ButtonStyle(padding=15),
                        ),
                        ft.FilledButton(
                            "Guardar",
                            on_click=on_guardar,
                            style=ft.ButtonStyle(bgcolor=ft.Colors.TEAL, padding=15),
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                    spacing=20,
                ),
            ]
        )

        renombrar_lista_container.content = ft.Container(
            content=content_column,
            alignment=ft.Alignment.CENTER,
            expand=True,
        )
        page.update()

    def mostrar_eliminar_lista(list_id: str, nombre: str):
        nonlocal current_view
        current_view = "eliminar_lista"

        header.visible = True
        tabla_container.visible = False
        estadisticas_container.visible = False
        listas_container.visible = False
        lista_detalle_container.visible = False
        seleccionar_lista_container.visible = False
        renombrar_lista_container.visible = False
        eliminar_lista_container.visible = True
        crear_lista_container.visible = False

        search_field.visible = False
        materia_dropdown.visible = False
        epoca_dropdown.visible = False
        estadisticas_btn.visible = False
        listas_btn.visible = False
        volver_tabla_btn.visible = False
        volver_tabla_btn_green.visible = False
        volver_listas_desde_detalle_btn.visible = False
        detener_extraccion_btn.visible = False
        detener_descarga_btn.visible = False

        def on_eliminar(e):
            if listas_manager.delete_list(list_id):
                actualizar_estado(f"Lista '{nombre}' eliminada", "")
                mostrar_listas()
            else:
                actualizar_estado("Error al eliminar la lista", "error")

        content_column = ft.Column(
            spacing=20,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                ft.Text("Eliminar lista", size=20, weight=ft.FontWeight.BOLD, color=ft.Colors.RED_800),
                ft.Container(height=10),
                ft.Icon(ft.Icons.WARNING, size=50, color=ft.Colors.RED_400),
                ft.Text(
                    f"¿Estás seguro de eliminar la lista '{nombre}'?",
                    size=16,
                    color=ft.Colors.BLUE_GREY_800,
                    text_align=ft.TextAlign.CENTER,
                ),
                ft.Text(
                    "Esta acción no se puede deshacer.",
                    size=14,
                    color=ft.Colors.BLUE_GREY_600,
                ),
                ft.Container(height=10),
                ft.Row(
                    [
                        ft.Button(
                            "Cancelar",
                            on_click=lambda e: mostrar_lista_detalle(list_id),
                            style=ft.ButtonStyle(padding=15),
                        ),
                        ft.FilledButton(
                            "Sí, eliminar",
                            on_click=on_eliminar,
                            style=ft.ButtonStyle(bgcolor=ft.Colors.RED, padding=15),
                        ),
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                    spacing=20,
                ),
            ]
        )

        eliminar_lista_container.content = ft.Container(
            content=content_column,
            alignment=ft.Alignment.CENTER,
            expand=True,
        )
        page.update()


    def detener_extraccion():
        nonlocal is_processing_extraccion
        global stop_extraction
        if is_processing_extraccion:
            stop_extraction = True
            actualizar_estado("Deteniendo extracción...", "Por favor espere")
            detener_extraccion_btn.disabled = True
            page.update()

    def detener_descarga():
        nonlocal is_processing_descarga
        global stop_download
        if is_processing_descarga:
            stop_download = True
            actualizar_estado("Deteniendo descarga...", "Por favor espere")
            detener_descarga_btn.disabled = True
            page.update()

    def iniciar_extraccion_completa():
        nonlocal is_processing_extraccion
        if is_processing_extraccion:
            actualizar_estado("Ya hay un proceso en ejecución")
            return
        global stop_extraction, extractor_thread
        stop_extraction = False
        is_processing_extraccion = True
        extraer_todas_btn_estadisticas.disabled = True
        descargar_pdfs_btn_estadisticas.disabled = True
        detener_extraccion_btn.visible = True
        detener_extraccion_btn.disabled = False
        page.update()

        def extraccion_background():
            nonlocal is_processing_extraccion, current_view, bd_version
            global stop_extraction
            try:
                extractor = SCJNTesisExtractor(GLOBAL_DB_PATH)
                total_combinaciones = len(extractor.configuraciones_epocas) * len(extractor.tipos_tesis)
                combinacion_actual = 0

                def callback_progreso(pagina_actual, max_paginas, mensaje, progreso_general=None):
                    if stop_extraction:
                        return None
                    if progreso_general is not None:
                        page.run_thread(lambda: actualizar_progreso_estadisticas(mensaje, f"Progreso: {progreso_general*100:.1f}%"))
                    else:
                        page.run_thread(lambda: actualizar_progreso_estadisticas(mensaje, ""))
                    return True

                for epoca_nombre in extractor.configuraciones_epocas.keys():
                    if stop_extraction:
                        break
                    for tipo_nombre in extractor.tipos_tesis.keys():
                        if stop_extraction:
                            break
                        combinacion_actual += 1
                        epoca_actual = epoca_nombre.replace('_', ' ')
                        tipo_actual = tipo_nombre
                        combo_actual = combinacion_actual
                        total_combo = total_combinaciones
                        page.run_thread(lambda: actualizar_progreso_estadisticas(
                            f"Extrayendo {epoca_actual} - {tipo_actual}... ({combo_actual}/{total_combo})",
                            f"Iniciando extracción..."
                        ))
                        extractor.procesar_epoca_tipo(
                            epoca=epoca_nombre,
                            tipo_tesis=tipo_nombre,
                            size=50,
                            max_paginas=1000,
                            combinacion_actual=combinacion_actual,
                            total_combinaciones=total_combinaciones,
                            callback_progreso=callback_progreso
                        )
                        if stop_extraction:
                            break
                        time.sleep(1)

                if stop_extraction:
                    page.run_thread(lambda: actualizar_progreso_estadisticas("Extracción detenida", "Proceso interrumpido por el usuario"))
                    page.run_thread(lambda: actualizar_estado("Extracción detenida"))
                else:
                    page.run_thread(lambda: actualizar_progreso_estadisticas("Extracción completada", "Proceso finalizado exitosamente"))
                    page.run_thread(lambda: actualizar_estado("Extracción completada exitosamente"))

                page.run_thread(lambda: incrementar_bd_version())

                if current_view == "estadisticas":
                    page.run_thread(mostrar_estadisticas)
                elif current_view == "tabla":
                    page.run_thread(lambda: buscar_tesis_con_filtros(reset_pagination=True))

            except Exception as e:
                error_msg = str(e)
                page.run_thread(lambda: actualizar_progreso_estadisticas(f"Error: {error_msg}", "Error en el proceso"))
                page.run_thread(lambda: actualizar_estado("Error en extracción", error_msg))

            finally:
                is_processing_extraccion = False
                stop_extraction = False
                page.run_thread(lambda: setattr(extraer_todas_btn_estadisticas, 'disabled', False))
                page.run_thread(lambda: setattr(descargar_pdfs_btn_estadisticas, 'disabled', False))
                page.run_thread(lambda: setattr(detener_extraccion_btn, 'visible', False))
                page.run_thread(page.update)

        extractor_thread = threading.Thread(target=extraccion_background, daemon=True)
        extractor_thread.start()

    def iniciar_descarga_pendientes():
        nonlocal is_processing_extraccion, is_processing_descarga
        if is_processing_extraccion or is_processing_descarga:
            actualizar_estado("Ya hay un proceso en ejecución")
            return
        stats = GLOBAL_DB.obtener_estadisticas()
        tesis_pendientes = stats['total_tesis'] - stats.get('tesis_descargadas', 0)
        if tesis_pendientes == 0:
            return
        global stop_download, descargador_thread
        stop_download = False
        is_processing_descarga = True
        extraer_todas_btn_estadisticas.disabled = True
        descargar_pdfs_btn_estadisticas.disabled = True
        detener_descarga_btn.visible = True
        detener_descarga_btn.disabled = False
        detener_extraccion_btn.visible = False
        actualizar_progreso_estadisticas(f"Preparando descarga de {tesis_pendientes} tesis...", "Inicializando...")
        page.update()

        def descarga_background():
            nonlocal is_processing_descarga, current_view, bd_version
            global stop_download
            is_processing_descarga = True
            try:
                db_thread = SCJNTesisDatabase(GLOBAL_DB_PATH)
                tesis_pendientes_list = db_thread.obtener_tesis_por_descargar(limite=None, incluir_fallidas=False)
                db_thread.close()
                total = len(tesis_pendientes_list)
                if total == 0:
                    extraer_todas_btn_estadisticas.disabled = False
                    descargar_pdfs_btn_estadisticas.disabled = False
                    detener_descarga_btn.visible = False
                    actualizar_progreso_estadisticas("No hay tesis pendientes", "")
                    if current_view == "tabla":
                        cargar_ultimas_tesis()
                    page.update()
                    return
                descargador = DescargadorTesis(GLOBAL_DB_PATH)
                exitos = 0
                fallos = 0
                def callback_progreso(actual, total_tesis, mensaje):
                    if stop_download:
                        return
                    page.run_thread(lambda: actualizar_progreso_estadisticas(mensaje, f"Progreso: {actual}/{total_tesis} ({actual/total_tesis*100:.1f}%)"))
                for i, tesis in enumerate(tesis_pendientes_list):
                    if stop_download:
                        break
                    ius = tesis['ius']
                    epoca_config = tesis['epoca_config']
                    callback_progreso(i+1, total, f"Descargando {ius}... ({i+1}/{total})")
                    try:
                        exito, _ = descargador.descargar_tesis_individual(ius, epoca_config)
                        if exito:
                            exitos += 1
                        else:
                            fallos += 1
                    except Exception as e:
                        fallos += 1
                        logging.error(f"Error descargando {ius}: {e}")
                    if not stop_download:
                        time.sleep(1)
                if stop_download:
                    actualizar_progreso_estadisticas("Descarga detenida", "Proceso interrumpido por el usuario")
                    actualizar_estado("Descarga detenida")
                else:
                    actualizar_progreso_estadisticas(
                        f"Descarga completada: {exitos} exitosas, {fallos} fallidas",
                        f"Total procesadas: {total}"
                    )
                    actualizar_estado(f"Descarga completada: {exitos} exitosas, {fallos} fallidas")

                page.run_thread(lambda: incrementar_bd_version())

                if current_view == "estadisticas":
                    mostrar_estadisticas()
                elif current_view == "tabla":
                    cargar_ultimas_tesis()
                descargador.cerrar()
            except Exception as e:
                error_msg = str(e)
                actualizar_progreso_estadisticas(f"Error: {error_msg}", "Error en el proceso")
                actualizar_estado("Error en descarga", error_msg)
            finally:
                is_processing_descarga = False
                stop_download = False
                page.run_thread(lambda: setattr(extraer_todas_btn_estadisticas, 'disabled', False))
                page.run_thread(lambda: setattr(descargar_pdfs_btn_estadisticas, 'disabled', False))
                page.run_thread(lambda: setattr(detener_descarga_btn, 'visible', False))
                page.run_thread(page.update)

        descargador_thread = threading.Thread(target=descarga_background, daemon=True)
        descargador_thread.start()

    def exportar_datos():
        try:
            actualizar_estado("Exportando datos...", "Por favor espere")
            archivo_csv = GLOBAL_DB.exportar_a_csv()
            archivo_excel = GLOBAL_DB.exportar_resumenes()
            def cerrar_dialogo(e):
                page.dialog.open = False
                page.update()
            dialog = ft.AlertDialog(
                title=ft.Text("Exportación Exitosa"),
                content=ft.Column([
                    ft.Text(f"CSV: {archivo_csv}"),
                    ft.Text(f"Excel: {archivo_excel}")
                ], tight=True),
                actions=[ft.TextButton("OK", on_click=cerrar_dialogo)]
            )
            page.dialog = dialog
            dialog.open = True
            page.update()
            actualizar_estado("Datos exportados exitosamente")
        except Exception as e:
            actualizar_estado("Error al exportar", str(e))

    def on_ius_click(ius, epoca_config, rubro):
        def procesar_tesis():
            nonlocal bd_version
            actualizar_estado(f"Procesando tesis {ius}...", "Verificando si ya está descargada")
            db_thread = SCJNTesisDatabase(GLOBAL_DB_PATH)
            descargado, ubicacion = db_thread.verificar_estado_descarga(ius)
            descargador = DescargadorTesis(GLOBAL_DB_PATH)
            carpeta_epoca = descargador.obtener_carpeta_epoca(epoca_config)
            ruta_esperada = os.path.join(carpeta_epoca, f"tesis_{ius}.pdf")
            if descargado and ubicacion and os.path.exists(ubicacion):
                db_thread.close()
                exito = abrir_archivo_con_aplicacion_predeterminada(ubicacion)
                if exito:
                    actualizar_estado(f"PDF abierto: {os.path.basename(ubicacion)}")
                else:
                    actualizar_estado(f"Error al abrir el archivo", "Verifique que tenga una aplicación para abrir PDFs")
                return
            elif descargado and os.path.exists(ruta_esperada):
                db_thread.marcar_como_descargado(ius, ruta_esperada)
                db_thread.close()
                exito = abrir_archivo_con_aplicacion_predeterminada(ruta_esperada)
                if exito:
                    actualizar_estado(f"PDF abierto: {os.path.basename(ruta_esperada)}")
                else:
                    actualizar_estado(f"Error al abrir el archivo", "Verifique que tenga una aplicación para abrir PDFs")
                page.run_thread(incrementar_bd_version)
                return
            elif os.path.exists(ruta_esperada):
                db_thread.marcar_como_descargado(ius, ruta_esperada)
                db_thread.close()
                exito = abrir_archivo_con_aplicacion_predeterminada(ruta_esperada)
                if exito:
                    actualizar_estado(f"PDF abierto: {os.path.basename(ruta_esperada)}")
                else:
                    actualizar_estado(f"Error al abrir el archivo", "Verifique que tenga una aplicación para abrir PDFs")
                page.run_thread(incrementar_bd_version)
                return
            actualizar_estado(f"Descargando tesis {ius}...", "Por favor espere")
            exito, ruta_pdf = descargador.descargar_tesis_individual(ius, epoca_config)
            if exito:
                exito_apertura = abrir_archivo_con_aplicacion_predeterminada(ruta_pdf)
                if exito_apertura:
                    actualizar_estado(f"PDF descargado y abierto: {os.path.basename(ruta_pdf)}")
                    if search_field.value.strip() or current_materia_filter != "Todas" or current_epoca_filter != "Todas":
                        buscar_tesis_con_filtros()
                    else:
                        cargar_ultimas_tesis()
                else:
                    actualizar_estado(f"Error al abrir el archivo descargado", "Verifique que tenga una aplicación para abrir PDFs")
            else:
                actualizar_estado(f"Error al descargar tesis {ius}", "Intente nuevamente más tarde")
            db_thread.close()
            page.run_thread(incrementar_bd_version)
        threading.Thread(target=procesar_tesis, daemon=True).start()

    def incrementar_bd_version():
        nonlocal bd_version
        bd_version += 1
        logging.debug(f"bd_version incrementada a {bd_version}")

    header = ft.Container(
        content=ft.Column([
            ft.Row([
                search_field,
                estadisticas_btn,
                listas_btn,
            ], spacing=15),
            ft.Row([
                materia_dropdown,
                epoca_dropdown,
                ft.Container(content=message_area, expand=True, padding=ft.Padding(20, 0, 0, 0)),
                ft.Container(content=volver_tabla_btn, padding=ft.Padding(20, 0, 0, 0)),
                ft.Container(content=volver_listas_desde_detalle_btn, padding=ft.Padding(20, 0, 0, 0)),
                ft.Container(content=volver_tabla_btn_green, padding=ft.Padding(20, 0, 0, 0))
            ], spacing=15, alignment=ft.MainAxisAlignment.START),
        ]),
        padding=15,
        bgcolor=ft.Colors.WHITE,
        border_radius=12,
        shadow=ft.BoxShadow(spread_radius=0, blur_radius=10, color=ft.Colors.BLACK12),
        margin=ft.Margin(30, 20, 30, 10)
    )

    main_area = ft.Container(
        content=ft.Column([
            tabla_container,
            estadisticas_container,
            listas_container,
            lista_detalle_container,
            seleccionar_lista_container,
            renombrar_lista_container,
            eliminar_lista_container,
            crear_lista_container,
        ]),
        expand=True,
        padding=ft.Padding(30, 0, 30, 20)
    )

    page.add(ft.Column([header, main_area], expand=True, spacing=0))

    def refresh_dropdowns():
        materia_dropdown.options = [ft.dropdownm2.Option(m) for m in GLOBAL_DB.obtener_materias_unicas()]
        epoca_dropdown.options = [ft.dropdownm2.Option(e) for e in GLOBAL_DB.obtener_epocas_unicas()]
        page.update()

    refresh_dropdowns()
    cargar_ultimas_tesis()

    def on_close(e):
        global stop_extraction, stop_download
        stop_extraction = True
        stop_download = True
        if GLOBAL_DB:
            GLOBAL_DB.close()
        os._exit(0)
    page.on_close = on_close

if __name__ == "__main__":
    ft.run(main)