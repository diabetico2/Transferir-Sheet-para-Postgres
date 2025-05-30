import os
import sys
import logging
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import psycopg2
from datetime import datetime
import time

print("Script sheets_to_postgres.py iniciado...")

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações do banco de dados
DB_CONFIG = {
    'dbname': 'seu_banco',
    'user': 'seu_usuario',
    'password': 'sua_senha',
    'host': 'seu_host',
    'port': 'sua_porta'
}

# Configurações
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/drive.readonly'
]
SERVICE_ACCOUNT_FILE = 'service-account.json'  # Arquivo de credenciais da conta de serviço
FOLDER_ID = 'seu_folder_id'  # ID da pasta do Google Drive

# Tipos MIME aceitos para planilhas
SHEET_MIME_TYPES = [
    'application/vnd.google-apps.spreadsheet',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
]

# Nome da tabela e coluna identificadora
TABLE_NAME = 'sua_tabela'
ID_COLUMN = 'codigo_da_transacao'
SHEET_ID_COLUMN = 'codigo_da_transacao'

def verify_folder_access(drive_service):
    """Verifica acesso à pasta e lista seu conteúdo"""
    try:
        folder = drive_service.files().get(
            fileId=FOLDER_ID,
            fields='name, mimeType'
        ).execute()
        
        logger.info(f"Pasta encontrada: {folder.get('name')}")
        
        query = f"'{FOLDER_ID}' in parents"
        results = drive_service.files().list(
            q=query,
            fields="files(id, name, mimeType)"
        ).execute()
        
        files = results.get('files', [])
        logger.info(f"Total de arquivos na pasta: {len(files)}")
        
        return files
        
    except Exception as error:
        logger.error(f"Erro ao acessar pasta: {str(error)}")
        return []

def get_sheets_from_folder(drive_service):
    """Lista todas as planilhas na pasta"""
    try:
        mime_types = " or ".join([f"mimeType='{mime}'" for mime in SHEET_MIME_TYPES])
        query = f"'{FOLDER_ID}' in parents and ({mime_types})"
        
        results = drive_service.files().list(
            q=query,
            fields="files(id, name, mimeType, createdTime)",
            orderBy="createdTime desc"
        ).execute()
        
        files = results.get('files', [])
        
        sheet_groups = {}
        for file in files:
            name = file['name'].rsplit('.', 1)[0]
            if name not in sheet_groups:
                sheet_groups[name] = []
            sheet_groups[name].append(file)
        
        latest_sheets = []
        for name, versions in sheet_groups.items():
            google_sheet = next(
                (f for f in versions if f['mimeType'] == 'application/vnd.google-apps.spreadsheet'),
                None
            )
            if google_sheet:
                latest_sheets.append(google_sheet)
            else:
                xlsx = sorted(versions, key=lambda x: x['createdTime'], reverse=True)[0]
                latest_sheets.append(xlsx)
        
        return latest_sheets
        
    except Exception as error:
        logger.error(f"Erro ao listar planilhas: {str(error)}")
        return []

def get_sheet_data(sheets_service, sheet_id, max_retries=3):
    """Lê os dados de uma planilha específica com retry em caso de timeout"""
    for attempt in range(max_retries):
        try:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=sheet_id,
                range='A1:ZZ'
            ).execute()
            
            values = result.get('values', [])
            if not values:
                logger.error(f"Planilha vazia: {sheet_id}")
                return None
                
            df = pd.DataFrame(values[1:], columns=values[0])
            logger.info(f"Dados obtidos: {len(df)} linhas, {len(df.columns)} colunas")
            return df
            
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                logger.warning(f"Tentativa {attempt + 1} falhou. Aguardando {wait_time}s antes de tentar novamente...")
                time.sleep(wait_time)
            else:
                logger.error(f"Erro ao ler planilha {sheet_id} após {max_retries} tentativas: {str(e)}")
                return None

def normalize_column_name(column):
    """Normaliza o nome da coluna para formato compatível com PostgreSQL"""
    return (column.strip()
            .lower()
            .replace(' ', '_')
            .replace('ç', 'c')
            .replace('ã', 'a')
            .replace('é', 'e')
            .replace('í', 'i')
            .replace('ó', 'o')
            .replace('ú', 'u')
            .replace('â', 'a')
            .replace('ê', 'e')
            .replace('î', 'i')
            .replace('ô', 'o')
            .replace('û', 'u')
            .replace('à', 'a')
            .replace('-', '_')
            .replace('(', '')
            .replace(')', '')
            .replace('/', '_'))

def clean_data(df):
    """Limpa e formata os dados"""
    try:
        df.columns = [normalize_column_name(col) for col in df.columns]
        
        # Exemplo de colunas monetárias - ajuste conforme sua necessidade
        monetary_cols = [
            'valor',
            'preco',
            'taxa'
        ]
        
        for col in monetary_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace('R$', '')
                           .str.replace('.', '')
                           .str.replace(',', '.')
                           .str.strip(),
                    errors='coerce'
                )
        
        # Exemplo de colunas de data - ajuste conforme sua necessidade
        date_cols = [
            'data',
            'data_criacao',
            'data_atualizacao'
        ]
        
        for col in date_cols:
            if col in df.columns:
                df[col] = df[col].replace('', pd.NA)
                df[col] = pd.to_datetime(
                    df[col], 
                    format='%d/%m/%Y %H:%M:%S', 
                    errors='coerce'
                )
                if df[col].isna().all():
                    df[col] = pd.to_datetime(
                        df[col], 
                        format='%d/%m/%Y', 
                        errors='coerce'
                    )
        
        return df
        
    except Exception as e:
        logger.error(f"Erro na limpeza dos dados: {str(e)}")
        return None

def insert_to_postgres(df, conn, batch_size=50):
    """Insere dados no PostgreSQL em lotes"""
    try:
        cur = conn.cursor()
        
        if ID_COLUMN not in df.columns:
            logger.error(f"Coluna {ID_COLUMN} não encontrada no DataFrame")
            return
            
        df = df[df[ID_COLUMN].notna() & (df[ID_COLUMN] != '')]
        
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{TABLE_NAME}'
        """)
        valid_columns = [row[0] for row in cur.fetchall()]
        
        columns = [col for col in df.columns if col in valid_columns]
        placeholders = ', '.join(['%s'] * len(columns))
        
        query = f"""
        INSERT INTO {TABLE_NAME} ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT ({ID_COLUMN}) 
        DO UPDATE SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns if col != ID_COLUMN)}
        """
        
        total_rows = len(df)
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]
            
            batch_values = []
            for _, row in batch.iterrows():
                values = []
                for col in columns:
                    val = row[col]
                    values.append(None if pd.isna(val) or val == '' else val)
                batch_values.append(values)
            
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    cur.executemany(query, batch_values)
                    conn.commit()
                    logger.info(f"Processados {end_idx} de {total_rows} registros")
                    break
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Erro de conexão (tentativa {attempt + 1}/{max_retries}): {str(e)}")
                        try:
                            conn.close()
                        except:
                            pass
                        conn = psycopg2.connect(**DB_CONFIG)
                        cur = conn.cursor()
                        time.sleep(5)
                    else:
                        raise
        
        logger.info(f"Inseridos/atualizados {total_rows} registros")
        
    except Exception as e:
        try:
            conn.rollback()
        except:
            pass
        logger.error(f"Erro na inserção: {str(e)}")
        raise
    finally:
        try:
            cur.close()
        except:
            pass

def setup_database(conn):
    """Configura a tabela com as constraints necessárias"""
    try:
        cur = conn.cursor()
        
        cur.execute(f"""
            DROP TABLE IF EXISTS {TABLE_NAME} CASCADE;
        """)
        conn.commit()
        
        # Exemplo de criação de tabela - ajuste conforme sua necessidade
        logger.info(f"Criando tabela {TABLE_NAME}...")
        cur.execute(f"""
            CREATE TABLE {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                codigo_da_transacao VARCHAR(255) UNIQUE,
                data_criacao TIMESTAMP,
                valor DECIMAL(15, 2),
                status VARCHAR(100),
                descricao TEXT
            );
        """)
        conn.commit()
        logger.info("Tabela criada com sucesso!")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao configurar banco: {str(e)}")
        raise
    finally:
        cur.close()

def main():
    try:
        logger.info("Iniciando processamento...")
        
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Conexão com banco estabelecida")
        
        setup_database(conn)
        
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, 
            scopes=SCOPES
        )
        
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        # Lista de IDs das planilhas a processar
        sheet_ids = [
            'seu_sheet_id_1',
            'seu_sheet_id_2'
        ]
        
        for sheet_id in sheet_ids:
            logger.info(f"Processando planilha: {sheet_id}")
            
            df = get_sheet_data(sheets_service, sheet_id)
            if df is None:
                continue
                
            df_clean = clean_data(df)
            if df_clean is None:
                continue
                
            try:
                insert_to_postgres(df_clean, conn)
            except Exception as e:
                logger.error(f"Erro durante a inserção: {str(e)}")
                try:
                    conn.close()
                except:
                    pass
                conn = psycopg2.connect(**DB_CONFIG)
                logger.info("Reconectado ao banco de dados")
                insert_to_postgres(df_clean, conn)
        
        logger.info("Processamento concluído com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro durante execução: {str(e)}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")
    finally:
        try:
            if 'conn' in locals():
                conn.close()
                logger.info("Conexão com banco fechada")
        except:
            pass

if __name__ == "__main__":
    main() 