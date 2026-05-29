import streamlit as st
import pandas as pd
from datetime import timedelta
import plotly.express as px
import io
import base64
import json
import uuid
import gc
import datetime
import pytz
import psycopg2 # Importar o psycopg2
from sqlalchemy import create_engine, text # Importar create_engine e text do SQLAlchemy
from sqlalchemy.exc import ProgrammingError, DBAPIError # Importar ProgrammingError e DBAPIError do SQLAlchemy
from sqlalchemy.types import String, DateTime, Float # Importar tipos para criação de tabela

# Configura o fuso horário do Brasil
fuso_br = pytz.timezone('America/Sao_Paulo')
hora_atual = datetime.datetime.now(fuso_br).hour

# Define o funcionamento das 08h às 18h (por exemplo)
if hora_atual < 8 or hora_atual >= 18:
    st.cache_data.clear()
    st.title("🌙 Sistema em Repouso")
    st.info("O painel de análise funciona apenas das 08h às 18h para economia de recursos.")
    st.stop() # Interrompe a execução de todo o resto do código abaixo

# ══════════════════════════════════════════════════════════════
# SISTEMA DE LOGIN
# ══════════════════════════════════════════════════════════════

def get_users():
    users = {}
    try:
        secrets  = st.secrets["users"]
        prefixes = set()
        for key in secrets:
            if key.endswith("_user"):
                prefixes.add(key[:-5])
        for prefix in prefixes:
            username = secrets.get(f"{prefix}_user", "")
            password = secrets.get(f"{prefix}_password", "")
            role     = secrets.get(f"{prefix}_role", "user")
            if username:
                users[username] = {"password": password, "role": role}
    except Exception:
        pass
    return users

def login_screen():
    st.title("🔐 Login")
    st.markdown("Faça login para acessar o sistema.")
    with st.form("login_form"):
        username  = st.text_input("Usuário")
        password  = st.text_input("Senha", type="password")
        submitted = st.form_submit_button("Entrar")
    if submitted:
        users = get_users()
        if username in users and users[username]["password"] == password:
            st.session_state["logged_in"] = True
            st.session_state["username"]  = username
            st.session_state["role"]      = users[username]["role"]
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos.")

def is_admin():
    return st.session_state.get("role") == "admin"

# ══════════════════════════════════════════════════════════════
# POSTGRES — Integração
# ══════════════════════════════════════════════════════════════

# Função para obter a string de conexão com o PostgreSQL
def get_postgres_connection_string():
    try:
        pg_secrets = st.secrets["postgres"]
        return (
            f"postgresql+psycopg2://{pg_secrets['user']}:{pg_secrets['password']}@"
            f"{pg_secrets['host']}:{pg_secrets['port']}/{pg_secrets['database']}"
        )
    except Exception as e:
        st.error(f"Erro ao carregar configurações do PostgreSQL: {e}")
        return None

# Cache da engine do SQLAlchemy para reutilização
@st.cache_resource
def get_sql_engine():
    conn_string = get_postgres_connection_string()
    if conn_string:
        return create_engine(conn_string)
    return None

# Função para ler dados do PostgreSQL
@st.cache_data(ttl=3600) # Cache por 1 hora
def read_from_postgres(table_name, columns=None):
    engine = get_sql_engine()
    if engine is None:
        return pd.DataFrame() # Retorna DataFrame vazio se não houver conexão

    try:
        if columns:
            columns_str = ", ".join([f'"{col.lower()}"' for col in columns]) # Garante minúsculas e aspas duplas
            query = f'SELECT {columns_str} FROM "{table_name.lower()}"'
        else:
            query = f'SELECT * FROM "{table_name.lower()}"' # Garante minúsculas e aspas duplas
        df = pd.read_sql(query, engine)
        return df
    except (ProgrammingError, DBAPIError) as e:
        # Verifica se a exceção original é de tabela não existente
        if hasattr(e, 'orig') and isinstance(e.orig, psycopg2.errors.UndefinedTable):
            # st.warning(f"Tabela '{table_name}' não encontrada. Retornando DataFrame vazio.")
            return pd.DataFrame()
        else:
            st.error(f"Erro ao ler da tabela {table_name}: {e}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao ler da tabela {table_name}: {e}")
        return pd.DataFrame()

# Função para escrever dados no PostgreSQL
def write_to_postgres(df, table_name, if_exists='append', index=False):
    engine = get_sql_engine()
    if engine is None:
        st.error("Conexão com o banco de dados não estabelecida.")
        return False

    try:
        # Garante que os nomes das colunas do DataFrame estejam em minúsculas
        df.columns = [col.lower() for col in df.columns]
        df.to_sql(table_name.lower(), engine, if_exists=if_exists, index=index)
        return True
    except Exception as e:
        st.error(f"Erro ao escrever na tabela {table_name}: {e}")
        return False

# Função para executar comandos SQL diretamente (para DELETE TABLE)
def execute_sql_command(command):
    engine = get_sql_engine()
    if engine is None:
        st.error("Conexão com o banco de dados não estabelecida.")
        return False
    try:
        with engine.connect() as connection:
            connection.execute(text(command))
            connection.commit()
        return True
    except Exception as e:
        st.error(f"Erro ao executar comando SQL: {e}")
        return False

# ══════════════════════════════════════════════════════════════
# CONSTANTES DE TABELAS
# ══════════════════════════════════════════════════════════════

TABLE_META_CAMPANHAS = "campanhas_meta"
TABLE_ENVIOS_PREFIX  = "campanha_envios_"
TABLE_CLIENTES_PREFIX = "campanha_clientes_"
TABLE_PAGAMENTOS     = "pagamentos"

# ══════════════════════════════════════════════════════════════
# GARANTIR EXISTÊNCIA DA TABELA DE PAGAMENTOS
# ══════════════════════════════════════════════════════════════

def ensure_pagamentos_table_exists():
    engine = get_sql_engine()
    if engine is None:
        return False

    # Definir o esquema mínimo esperado para a tabela de pagamentos
    # É importante que essas colunas e tipos correspondam ao que o seu DataFrame de pagamentos
    # irá tentar inserir.
    schema = {
        "matricula_pagamento": String(50),
        "data_pagamento": DateTime(),
        "valor_pago": Float(),
        "cidade": String(100),
        "tipo_pagamento": String(50),
        "vencimento": DateTime(),
        "utilizacao": String(100),
        "tipo_fatura": String(50)
    }

    # Criar um DataFrame vazio com o esquema desejado
    df_empty = pd.DataFrame(columns=schema.keys())
    # Mapear os tipos do Pandas para os tipos do SQLAlchemy
    dtype_map = {col: type_obj for col, type_obj in schema.items()}

    try:
        # Tentar criar a tabela. if_exists='append' criará a tabela se ela não existir
        # e não fará nada se ela já existir.
        # Passamos um DataFrame vazio, então nenhum dado será inserido, apenas a estrutura.
        df_empty.to_sql(TABLE_PAGAMENTOS.lower(), engine, if_exists='append', index=False, dtype=dtype_map)
        return True
    except Exception as e:
        # Se houver um erro aqui, pode ser um problema de permissão ou outro erro de DB
        st.error(f"Erro ao garantir a existência da tabela '{TABLE_PAGAMENTOS}': {e}")
        return False

# ══════════════════════════════════════════════════════════════
# CAMPANHAS E PAGAMENTOS (AGORA USANDO POSTGRES)
# ══════════════════════════════════════════════════════════════

def load_campanhas_meta():
    df = read_from_postgres(TABLE_META_CAMPANHAS)
    if df.empty:
        return pd.DataFrame(columns=['id', 'nome', 'criado_em', 'total_envios', 'total_clientes'])
    return df

def save_campanha(nome, df_envios, df_clientes):
    if df_envios is None or df_clientes is None:
        return None, "Dados de envios ou clientes inválidos."

    campanha_id = str(uuid.uuid4())[:8]
    table_envios = f"{TABLE_ENVIOS_PREFIX}{campanha_id}"
    table_clientes = f"{TABLE_CLIENTES_PREFIX}{campanha_id}"

    ok_envios = write_to_postgres(df_envios, table_envios, if_exists='replace')
    ok_clientes = write_to_postgres(df_clientes, table_clientes, if_exists='replace')

    if not ok_envios or not ok_clientes:
        # Se falhar, tenta limpar o que foi salvo
        execute_sql_command(f'DROP TABLE IF EXISTS "{table_envios.lower()}"')
        execute_sql_command(f'DROP TABLE IF EXISTS "{table_clientes.lower()}"')
        return None, "Erro ao salvar dados da campanha no banco de dados."

    df_meta = load_campanhas_meta()
    nova = pd.DataFrame([{
        'id': campanha_id, 'nome': nome, 'criado_em': pd.Timestamp.now(fuso_br),
        'total_envios': df_envios['telefone_envio'].nunique(), 'total_clientes': len(df_clientes)
    }])
    df_meta = pd.concat([df_meta, nova], ignore_index=True)

    # Salva os metadados, substituindo a tabela existente
    ok_meta = write_to_postgres(df_meta, TABLE_META_CAMPANHAS, if_exists='replace')
    if not ok_meta:
        return None, "Erro ao atualizar metadados da campanha."

    load_campanhas_meta.clear() # Limpa o cache para recarregar
    return campanha_id, None

def update_campanha(campanha_id, nome, df_envios_novos=None, df_clientes_novos=None):
    df_meta = load_campanhas_meta()
    idx = df_meta.index[df_meta['id'] == campanha_id].tolist()
    if not idx: return False, "Campanha não encontrada."

    table_envios = f"{TABLE_ENVIOS_PREFIX}{campanha_id}"
    table_clientes = f"{TABLE_CLIENTES_PREFIX}{campanha_id}"

    if df_envios_novos is not None and not df_envios_novos.empty:
        df_envios_existente = load_campanha_envios(campanha_id)
        df_envios_combined = pd.concat([df_envios_existente, df_envios_novos], ignore_index=True) if df_envios_existente is not None else df_envios_novos
        df_envios_combined = df_envios_combined.drop_duplicates(subset=['telefone_envio', 'data_envio'], keep='last')
        if not write_to_postgres(df_envios_combined, table_envios, if_exists='replace'):
            return False, "Erro ao atualizar envios da campanha."
        df_meta.at[idx[0], 'total_envios'] = df_envios_combined['telefone_envio'].nunique()

    if df_clientes_novos is not None and not df_clientes_novos.empty:
        df_clientes_existente = load_campanha_clientes(campanha_id)
        df_clientes_combined = pd.concat([df_clientes_existente, df_clientes_novos], ignore_index=True) if df_clientes_existente is not None else df_clientes_novos
        df_clientes_combined = df_clientes_combined.drop_duplicates(subset=['telefone_cliente', 'matricula_cliente'], keep='last')
        if not write_to_postgres(df_clientes_combined, table_clientes, if_exists='replace'):
            return False, "Erro ao atualizar clientes da campanha."
        df_meta.at[idx[0], 'total_clientes'] = len(df_clientes_combined)

    # Salva os metadados atualizados
    if not write_to_postgres(df_meta, TABLE_META_CAMPANHAS, if_exists='replace'):
        return False, "Erro ao atualizar metadados da campanha."

    load_campanhas_meta.clear()
    load_campanha_envios.clear()
    load_campanha_clientes.clear()
    return True, None

@st.cache_data(ttl=3600, max_entries=2)
def load_campanha_envios(campanha_id):
    return read_from_postgres(f"{TABLE_ENVIOS_PREFIX}{campanha_id}")

@st.cache_data(ttl=3600, max_entries=2)
def load_campanha_clientes(campanha_id):
    colunas_cli = ['telefone_cliente', 'matricula_cliente', 'situacao', 'cidade', 'diretoria']
    return read_from_postgres(f"{TABLE_CLIENTES_PREFIX}{campanha_id}", columns=colunas_cli)

def delete_campanha(campanha_id, nome):
    df_meta = load_campanhas_meta()
    df_meta = df_meta[df_meta['id'] != campanha_id]

    if not write_to_postgres(df_meta, TABLE_META_CAMPANHAS, if_exists='replace'):
        return False

    # Exclui as tabelas de envios e clientes da campanha
    ok_envios = execute_sql_command(f'DROP TABLE IF EXISTS "{TABLE_ENVIOS_PREFIX}{campanha_id.lower()}"')
    ok_clientes = execute_sql_command(f'DROP TABLE IF EXISTS "{TABLE_CLIENTES_PREFIX}{campanha_id.lower()}"')

    if not ok_envios or not ok_clientes:
        return False # Indica que a exclusão das tabelas falhou

    load_campanhas_meta.clear()
    load_campanha_envios.clear()
    load_campanha_clientes.clear()
    return True

@st.cache_data(ttl=900, max_entries=1)
def load_pagamentos_db(): # Renomeada para refletir o uso do DB
    # As colunas são carregadas em minúsculas pelo read_from_postgres
    colunas_uteis = ["matricula_pagamento", "data_pagamento", "valor_pago", "cidade", "tipo_pagamento", "vencimento", "utilizacao", "tipo_fatura"]
    df = read_from_postgres(TABLE_PAGAMENTOS, columns=colunas_uteis)

    if df.empty: return None

    # Downcasting imediato (converte textos repetidos em categorias leves)
    colunas_categoricas = ['cidade', 'tipo_pagamento']
    for col in colunas_categoricas:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Reduz o peso da coluna de valor
    if 'valor_pago' in df.columns:
        df['valor_pago'] = pd.to_numeric(df['valor_pago'], downcast='float', errors='coerce')

    return df

def update_pagamentos_db(df_novo): # Renomeada para update_pagamentos_db
    df_existente = load_pagamentos_db() # Carrega do DB
    if df_existente is not None and not df_existente.empty:
        df_combined = pd.concat([df_existente, df_novo], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['matricula_pagamento', 'data_pagamento', 'valor_pago'], keep='last')
    else:
        df_combined = df_novo.copy()

    total_antes = len(df_existente) if df_existente is not None else 0
    novos = len(df_combined) - total_antes

    # Escreve a base combinada de volta no PostgreSQL, adicionando os novos dados
    # if_exists='append' criará a tabela se ela não existir, e adicionará se existir.
    ok = write_to_postgres(df_combined, TABLE_PAGAMENTOS, if_exists='append')
    load_pagamentos_db.clear() # Limpa o cache após a atualização
    return ok, len(df_combined), novos

# ══════════════════════════════════════════════════════════════
# PROCESSAMENTO DE ARQUIVOS (MANTIDO, pois o upload ainda é de arquivos)
# ══════════════════════════════════════════════════════════════

@st.cache_data
def load_and_process_envios(uploaded_file):
    try:
        # Verifica a extensão para ler corretamente
        if uploaded_file.name.endswith('.parquet'):
            file_bytes = uploaded_file.read()
            df = pd.read_parquet(io.BytesIO(file_bytes), engine='pyarrow')
        else:
            df = pd.read_excel(uploaded_file)

        # Verifica se a coluna Reason existe no arquivo
        colunas_ler = ['To', 'Send At']
        if 'Reason' in df.columns:
            colunas_ler.append('Reason')

        df_envios = df[colunas_ler].copy()

        renomear = {'To': 'TELEFONE_ENVIO', 'Send At': 'DATA_ENVIO'}
        if 'Reason' in df.columns:
            renomear['Reason'] = 'STATUS_ENVIO'

        df_envios.rename(columns=renomear, inplace=True)

        # Fallback: se for um arquivo antigo sem a coluna Reason, assume que todos foram entregues
        if 'STATUS_ENVIO' not in df_envios.columns:
            df_envios['STATUS_ENVIO'] = 'DELIVERED_TO_HANDSET'

        df_envios['TELEFONE_ENVIO'] = df_envios['TELEFONE_ENVIO'].astype(str).str.replace(r'^55|\.0$', '', regex=True).str.strip()
        df_envios['DATA_ENVIO'] = pd.to_datetime(df_envios['DATA_ENVIO'], errors='coerce', dayfirst=True)
        df_envios.dropna(subset=['DATA_ENVIO'], inplace=True)
        return df_envios
    except Exception as e:
        st.error(f"Erro ao processar Envios: {e}")
        return None

@st.cache_data
def load_and_process_clientes(uploaded_file):
    try:
        # Verifica a extensão para ler corretamente
        if uploaded_file.name.endswith('.parquet'):
            file_bytes = uploaded_file.read()
            df = pd.read_parquet(io.BytesIO(file_bytes), engine='pyarrow')
        else:
            df = pd.read_excel(uploaded_file)

        colunas_ler = ['TELEFONE', 'MATRICULA', 'SITUACAO']
        for col in ['CIDADE', 'DIRETORIA']:
            if col in df.columns: colunas_ler.append(col)

        df_clientes = df[colunas_ler].copy()
        df_clientes.rename(columns={'TELEFONE': 'TELEFONE_CLIENTE', 'MATRICULA': 'MATRICULA_CLIENTE'}, inplace=True)
        df_clientes['TELEFONE_CLIENTE'] = df_clientes['TELEFONE_CLIENTE'].astype(str).str.replace(r'^55|\.0$', '', regex=True).str.strip()
        df_clientes['MATRICULA_CLIENTE'] = df_clientes['MATRICULA_CLIENTE'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df_clientes['SITUACAO'] = pd.to_numeric(df_clientes['SITUACAO'], errors='coerce').fillna(0)

        if 'CIDADE' in df_clientes.columns: df_clientes['CIDADE'] = df_clientes['CIDADE'].astype(str).str.strip()
        if 'DIRETORIA' in df_clientes.columns: df_clientes['DIRETORIA'] = df_clientes['DIRETORIA'].astype(str).str.strip()

        df_clientes.drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], inplace=True)
        return df_clientes
    except Exception as e:
        st.error(f"Erro ao processar Clientes: {e}")
        return None

@st.cache_data
def load_and_process_pagamentos(uploaded_file):
    try:
        df = None
        # 1. Leitura do Arquivo garantindo a extração dos bytes para o Parquet
        if uploaded_file.name.endswith('.parquet'):
            file_bytes = uploaded_file.read()
            df = pd.read_parquet(io.BytesIO(file_bytes), engine='pyarrow')
        elif uploaded_file.name.endswith('.csv'):
            for encoding in ['latin1', 'utf-8', 'cp1252']:
                try:
                    uploaded_file.seek(0)
                    df = pd.read_csv(uploaded_file, sep=';', decimal=',', encoding=encoding)
                    break
                except Exception:
                    continue
        elif uploaded_file.name.endswith('.xlsx'):
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file)
        else:
            raise ValueError("Formato não suportado.")

        if df is None or df.empty:
            st.error("Arquivo de Pagamentos está vazio.")
            return None

        # 2. Mapeamento Inteligente de Colunas (Por Nome)
        mapeamento_nomes = {
            'Nº Ligação': 'MATRICULA_PAGAMENTO',
            'Data Pagto.': 'DATA_PAGAMENTO',
            'Valor Pago': 'VALOR_PAGO',
            'Cidade': 'CIDADE',
            'Diretoria': 'DIRETORIA',
            'Arrecadador': 'TIPO_PAGAMENTO',
            'Vencimento': 'VENCIMENTO',
            'Tipo Fatura': 'TIPO_FATURA',
            'Utilização (Sub. Categ.)': 'UTILIZACAO',
            'UTILIZACAO': 'UTILIZACAO'
        }
        df.rename(columns=mapeamento_nomes, inplace=True)

        # 3. Verifica se as colunas principais existem. Se não, tenta por índice (Fallback)
        if not all(c in df.columns for c in ['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO']):
            df.columns = range(len(df.columns))
            if df.shape[1] < 10:
                st.error(f"Esperava pelo menos 10 colunas, encontrou {df.shape[1]}.")
                return None

            col_indices = [0, 5, 8]
            col_names   = ['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO']

            for i, col_name in enumerate(col_names):
                if col_indices[i] < df.shape[1]:
                    df.rename(columns={col_indices[i]: col_name}, inplace=True)
                else:
                    st.error(f"Coluna '{col_name}' não encontrada no arquivo de pagamentos pelo índice.")
                    return None

        # 4. Padronização e Limpeza
        df['MATRICULA_PAGAMENTO'] = df['MATRICULA_PAGAMENTO'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df['DATA_PAGAMENTO'] = pd.to_datetime(df['DATA_PAGAMENTO'], errors='coerce', dayfirst=True)
        df['VALOR_PAGO'] = pd.to_numeric(df['VALOR_PAGO'], errors='coerce')

        # Colunas opcionais
        for col in ['CIDADE', 'DIRETORIA', 'TIPO_PAGAMENTO', 'TIPO_FATURA', 'UTILIZACAO']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
            else:
                df[col] = None # Garante que a coluna exista, mesmo que vazia

        if 'VENCIMENTO' in df.columns:
            df['VENCIMENTO'] = pd.to_datetime(df['VENCIMENTO'], errors='coerce', dayfirst=True)
        else:
            df['VENCIMENTO'] = None

        df.dropna(subset=['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO'], inplace=True)
        df = df[df['VALOR_PAGO'] > 0] # Remove pagamentos com valor zero ou negativo

        return df
    except Exception as e:
        st.error(f"Erro ao processar Pagamentos: {e}")
        return None

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE ANÁLISE
# ══════════════════════════════════════════════════════════════

def calcular_kpis(df_envios, df_clientes, df_pagamentos_campanha):
    total_envios = df_envios['telefone_envio'].nunique()
    total_clientes = df_clientes['matricula_cliente'].nunique()
    total_pagamentos = df_pagamentos_campanha['matricula'].nunique()
    valor_total_pago = df_pagamentos_campanha['valor_pago'].sum()

    taxa_conversao = (total_pagamentos / total_envios) * 100 if total_envios > 0 else 0
    ticket_medio = valor_total_pago / total_pagamentos if total_pagamentos > 0 else 0

    return {
        "total_envios": total_envios,
        "total_clientes": total_clientes,
        "total_pagamentos": total_pagamentos,
        "valor_total_pago": valor_total_pago,
        "taxa_conversao": taxa_conversao,
        "ticket_medio": ticket_medio
    }

def fmt_brl(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ══════════════════════════════════════════════════════════════
# INTERFACE DO STREAMLIT
# ══════════════════════════════════════════════════════════════

if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    login_screen()
    st.stop()

# --- Garante que a tabela de pagamentos exista antes de qualquer operação ---
ensure_pagamentos_table_exists()

st.sidebar.title("Menu")
pagina_selecionada = st.sidebar.radio("Navegar", ["Dashboard", "Gestão de Campanhas", "Upload de Pagamentos"])

if pagina_selecionada == "Upload de Pagamentos":
    st.title("📤 Upload de Arquivos de Pagamentos")
    st.markdown("Faça o upload de novos arquivos de pagamentos para atualizar a base de dados.")

    uploaded_file_pagamentos = st.file_uploader("Escolha um arquivo de pagamentos (CSV, XLSX, Parquet)", type=["csv", "xlsx", "parquet"], key="upload_pagamentos")

    if uploaded_file_pagamentos:
        df_pagamentos_upload = load_and_process_pagamentos(uploaded_file_pagamentos)

        if df_pagamentos_upload is not None and not df_pagamentos_upload.empty:
            st.success("Arquivo de pagamentos processado com sucesso!")
            st.dataframe(df_pagamentos_upload.head())

            if st.button("Salvar Pagamentos no Banco de Dados"):
                with st.spinner("Salvando pagamentos..."):
                    ok, total_registros, novos_registros = update_pagamentos_db(df_pagamentos_upload)
                    if ok:
                        st.success(f"Pagamentos salvos com sucesso! Total de registros na base: {total_registros}. Novos registros adicionados: {novos_registros}.")
                        load_pagamentos_db.clear() # Limpa o cache para recarregar os dados atualizados
                    else:
                        st.error("Falha ao salvar pagamentos no banco de dados.")
        else:
            st.error("Não foi possível processar o arquivo de pagamentos ou ele está vazio.")

elif pagina_selecionada == "Gestão de Campanhas":
    st.title("⚙️ Gestão de Campanhas")

    df_campanhas = load_campanhas_meta()

    if is_admin():
        with st.expander("Criar Nova Campanha"):
            with st.form("nova_campanha_form"):
                nome_campanha = st.text_input("Nome da Campanha")
                uploaded_file_envios = st.file_uploader("Upload de Arquivo de Envios (XLSX, Parquet)", type=["xlsx", "parquet"], key="envios_nova")
                uploaded_file_clientes = st.file_uploader("Upload de Arquivo de Clientes (XLSX, Parquet)", type=["xlsx", "parquet"], key="clientes_nova")
                submitted = st.form_submit_button("Criar Campanha")

                if submitted:
                    if not nome_campanha:
                        st.error("O nome da campanha não pode ser vazio.")
                    elif uploaded_file_envios is None:
                        st.error("Por favor, faça o upload do arquivo de envios.")
                    elif uploaded_file_clientes is None:
                        st.error("Por favor, faça o upload do arquivo de clientes.")
                    else:
                        df_envios = load_and_process_envios(uploaded_file_envios)
                        df_clientes = load_and_process_clientes(uploaded_file_clientes)

                        if df_envios is not None and df_clientes is not None:
                            with st.spinner("Criando campanha..."):
                                campanha_id, erro = save_campanha(nome_campanha, df_envios, df_clientes)
                                if campanha_id:
                                    st.success(f"Campanha '{nome_campanha}' criada com sucesso! ID: {campanha_id}")
                                    st.rerun()
                                else:
                                    st.error(f"Erro ao criar campanha: {erro}")
                        else:
                            st.error("Erro ao processar arquivos de envios ou clientes.")

        st.subheader("Campanhas Existentes")
        if not df_campanhas.empty:
            for index, row in df_campanhas.iterrows():
                col1, col2, col3, col4, col5 = st.columns([2, 2, 1, 1, 1])
                with col1: st.write(f"**{row['nome']}** (ID: {row['id']})")
                with col2: st.write(f"Criada em: {row['criado_em'].strftime('%d/%m/%Y %H:%M')}")
                with col3: st.write(f"Envios: {row['total_envios']}")
                with col4: st.write(f"Clientes: {row['total_clientes']}")
                with col5:
                    if st.button("Excluir", key=f"delete_{row['id']}"):
                        if delete_campanha(row['id'], row['nome']):
                            st.success(f"Campanha '{row['nome']}' excluída com sucesso.")
                            st.rerun()
                        else:
                            st.error(f"Erro ao excluir campanha '{row['nome']}'.")
        else:
            st.info("Nenhuma campanha cadastrada ainda.")
    else:
        st.warning("Você não tem permissão para gerenciar campanhas.")

elif pagina_selecionada == "Dashboard":
    st.title("📊 Dashboard de Análise de Campanhas")

    df_campanhas = load_campanhas_meta()
    campanhas_disponiveis = df_campanhas.set_index('id')['nome'].to_dict()

    campanha_selecionada_id = st.sidebar.selectbox(
        "Selecione uma Campanha",
        options=[None] + list(campanhas_disponiveis.keys()),
        format_func=lambda x: campanhas_disponiveis.get(x, "Selecione...")
    )

    campanha_selecionada = None
    if campanha_selecionada_id:
        campanha_selecionada = df_campanhas[df_campanhas['id'] == campanha_selecionada_id].iloc[0]
        st.sidebar.write(f"**Campanha:** {campanha_selecionada['nome']}")
        st.sidebar.write(f"**Criada em:** {campanha_selecionada['criado_em'].strftime('%d/%m/%Y')}")

    # Filtro de dias após o envio
    dias_apos_envio_max = st.sidebar.slider(
        "Filtrar pagamentos até X dias após o envio",
        min_value=1, max_value=60, value=30, step=1
    )

    executar_analise = st.sidebar.button("Executar Análise")

    df_pagamentos = None
    df_envios = None
    df_clientes = None
    df_pagamentos_campanha = pd.DataFrame()
    dados_prontos = False

    if executar_analise and campanha_selecionada:
        with st.spinner("Carregando e processando dados..."):
            df_pagamentos = load_pagamentos_db()
            df_envios = load_campanha_envios(campanha_selecionada_id)
            df_clientes = load_campanha_clientes(campanha_selecionada_id)

            if df_pagamentos is not None and not df_pagamentos.empty and \
               df_envios is not None and not df_envios.empty and \
               df_clientes is not None and not df_clientes.empty:

                # 1. Merge df_envios com df_clientes para obter dados do cliente no envio
                df_envios_clientes = pd.merge(
                    df_envios,
                    df_clientes[['telefone_cliente', 'matricula_cliente', 'cidade', 'diretoria']],
                    left_on='telefone_envio',
                    right_on='telefone_cliente',
                    how='left'
                )
                df_envios_clientes.rename(columns={'matricula_cliente': 'matricula'}, inplace=True)
                df_envios_clientes.drop(columns=['telefone_cliente'], inplace=True)

                # 2. Merge df_pagamentos com df_envios_clientes para atribuir pagamentos aos envios
                # Usar 'matricula_pagamento' para merge com 'matricula' (do cliente)
                df_pagamentos_atribuidos = pd.merge(
                    df_pagamentos,
                    df_envios_clientes[['matricula', 'data_envio', 'telefone_envio', 'cidade', 'diretoria']].drop_duplicates(subset=['matricula', 'data_envio']),
                    left_on='matricula_pagamento',
                    right_on='matricula',
                    how='inner' # Apenas pagamentos que podem ser atribuídos a um envio
                )

                # 3. Filtrar pagamentos dentro da janela de tempo
                df_pagamentos_atribuidos['dias_apos_envio'] = (df_pagamentos_atribuidos['data_pagamento'] - df_pagamentos_atribuidos['data_envio']).dt.days
                df_pagamentos_campanha = df_pagamentos_atribuidos[
                    (df_pagamentos_atribuidos['dias_apos_envio'] >= 0) &
                    (df_pagamentos_atribuidos['dias_apos_envio'] <= dias_apos_envio_max)
                ].copy()

                # Garantir que 'matricula' e 'matricula_pagamento' sejam a mesma coluna para análise
                df_pagamentos_campanha['matricula'] = df_pagamentos_campanha['matricula_pagamento']

                dados_prontos = True
            else:
                st.error("Não foi possível carregar todos os dados necessários para a análise. Verifique se as bases de pagamentos, envios e clientes estão disponíveis e não vazias.")

    if dados_prontos:
        kpis = calcular_kpis(df_envios, df_clientes, df_pagamentos_campanha)

        st.subheader(f"Resultados da Campanha: {campanha_selecionada['nome']}")

        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("Total de Envios", f"{kpis['total_envios']:,}".replace(",", "."))
        col2.metric("Total de Clientes", f"{kpis['total_clientes']:,}".replace(",", "."))
        col3.metric("Pagamentos Atribuídos", f"{kpis['total_pagamentos']:,}".replace(",", "."))
        col4.metric("Valor Total Pago", fmt_brl(kpis['valor_total_pago']))
        col5.metric("Taxa de Conversão", f"{kpis['taxa_conversao']:.2f}%")
        col6.metric("Ticket Médio", fmt_brl(kpis['ticket_medio']))

        aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
            "Visão Geral", "Arrecadação por Tempo", "Arrecadação por Canal",
            "Arrecadação por Local", "Detalhes", "Laboratório"
        ])

        # ══════════════════════════════════════════════════════════
        # ABA 1 — VISÃO GERAL
        # ══════════════════════════════════════════════════════════
        with aba1:
            st.subheader("Visão Geral da Campanha")

            # Gráfico de Arrecadação Diária
            st.subheader("Arrecadação Diária")
            df_arrecadacao_diaria = df_pagamentos_campanha.groupby('data_pagamento')['valor_pago'].sum().reset_index()
            fig_diaria = px.bar(
                df_arrecadacao_diaria, x='data_pagamento', y='valor_pago',
                title='Arrecadação Diária da Campanha',
                labels={'data_pagamento': 'Data do Pagamento', 'valor_pago': 'Valor Arrecadado (R$)'}
            )
            st.plotly_chart(fig_diaria, use_container_width=True)

            # Gráfico de Pagamentos por Dia da Semana
            st.subheader("Pagamentos por Dia da Semana")
            df_pagamentos_campanha['dia_semana'] = df_pagamentos_campanha['data_pagamento'].dt.day_name(locale='pt_BR')
            ordem_dias = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
            pagamentos_por_dia = df_pagamentos_campanha.groupby('dia_semana')['valor_pago'].sum().reindex(ordem_dias).reset_index()
            fig_dia_semana = px.bar(
                pagamentos_por_dia, x='dia_semana', y='valor_pago',
                title='Arrecadação por Dia da Semana',
                labels={'dia_semana': 'Dia da Semana', 'valor_pago': 'Valor Arrecadado (R$)'},
                category_orders={'dia_semana': ordem_dias}
            )
            st.plotly_chart(fig_dia_semana, use_container_width=True)

        # ══════════════════════════════════════════════════════════
        # ABA 2 — ARRECADAÇÃO POR TEMPO
        # ══════════════════════════════════════════════════════════
        with aba2:
            st.subheader("Arrecadação por Tempo de Resposta")

            # Gráfico de Arrecadação por Dias Após Envio
            st.subheader("Arrecadação por Dias Após o Envio")
            arrecadacao_dias = df_pagamentos_campanha.groupby('dias_apos_envio')['valor_pago'].sum().reset_index()
            fig_dias_apos_envio = px.bar(
                arrecadacao_dias, x='dias_apos_envio', y='valor_pago',
                title='Arrecadação Total por Dias Após o Envio',
                labels={'dias_apos_envio': 'Dias Após o Envio', 'valor_pago': 'Valor Arrecadado (R$)'}
            )
            st.plotly_chart(fig_dias_apos_envio, use_container_width=True)

            # Gráfico de Pagamentos Acumulados
            st.subheader("Pagamentos Acumulados ao Longo do Tempo")
            df_acumulado = df_pagamentos_campanha.groupby('dias_apos_envio')['valor_pago'].sum().reset_index()
            df_acumulado['valor_acumulado'] = df_acumulado['valor_pago'].cumsum()
            fig_acumulado = px.line(
                df_acumulado, x='dias_apos_envio', y='valor_acumulado',
                title='Evolução da Arrecadação (Acumulada)',
                labels={'dias_apos_envio': 'Dias Após o Envio', 'valor_acumulado': 'Valor Acumulado (R$)'},
                markers=True
            )
            st.plotly_chart(fig_acumulado, use_container_width=True)

        # ══════════════════════════════════════════════════════════
        # ABA 3 — ARRECADAÇÃO POR CANAL
        # ══════════════════════════════════════════════════════════
        with aba3:
            st.subheader("Arrecadação por Canal de Pagamento")

            if 'tipo_pagamento' in df_pagamentos_campanha.columns:
                pagamentos_por_canal = df_pagamentos_campanha.groupby('tipo_pagamento')['valor_pago'].sum().reset_index()
                pagamentos_por_canal = pagamentos_por_canal.sort_values('valor_pago', ascending=False)

                fig_canal = px.pie(
                    pagamentos_por_canal, names='tipo_pagamento', values='valor_pago',
                    title='Distribuição da Arrecadação por Canal de Pagamento',
                    hole=0.4
                )
                st.plotly_chart(fig_canal, use_container_width=True)

                st.subheader("Clientes que Pagaram por Canal")
                qtd_por_canal = df_pagamentos_campanha.groupby('tipo_pagamento')['matricula'].nunique().reset_index()
                qtd_por_canal.rename(columns={'matricula': 'Clientes que Pagaram'}, inplace=True)
                qtd_por_canal = qtd_por_canal.sort_values('Clientes que Pagaram', ascending=False)

                fig_canal_qtd = px.bar(
                    qtd_por_canal, x='tipo_pagamento', y='Clientes que Pagaram',
                    title='Clientes que Pagaram por Canal',
                    labels={'tipo_pagamento': 'Canal de Pagamento', 'Clientes que Pagaram': 'Clientes que Pagaram'},
                    color='tipo_pagamento'
                )
                st.plotly_chart(fig_canal_qtd, use_container_width=True)

                tab_canal = pd.merge(pagamentos_por_canal, qtd_por_canal, on='tipo_pagamento')
                tab_canal.columns = ['Canal de Pagamento', 'Valor Total Pago', 'Clientes que Pagaram']
                tab_canal['Valor Total Pago'] = tab_canal['Valor Total Pago'].apply(fmt_brl)
                st.dataframe(tab_canal, use_container_width=True, hide_index=True)

                # --- NOVO: Cruzamento de Canal por Diretoria e Cidade ---
                tem_cidade    = 'cidade'    in df_pagamentos_campanha.columns
                tem_diretoria = 'diretoria' in df_pagamentos_campanha.columns

                if tem_diretoria:
                    st.subheader("Canal de Pagamento por Diretoria")
                    canal_diretoria = df_pagamentos_campanha.groupby(['diretoria', 'tipo_pagamento'])['valor_pago'].sum().reset_index()
                    fig_canal_dir = px.bar(
                        canal_diretoria, x='diretoria', y='valor_pago', color='tipo_pagamento',
                        title='Valor Arrecadado: Diretoria x Canal de Pagamento',
                        labels={'diretoria': 'Diretoria', 'valor_pago': 'Valor (R$)', 'tipo_pagamento': 'Canal'},
                        barmode='stack' # Empilha as barras para ver a composição
                    )
                    st.plotly_chart(fig_canal_dir, use_container_width=True)

                if tem_cidade:
                    st.subheader("Canal de Pagamento por Cidade")
                    canal_cidade = df_pagamentos_campanha.groupby(['cidade', 'tipo_pagamento'])['valor_pago'].sum().reset_index()
                    # Ordena para as cidades com maior arrecadação aparecerem primeiro
                    ordem_cidades = canal_cidade.groupby('cidade')['valor_pago'].sum().sort_values(ascending=False).index
                    fig_canal_cid = px.bar(
                        canal_cidade, x='cidade', y='valor_pago', color='tipo_pagamento',
                        title='Valor Arrecadado: Cidade x Canal de Pagamento',
                        labels={'cidade': 'Cidade', 'valor_pago': 'Valor (R$)', 'tipo_pagamento': 'Canal'},
                        barmode='stack',
                        category_orders={'cidade': ordem_cidades}
                    )
                    st.plotly_chart(fig_canal_cid, use_container_width=True)

            else:
                st.info("Coluna 'tipo_pagamento' não encontrada no arquivo de pagamentos.")

        # ══════════════════════════════════════════════════════════
        # ABA 4 — ARRECADAÇÃO POR LOCAL
        # ══════════════════════════════════════════════════════════
        with aba4:
            st.subheader("Arrecadação por Localidade")

            tem_cidade = 'cidade' in df_pagamentos_campanha.columns
            tem_diretoria = 'diretoria' in df_pagamentos_campanha.columns

            if tem_cidade:
                st.subheader("Arrecadação por Cidade")
                arrecadacao_cidade = df_pagamentos_campanha.groupby('cidade')['valor_pago'].sum().reset_index()
                arrecadacao_cidade = arrecadacao_cidade.sort_values('valor_pago', ascending=False)
                fig_cidade = px.bar(
                    arrecadacao_cidade, x='cidade', y='valor_pago',
                    title='Arrecadação Total por Cidade',
                    labels={'cidade': 'Cidade', 'valor_pago': 'Valor Arrecadado (R$)'}
                )
                st.plotly_chart(fig_cidade, use_container_width=True)
            else:
                st.info("Coluna 'cidade' não encontrada nos dados de pagamentos.")

            if tem_diretoria:
                st.subheader("Arrecadação por Diretoria")
                arrecadacao_diretoria = df_pagamentos_campanha.groupby('diretoria')['valor_pago'].sum().reset_index()
                arrecadacao_diretoria = arrecadacao_diretoria.sort_values('valor_pago', ascending=False)
                fig_diretoria = px.bar(
                    arrecadacao_diretoria, x='diretoria', y='valor_pago',
                    title='Arrecadação Total por Diretoria',
                    labels={'diretoria': 'Diretoria', 'valor_pago': 'Valor Arrecadado (R$)'}
                )
                st.plotly_chart(fig_diretoria, use_container_width=True)
            else:
                st.info("Coluna 'diretoria' não encontrada nos dados de pagamentos.")

        # ══════════════════════════════════════════════════════════
        # ABA 5 — DETALHES
        # ══════════════════════════════════════════════════════════
        with aba5:
            if not df_pagamentos_campanha.empty:
                st.subheader("Detalhes dos Pagamentos Atribuídos à Campanha")

                colunas_possiveis = [
                    'matricula', 'cidade', 'diretoria', 'telefone_envio',
                    'data_envio', 'data_pagamento', 'vencimento',
                    'valor_pago', 'dias_apos_envio',
                    'tipo_fatura', 'utilizacao', 'tipo_pagamento'
                ]
                colunas_exibicao = [c for c in colunas_possiveis if c in df_pagamentos_campanha.columns]
                df_detalhes = df_pagamentos_campanha[colunas_exibicao].drop_duplicates(
                    subset=['matricula', 'data_pagamento', 'valor_pago']
                )

                st.dataframe(df_detalhes, use_container_width=True, hide_index=True)

                csv_output = df_detalhes.to_csv(index=False, sep=';', decimal=',')
                st.download_button(
                    label="⬇️ Baixar Detalhes dos Pagamentos (CSV)",
                    data=csv_output,
                    file_name="pagamentos_campanha.csv",
                    mime="text/csv"
                )
            else:
                st.info("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

        # ══════════════════════════════════════════════════════════
        # ABA 6 — NOVAS VISUALIZAÇÕES (LABORATÓRIO)
        # ══════════════════════════════════════════════════════════
        with aba6:
            if not df_pagamentos_campanha.empty:
                st.header("Exploração de Novas Visualizações")
                st.markdown("Avalie estes gráficos. Os que forem úteis podem ser movidos para as abas principais depois.")

                # 1. Curva de Arrecadação Acumulada
                st.subheader("📈 Curva de Arrecadação Acumulada")
                df_acumulado = df_pagamentos_campanha.groupby('dias_apos_envio')['valor_pago'].sum().reset_index()
                df_acumulado['valor_acumulado'] = df_acumulado['valor_pago'].cumsum()
                fig_acumulado = px.line(
                    df_acumulado, x='dias_apos_envio', y='valor_acumulado',
                    title='Evolução da Arrecadação (Acumulada ao longo dos dias)',
                    labels={'dias_apos_envio': 'Dias Após o Envio', 'valor_acumulado': 'Valor Acumulado (R$)'},
                    markers=True
                )
                st.plotly_chart(fig_acumulado, use_container_width=True, key="fig_acumulado_aba6")

                # 2. Canal de Pagamento por Cidade (Gráfico Empilhado)
                if 'cidade' in df_pagamentos_campanha.columns and 'tipo_pagamento' in df_pagamentos_campanha.columns:
                    st.subheader("🏙️ Canal de Pagamento por Cidade")
                    canal_cidade = df_pagamentos_campanha.groupby(['cidade', 'tipo_pagamento'])['valor_pago'].sum().reset_index()
                    ordem_cidades = canal_cidade.groupby('cidade')['valor_pago'].sum().sort_values(ascending=False).index
                    fig_canal_cid = px.bar(
                        canal_cidade, x='cidade', y='valor_pago', color='tipo_pagamento',
                        title='Valor Arrecadado: Cidade x Canal de Pagamento',
                        labels={'cidade': 'Cidade', 'valor_pago': 'Valor (R$)', 'tipo_pagamento': 'Canal'},
                        barmode='stack',
                        category_orders={'cidade': ordem_cidades}
                    )
                    st.plotly_chart(fig_canal_cid, use_container_width=True, key="fig_canal_cid_aba6")

                # 3. Ticket Médio por Cidade
                if 'cidade' in df_pagamentos_campanha.columns:
                    st.subheader("🎫 Ticket Médio por Cidade")
                    tm_cidade = df_pagamentos_campanha.groupby('cidade').agg(
                        Valor=('valor_pago', 'sum'),
                        Clientes=('matricula', 'nunique')
                    ).reset_index()
                    tm_cidade['ticket_medio'] = tm_cidade['Valor'] / tm_cidade['Clientes']
                    tm_cidade = tm_cidade.sort_values('ticket_medio', ascending=False)
                    fig_tm_cid = px.bar(
                        tm_cidade, x='cidade', y='ticket_medio',
                        title='Ticket Médio por Cidade',
                        labels={'cidade': 'Cidade', 'ticket_medio': 'Ticket Médio (R$)'},
                        text_auto='.2f'
                    )
                    st.plotly_chart(fig_tm_cid, use_container_width=True, key="fig_tm_cid_aba6")

                # 4. Mapa de Calor: Dia do Pagamento x Canal
                if 'tipo_pagamento' in df_pagamentos_campanha.columns:
                    st.subheader("🔥 Concentração: Tempo de Pagamento x Canal")
                    heatmap_data = df_pagamentos_campanha.groupby(['tipo_pagamento', 'dias_apos_envio'])['valor_pago'].sum().reset_index()
                    fig_heat = px.density_heatmap(
                        heatmap_data, x='dias_apos_envio', y='tipo_pagamento', z='valor_pago',
                        title='Mapa de Calor: Em quais dias cada canal arrecada mais?',
                        labels={'dias_apos_envio': 'Dias Após Envio', 'tipo_pagamento': 'Canal', 'valor_pago': 'Valor (R$)'},
                        color_continuous_scale='Viridis'
                    )
                    st.plotly_chart(fig_heat, use_container_width=True, key="fig_heat_aba6")

                # 5. Utilização (Subcategoria)
                if 'utilizacao' in df_pagamentos_campanha.columns:
                    st.subheader("💧 Arrecadação por Tipo de Utilização")
                    util_resumo = df_pagamentos_campanha.groupby('utilizacao')['valor_pago'].sum().reset_index().sort_values('valor_pago', ascending=False)
                    fig_util = px.pie(
                        util_resumo, names='utilizacao', values='valor_pago',
                        title='Distribuição por Utilização (Subcategoria)',
                        hole=0.4
                    )
                    st.plotly_chart(fig_util, use_container_width=True, key="fig_util_aba6")

    elif executar_analise and not dados_prontos:
        if campanha_selecionada is None:
            st.warning("Selecione uma campanha antes de executar a análise.")
        elif df_pagamentos is None or df_pagamentos.empty:
            st.warning("Base de pagamentos não disponível ou vazia. Um administrador precisa fazer o upload.")
        elif df_envios is None or df_envios.empty:
            st.warning("Não foi possível carregar os envios da campanha selecionada ou a tabela está vazia.")
        elif df_clientes is None or df_clientes.empty:
            st.warning("Não foi possível carregar os clientes da campanha selecionada ou a tabela está vazia.")

    elif not executar_analise:
        if campanha_selecionada is None:
            st.info("👈 Selecione uma campanha na barra lateral para começar.")
        else:
            st.info("👈 Clique em **Executar Análise** na barra lateral para gerar os resultados.")