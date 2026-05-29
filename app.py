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
from sqlalchemy.exc import ProgrammingError # Importar ProgrammingError do SQLAlchemy

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
    except ProgrammingError as e:
        # Captura o erro específico de tabela não existente
        if "relation" in str(e) and "does not exist" in str(e):
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

    # Dropar a tabela explicitamente antes de reescrever
    # Isso garante que o esquema seja recriado do zero com base no df_combined
    # e evita o erro de "column already present" se houver alguma inconsistência
    # no esquema anterior ou na forma como o 'replace' estava sendo interpretado.
    execute_sql_command(f'DROP TABLE IF EXISTS "{TABLE_PAGAMENTOS.lower()}"')

    # Escreve a base combinada de volta no PostgreSQL.
    # Com a tabela dropada, 'append' irá criá-la com o esquema do df_combined.
    # Se a tabela já existisse (o que não deve acontecer após o DROP), 'append' adicionaria.
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
            if df.shape[1] > 12:
                col_indices.extend([1, 2, 10, 11, 12])
                col_names.extend(['CIDADE', 'DIRETORIA', 'TIPO_PAGAMENTO', 'VENCIMENTO', 'TIPO_FATURA'])
            elif df.shape[1] >= 10:
                col_indices.extend([1, 2, 9])
                col_names.extend(['CIDADE', 'DIRETORIA', 'TIPO_PAGAMENTO'])

            df_pag = df.iloc[:, col_indices].copy()
            df_pag.columns = col_names
        else:
            # Mantém apenas as colunas úteis que foram encontradas
            colunas_desejadas = ['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO']
            for col in ['CIDADE', 'DIRETORIA', 'TIPO_PAGAMENTO', 'VENCIMENTO', 'TIPO_FATURA', 'UTILIZACAO']:
                if col in df.columns:
                    colunas_desejadas.append(col)
            df_pag = df[colunas_desejadas].copy()

        # 4. Tratamento e Limpeza dos Dados
        df_pag['MATRICULA_PAGAMENTO'] = (
            df_pag['MATRICULA_PAGAMENTO']
            .astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
        )

        df_pag['DATA_PAGAMENTO'] = pd.to_datetime(df_pag['DATA_PAGAMENTO'], errors='coerce', dayfirst=True)

        # Tratamento de Valor Pago (remove R$, espaços, converte vírgula pra ponto)
        if df_pag['VALOR_PAGO'].dtype == object:
            df_pag['VALOR_PAGO'] = (
                df_pag['VALOR_PAGO']
                .astype(str)
                .str.replace('R$', '', regex=False)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .str.strip()
            )
        df_pag['VALOR_PAGO'] = pd.to_numeric(df_pag['VALOR_PAGO'], errors='coerce')

        df_pag.dropna(subset=['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO'], inplace=True)

        if df_pag.empty:
            st.error("Nenhuma linha válida restou após o processamento. Verifique os formatos de data e valor.")
            return None

        # 5. Colunas Opcionais
        if 'TIPO_PAGAMENTO' in df_pag.columns:
            df_pag['TIPO_PAGAMENTO'] = df_pag['TIPO_PAGAMENTO'].astype(str).str.strip().replace('nan', 'Não informado')

        if 'VENCIMENTO' in df_pag.columns:
            df_pag['VENCIMENTO']     = pd.to_datetime(df_pag['VENCIMENTO'], errors='coerce', dayfirst=True)
            df_pag['MES_FATURA']     = df_pag['VENCIMENTO'].dt.month
            df_pag['ANO_FATURA']     = df_pag['VENCIMENTO'].dt.year
            df_pag['MES_ANO_FATURA'] = df_pag['VENCIMENTO'].dt.strftime('%m/%Y')

        if 'TIPO_FATURA' in df_pag.columns:
            df_pag['TIPO_FATURA'] = df_pag['TIPO_FATURA'].astype(str).str.strip().replace('nan', 'Não informado')

        if 'UTILIZACAO' in df_pag.columns:
            df_pag['UTILIZACAO'] = df_pag['UTILIZACAO'].astype(str).str.strip().replace('nan', 'Não informado')

        # Otimização de Memória (Downcasting)
        colunas_categoricas = ['CIDADE', 'TIPO_PAGAMENTO', 'TIPO_FATURA', 'UTILIZACAO']
        for col in colunas_categoricas:
            if col in df_pag.columns:
                df_pag[col] = df_pag[col].astype('category')

        return df_pag

    except Exception as e:
        st.error(f"Erro ao processar Pagamentos: {e}")
        return None

def fmt_brl(valor):
    try: return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

# ══════════════════════════════════════════════════════════════
# INTERFACE STREAMLIT
# ══════════════════════════════════════════════════════════════

st.set_page_config(layout="wide", page_title="Análise de campanha de cobrança")

if not st.session_state.get("logged_in"):
    login_screen()
    st.stop()

# Exibe mensagens de sucesso persistentes
if "msg_sucesso" in st.session_state:
    st.sidebar.success(st.session_state["msg_sucesso"])
    del st.session_state["msg_sucesso"]

st.title("📊 Análise de eficiência de campanha de cobrança via Whatsapp")

st.sidebar.markdown(f"👤 **{st.session_state['username']}**")
if st.sidebar.button("Sair"):
    st.session_state.clear()
    st.rerun()

st.sidebar.markdown("---")

# --- NOVO: Indicador fixo de pagamentos na base ---
st.sidebar.header("🏦 Resumo da Base")
df_pag_geral = load_pagamentos_db() # Agora carrega do DB
total_pag_geral = len(df_pag_geral) if df_pag_geral is not None else 0
st.sidebar.metric("Total de Pagamentos Cadastrados", f"{total_pag_geral:,}".replace(",", "."))
st.sidebar.markdown("---")

st.sidebar.header("📋 Campanhas")
df_meta = load_campanhas_meta()
campanhas_disponiveis = df_meta['nome'].tolist() if not df_meta.empty else []

campanha_selecionada_nome = st.sidebar.selectbox("Selecionar campanha", ["(nenhuma)"] + campanhas_disponiveis)
campanha_selecionada = None

if campanha_selecionada_nome != "(nenhuma)":
    campanha_selecionada = df_meta[df_meta['nome'] == campanha_selecionada_nome].iloc[0]
    if is_admin() and st.sidebar.button("🗑️ Excluir esta campanha"):
        if delete_campanha(campanha_selecionada['id'], campanha_selecionada_nome):
            st.session_state["msg_sucesso"] = "Campanha excluída com sucesso!"
        else:
            st.error("Erro ao excluir campanha.")
        st.rerun()

janela_dias = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 10)
executar_analise = st.sidebar.button("▶️ Executar Análise")

if is_admin():
    st.sidebar.header("🔧 Administração")
    with st.sidebar.expander("➕ Nova Campanha"):
        nome_nova = st.text_input("Nome da campanha")
        up_env = st.file_uploader("Envios (.xlsx, .parquet)", type=["xlsx", "parquet"], key="n_env")
        up_cli = st.file_uploader("Clientes (.xlsx, .parquet)", type=["xlsx", "parquet"], key="n_cli")
        if st.button("Salvar campanha") and nome_nova and up_env and up_cli:
            camp_id, erro = save_campanha(nome_nova, load_and_process_envios(up_env), load_and_process_clientes(up_cli))
            if camp_id:
                st.session_state["msg_sucesso"] = "Campanha salva!"
            else:
                st.error(erro)
            st.rerun()

    with st.sidebar.expander("🔄 Atualizar Campanha"):
        if not df_meta.empty:
            camp_upd = st.selectbox("Campanha", df_meta['nome'].tolist())
            up_env_u = st.file_uploader("Novos Envios", type=["xlsx", "parquet"], key="u_env")
            up_cli_u = st.file_uploader("Novos Clientes", type=["xlsx", "parquet"], key="u_cli")
            if st.button("Atualizar") and (up_env_u or up_cli_u):
                cid = df_meta[df_meta['nome'] == camp_upd].iloc[0]['id']
                ok, erro = update_campanha(cid, camp_upd, load_and_process_envios(up_env_u) if up_env_u else None, load_and_process_clientes(up_cli_u) if up_cli_u else None)
                if ok:
                    st.session_state["msg_sucesso"] = "Campanha atualizada!"
                else:
                    st.error(erro)
                st.rerun()

    with st.sidebar.expander("💰 Base de Pagamentos"):
        up_pag = st.file_uploader("Pagamentos", type=["csv", "xlsx", "parquet"])
        if st.button("Enviar Pagamentos") and up_pag:
            ok, total, novos = update_pagamentos_db(load_and_process_pagamentos(up_pag)) # Agora atualiza no DB
            if ok: st.success(f"Pagamentos atualizados! Total: {total} | Novos: {novos}")
            else: st.error("Erro ao atualizar pagamentos.")

# ══════════════════════════════════════════════════════════════
# CARREGAMENTO DOS DADOS
# ══════════════════════════════════════════════════════════════

df_envios     = None
df_clientes   = None
df_pagamentos = None
dados_prontos = False

if campanha_selecionada is not None:
    with st.spinner("Carregando dados da campanha..."):
        df_envios   = load_campanha_envios(campanha_selecionada['id'])
        df_clientes = load_campanha_clientes(campanha_selecionada['id'])
        df_pagamentos = load_pagamentos_db() # Agora carrega do DB

    # Verifica se todos os 3 arquivos foram carregados com sucesso
    dados_prontos = (
        df_envios is not None and not df_envios.empty and
        df_clientes is not None and not df_clientes.empty and
        df_pagamentos is not None and not df_pagamentos.empty
    )
    if not dados_prontos:
        st.warning("Alguns dados da campanha não puderam ser carregados. Verifique se as tabelas existem ou se há dados nelas.")

# ══════════════════════════════════════════════════════════════
# ANÁLISE (VERSÃO OTIMIZADA PARA MEMÓRIA)
# ══════════════════════════════════════════════════════════════

if executar_analise and dados_prontos:

    # ── Cruzamento envios x clientes ──────────────────────────
    total_clientes_unicos_base_envios = df_envios['telefone_envio'].nunique()
    total_base_envio = df_envios['telefone_envio'].count()
    total_clientes_notificados = df_envios[df_envios['status_envio'] == 'delivered_to_handset']['telefone_envio'].nunique()
    total_envios_rejeitados    = df_envios[df_envios['status_envio'] != 'delivered_to_handset']['telefone_envio'].count()
    taxa_eficiencia_disparos   = (total_clientes_notificados / total_clientes_unicos_base_envios * 100) if total_clientes_unicos_base_envios > 0 else 0

    df_merge = pd.merge(
        df_envios,
        df_clientes,
        left_on='telefone_envio',
        right_on='telefone_cliente',
        how='inner'
    )

    if df_merge.empty:
        st.error("Nenhum cliente encontrado após cruzamento entre envios e clientes.")
        st.stop()

    # Garante tipo string nos campos de matrícula para o merge
    df_merge['matricula_cliente'] = df_merge['matricula_cliente'].astype(str).str.strip()
    df_pagamentos['matricula_pagamento'] = df_pagamentos['matricula_pagamento'].astype(str).str.strip()

    # ── Cálculos de Dívida (Removendo duplicatas por cliente) ──
    total_divida_base_envios = df_merge.drop_duplicates(subset=['matricula_cliente'])['situacao'].sum()

    df_entregues = df_merge[df_merge['status_envio'] == 'delivered_to_handset']
    total_divida_notificados = df_entregues.drop_duplicates(subset=['matricula_cliente'])['situacao'].sum()

    # ── OTIMIZAÇÃO DE MEMÓRIA: Pré-filtragem ──────────────────
    # Descobre quais matrículas realmente importam para esta campanha
    matriculas_alvo = df_merge['matricula_cliente'].unique()

    # Filtra a base gigante de pagamentos ANTES de fazer o merge
    # Aqui, a otimização é que df_pagamentos já vem filtrado do DB se possível,
    # mas o Pandas ainda faz a filtragem final em memória.
    df_pagamentos_filtrado = df_pagamentos[df_pagamentos['matricula_pagamento'].isin(matriculas_alvo)].copy()

    # Libera a memória da base gigante original (opcional, mas recomendado)
    del df_pagamentos
    load_pagamentos_db.clear() # Limpa o cache da função de carregamento de pagamentos
    gc.collect() # Força a limpeza da RAM

    # --- AJUSTE: Remove Cidade/Diretoria do cliente para usar a do Pagamento ---
    colunas_remover_cliente = [c for c in ['cidade', 'diretoria'] if c in df_merge.columns]
    df_merge = df_merge.drop(columns=colunas_remover_cliente)

    # ── Cruzamento final (agora muito mais leve) ──────────────
    df_cruzado = pd.merge(
        df_merge,
        df_pagamentos_filtrado,
        left_on='matricula_cliente',
        right_on='matricula_pagamento',
        how='inner'
    )

    # Limpa variáveis temporárias que não serão mais usadas
    del df_merge
    del df_pagamentos_filtrado
    gc.collect()

    if df_cruzado.empty:
        st.error("Nenhum pagamento encontrado após cruzamento com a base de clientes.")
        st.stop()

    df_cruzado['dias_apos_envio'] = (
        df_cruzado['data_pagamento'] - df_cruzado['data_envio']
    ).dt.days

    df_pagamentos_campanha = df_cruzado[
        (df_cruzado['dias_apos_envio'] >= 0) &
        (df_cruzado['dias_apos_envio'] <= janela_dias)
    ].copy()

    # Limpa o dataframe cruzado completo
    del df_cruzado
    gc.collect()

    df_pagamentos_campanha = df_pagamentos_campanha.drop_duplicates(
        subset=['matricula_cliente', 'data_pagamento', 'valor_pago', 'vencimento'],
        keep='first'
    )

    # Garante que 'matricula' exista para os gráficos
    if 'matricula_cliente' in df_pagamentos_campanha.columns:
        df_pagamentos_campanha.rename(columns={'matricula_cliente': 'matricula'}, inplace=True)
    elif 'matricula_pagamento' in df_pagamentos_campanha.columns:
        df_pagamentos_campanha.rename(columns={'matricula_pagamento': 'matricula'}, inplace=True)

    # ── Métricas da Campanha ──────────────────────────────────
    clientes_unicos_que_pagaram_matriculas = df_pagamentos_campanha['matricula'].nunique()
    qtd_pagamentos = len(df_pagamentos_campanha)
    valor_total_arrecadado = df_pagamentos_campanha['valor_pago'].sum()

    taxa_eficiencia_clientes_base_envios = (clientes_unicos_que_pagaram_matriculas / total_clientes_unicos_base_envios * 100) if total_clientes_unicos_base_envios > 0 else 0
    taxa_eficiencia_clientes_notificados = (clientes_unicos_que_pagaram_matriculas / total_clientes_notificados * 100) if total_clientes_notificados > 0 else 0

    taxa_eficiencia_valor_base = (valor_total_arrecadado / total_divida_base_envios * 100) if total_divida_base_envios > 0 else 0
    taxa_eficiencia_valor_notificados = (valor_total_arrecadado / total_divida_notificados * 100) if total_divida_notificados > 0 else 0

    ticket_medio = (valor_total_arrecadado / clientes_unicos_que_pagaram_matriculas) if clientes_unicos_que_pagaram_matriculas > 0 else 0

    # Custo da campanha (exemplo: R$ 0.10 por envio entregue)
    custo_por_envio = 0.10
    custo_campanha = total_clientes_notificados * custo_por_envio
    roi = ((valor_total_arrecadado - custo_campanha) / custo_campanha * 100) if custo_campanha > 0 else 0

    # ══════════════════════════════════════════════════════════
    # ABAS DE VISUALIZAÇÃO
    # ══════════════════════════════════════════════════════════
    aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
        "Resumo", "Cidade e Diretoria", "Faturas", "Canais", "Detalhes", "Laboratório"
    ])

    # ══════════════════════════════════════════════════════════
    # ABA 1 — RESUMO
    # ══════════════════════════════════════════════════════════
    with aba1:
        st.header(f"Resultados da Campanha: {campanha_selecionada_nome}")

        st.markdown("##### 📱 Funil de Disparos")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Clientes na base de envios", f"{total_clientes_unicos_base_envios:,}")
        col2.metric("Clientes notificados", f"{total_clientes_notificados:,}")
        col3.metric("Envios rejeitados", f"{total_envios_rejeitados:,}")
        col4.metric("Eficiência dos disparos", f"{taxa_eficiencia_disparos:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        st.markdown("##### 💰 Conversão e Arrecadação")
        col5, col6 = st.columns(2)
        col5.metric("Clientes que pagaram",   f"{clientes_unicos_que_pagaram_matriculas:,}")
        col6.metric("Quantidade de pagamentos", f"{qtd_pagamentos:,}")

        col7, col8 = st.columns(2)
        col7.metric("Taxa de eficiência base envios",  f"{taxa_eficiencia_clientes_base_envios:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)
        col8.metric("Taxa de eficiência clientes notificados",  f"{taxa_eficiencia_clientes_notificados:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)

        col9, col10, col11 = st.columns(3)
        col9.metric("Dívida total da base", fmt_brl(total_divida_base_envios))
        col10.metric("Dívida dos notificados", fmt_brl(total_divida_notificados))
        col11.metric("Valor total arrecadado",          fmt_brl(valor_total_arrecadado))

        col12, col13 = st.columns(2)
        col12.metric("Taxa eficiênica dívida total", f"{taxa_eficiencia_valor_base:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)
        col13.metric("Taxa eficiênica dívida notificada", f"{taxa_eficiencia_valor_notificados:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)

        col14, col15, col16, col17 = st.columns(4)
        col14.metric("Ticket médio",   fmt_brl(ticket_medio))
        col15.metric("Total de disparos", f"{total_base_envio:,}")
        col16.metric("Custo da campanha", fmt_brl(custo_campanha))
        col17.metric("ROI",              f"{roi:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        if not df_pagamentos_campanha.empty:
            st.subheader(f"Pagamentos por Dia Após o Envio (Janela de {janela_dias} dias)")

            pagamentos_por_dia = df_pagamentos_campanha.groupby('dias_apos_envio')['valor_pago'].sum().reset_index()
            pagamentos_por_dia.rename(columns={'dias_apos_envio': 'Dias Após Envio', 'valor_pago': 'Valor Total Pago'}, inplace=True)

            fig_dias = px.bar(
                pagamentos_por_dia,
                x='Dias Após Envio', y='Valor Total Pago',
                title='Valor Arrecadado por Dia Após o Envio',
                labels={'Dias Após Envio': 'Dias Após o Envio', 'Valor Total Pago': 'Valor Total Pago (R$)'},
                hover_data={'Valor Total Pago': ':.2f'}
            )
            fig_dias.update_layout(xaxis_title="Dias Após o Envio", yaxis_title="Valor Total Pago (R$)")
            st.plotly_chart(fig_dias, use_container_width=True, key="fig_dias")

    # ══════════════════════════════════════════════════════════
    # ABA 2 — CIDADE E DIRETORIA
    # ══════════════════════════════════════════════════════════
    with aba2:
        if not df_pagamentos_campanha.empty:
            tem_cidade    = 'cidade'    in df_pagamentos_campanha.columns
            tem_diretoria = 'diretoria' in df_pagamentos_campanha.columns

            if tem_cidade:
                st.subheader("Análise por Cidade")
                cidade_resumo = df_pagamentos_campanha.groupby('cidade').agg(
                    Clientes_que_Pagaram=('matricula', 'nunique'),
                    Valor_Arrecadado=('valor_pago', 'sum')
                ).reset_index().sort_values('Valor_Arrecadado', ascending=False)

                fig_cidade_valor = px.bar(
                    cidade_resumo, x='cidade', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Cidade',
                    labels={'cidade': 'Cidade', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'}
                )
                st.plotly_chart(fig_cidade_valor, use_container_width=True, key="fig_cidade_valor")

            if tem_diretoria:
                st.subheader("Análise por Diretoria")
                diretoria_resumo = df_pagamentos_campanha.groupby('diretoria').agg(
                    Clientes_que_Pagaram=('matricula', 'nunique'),
                    Valor_Arrecadado=('valor_pago', 'sum')
                ).reset_index().sort_values('Valor_Arrecadado', ascending=False)

                fig_diretoria_valor = px.bar(
                    diretoria_resumo, x='diretoria', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Diretoria',
                    labels={'diretoria': 'Diretoria', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'}
                )
                st.plotly_chart(fig_diretoria_valor, use_container_width=True, key="fig_diretoria_valor")

            if not tem_cidade and not tem_diretoria:
                st.info("Colunas 'cidade' e 'diretoria' não encontradas na base de clientes.")

    # ══════════════════════════════════════════════════════════
    # ABA 3 — ANÁLISE DAS FATURAS
    # ══════════════════════════════════════════════════════════
    with aba3:
        if not df_pagamentos_campanha.empty:
            if 'vencimento' in df_pagamentos_campanha.columns:
                st.subheader("Antiguidade da Dívida Paga")
                df_pagamentos_campanha['antiguidade_dias'] = (df_pagamentos_campanha['data_pagamento'] - df_pagamentos_campanha['vencimento']).dt.days

                def classificar_antiguidade(dias):
                    if pd.isna(dias): return 'Não informado'
                    elif dias <= 10:  return '0-10 dias'
                    elif dias <= 20:  return '11-20 dias'
                    elif dias <= 30:  return '21-30 dias'
                    elif dias <= 60:  return '31-60 dias'
                    else:             return 'Mais de 61 dias'

                df_pagamentos_campanha['faixa_antiguidade'] = df_pagamentos_campanha['antiguidade_dias'].apply(classificar_antiguidade)
                antiguidade_resumo = df_pagamentos_campanha.groupby('faixa_antiguidade')['valor_pago'].sum().reset_index()

                fig_ant_valor = px.bar(
                    antiguidade_resumo, x='faixa_antiguidade', y='valor_pago',
                    title='Valor Pago por Faixa de Antiguidade da Dívida',
                    labels={'faixa_antiguidade': 'Faixa de Antiguidade', 'valor_pago': 'Valor Pago (R$)'}
                )
                st.plotly_chart(fig_ant_valor, use_container_width=True, key="fig_ant_valor")

            if 'mes_ano_fatura' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Mês/Ano da Fatura")
                mes_ano_resumo = df_pagamentos_campanha.groupby('mes_ano_fatura')['valor_pago'].sum().reset_index()
                fig_mes_ano = px.bar(
                    mes_ano_resumo, x='mes_ano_fatura', y='valor_pago',
                    title='Valor Pago por Mês/Ano da Fatura',
                    labels={'mes_ano_fatura': 'Mês/Ano da Fatura', 'valor_pago': 'Valor Pago (R$)'}
                )
                st.plotly_chart(fig_mes_ano, use_container_width=True, key="fig_mes_ano")

    # ══════════════════════════════════════════════════════════
    # ABA 4 — CANAL DE PAGAMENTO
    # ══════════════════════════════════════════════════════════
    with aba4:
        if not df_pagamentos_campanha.empty and 'tipo_pagamento' in df_pagamentos_campanha.columns:
            st.subheader("Valor Arrecadado por Canal de Pagamento")
            pagamentos_por_canal = df_pagamentos_campanha.groupby('tipo_pagamento')['valor_pago'].sum().reset_index()
            pagamentos_por_canal = pagamentos_por_canal.sort_values('valor_pago', ascending=False)

            fig_canal_aba4 = px.bar(
                pagamentos_por_canal, x='tipo_pagamento', y='valor_pago',
                title='Valor Arrecadado por Canal de Pagamento',
                labels={'tipo_pagamento': 'Canal de Pagamento', 'valor_pago': 'Valor Total Pago (R$)'},
                color='tipo_pagamento'
            )
            st.plotly_chart(fig_canal_aba4, use_container_width=True, key="fig_canal_aba4")

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
            st.plotly_chart(fig_canal_qtd, use_container_width=True, key="fig_canal_qtd")

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