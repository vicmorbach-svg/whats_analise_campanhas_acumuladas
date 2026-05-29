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
from sqlalchemy.types import String, DateTime, Float, Numeric # Importar tipos para criação de tabela

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
def write_to_postgres(df, table_name, if_exists='append', index=False, dtype=None):
    engine = get_sql_engine()
    if engine is None:
        st.error("Conexão com o banco de dados não estabelecida.")
        return False

    try:
        # Garante que os nomes das colunas do DataFrame estejam em minúsculas
        df.columns = [col.lower() for col in df.columns]
        df.to_sql(table_name.lower(), engine, if_exists=if_exists, index=index, dtype=dtype)
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
        "valor_pago": Numeric(precision=10, scale=2), # Usar Numeric para valores monetários
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
    colunas_categoricas = ['cidade', 'tipo_pagamento', 'utilizacao', 'tipo_fatura']
    for col in colunas_categoricas:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Conversão de tipos de data e numéricos
    for col in ['data_pagamento', 'vencimento']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_convert(fuso_br)

    if 'valor_pago' in df.columns:
        df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce')

    return df

def process_uploaded_pagamentos(uploaded_file):
    try:
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file, sep=';', decimal=',')
        elif uploaded_file.name.endswith('.parquet'):
            df = pd.read_parquet(uploaded_file)
        else:
            st.error("Formato de arquivo não suportado. Por favor, envie um arquivo CSV ou Parquet.")
            return None

        # Padronização dos nomes das colunas para minúsculas
        df.columns = [col.lower() for col in df.columns]

        # Renomear colunas para o padrão do banco de dados
        df = df.rename(columns={
            'matricula': 'matricula_pagamento',
            'data': 'data_pagamento',
            'valor': 'valor_pago',
            'tipo': 'tipo_pagamento'
        })

        # Verificar colunas essenciais
        colunas_essenciais = ['matricula_pagamento', 'data_pagamento', 'valor_pago']
        if not all(col in df.columns for col in colunas_essenciais):
            st.error(f"O arquivo de pagamentos deve conter as colunas: {', '.join(colunas_essenciais)}")
            return None

        # Conversão de tipos
        df['data_pagamento'] = pd.to_datetime(df['data_pagamento'], errors='coerce', dayfirst=True)
        df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce')

        # Remover linhas com valores nulos nas colunas essenciais
        df.dropna(subset=colunas_essenciais, inplace=True)

        # Converter data_pagamento para o fuso horário correto
        df['data_pagamento'] = df['data_pagamento'].dt.tz_localize(fuso_br, ambiguous='NaT', nonexistent='NaT')

        # Preencher colunas opcionais com valores padrão se não existirem
        colunas_opcionais = {
            'cidade': 'N/A',
            'tipo_pagamento': 'N/A',
            'vencimento': pd.NaT,
            'utilizacao': 'N/A',
            'tipo_fatura': 'N/A'
        }
        for col, default_val in colunas_opcionais.items():
            if col not in df.columns:
                df[col] = default_val
            # Garantir que 'vencimento' seja datetime se for preenchido
            if col == 'vencimento' and not df[col].empty:
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.tz_localize(fuso_br, ambiguous='NaT', nonexistent='NaT')


        return df
    except Exception as e:
        st.error(f"Erro ao processar o arquivo de pagamentos: {e}")
        return None

def update_pagamentos_db(df_novo):
    # Assegura que a tabela exista antes de qualquer operação
    if not ensure_pagamentos_table_exists():
        return False, 0, 0

    # Garante que as colunas do df_novo estejam em minúsculas
    df_novo.columns = [col.lower() for col in df_novo.columns]

    # Colunas que formam a chave única para identificar pagamentos
    chave_pagamento = ['matricula_pagamento', 'data_pagamento', 'valor_pago']

    # 1. Carregar apenas as chaves dos pagamentos existentes para economizar memória
    df_chaves_existentes = read_from_postgres(TABLE_PAGAMENTOS, columns=chave_pagamento)

    # Se não houver pagamentos existentes, todos os novos são únicos
    if df_chaves_existentes.empty:
        df_novos_unicos = df_novo
        num_existentes = 0
    else:
        # Converter colunas de data para o mesmo tipo para comparação
        for col in ['data_pagamento']:
            if col in df_chaves_existentes.columns:
                df_chaves_existentes[col] = pd.to_datetime(df_chaves_existentes[col], errors='coerce', utc=True).dt.tz_convert(fuso_br)
            if col in df_novo.columns:
                df_novo[col] = pd.to_datetime(df_novo[col], errors='coerce', utc=True).dt.tz_convert(fuso_br)

        # Criar uma "chave" para cada linha para facilitar a comparação de duplicatas
        df_chaves_existentes['__chave__'] = df_chaves_existentes[chave_pagamento].astype(str).agg('_'.join, axis=1)
        df_novo['__chave__'] = df_novo[chave_pagamento].astype(str).agg('_'.join, axis=1)

        # Filtrar apenas os pagamentos em df_novo que não estão em df_chaves_existentes
        df_novos_unicos = df_novo[~df_novo['__chave__'].isin(df_chaves_existentes['__chave__'])].drop(columns=['__chave__'])
        num_existentes = len(df_chaves_existentes)

    # Remover a coluna temporária '__chave__' do df_novo original, se ela existir
    if '__chave__' in df_novo.columns:
        df_novo = df_novo.drop(columns=['__chave__'])

    # 2. Inserir apenas os novos pagamentos no banco de dados
    if not df_novos_unicos.empty:
        # Mapear os tipos do Pandas para os tipos do SQLAlchemy para garantir a criação correta das colunas
        # Isso é importante para o 'append' se a tabela for criada pela primeira vez com este df_novos_unicos
        dtype_map = {
            "matricula_pagamento": String(50),
            "data_pagamento": DateTime(),
            "valor_pago": Numeric(precision=10, scale=2),
            "cidade": String(100),
            "tipo_pagamento": String(50),
            "vencimento": DateTime(),
            "utilizacao": String(100),
            "tipo_fatura": String(50)
        }
        # Filtrar dtype_map para incluir apenas as colunas presentes em df_novos_unicos
        dtype_map_filtered = {k: v for k, v in dtype_map.items() if k in df_novos_unicos.columns}

        ok = write_to_postgres(df_novos_unicos, TABLE_PAGAMENTOS, if_exists='append', dtype=dtype_map_filtered)
        if not ok:
            return False, 0, 0
    else:
        ok = True # Nada para inserir, então a operação foi "bem-sucedida"

    # Limpar o cache para que os dados atualizados sejam carregados na próxima vez
    load_pagamentos_db.clear()
    gc.collect() # Força a coleta de lixo para liberar memória

    # Contar o total de registros após a atualização (carregando apenas uma coluna para economizar memória)
    df_total_count = read_from_postgres(TABLE_PAGAMENTOS, columns=['matricula_pagamento'])
    total_registros_db = len(df_total_count) if df_total_count is not None else 0

    novos_adicionados = len(df_novos_unicos)

    return ok, total_registros_db, novos_adicionados

# ══════════════════════════════════════════════════════════════
# INTERFACE DO STREAMLIT
# ══════════════════════════════════════════════════════════════

if "logged_in" not in st.session_state:
    st.session_state["logged_in"] = False

if not st.session_state["logged_in"]:
    login_screen()
else:
    # Garante que a tabela de pagamentos exista ao iniciar o app
    ensure_pagamentos_table_exists()

    st.sidebar.title(f"Bem-vindo, {st.session_state['username']}!")
    st.sidebar.button("Sair", on_click=lambda: st.session_state.update({"logged_in": False, "username": None, "role": None}))

    st.sidebar.header("Configurações de Campanha")
    df_campanhas = load_campanhas_meta()
    campanhas_nomes = ["-- Selecione uma Campanha --"] + df_campanhas['nome'].tolist()
    campanha_selecionada_nome = st.sidebar.selectbox("Campanha Ativa", campanhas_nomes)

    campanha_selecionada = None
    if campanha_selecionada_nome != "-- Selecione uma Campanha --":
        campanha_selecionada_row = df_campanhas[df_campanhas['nome'] == campanha_selecionada_nome].iloc[0]
        campanha_selecionada = campanha_selecionada_row['id']
        st.sidebar.info(f"Campanha '{campanha_selecionada_nome}' (ID: {campanha_selecionada}) selecionada.")

    if is_admin():
        st.sidebar.header("Administração de Dados")
        with st.sidebar.expander("Gerenciar Campanhas"):
            st.subheader("Criar Nova Campanha")
            with st.form("nova_campanha_form"):
                novo_nome_campanha = st.text_input("Nome da Nova Campanha")
                uploaded_envios = st.file_uploader("Upload de Envios (CSV)", type=["csv"], key="upload_envios_nova")
                uploaded_clientes = st.file_uploader("Upload de Clientes (CSV)", type=["csv"], key="upload_clientes_nova")
                submit_nova_campanha = st.form_submit_button("Criar Campanha")

                if submit_nova_campanha:
                    if novo_nome_campanha and uploaded_envios and uploaded_clientes:
                        df_envios_new = process_uploaded_envios(uploaded_envios)
                        df_clientes_new = process_uploaded_clientes(uploaded_clientes)
                        if df_envios_new is not None and df_clientes_new is not None:
                            with st.spinner("Criando nova campanha..."):
                                campanha_id, erro = save_campanha(novo_nome_campanha, df_envios_new, df_clientes_new)
                                if campanha_id:
                                    st.success(f"Campanha '{novo_nome_campanha}' criada com sucesso! ID: {campanha_id}")
                                    st.rerun()
                                else:
                                    st.error(f"Falha ao criar campanha: {erro}")
                        else:
                            st.error("Erro ao processar arquivos de envios ou clientes.")
                    else:
                        st.warning("Por favor, preencha todos os campos para criar uma nova campanha.")

            st.subheader("Atualizar Campanha Existente")
            if campanha_selecionada:
                with st.form("atualizar_campanha_form"):
                    st.write(f"Atualizando campanha: **{campanha_selecionada_nome}** (ID: {campanha_selecionada})")
                    uploaded_envios_update = st.file_uploader("Upload de Envios Adicionais (CSV)", type=["csv"], key="upload_envios_update")
                    uploaded_clientes_update = st.file_uploader("Upload de Clientes Adicionais (CSV)", type=["csv"], key="upload_clientes_update")
                    submit_update_campanha = st.form_submit_button("Atualizar Campanha")

                    if submit_update_campanha:
                        df_envios_update = None
                        df_clientes_update = None
                        if uploaded_envios_update:
                            df_envios_update = process_uploaded_envios(uploaded_envios_update)
                        if uploaded_clientes_update:
                            df_clientes_update = process_uploaded_clientes(uploaded_clientes_update)

                        if (uploaded_envios_update and df_envios_update is None) or \
                           (uploaded_clientes_update and df_clientes_update is None):
                            st.error("Erro ao processar arquivos de atualização.")
                        else:
                            with st.spinner("Atualizando campanha..."):
                                ok, erro = update_campanha(campanha_selecionada, campanha_selecionada_nome, df_envios_update, df_clientes_update)
                                if ok:
                                    st.success(f"Campanha '{campanha_selecionada_nome}' atualizada com sucesso!")
                                    st.rerun()
                                else:
                                    st.error(f"Falha ao atualizar campanha: {erro}")
            else:
                st.info("Selecione uma campanha para atualizá-la.")

            st.subheader("Excluir Campanha")
            if campanha_selecionada:
                if st.button(f"Excluir Campanha '{campanha_selecionada_nome}'", key="delete_campanha_btn"):
                    if st.warning(f"Tem certeza que deseja excluir a campanha '{campanha_selecionada_nome}' (ID: {campanha_selecionada}) e todos os seus dados? Esta ação é irreversível."):
                        if st.button("Confirmar Exclusão", key="confirm_delete_campanha_btn"):
                            with st.spinner("Excluindo campanha..."):
                                if delete_campanha(campanha_selecionada, campanha_selecionada_nome):
                                    st.success(f"Campanha '{campanha_selecionada_nome}' excluída com sucesso.")
                                    st.rerun()
                                else:
                                    st.error("Falha ao excluir campanha.")
            else:
                st.info("Selecione uma campanha para excluí-la.")

        with st.sidebar.expander("Upload de Pagamentos"):
            st.subheader("Upload de Arquivo de Pagamentos")
            uploaded_pagamentos_file = st.file_uploader("Selecione um arquivo de pagamentos (CSV ou Parquet)", type=["csv", "parquet"], key="upload_pagamentos")

            if uploaded_pagamentos_file:
                if st.button("Processar e Salvar Pagamentos"):
                    with st.spinner("Processando e salvando pagamentos..."):
                        df_pagamentos_upload = process_uploaded_pagamentos(uploaded_pagamentos_file)
                        if df_pagamentos_upload is not None:
                            ok, total_registros, novos_adicionados = update_pagamentos_db(df_pagamentos_upload)
                            if ok:
                                st.success(f"Pagamentos processados e salvos com sucesso! Total no DB: {total_registros}. Novos adicionados: {novos_adicionados}.")
                                st.rerun()
                            else:
                                st.error("Falha ao salvar pagamentos no banco de dados.")
                        else:
                            st.error("Erro ao processar o arquivo de pagamentos.")

    st.sidebar.markdown("---")
    executar_analise = st.sidebar.button("Executar Análise", type="primary")

    df_pagamentos = load_pagamentos_db()
    df_envios = None
    df_clientes = None
    dados_prontos = False

    if campanha_selecionada:
        df_envios = load_campanha_envios(campanha_selecionada)
        df_clientes = load_campanha_clientes(campanha_selecionada)

        if df_pagamentos is not None and not df_pagamentos.empty and \
           df_envios is not None and not df_envios.empty and \
           df_clientes is not None and not df_clientes.empty:
            dados_prontos = True
        else:
            st.warning("Dados da campanha ou pagamentos não disponíveis. Verifique os uploads.")

    if executar_analise and dados_prontos:
        st.title(f"Análise da Campanha: {campanha_selecionada_nome}")

        # Mesclar df_envios com df_clientes para obter informações completas dos clientes
        df_envios_clientes = pd.merge(
            df_envios, df_clientes,
            left_on='telefone_envio', right_on='telefone_cliente',
            how='left', suffixes=('_envio', '_cliente')
        )
        # Renomear matricula_cliente para matricula para consistência
        df_envios_clientes = df_envios_clientes.rename(columns={'matricula_cliente': 'matricula'})

        # Mesclar df_envios_clientes com df_pagamentos
        # Usar 'matricula' e 'data_envio' para atribuir pagamentos à campanha
        df_pagamentos_campanha = pd.merge(
            df_envios_clientes, df_pagamentos,
            left_on='matricula', right_on='matricula_pagamento',
            how='inner'
        )

        # Filtrar pagamentos dentro da janela de 30 dias após o envio
        df_pagamentos_campanha['data_envio'] = pd.to_datetime(df_pagamentos_campanha['data_envio'], errors='coerce', utc=True).dt.tz_convert(fuso_br)
        df_pagamentos_campanha['data_pagamento'] = pd.to_datetime(df_pagamentos_campanha['data_pagamento'], errors='coerce', utc=True).dt.tz_convert(fuso_br)

        df_pagamentos_campanha = df_pagamentos_campanha[
            (df_pagamentos_campanha['data_pagamento'] >= df_pagamentos_campanha['data_envio']) &
            (df_pagamentos_campanha['data_pagamento'] <= df_pagamentos_campanha['data_envio'] + timedelta(days=30))
        ].copy() # Adicionado .copy() para evitar SettingWithCopyWarning

        if df_pagamentos_campanha.empty:
            st.warning("Nenhum pagamento encontrado para esta campanha dentro da janela de 30 dias após o envio.")
            dados_prontos = False # Reseta dados_prontos se não houver pagamentos atribuídos
        else:
            # Calcular dias_apos_envio
            df_pagamentos_campanha['dias_apos_envio'] = (df_pagamentos_campanha['data_pagamento'] - df_pagamentos_campanha['data_envio']).dt.days

            # Downcasting de colunas categóricas no DataFrame final da campanha
            colunas_categoricas_campanha = ['cidade', 'diretoria', 'tipo_pagamento', 'utilizacao', 'tipo_fatura']
            for col in colunas_categoricas_campanha:
                if col in df_pagamentos_campanha.columns:
                    df_pagamentos_campanha[col] = df_pagamentos_campanha[col].astype('category')

        # Limpar memória de DataFrames intermediários
        del df_envios_clientes
        gc.collect()

        aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
            "Visão Geral", "Arrecadação por Tempo", "Canais e Localidades",
            "Análise de Clientes", "Detalhes", "Novas Visualizações"
        ])

        # ══════════════════════════════════════════════════════════
        # ABA 1 — VISÃO GERAL
        # ══════════════════════════════════════════════════════════
        with aba1:
            if not df_pagamentos_campanha.empty:
                st.subheader("Resumo da Campanha")
                total_arrecadado = df_pagamentos_campanha['valor_pago'].sum()
                total_clientes_pagantes = df_pagamentos_campanha['matricula'].nunique()
                ticket_medio = total_arrecadado / total_clientes_pagantes if total_clientes_pagantes > 0 else 0

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Arrecadado", f"R$ {total_arrecadado:,.2f}")
                with col2:
                    st.metric("Clientes Pagantes", f"{total_clientes_pagantes}")
                with col3:
                    st.metric("Ticket Médio", f"R$ {ticket_medio:,.2f}")

                st.subheader("Distribuição de Pagamentos por Valor")
                fig_hist = px.histogram(
                    df_pagamentos_campanha, x='valor_pago', nbins=50,
                    title='Distribuição dos Valores de Pagamento',
                    labels={'valor_pago': 'Valor do Pagamento (R$)'}
                )
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("Nenhum pagamento encontrado para análise na campanha selecionada.")

        # ══════════════════════════════════════════════════════════
        # ABA 2 — ARRECADAÇÃO POR TEMPO
        # ══════════════════════════════════════════════════════════
        with aba2:
            if not df_pagamentos_campanha.empty:
                st.subheader("Arrecadação Diária")
                arrecadacao_diaria = df_pagamentos_campanha.groupby(df_pagamentos_campanha['data_pagamento'].dt.date)['valor_pago'].sum().reset_index()
                arrecadacao_diaria.columns = ['data', 'valor_pago']
                fig_diaria = px.line(
                    arrecadacao_diaria, x='data', y='valor_pago',
                    title='Arrecadação Diária da Campanha',
                    labels={'data': 'Data', 'valor_pago': 'Valor (R$)'},
                    markers=True
                )
                st.plotly_chart(fig_diaria, use_container_width=True)

                st.subheader("Arrecadação por Dias Após o Envio")
                arrecadacao_dias_apos_envio = df_pagamentos_campanha.groupby('dias_apos_envio')['valor_pago'].sum().reset_index()
                fig_dias_apos = px.bar(
                    arrecadacao_dias_apos_envio, x='dias_apos_envio', y='valor_pago',
                    title='Arrecadação por Dias Após o Envio da Mensagem',
                    labels={'dias_apos_envio': 'Dias Após Envio', 'valor_pago': 'Valor (R$)'}
                )
                st.plotly_chart(fig_dias_apos, use_container_width=True)
            else:
                st.info("Nenhum pagamento encontrado para análise de tempo na campanha selecionada.")

        # ══════════════════════════════════════════════════════════
        # ABA 3 — CANAIS E LOCALIDADES
        # ══════════════════════════════════════════════════════════
        with aba3:
            if 'tipo_pagamento' in df_pagamentos_campanha.columns:
                st.subheader("Arrecadação por Canal de Pagamento")
                canal_pagamento = df_pagamentos_campanha.groupby('tipo_pagamento')['valor_pago'].sum().reset_index()
                fig_canal = px.pie(
                    canal_pagamento, names='tipo_pagamento', values='valor_pago',
                    title='Distribuição da Arrecadação por Canal de Pagamento',
                    hole=0.4
                )
                st.plotly_chart(fig_canal, use_container_width=True)
            else:
                st.info("Coluna 'tipo_pagamento' não encontrada nos dados de pagamentos.")

            tem_cidade = 'cidade' in df_pagamentos_campanha.columns
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

        # ══════════════════════════════════════════════════════════
        # ABA 4 — ANÁLISE DE CLIENTES
        # ══════════════════════════════════════════════════════════
        with aba4:
            if not df_pagamentos_campanha.empty:
                st.subheader("Clientes Pagantes por Cidade")
                clientes_por_cidade = df_pagamentos_campanha.groupby('cidade')['matricula'].nunique().reset_index()
                clientes_por_cidade.columns = ['cidade', 'total_clientes']
                fig_clientes_cidade = px.bar(
                    clientes_por_cidade, x='cidade', y='total_clientes',
                    title='Número de Clientes Pagantes por Cidade',
                    labels={'cidade': 'Cidade', 'total_clientes': 'Total de Clientes'}
                )
                st.plotly_chart(fig_clientes_cidade, use_container_width=True)

                st.subheader("Ticket Médio por Cidade")
                ticket_medio_cidade = df_pagamentos_campanha.groupby('cidade')['valor_pago'].sum() / df_pagamentos_campanha.groupby('cidade')['matricula'].nunique()
                ticket_medio_cidade = ticket_medio_cidade.reset_index()
                ticket_medio_cidade.columns = ['cidade', 'ticket_medio']
                fig_tm_cidade = px.bar(
                    ticket_medio_cidade, x='cidade', y='ticket_medio',
                    title='Ticket Médio por Cliente Pagante por Cidade',
                    labels={'cidade': 'Cidade', 'ticket_medio': 'Ticket Médio (R$)'}
                )
                st.plotly_chart(fig_tm_cidade, use_container_width=True)
            else:
                st.info("Nenhum pagamento encontrado para análise de clientes na campanha selecionada.")

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