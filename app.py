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
    colunas_categoricas = ['cidade', 'tipo_pagamento', 'utilizacao', 'tipo_fatura'] # Adicionei mais colunas
    for col in colunas_categoricas:
        if col in df.columns:
            df[col] = df[col].astype('category')

    # Reduz o peso da coluna de valor
    if 'valor_pago' in df.columns:
        df['valor_pago'] = pd.to_numeric(df['valor_pago'], downcast='float', errors='coerce')

    return df

def update_pagamentos_db(df_novo): # Renomeada para update_pagamentos_db
    # Garante que as colunas do df_novo estejam em minúsculas para consistência
    df_novo.columns = [col.lower() for col in df_novo.columns]

    # Carrega apenas as colunas de identificação para verificar duplicatas
    # Isso evita carregar o DataFrame completo se ele for muito grande
    colunas_chave = ['matricula_pagamento', 'data_pagamento', 'valor_pago']
    df_existente_chaves = read_from_postgres(TABLE_PAGAMENTOS, columns=colunas_chave)

    if df_existente_chaves is not None and not df_existente_chaves.empty:
        # Cria uma coluna de chave composta para facilitar a comparação
        df_novo['__chave__'] = df_novo[colunas_chave].astype(str).agg('_'.join, axis=1)
        df_existente_chaves['__chave__'] = df_existente_chaves[colunas_chave].astype(str).agg('_'.join, axis=1)

        # Filtra df_novo para manter apenas os registros que não estão em df_existente_chaves
        df_novos_unicos = df_novo[~df_novo['__chave__'].isin(df_existente_chaves['__chave__'])].copy()
        df_novos_unicos = df_novos_unicos.drop(columns=['__chave__']) # Remove a coluna chave temporária
    else:
        # Se não há dados existentes, todos os novos são únicos
        df_novos_unicos = df_novo.copy()

    total_novos_a_inserir = len(df_novos_unicos)

    if total_novos_a_inserir > 0:
        # Inserir apenas os registros novos e únicos
        ok = write_to_postgres(df_novos_unicos, TABLE_PAGAMENTOS, if_exists='append')
        if not ok:
            return False, 0, 0 # Retorna erro se a inserção falhar
    else:
        ok = True # Nada para inserir, mas a operação foi "bem-sucedida"

    # Recarrega o total de registros após a possível inserção
    df_total_pagamentos = read_from_postgres(TABLE_PAGAMENTOS, columns=['matricula_pagamento']) # Carrega apenas uma coluna para contar
    total_registros_apos = len(df_total_pagamentos) if df_total_pagamentos is not None else 0

    load_pagamentos_db.clear() # Limpa o cache após a atualização
    return ok, total_registros_apos, total_novos_a_inserir

# ══════════════════════════════════════════════════════════════
# PROCESSAMENTO DE ARQUIVOS (MANTIDO, pois o upload ainda é de arquivos)
# ══════════════════════════════════════════════════════════════

def process_pagamentos_file(uploaded_file):
    try:
        df_pagamentos_raw = pd.read_csv(uploaded_file, sep=';', decimal=',')

        # Padronização de nomes de colunas para minúsculas
        df_pagamentos_raw.columns = [col.lower().strip() for col in df_pagamentos_raw.columns]

        # Renomear colunas para o padrão esperado no DB
        col_mapping = {
            'matricula': 'matricula_pagamento',
            'data_pagto': 'data_pagamento',
            'valor_pago': 'valor_pago',
            'tipo_pagto': 'tipo_pagamento',
            'vencimento': 'vencimento',
            'utilizacao': 'utilizacao',
            'tipo_fatura': 'tipo_fatura',
            'cidade': 'cidade'
        }
        df_pagamentos_raw.rename(columns=col_mapping, inplace=True)

        # Conversão de tipos
        if 'data_pagamento' in df_pagamentos_raw.columns:
            df_pagamentos_raw['data_pagamento'] = pd.to_datetime(df_pagamentos_raw['data_pagamento'], errors='coerce')
        if 'vencimento' in df_pagamentos_raw.columns:
            df_pagamentos_raw['vencimento'] = pd.to_datetime(df_pagamentos_raw['vencimento'], errors='coerce')
        if 'valor_pago' in df_pagamentos_raw.columns:
            df_pagamentos_raw['valor_pago'] = pd.to_numeric(df_pagamentos_raw['valor_pago'], errors='coerce')

        # Remover linhas com valores essenciais nulos
        df_pagamentos_raw.dropna(subset=['matricula_pagamento', 'data_pagamento', 'valor_pago'], inplace=True)

        return df_pagamentos_raw
    except Exception as e:
        st.error(f"Erro ao processar arquivo de pagamentos: {e}")
        return None

def process_envios_file(uploaded_file):
    try:
        df_envios_raw = pd.read_csv(uploaded_file, sep=';', decimal=',')
        df_envios_raw.columns = [col.lower().strip() for col in df_envios_raw.columns]
        col_mapping = {
            'telefone': 'telefone_envio',
            'data_envio': 'data_envio',
            'campanha': 'campanha_nome'
        }
        df_envios_raw.rename(columns=col_mapping, inplace=True)
        if 'data_envio' in df_envios_raw.columns:
            df_envios_raw['data_envio'] = pd.to_datetime(df_envios_raw['data_envio'], errors='coerce')
        df_envios_raw.dropna(subset=['telefone_envio', 'data_envio'], inplace=True)
        return df_envios_raw
    except Exception as e:
        st.error(f"Erro ao processar arquivo de envios: {e}")
        return None

def process_clientes_file(uploaded_file):
    try:
        df_clientes_raw = pd.read_csv(uploaded_file, sep=';', decimal=',')
        df_clientes_raw.columns = [col.lower().strip() for col in df_clientes_raw.columns]
        col_mapping = {
            'telefone': 'telefone_cliente',
            'matricula': 'matricula_cliente',
            'situacao': 'situacao',
            'cidade': 'cidade',
            'diretoria': 'diretoria'
        }
        df_clientes_raw.rename(columns=col_mapping, inplace=True)
        df_clientes_raw.dropna(subset=['telefone_cliente', 'matricula_cliente'], inplace=True)
        return df_clientes_raw
    except Exception as e:
        st.error(f"Erro ao processar arquivo de clientes: {e}")
        return None

# ══════════════════════════════════════════════════════════════
# FUNÇÕES DE CÁLCULO E FORMATAÇÃO
# ══════════════════════════════════════════════════════════════

def fmt_brl(valor):
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ══════════════════════════════════════════════════════════════
# LAYOUT DO STREAMLIT
# ══════════════════════════════════════════════════════════════

if "logged_in" not in st.session_state or not st.session_state["logged_in"]:
    login_screen()
else:
    # Garante que a tabela de pagamentos exista antes de qualquer operação
    ensure_pagamentos_table_exists()

    st.sidebar.title(f"Bem-vindo, {st.session_state['username']}!")
    if st.sidebar.button("Sair"):
        st.session_state["logged_in"] = False
        st.session_state["username"]  = None
        st.session_state["role"]      = None
        st.rerun()

    st.sidebar.header("Gerenciamento de Campanhas")
    campanhas_meta = load_campanhas_meta()
    campanha_nomes = ["-- Selecione uma Campanha --"] + campanhas_meta['nome'].tolist()
    campanha_selecionada_nome = st.sidebar.selectbox("Campanha Ativa", campanha_nomes)

    campanha_selecionada = None
    if campanha_selecionada_nome != "-- Selecione uma Campanha --":
        campanha_selecionada = campanhas_meta[campanhas_meta['nome'] == campanha_selecionada_nome].iloc[0]

    if is_admin():
        st.sidebar.subheader("Administração")
        with st.sidebar.expander("Upload de Dados"):
            st.markdown("---")
            st.markdown("### Nova Campanha")
            nova_campanha_nome = st.text_input("Nome da Nova Campanha", key="nova_campanha_nome_input")
            uploaded_envios = st.file_uploader("Upload Envios (CSV)", type="csv", key="upload_envios_nova")
            uploaded_clientes = st.file_uploader("Upload Clientes (CSV)", type="csv", key="upload_clientes_nova")
            if st.button("Criar Nova Campanha", key="btn_criar_campanha"):
                if nova_campanha_nome and uploaded_envios and uploaded_clientes:
                    df_envios = process_envios_file(uploaded_envios)
                    df_clientes = process_clientes_file(uploaded_clientes)
                    if df_envios is not None and df_clientes is not None:
                        with st.spinner("Criando campanha..."):
                            camp_id, erro = save_campanha(nova_campanha_nome, df_envios, df_clientes)
                            if camp_id:
                                st.success(f"Campanha '{nova_campanha_nome}' criada com sucesso! ID: {camp_id}")
                                load_campanhas_meta.clear() # Limpa o cache para atualizar a lista
                                st.rerun()
                            else:
                                st.error(f"Erro ao criar campanha: {erro}")
                    else:
                        st.error("Erro ao processar arquivos de envios ou clientes.")
                else:
                    st.warning("Preencha o nome e faça upload de ambos os arquivos para criar a campanha.")
            st.markdown("---")
            st.markdown("### Atualizar Campanha Existente")
            if campanha_selecionada is not None:
                st.write(f"Atualizando: **{campanha_selecionada['nome']}**")
                uploaded_envios_update = st.file_uploader("Upload Novos Envios (CSV)", type="csv", key="upload_envios_update")
                uploaded_clientes_update = st.file_uploader("Upload Novos Clientes (CSV)", type="csv", key="upload_clientes_update")
                if st.button("Atualizar Campanha", key="btn_atualizar_campanha"):
                    if uploaded_envios_update or uploaded_clientes_update:
                        df_envios_update = None
                        df_clientes_update = None
                        if uploaded_envios_update:
                            df_envios_update = process_envios_file(uploaded_envios_update)
                        if uploaded_clientes_update:
                            df_clientes_update = process_clientes_file(uploaded_clientes_update)

                        if (uploaded_envios_update and df_envios_update is None) or \
                           (uploaded_clientes_update and df_clientes_update is None):
                            st.error("Erro ao processar um dos arquivos de atualização.")
                        else:
                            with st.spinner("Atualizando campanha..."):
                                sucesso, erro = update_campanha(
                                    campanha_selecionada['id'],
                                    campanha_selecionada['nome'],
                                    df_envios_update,
                                    df_clientes_update
                                )
                                if sucesso:
                                    st.success(f"Campanha '{campanha_selecionada['nome']}' atualizada com sucesso!")
                                    st.rerun()
                                else:
                                    st.error(f"Erro ao atualizar campanha: {erro}")
                    else:
                        st.warning("Faça upload de pelo menos um arquivo para atualizar a campanha.")
            else:
                st.info("Selecione uma campanha para atualizá-la.")
            st.markdown("---")
            st.markdown("### Upload de Pagamentos")
            uploaded_pagamentos = st.file_uploader("Upload Pagamentos (CSV)", type="csv", key="upload_pagamentos")
            if st.button("Processar Pagamentos", key="btn_processar_pagamentos"):
                if uploaded_pagamentos:
                    df_pagamentos_processado = process_pagamentos_file(uploaded_pagamentos)
                    if df_pagamentos_processado is not None and not df_pagamentos_processado.empty:
                        with st.spinner("Atualizando base de pagamentos..."):
                            ok, total_registros, novos_inseridos = update_pagamentos_db(df_pagamentos_processado)
                            if ok:
                                st.success(f"Pagamentos processados com sucesso! Total de registros: {total_registros}. Novos inseridos: {novos_inseridos}.")
                                load_pagamentos_db.clear() # Limpa o cache para recarregar
                                gc.collect() # Força a coleta de lixo
                                st.rerun()
                            else:
                                st.error("Erro ao atualizar base de pagamentos.")
                    else:
                        st.error("Arquivo de pagamentos vazio ou com erro no processamento.")
                else:
                    st.warning("Faça upload de um arquivo de pagamentos.")
            st.markdown("---")
            st.markdown("### Excluir Campanha")
            if campanha_selecionada is not None:
                st.write(f"Excluir: **{campanha_selecionada['nome']}** (ID: {campanha_selecionada['id']})")
                confirm_delete = st.checkbox(f"Confirmo a exclusão da campanha '{campanha_selecionada['nome']}'", key="confirm_delete_campanha")
                if confirm_delete and st.button("Excluir Campanha", key="btn_excluir_campanha"):
                    with st.spinner("Excluindo campanha..."):
                        if delete_campanha(campanha_selecionada['id'], campanha_selecionada['nome']):
                            st.success(f"Campanha '{campanha_selecionada['nome']}' excluída com sucesso!")
                            load_campanhas_meta.clear()
                            st.rerun()
                        else:
                            st.error("Erro ao excluir campanha.")
            else:
                st.info("Selecione uma campanha para excluí-la.")

    st.sidebar.markdown("---")
    executar_analise = st.sidebar.button("Executar Análise", type="primary")

    df_pagamentos = None
    df_envios = None
    df_clientes = None
    dados_prontos = False

    if campanha_selecionada is not None:
        df_pagamentos = load_pagamentos_db()
        df_envios = load_campanha_envios(campanha_selecionada['id'])
        df_clientes = load_campanha_clientes(campanha_selecionada['id'])

        if df_pagamentos is not None and not df_pagamentos.empty and \
           df_envios is not None and not df_envios.empty and \
           df_clientes is not None and not df_clientes.empty:
            dados_prontos = True

    if executar_analise and dados_prontos:
        st.title(f"Análise da Campanha: {campanha_selecionada['nome']}")

        # --- Processamento e Merge dos Dados ---
        # Certifica-se de que as colunas de merge estão no formato correto
        df_envios['telefone_envio'] = df_envios['telefone_envio'].astype(str)
        df_clientes['telefone_cliente'] = df_clientes['telefone_cliente'].astype(str)
        df_pagamentos['matricula_pagamento'] = df_pagamentos['matricula_pagamento'].astype(str)
        df_clientes['matricula_cliente'] = df_clientes['matricula_cliente'].astype(str)

        # Merge 1: Envios com Clientes
        df_campanha = pd.merge(
            df_envios, df_clientes,
            left_on='telefone_envio', right_on='telefone_cliente',
            how='inner', suffixes=('_envio', '_cliente')
        )
        df_campanha.drop(columns=['telefone_cliente'], inplace=True) # Remove coluna duplicada

        # Merge 2: Campanha com Pagamentos
        df_pagamentos_campanha = pd.merge(
            df_campanha, df_pagamentos,
            left_on='matricula_cliente', right_on='matricula_pagamento',
            how='inner', suffixes=('_campanha', '_pagamento')
        )
        df_pagamentos_campanha.drop(columns=['matricula_pagamento'], inplace=True) # Remove coluna duplicada

        # Filtrar pagamentos dentro da janela de 30 dias após o envio
        df_pagamentos_campanha['dias_apos_envio'] = (df_pagamentos_campanha['data_pagamento'] - df_pagamentos_campanha['data_envio']).dt.days
        df_pagamentos_campanha = df_pagamentos_campanha[
            (df_pagamentos_campanha['dias_apos_envio'] >= 0) &
            (df_pagamentos_campanha['dias_apos_envio'] <= 30)
        ]

        # Renomear matricula_cliente para matricula para consistência
        df_pagamentos_campanha.rename(columns={'matricula_cliente': 'matricula'}, inplace=True)

        # Limpeza de memória de DataFrames intermediários
        del df_campanha
        gc.collect()

        # --- Métricas Principais ---
        total_clientes_campanha = df_clientes['matricula_cliente'].nunique()
        total_envios_campanha = df_envios['telefone_envio'].nunique()
        total_pagamentos_atribuidos = df_pagamentos_campanha['matricula'].nunique()
        valor_total_arrecadado = df_pagamentos_campanha['valor_pago'].sum()
        ticket_medio = valor_total_arrecadado / total_pagamentos_atribuidos if total_pagamentos_atribuidos > 0 else 0

        st.subheader("Métricas Chave")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Clientes na Campanha", total_clientes_campanha)
        col2.metric("Envios Realizados", total_envios_campanha)
        col3.metric("Clientes que Pagararam (Atribuídos)", total_pagamentos_atribuidos)
        col4.metric("Valor Total Arrecadado", fmt_brl(valor_total_arrecadado))
        st.metric("Ticket Médio por Cliente", fmt_brl(ticket_medio))

        aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
            "Visão Geral", "Arrecadação por Tempo", "Arrecadação por Canal",
            "Arrecadação por Local", "Detalhes", "Laboratório"
        ])

        # ══════════════════════════════════════════════════════════
        # ABA 1 — VISÃO GERAL
        # ══════════════════════════════════════════════════════════
        with aba1:
            st.subheader("Visão Geral da Arrecadação")
            # Gráfico de arrecadação diária
            arrecadacao_diaria = df_pagamentos_campanha.groupby('data_pagamento')['valor_pago'].sum().reset_index()
            fig_diaria = px.line(
                arrecadacao_diaria, x='data_pagamento', y='valor_pago',
                title='Arrecadação Diária',
                labels={'data_pagamento': 'Data do Pagamento', 'valor_pago': 'Valor Arrecadado (R$)'}
            )
            st.plotly_chart(fig_diaria, use_container_width=True)

            # Gráfico de clientes únicos pagantes por dia
            clientes_pagantes_diarios = df_pagamentos_campanha.groupby('data_pagamento')['matricula'].nunique().reset_index()
            fig_clientes_diarios = px.line(
                clientes_pagantes_diarios, x='data_pagamento', y='matricula',
                title='Clientes Únicos Pagantes por Dia',
                labels={'data_pagamento': 'Data do Pagamento', 'matricula': 'Número de Clientes'}
            )
            st.plotly_chart(fig_clientes_diarios, use_container_width=True)

        # ══════════════════════════════════════════════════════════
        # ABA 2 — ARRECADAÇÃO POR TEMPO
        # ══════════════════════════════════════════════════════════
        with aba2:
            st.subheader("Arrecadação por Tempo de Resposta")
            arrecadacao_por_dias = df_pagamentos_campanha.groupby('dias_apos_envio')['valor_pago'].sum().reset_index()
            fig_dias = px.bar(
                arrecadacao_por_dias, x='dias_apos_envio', y='valor_pago',
                title='Arrecadação por Dias Após o Envio',
                labels={'dias_apos_envio': 'Dias Após o Envio', 'valor_pago': 'Valor Arrecadado (R$)'}
            )
            st.plotly_chart(fig_dias, use_container_width=True)

            st.subheader("Clientes Pagantes por Tempo de Resposta")
            clientes_por_dias = df_pagamentos_campanha.groupby('dias_apos_envio')['matricula'].nunique().reset_index()
            fig_clientes_dias = px.bar(
                clientes_por_dias, x='dias_apos_envio', y='matricula',
                title='Clientes Únicos Pagantes por Dias Após o Envio',
                labels={'dias_apos_envio': 'Dias Após o Envio', 'matricula': 'Número de Clientes'}
            )
            st.plotly_chart(fig_clientes_dias, use_container_width=True)

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
                st.plotly_chart(fig_canal, use_container_width=True, key="fig_canal_aba4")

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
        # ABA 4 — ARRECADAÇÃO POR LOCAL
        # ══════════════════════════════════════════════════════════
        with aba4:
            st.subheader("Arrecadação por Localidade")

            tem_cidade    = 'cidade'    in df_pagamentos_campanha.columns
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