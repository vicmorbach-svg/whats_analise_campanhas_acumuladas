import streamlit as st
import polars as pl
import pandas as pd
from datetime import timedelta
import plotly.express as px
import io
import base64
import requests
import json
import uuid
import gc
import datetime
import pytz

# Configura o fuso horário do Brasil
fuso_br = pytz.timezone('America/Sao_Paulo')
hora_atual = datetime.datetime.now(fuso_br).hour

# Define o funcionamento das 08h às 18h
if hora_atual < 8 or hora_atual >= 18:
    st.cache_data.clear()
    st.title("🌙 Sistema em Repouso")
    st.info("O painel de análise funciona apenas das 08h às 18h para economia de recursos.")
    st.stop()

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
        if username in users and str(users[username]["password"]) == str(password):
            st.session_state["logged_in"] = True
            st.session_state["username"]  = username
            st.session_state["role"]      = users[username]["role"]
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos.")

def is_admin():
    return st.session_state.get("role") == "admin"

# ══════════════════════════════════════════════════════════════
# GITHUB — Integração
# ══════════════════════════════════════════════════════════════

def get_github_config():
    try:
        token  = st.secrets["github"]["token"]
        repo   = st.secrets["github"]["repo"]
        branch = st.secrets["github"].get("branch", "main")
        return token, repo, branch
    except Exception:
        return None, None, None

def get_github_headers():
    token, _, _ = get_github_config()
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

def get_file_sha(path):
    token, repo, branch = get_github_config()
    if not token: return None
    url = f"https://api.github.com/repos/{repo}/contents/{path}?ref={branch}"
    r   = requests.get(url, headers=get_github_headers())
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict): return data.get("sha")
    return None

def get_file_from_github(path):
    token, repo, branch = get_github_config()
    if not token: return None, None
    raw_url = f"https://raw.githubusercontent.com/{repo}/{branch}/{path}"
    r = requests.get(raw_url, headers={"Authorization": f"token {token}"})
    if r.status_code == 200 and len(r.content) > 0:
        return r.content, get_file_sha(path)
    return None, None

def save_file_to_github(path, content_bytes, message):
    token, repo, branch = get_github_config()
    if not token: return False
    sha = get_file_sha(path)
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    payload = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch":  branch
    }
    if sha: payload["sha"] = sha
    r = requests.put(url, headers=get_github_headers(), data=json.dumps(payload))
    return r.status_code in [200, 201]

def delete_file_from_github(path, message):
    token, repo, branch = get_github_config()
    if not token: return False
    sha = get_file_sha(path)
    if not sha: return True
    url = f"https://api.github.com/repos/{repo}/contents/{path}"
    payload = {"message": message, "sha": sha, "branch": branch}
    r = requests.delete(url, headers=get_github_headers(), data=json.dumps(payload))
    return r.status_code == 200

def df_to_parquet_bytes(df: pl.DataFrame):
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    return buf.getvalue()

def parquet_bytes_to_df(content_bytes, colunas=None):
    if not content_bytes: return None
    try:
        buf = io.BytesIO(content_bytes)
        df = pl.read_parquet(buf)
        if colunas:
            cols_to_select = [c for c in colunas if c in df.columns]
            df = df.select(cols_to_select)
        return df
    except:
        return None

# ══════════════════════════════════════════════════════════════
# CAMPANHAS E PAGAMENTOS
# ══════════════════════════════════════════════════════════════

META_PATH = "data/campanhas_meta.parquet"
PAG_PATH  = "data/pagamentos.parquet"

def load_campanhas_meta():
    content, _ = get_file_from_github(META_PATH)
    if content:
        df = parquet_bytes_to_df(content)
        if df is not None: return df

    schema = {'id': pl.String, 'nome': pl.String, 'criado_em': pl.Datetime, 'total_envios': pl.Int64, 'total_clientes': pl.Int64}
    return pl.DataFrame(schema=schema)

def save_campanha(nome, df_envios: pl.DataFrame, df_clientes: pl.DataFrame):
    campanha_id = str(uuid.uuid4())[:8]
    ok_envios = save_file_to_github(f"data/campanhas/{campanha_id}_envios.parquet", df_to_parquet_bytes(df_envios), f"Campanha {nome}: envios")
    ok_clientes = save_file_to_github(f"data/campanhas/{campanha_id}_clientes.parquet", df_to_parquet_bytes(df_clientes), f"Campanha {nome}: clientes")

    if not ok_envios or not ok_clientes: return None, "Erro ao salvar arquivos da campanha."

    df_meta = load_campanhas_meta()

    nova = pl.DataFrame({
        'id': [campanha_id], 
        'nome': [nome], 
        'criado_em': [datetime.datetime.now()],
        'total_envios': [df_envios.select(pl.col('TELEFONE_ENVIO').n_unique()).item()], 
        'total_clientes': [df_clientes.height]
    })

    df_meta = pl.concat([df_meta, nova], how="diagonal_relaxed")
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} criada")
    return campanha_id, None

def update_campanha(campanha_id, nome, df_envios_novos=None, df_clientes_novos=None):
    df_meta = load_campanhas_meta()
    if df_meta.filter(pl.col('id') == campanha_id).is_empty(): 
        return False, "Campanha não encontrada."

    if df_envios_novos is not None:
        df_envios_existente = load_campanha_envios(campanha_id)
        if df_envios_existente is not None and not df_envios_existente.is_empty():
            df_envios_combined = pl.concat([df_envios_existente, df_envios_novos], how="diagonal_relaxed")
        else:
            df_envios_combined = df_envios_novos

        df_envios_combined = df_envios_combined.unique(subset=['TELEFONE_ENVIO', 'DATA_ENVIO'], keep='last')
        save_file_to_github(f"data/campanhas/{campanha_id}_envios.parquet", df_to_parquet_bytes(df_envios_combined), f"Campanha {nome}: atualização envios")

        df_meta = df_meta.with_columns(
            pl.when(pl.col('id') == campanha_id)
            .then(df_envios_combined.select(pl.col('TELEFONE_ENVIO').n_unique()).item())
            .otherwise(pl.col('total_envios'))
            .alias('total_envios')
        )

    if df_clientes_novos is not None:
        df_clientes_existente = load_campanha_clientes(campanha_id)
        if df_clientes_existente is not None and not df_clientes_existente.is_empty():
            df_clientes_combined = pl.concat([df_clientes_existente, df_clientes_novos], how="diagonal_relaxed")
        else:
            df_clientes_combined = df_clientes_novos

        df_clientes_combined = df_clientes_combined.unique(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], keep='last')
        save_file_to_github(f"data/campanhas/{campanha_id}_clientes.parquet", df_to_parquet_bytes(df_clientes_combined), f"Campanha {nome}: atualização clientes")

        df_meta = df_meta.with_columns(
            pl.when(pl.col('id') == campanha_id)
            .then(df_clientes_combined.height)
            .otherwise(pl.col('total_clientes'))
            .alias('total_clientes')
        )

    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} atualizada")
    load_campanha_envios.clear()
    load_campanha_clientes.clear()
    return True, None

@st.cache_data(ttl=3600, max_entries=2)
def load_campanha_envios(campanha_id):
    content, _ = get_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet")
    return parquet_bytes_to_df(content) if content else None

@st.cache_data(ttl=3600, max_entries=2)
def load_campanha_clientes(campanha_id):
    content, _ = get_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet")
    colunas_cli = ['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE', 'SITUACAO', 'CIDADE', 'DIRETORIA']
    return parquet_bytes_to_df(content, colunas=colunas_cli) if content else None

def delete_campanha(campanha_id, nome):
    df_meta = load_campanhas_meta()
    df_meta = df_meta.filter(pl.col('id') != campanha_id)
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} removida")
    delete_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet", f"Removendo envios {nome}")
    delete_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet", f"Removendo clientes {nome}")

@st.cache_data(ttl=900, max_entries=1)
def load_pagamentos_github():
    content, _ = get_file_from_github(PAG_PATH)
    if not content: return None

    colunas_uteis = ["MATRICULA_PAGAMENTO", "DATA_PAGAMENTO", "VALOR_PAGO", "CIDADE", "TIPO_PAGAMENTO", "VENCIMENTO", "UTILIZACAO", "TIPO_FATURA"]
    df = parquet_bytes_to_df(content, colunas=colunas_uteis)
    return df

def update_pagamentos_github(df_novo: pl.DataFrame):
    df_existente = load_pagamentos_github()
    if df_existente is not None and not df_existente.is_empty():
        df_combined = pl.concat([df_existente, df_novo], how="diagonal_relaxed")
        df_combined = df_combined.unique(subset=['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO'], keep='last')
    else:
        df_combined = df_novo

    total_antes = df_existente.height if df_existente is not None else 0
    novos = df_combined.height - total_antes
    ok = save_file_to_github(PAG_PATH, df_to_parquet_bytes(df_combined), "Pagamentos: atualização")
    load_pagamentos_github.clear() 
    return ok, df_combined.height, novos

# ══════════════════════════════════════════════════════════════
# PROCESSAMENTO DE ARQUIVOS
# ══════════════════════════════════════════════════════════════

@st.cache_data
def load_and_process_envios(uploaded_file):
    try:
        if uploaded_file.name.endswith('.parquet'):
            df = pl.read_parquet(io.BytesIO(uploaded_file.read()))
        else:
            df = pl.read_excel(uploaded_file.read())

        colunas_ler = ['To', 'Send At']
        if 'Reason' in df.columns:
            colunas_ler.append('Reason')

        df_envios = df.select([c for c in colunas_ler if c in df.columns])

        renomear = {'To': 'TELEFONE_ENVIO', 'Send At': 'DATA_ENVIO'}
        if 'Reason' in df.columns:
            renomear['Reason'] = 'STATUS_ENVIO'

        df_envios = df_envios.rename(renomear)

        if 'STATUS_ENVIO' not in df_envios.columns:
            df_envios = df_envios.with_columns(pl.lit('DELIVERED_TO_HANDSET').alias('STATUS_ENVIO'))

        df_envios = df_envios.with_columns([
            pl.col('TELEFONE_ENVIO').cast(pl.String).str.replace(r'^55|\.0$', '').str.strip_chars(),
            pl.col('STATUS_ENVIO').cast(pl.String)
        ])

        # Tratamento de data
        if df_envios.schema['DATA_ENVIO'] in [pl.Utf8, pl.String]:
            df_envios = df_envios.with_columns(
                pl.coalesce([
                    pl.col('DATA_ENVIO').str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False),
                    pl.col('DATA_ENVIO').str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
                    pl.col('DATA_ENVIO').str.strptime(pl.Datetime, "%d/%m/%Y", strict=False)
                ]).alias('DATA_ENVIO')
            )
        else:
            df_envios = df_envios.with_columns(pl.col('DATA_ENVIO').cast(pl.Datetime))

        df_envios = df_envios.drop_nulls(subset=['DATA_ENVIO'])
        return df_envios
    except Exception as e:
        st.error(f"Erro ao processar Envios: {e}")
        return None

@st.cache_data
def load_and_process_clientes(uploaded_file):
    try:
        if uploaded_file.name.endswith('.parquet'):
            df = pl.read_parquet(io.BytesIO(uploaded_file.read()))
        else:
            df = pl.read_excel(uploaded_file.read())

        colunas_ler = ['TELEFONE', 'MATRICULA', 'SITUACAO']
        for col in ['CIDADE', 'DIRETORIA']:
            if col in df.columns: colunas_ler.append(col)

        df_clientes = df.select([c for c in colunas_ler if c in df.columns])
        df_clientes = df_clientes.rename({'TELEFONE': 'TELEFONE_CLIENTE', 'MATRICULA': 'MATRICULA_CLIENTE'})

        df_clientes = df_clientes.with_columns([
            pl.col('TELEFONE_CLIENTE').cast(pl.String).str.replace(r'^55|\.0$', '').str.strip_chars(),
            pl.col('MATRICULA_CLIENTE').cast(pl.String).str.replace(r'\.0$', '').str.strip_chars(),
            pl.col('SITUACAO').cast(pl.Float32, strict=False).fill_null(0.0)
        ])

        if 'CIDADE' in df_clientes.columns: 
            df_clientes = df_clientes.with_columns(
                pl.col('CIDADE').cast(pl.String).str.strip_chars().str.to_uppercase()
                .str.replace('NAN', 'DESCONHECIDO').str.replace('NONE', 'DESCONHECIDO')
            )
        if 'DIRETORIA' in df_clientes.columns: 
            df_clientes = df_clientes.with_columns(
                pl.col('DIRETORIA').cast(pl.String).str.strip_chars().str.to_uppercase()
                .str.replace('NAN', 'DESCONHECIDO').str.replace('NONE', 'DESCONHECIDO')
            )

        df_clientes = df_clientes.unique(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], keep='first')
        return df_clientes
    except Exception as e:
        st.error(f"Erro ao processar Clientes: {e}")
        return None

@st.cache_data
def load_and_process_pagamentos(uploaded_file):
    try:
        df = None
        if uploaded_file.name.endswith('.parquet'):
            df = pl.read_parquet(io.BytesIO(uploaded_file.read()))
        elif uploaded_file.name.endswith('.csv'):
            df = pl.read_csv(uploaded_file.read(), separator=';', infer_schema_length=10000, ignore_errors=True)
        elif uploaded_file.name.endswith('.xlsx'):
            df = pl.read_excel(uploaded_file.read())
        else:
            raise ValueError("Formato não suportado.")

        if df is None or df.is_empty():
            st.error("Arquivo de Pagamentos está vazio.")
            return None

        mapeamento_nomes = {
            'Nº Ligação': 'MATRICULA_PAGAMENTO',
            'Data Pagto.': 'DATA_PAGAMENTO',
            'Valor Pago': 'VALOR_PAGO',
            'Cidade': 'CIDADE',
            'Diretoria': 'DIRETORIA',
            'Arrecadador': 'TIPO_PAGAMENTO',
            'Vencimento': 'VENCIMENTO',
            'Tipo Fatura': 'TIPO_FATURA',
            'Utilização': 'UTILIZACAO'
        }

        renomear = {k: v for k, v in mapeamento_nomes.items() if k in df.columns}
        df_pag = df.rename(renomear)

        colunas_desejadas = ['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO']
        for col in ['CIDADE', 'DIRETORIA', 'TIPO_PAGAMENTO', 'VENCIMENTO', 'TIPO_FATURA', 'UTILIZACAO']:
            if col in df_pag.columns:
                colunas_desejadas.append(col)

        df_pag = df_pag.select([c for c in colunas_desejadas if c in df_pag.columns])

        df_pag = df_pag.with_columns(
            pl.col('MATRICULA_PAGAMENTO').cast(pl.String).str.replace(r'\.0$', '').str.strip_chars()
        )

        # Tratamento de DATA_PAGAMENTO
        if df_pag.schema['DATA_PAGAMENTO'] in [pl.Utf8, pl.String]:
            df_pag = df_pag.with_columns(
                pl.coalesce([
                    pl.col('DATA_PAGAMENTO').str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False),
                    pl.col('DATA_PAGAMENTO').str.strptime(pl.Datetime, "%d/%m/%Y", strict=False),
                    pl.col('DATA_PAGAMENTO').str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                ]).alias('DATA_PAGAMENTO')
            )
        else:
            df_pag = df_pag.with_columns(pl.col('DATA_PAGAMENTO').cast(pl.Datetime))

        # Tratamento de VALOR_PAGO
        if df_pag.schema['VALOR_PAGO'] in [pl.Utf8, pl.String]:
            df_pag = df_pag.with_columns(
                pl.col('VALOR_PAGO')
                .str.replace_all('R\$', '')
                .str.replace_all(r'\.', '')
                .str.replace_all(',', '.')
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
            )
        else:
            df_pag = df_pag.with_columns(pl.col('VALOR_PAGO').cast(pl.Float64))

        df_pag = df_pag.drop_nulls(subset=['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO'])

        if df_pag.is_empty():
            st.error("Nenhuma linha válida restou após o processamento. Verifique os formatos de data e valor.")
            return None

        if 'TIPO_PAGAMENTO' in df_pag.columns:
            df_pag = df_pag.with_columns(pl.col('TIPO_PAGAMENTO').cast(pl.String).str.strip_chars().fill_null('Não informado'))

        if 'VENCIMENTO' in df_pag.columns:
            if df_pag.schema['VENCIMENTO'] in [pl.Utf8, pl.String]:
                df_pag = df_pag.with_columns(
                    pl.coalesce([
                        pl.col('VENCIMENTO').str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False),
                        pl.col('VENCIMENTO').str.strptime(pl.Datetime, "%d/%m/%Y", strict=False)
                    ]).alias('VENCIMENTO')
                )
            else:
                df_pag = df_pag.with_columns(pl.col('VENCIMENTO').cast(pl.Datetime))

            df_pag = df_pag.with_columns([
                pl.col('VENCIMENTO').dt.month().alias('MES_FATURA'),
                pl.col('VENCIMENTO').dt.year().alias('ANO_FATURA'),
                pl.col('VENCIMENTO').dt.strftime('%m/%Y').alias('MES_ANO_FATURA')
            ])

        if 'TIPO_FATURA' in df_pag.columns:
            df_pag = df_pag.with_columns(pl.col('TIPO_FATURA').cast(pl.String).str.strip_chars().fill_null('Não informado'))

        if 'UTILIZACAO' in df_pag.columns:
            df_pag = df_pag.with_columns(pl.col('UTILIZACAO').cast(pl.String).str.strip_chars().fill_null('Não informado'))

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

if "msg_sucesso" in st.session_state:
    st.sidebar.success(st.session_state["msg_sucesso"])
    del st.session_state["msg_sucesso"]

st.title("📊 Análise de eficiência de campanha de cobrança via Whatsapp")

st.sidebar.markdown(f"👤 **{st.session_state['username']}**")
if st.sidebar.button("Sair"):
    st.session_state.clear()
    st.rerun()

st.sidebar.markdown("---")

st.sidebar.header("🏦 Resumo da Base")
df_pag_geral = load_pagamentos_github()
total_pag_geral = df_pag_geral.height if df_pag_geral is not None else 0
st.sidebar.metric("Total de Pagamentos Cadastrados", f"{total_pag_geral:,}".replace(",", "."))
st.sidebar.markdown("---")

st.sidebar.header("📋 Campanhas")
df_meta = load_campanhas_meta()
campanhas_disponiveis = df_meta['nome'].to_list() if not df_meta.is_empty() else []

campanha_selecionada_nome = st.sidebar.selectbox("Selecionar campanha", ["(nenhuma)"] + campanhas_disponiveis)
campanha_selecionada = None

if campanha_selecionada_nome != "(nenhuma)":
    campanha_selecionada = df_meta.filter(pl.col('nome') == campanha_selecionada_nome).to_dicts()[0]
    if is_admin() and st.sidebar.button("🗑️ Excluir esta campanha"):
        delete_campanha(campanha_selecionada['id'], campanha_selecionada_nome)
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
            save_campanha(nome_nova, load_and_process_envios(up_env), load_and_process_clientes(up_cli))
            st.success("Campanha salva!")
            st.rerun()

    with st.sidebar.expander("🔄 Atualizar Campanha"):
        if not df_meta.is_empty():
            camp_upd = st.selectbox("Campanha", df_meta['nome'].to_list())
            up_env_u = st.file_uploader("Novos Envios", type=["xlsx", "parquet"], key="u_env")
            up_cli_u = st.file_uploader("Novos Clientes", type=["xlsx", "parquet"], key="u_cli")
            if st.button("Atualizar") and (up_env_u or up_cli_u):
                cid = df_meta.filter(pl.col('nome') == camp_upd).select('id').item()
                update_campanha(cid, camp_upd, load_and_process_envios(up_env_u) if up_env_u else None, load_and_process_clientes(up_cli_u) if up_cli_u else None)
                st.success("Campanha atualizada!")
                st.rerun()

    with st.sidebar.expander("💰 Base de Pagamentos"):
        up_pag = st.file_uploader("Pagamentos", type=["csv", "xlsx", "parquet"])
        if st.button("Enviar Pagamentos") and up_pag:
            ok, total, novos = update_pagamentos_github(load_and_process_pagamentos(up_pag))
            if ok: st.success(f"Pagamentos atualizados! Total: {total} | Novos: {novos}")

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
        df_pagamentos = load_pagamentos_github()

    dados_prontos = (
        df_envios is not None and
        df_clientes is not None and
        df_pagamentos is not None
    )

# ══════════════════════════════════════════════════════════════
# ANÁLISE
# ══════════════════════════════════════════════════════════════

if executar_analise and dados_prontos:

    # ── Cruzamento envios x clientes ──────────────────────────
    total_clientes_unicos_base_envios = df_envios.select(pl.col('TELEFONE_ENVIO').n_unique()).item()
    total_base_envio = df_envios.height

    df_merge = df_envios.join(
        df_clientes,
        left_on='TELEFONE_ENVIO',
        right_on='TELEFONE_CLIENTE',
        how='left'
    )

    df_merge = df_merge.drop_nulls(subset=['MATRICULA_CLIENTE'])

    if df_merge.is_empty():
        st.error("Nenhum cliente encontrado após cruzamento entre envios e clientes.")
        st.stop()

    df_merge = df_merge.with_columns(
        (pl.col('STATUS_ENVIO') == 'DELIVERED_TO_HANDSET').alias('NOTIFICADO')
    )

    total_clientes_notificados = df_merge.filter(pl.col('NOTIFICADO')).select(pl.col('MATRICULA_CLIENTE').n_unique()).item()
    total_clientes_nao_notificados = df_merge.filter(~pl.col('NOTIFICADO')).select(pl.col('MATRICULA_CLIENTE').n_unique()).item()
    total_envios_rejeitados = df_envios.filter(pl.col('STATUS_ENVIO') != 'DELIVERED_TO_HANDSET').height
    taxa_eficiencia_disparos = (total_clientes_notificados / total_clientes_unicos_base_envios * 100) if total_clientes_unicos_base_envios > 0 else 0

    total_divida_base_envios = df_merge.unique(subset=['MATRICULA_CLIENTE']).select(pl.col('SITUACAO').sum()).item()

    df_entregues = df_merge.filter(pl.col('NOTIFICADO'))
    total_divida_notificados = df_entregues.unique(subset=['MATRICULA_CLIENTE']).select(pl.col('SITUACAO').sum()).item()

    matriculas_alvo = df_merge.select('MATRICULA_CLIENTE').unique().to_series().to_list()
    df_pagamentos_filtrado = df_pagamentos.filter(pl.col('MATRICULA_PAGAMENTO').is_in(matriculas_alvo))

    del df_pagamentos
    load_pagamentos_github.clear()
    gc.collect()

    colunas_remover_pagamento = [c for c in ['CIDADE', 'DIRETORIA'] if c in df_pagamentos_filtrado.columns]
    if colunas_remover_pagamento:
        df_pagamentos_filtrado = df_pagamentos_filtrado.drop(colunas_remover_pagamento)

    df_cruzado = df_merge.join(
        df_pagamentos_filtrado,
        left_on='MATRICULA_CLIENTE',
        right_on='MATRICULA_PAGAMENTO',
        how='inner'
    )

    del df_merge
    del df_pagamentos_filtrado
    gc.collect()

    if df_cruzado.is_empty():
        st.error("Nenhum pagamento encontrado após cruzamento com a base de clientes.")
        st.stop()

    df_cruzado = df_cruzado.with_columns(
        (pl.col('DATA_PAGAMENTO') - pl.col('DATA_ENVIO')).dt.total_days().alias('DIAS_APOS_ENVIO')
    )

    df_pagamentos_campanha = df_cruzado.filter(
        (pl.col('DIAS_APOS_ENVIO') >= 0) & 
        (pl.col('DIAS_APOS_ENVIO') <= janela_dias)
    )

    del df_cruzado
    gc.collect()

    subset_unique = ['MATRICULA_CLIENTE', 'DATA_PAGAMENTO', 'VALOR_PAGO']
    if 'VENCIMENTO' in df_pagamentos_campanha.columns:
        subset_unique.append('VENCIMENTO')

    df_pagamentos_campanha = df_pagamentos_campanha.unique(
        subset=subset_unique,
        keep='first'
    ).rename({'MATRICULA_CLIENTE': 'MATRICULA'})

    # ── Identificação de Múltiplos Pagamentos ─────────────────
    pagamentos_multiplos = df_pagamentos_campanha.filter(pl.col('MATRICULA').is_duplicated())
    qtd_clientes_multiplos = pagamentos_multiplos.select(pl.col('MATRICULA').n_unique()).item()

    # ── Métricas ──────────────────────────────────────────────
    clientes_unicos_que_pagaram_matriculas = df_pagamentos_campanha.select(pl.col('MATRICULA').n_unique()).item()
    qtd_pagamentos = df_pagamentos_campanha.height
    valor_total_arrecadado = df_pagamentos_campanha.select(pl.col('VALOR_PAGO').sum()).item() if not df_pagamentos_campanha.is_empty() else 0

    taxa_eficiencia_clientes_notificados = (clientes_unicos_que_pagaram_matriculas / total_clientes_notificados * 100) if total_clientes_notificados > 0 else 0
    taxa_eficiencia_valor_notificados = (valor_total_arrecadado / total_divida_notificados * 100) if total_divida_notificados > 0 else 0
    taxa_eficiencia_clientes_base_envios = (clientes_unicos_que_pagaram_matriculas / total_clientes_unicos_base_envios * 100) if total_clientes_unicos_base_envios > 0 else 0
    taxa_eficiencia_valor_base = (valor_total_arrecadado / total_divida_base_envios * 100) if total_divida_base_envios > 0 else 0

    ticket_medio = (valor_total_arrecadado / clientes_unicos_que_pagaram_matriculas) if clientes_unicos_que_pagaram_matriculas > 0 else 0
    custo_campanha = total_base_envio * 0.05
    roi = ((valor_total_arrecadado - custo_campanha) / custo_campanha * 100) if custo_campanha > 0 else 0

    # ── Abas ─────────────────────────────────────────────────
    aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
        "📊 Visão Geral",
        "🏙️ Cidade e Diretoria",
        "📅 Análise das Faturas",
        "💳 Canal de Pagamento",
        "📋 Detalhes",
        "🧪 Novas Visualizações"
    ])

    # ══════════════════════════════════════════════════════════
    # ABA 1 — VISÃO GERAL
    # ══════════════════════════════════════════════════════════
    with aba1:
        st.subheader("Resultados da Análise da Campanha")

        st.markdown("##### 📱 Funil de Disparos")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Clientes na base de envios", f"{total_clientes_unicos_base_envios:,}")
        col2.metric("Clientes notificados", f"{total_clientes_notificados:,}")
        col3.metric("Clientes NÃO notificados", f"{total_clientes_nao_notificados:,}")
        col4.metric("Eficiência dos disparos", f"{taxa_eficiencia_disparos:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        st.markdown("##### 💰 Conversão e Arrecadação")

        if qtd_clientes_multiplos > 0:
            st.info(f"ℹ️ **Nota:** {qtd_clientes_multiplos} clientes realizaram mais de um pagamento nesta campanha. Eles estão contabilizados no volume total.")

        col5, col6 = st.columns(2)
        col5.metric("Clientes únicos que pagaram", f"{clientes_unicos_que_pagaram_matriculas:,}")
        col6.metric("Quantidade total de pagamentos", f"{qtd_pagamentos:,}")

        col7, col8 = st.columns(2)
        col7.metric("Taxa de eficiência base envios", f"{taxa_eficiencia_clientes_base_envios:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)
        col8.metric("Taxa de eficiência clientes notificados", f"{taxa_eficiencia_clientes_notificados:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)

        col9, col10, col11 = st.columns(3)
        col9.metric("Dívida total da base", fmt_brl(total_divida_base_envios))
        col10.metric("Dívida dos notificados", fmt_brl(total_divida_notificados))
        col11.metric("Valor total arrecadado", fmt_brl(valor_total_arrecadado))

        col12, col13 = st.columns(2)
        col12.metric("Taxa eficiência dívida total", f"{taxa_eficiencia_valor_base:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)
        col13.metric("Taxa eficiência dívida notificada", f"{taxa_eficiencia_valor_notificados:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)

        col14, col15, col16, col17 = st.columns(4)  
        col14.metric("Ticket médio", fmt_brl(ticket_medio))
        col15.metric("Total de disparos", f"{total_base_envio:,}")
        col16.metric("Custo da campanha", fmt_brl(custo_campanha))
        col17.metric("ROI", f"{roi:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        if not df_pagamentos_campanha.is_empty():
            st.subheader(f"Pagamentos por Dia Após o Envio (Janela de {janela_dias} dias)")

            pagamentos_por_dia = (
                df_pagamentos_campanha.group_by('DIAS_APOS_ENVIO')
                .agg(pl.col('VALOR_PAGO').sum().alias('Valor Total Pago'))
                .rename({'DIAS_APOS_ENVIO': 'Dias Após Envio'})
                .sort('Dias Após Envio')
            ).to_pandas()

            fig_dias = px.bar(
                pagamentos_por_dia,
                x='Dias Após Envio', y='Valor Total Pago',
                title='Valor Arrecadado por Dia Após o Envio',
                labels={'Dias Após Envio': 'Dias Após o Envio', 'Valor Total Pago': 'Valor Total Pago (R$)'},
                hover_data={'Valor Total Pago': ':.2f'}
            )
            fig_dias.update_layout(xaxis_title="Dias Após o Envio", yaxis_title="Valor Total Pago (R$)")
            st.plotly_chart(fig_dias, use_container_width=True, key="fig_dias")

            st.subheader("Distribuição do Tempo para Pagamento")
            fig_tempo_pagamento = px.histogram(
                df_pagamentos_campanha.to_pandas(), 
                x='DIAS_APOS_ENVIO', 
                nbins=janela_dias+1, 
                title='Frequência de Pagamentos por Dia',
                labels={'DIAS_APOS_ENVIO': 'Dias Após o Envio'}
            )
            st.plotly_chart(fig_tempo_pagamento, use_container_width=True, key="fig_tempo_pagamento")

    # ══════════════════════════════════════════════════════════
    # ABA 2 — CIDADE E DIRETORIA
    # ══════════════════════════════════════════════════════════
    with aba2:
        if not df_pagamentos_campanha.is_empty():
            tem_cidade    = 'CIDADE'    in df_pagamentos_campanha.columns
            tem_diretoria = 'DIRETORIA' in df_pagamentos_campanha.columns

            if tem_cidade:
                st.subheader("Análise por Cidade")
                cidade_resumo = (
                    df_pagamentos_campanha.group_by('CIDADE')
                    .agg([
                        pl.col('MATRICULA').n_unique().alias('Clientes_que_Pagaram'),
                        pl.col('VALOR_PAGO').sum().alias('Valor_Arrecadado')
                    ])
                    .sort('Valor_Arrecadado', descending=True)
                ).to_pandas()

                fig_cidade_valor = px.bar(
                    cidade_resumo, x='CIDADE', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Cidade',
                    labels={'CIDADE': 'Cidade', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'}
                )
                st.plotly_chart(fig_cidade_valor, use_container_width=True, key="fig_cidade_valor")

            if tem_diretoria:
                st.subheader("Análise por Diretoria")
                diretoria_resumo = (
                    df_pagamentos_campanha.group_by('DIRETORIA')
                    .agg([
                        pl.col('MATRICULA').n_unique().alias('Clientes_que_Pagaram'),
                        pl.col('VALOR_PAGO').sum().alias('Valor_Arrecadado')
                    ])
                    .sort('Valor_Arrecadado', descending=True)
                ).to_pandas()

                fig_diretoria_valor = px.bar(
                    diretoria_resumo, x='DIRETORIA', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Diretoria',
                    labels={'DIRETORIA': 'Diretoria', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'}
                )
                st.plotly_chart(fig_diretoria_valor, use_container_width=True, key="fig_diretoria_valor")

            if not tem_cidade and not tem_diretoria:
                st.info("Colunas 'CIDADE' e 'DIRETORIA' não encontradas na base de clientes.")

    # ══════════════════════════════════════════════════════════
    # ABA 3 — ANÁLISE DAS FATURAS
    # ══════════════════════════════════════════════════════════
    with aba3:
        if not df_pagamentos_campanha.is_empty():
            if 'VENCIMENTO' in df_pagamentos_campanha.columns:
                st.subheader("Antiguidade da Dívida Paga")
                df_pagamentos_campanha = df_pagamentos_campanha.with_columns(
                    (pl.col('DATA_PAGAMENTO') - pl.col('VENCIMENTO')).dt.total_days().alias('ANTIGUIDADE_DIAS')
                )

                df_pagamentos_campanha = df_pagamentos_campanha.with_columns(
                    pl.when(pl.col('ANTIGUIDADE_DIAS').is_null()).then(pl.lit('Não informado'))
                    .when(pl.col('ANTIGUIDADE_DIAS') <= 10).then(pl.lit('0-10 dias'))
                    .when(pl.col('ANTIGUIDADE_DIAS') <= 20).then(pl.lit('11-20 dias'))
                    .when(pl.col('ANTIGUIDADE_DIAS') <= 30).then(pl.lit('21-30 dias'))
                    .when(pl.col('ANTIGUIDADE_DIAS') <= 60).then(pl.lit('31-60 dias'))
                    .otherwise(pl.lit('Mais de 61 dias'))
                    .alias('FAIXA_ANTIGUIDADE')
                )

                antiguidade_resumo = (
                    df_pagamentos_campanha.group_by('FAIXA_ANTIGUIDADE')
                    .agg(pl.col('VALOR_PAGO').sum())
                ).to_pandas()

                fig_ant_valor = px.bar(
                    antiguidade_resumo, x='FAIXA_ANTIGUIDADE', y='VALOR_PAGO',
                    title='Valor Pago por Faixa de Antiguidade da Dívida',
                    labels={'FAIXA_ANTIGUIDADE': 'Faixa de Antiguidade', 'VALOR_PAGO': 'Valor Pago (R$)'}
                )
                st.plotly_chart(fig_ant_valor, use_container_width=True, key="fig_ant_valor")

            if 'MES_ANO_FATURA' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Mês/Ano da Fatura")
                mes_ano_resumo = (
                    df_pagamentos_campanha.group_by('MES_ANO_FATURA')
                    .agg(pl.col('VALOR_PAGO').sum())
                ).to_pandas()

                fig_mes_ano = px.bar(
                    mes_ano_resumo, x='MES_ANO_FATURA', y='VALOR_PAGO',
                    title='Valor Pago por Mês/Ano da Fatura',
                    labels={'MES_ANO_FATURA': 'Mês/Ano da Fatura', 'VALOR_PAGO': 'Valor Pago (R$)'}
                )
                st.plotly_chart(fig_mes_ano, use_container_width=True, key="fig_mes_ano")

    # ══════════════════════════════════════════════════════════
    # ABA 4 — CANAL DE PAGAMENTO
    # ══════════════════════════════════════════════════════════
    with aba4:
        if not df_pagamentos_campanha.is_empty() and 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
            st.subheader("Valor Arrecadado por Canal de Pagamento")
            pagamentos_por_canal = (
                df_pagamentos_campanha.group_by('TIPO_PAGAMENTO')
                .agg(pl.col('VALOR_PAGO').sum())
                .sort('VALOR_PAGO', descending=True)
            ).to_pandas()

            fig_canal_aba4 = px.bar(
                pagamentos_por_canal, x='TIPO_PAGAMENTO', y='VALOR_PAGO',
                title='Valor Arrecadado por Canal de Pagamento',
                labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'VALOR_PAGO': 'Valor Total Pago (R$)'},
                color='TIPO_PAGAMENTO'
            )
            st.plotly_chart(fig_canal_aba4, use_container_width=True, key="fig_canal_aba4")

            st.subheader("Clientes que Pagaram por Canal")
            qtd_por_canal = (
                df_pagamentos_campanha.group_by('TIPO_PAGAMENTO')
                .agg(pl.col('MATRICULA').n_unique().alias('Clientes que Pagaram'))
                .sort('Clientes que Pagaram', descending=True)
            ).to_pandas()

            fig_canal_qtd = px.bar(
                qtd_por_canal, x='TIPO_PAGAMENTO', y='Clientes que Pagaram',
                title='Clientes que Pagaram por Canal',
                labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'Clientes que Pagaram': 'Clientes que Pagaram'},
                color='TIPO_PAGAMENTO'
            )
            st.plotly_chart(fig_canal_qtd, use_container_width=True, key="fig_canal_qtd")

            tab_canal = pd.merge(pagamentos_por_canal, qtd_por_canal, on='TIPO_PAGAMENTO')
            tab_canal.columns = ['Canal de Pagamento', 'Valor Total Pago', 'Clientes que Pagaram']
            tab_canal['Valor Total Pago'] = tab_canal['Valor Total Pago'].apply(fmt_brl)
            st.dataframe(tab_canal, use_container_width=True, hide_index=True)

            tem_cidade    = 'CIDADE'    in df_pagamentos_campanha.columns
            tem_diretoria = 'DIRETORIA' in df_pagamentos_campanha.columns

            if tem_diretoria:
                st.subheader("Canal de Pagamento por Diretoria")
                canal_diretoria = (
                    df_pagamentos_campanha.group_by(['DIRETORIA', 'TIPO_PAGAMENTO'])
                    .agg(pl.col('VALOR_PAGO').sum())
                ).to_pandas()

                fig_canal_dir = px.bar(
                    canal_diretoria, x='DIRETORIA', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                    title='Valor Arrecadado: Diretoria x Canal de Pagamento',
                    labels={'DIRETORIA': 'Diretoria', 'VALOR_PAGO': 'Valor (R$)', 'TIPO_PAGAMENTO': 'Canal'},
                    barmode='stack'
                )
                st.plotly_chart(fig_canal_dir, use_container_width=True, key="fig_canal_dir_aba4")

            if tem_cidade:
                st.subheader("Canal de Pagamento por Cidade")
                canal_cidade = (
                    df_pagamentos_campanha.group_by(['CIDADE', 'TIPO_PAGAMENTO'])
                    .agg(pl.col('VALOR_PAGO').sum())
                ).to_pandas()

                ordem_cidades = canal_cidade.groupby('CIDADE')['VALOR_PAGO'].sum().sort_values(ascending=False).index

                fig_canal_cid = px.bar(
                    canal_cidade, x='CIDADE', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                    title='Valor Arrecadado: Cidade x Canal de Pagamento',
                    labels={'CIDADE': 'Cidade', 'VALOR_PAGO': 'Valor (R$)', 'TIPO_PAGAMENTO': 'Canal'},
                    barmode='stack',
                    category_orders={'CIDADE': ordem_cidades}
                )
                st.plotly_chart(fig_canal_cid, use_container_width=True, key="fig_canal_cid_aba4")            

        else:
            st.info("Coluna 'TIPO_PAGAMENTO' não encontrada no arquivo de pagamentos.")

    # ══════════════════════════════════════════════════════════
    # ABA 5 — DETALHES
    # ══════════════════════════════════════════════════════════
    with aba5:
        if not df_pagamentos_campanha.is_empty():
            st.subheader("Detalhes dos Pagamentos Atribuídos à Campanha")

            colunas_possiveis = [
                'MATRICULA', 'CIDADE', 'DIRETORIA', 'TELEFONE_ENVIO',
                'DATA_ENVIO', 'DATA_PAGAMENTO', 'VENCIMENTO',
                'VALOR_PAGO', 'DIAS_APOS_ENVIO', 'NOTIFICADO',
                'TIPO_FATURA', 'UTILIZACAO', 'TIPO_PAGAMENTO'
            ]
            colunas_exibicao = [c for c in colunas_possiveis if c in df_pagamentos_campanha.columns]

            df_detalhes = df_pagamentos_campanha.select(colunas_exibicao).unique(
                subset=['MATRICULA', 'DATA_PAGAMENTO', 'VALOR_PAGO']
            )

            if 'NOTIFICADO' in df_detalhes.columns:
                df_detalhes = df_detalhes.with_columns(
                    pl.when(pl.col('NOTIFICADO')).then(pl.lit('Sim')).otherwise(pl.lit('Não')).alias('NOTIFICADO')
                )

            df_detalhes_pd = df_detalhes.to_pandas()
            st.dataframe(df_detalhes_pd, use_container_width=True, hide_index=True)

            csv_output = df_detalhes_pd.to_csv(index=False, sep=';', decimal=',')
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
        if not df_pagamentos_campanha.is_empty():
            st.header("Exploração de Novas Visualizações")

            st.subheader("📅 Análise de Cohort (Mês de Envio)")
            df_pagamentos_campanha = df_pagamentos_campanha.with_columns(
                pl.col('DATA_ENVIO').dt.strftime('%Y-%m').alias('MES_ENVIO')
            )

            cohort_analysis = (
                df_pagamentos_campanha.group_by('MES_ENVIO')
                .agg([
                    pl.col('VALOR_PAGO').sum().alias('Total_Pagamentos'),
                    pl.col('MATRICULA').n_unique().alias('Clientes_Pagaram')
                ])
            ).to_pandas()

            fig_cohort = px.bar(
                cohort_analysis, x='MES_ENVIO', y='Total_Pagamentos',
                title='Valor Arrecadado por Mês de Envio da Campanha',
                labels={'MES_ENVIO': 'Mês de Envio', 'Total_Pagamentos': 'Valor Arrecadado (R$)'},
                text_auto='.2s'
            )
            st.plotly_chart(fig_cohort, use_container_width=True, key="fig_cohort_aba6")

            st.subheader("📈 Curva de Arrecadação Acumulada")
            df_acumulado = (
                df_pagamentos_campanha.group_by('DIAS_APOS_ENVIO')
                .agg(pl.col('VALOR_PAGO').sum())
                .sort('DIAS_APOS_ENVIO')
            )
            df_acumulado = df_acumulado.with_columns(
                pl.col('VALOR_PAGO').cum_sum().alias('VALOR_ACUMULADO')
            ).to_pandas()

            fig_acumulado = px.line(
                df_acumulado, x='DIAS_APOS_ENVIO', y='VALOR_ACUMULADO',
                title='Evolução da Arrecadação (Acumulada ao longo dos dias)',
                labels={'DIAS_APOS_ENVIO': 'Dias Após o Envio', 'VALOR_ACUMULADO': 'Valor Acumulado (R$)'},
                markers=True
            )
            st.plotly_chart(fig_acumulado, use_container_width=True, key="fig_acumulado_aba6")

            if 'CIDADE' in df_pagamentos_campanha.columns and 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                st.subheader("🏙️ Canal de Pagamento por Cidade")
                canal_cidade = (
                    df_pagamentos_campanha.group_by(['CIDADE', 'TIPO_PAGAMENTO'])
                    .agg(pl.col('VALOR_PAGO').sum())
                ).to_pandas()

                ordem_cidades = canal_cidade.groupby('CIDADE')['VALOR_PAGO'].sum().sort_values(ascending=False).index

                fig_canal_cid = px.bar(
                    canal_cidade, x='CIDADE', y='VALOR_PAGO', color='TIPO_PAGAMENTO',
                    title='Valor Arrecadado: Cidade x Canal de Pagamento',
                    labels={'CIDADE': 'Cidade', 'VALOR_PAGO': 'Valor (R$)', 'TIPO_PAGAMENTO': 'Canal'},
                    barmode='stack',
                    category_orders={'CIDADE': ordem_cidades}
                )
                st.plotly_chart(fig_canal_cid, use_container_width=True, key="fig_canal_cid_aba6")

            if 'CIDADE' in df_pagamentos_campanha.columns:
                st.subheader("🎫 Ticket Médio por Cidade")
                tm_cidade = (
                    df_pagamentos_campanha.group_by('CIDADE')
                    .agg([
                        pl.col('VALOR_PAGO').sum().alias('Valor'),
                        pl.col('MATRICULA').n_unique().alias('Clientes')
                    ])
                ).to_pandas()

                tm_cidade['Ticket_Medio'] = tm_cidade['Valor'] / tm_cidade['Clientes']
                tm_cidade = tm_cidade.sort_values('Ticket_Medio', ascending=False)

                fig_tm_cid = px.bar(
                    tm_cidade, x='CIDADE', y='Ticket_Medio',
                    title='Ticket Médio por Cidade',
                    labels={'CIDADE': 'Cidade', 'Ticket_Medio': 'Ticket Médio (R$)'},
                    text_auto='.2f'
                )
                st.plotly_chart(fig_tm_cid, use_container_width=True, key="fig_tm_cid_aba6")

            if 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                st.subheader("🔥 Concentração: Tempo de Pagamento x Canal")
                heatmap_data = (
                    df_pagamentos_campanha.group_by(['TIPO_PAGAMENTO', 'DIAS_APOS_ENVIO'])
                    .agg(pl.col('VALOR_PAGO').sum())
                ).to_pandas()

                fig_heat = px.density_heatmap(
                    heatmap_data, x='DIAS_APOS_ENVIO', y='TIPO_PAGAMENTO', z='VALOR_PAGO',
                    title='Mapa de Calor: Em quais dias cada canal arrecada mais?',
                    labels={'DIAS_APOS_ENVIO': 'Dias Após Envio', 'TIPO_PAGAMENTO': 'Canal', 'VALOR_PAGO': 'Valor (R$)'},
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig_heat, use_container_width=True, key="fig_heat_aba6")

            if 'UTILIZACAO' in df_pagamentos_campanha.columns:
                st.subheader("💧 Arrecadação por Tipo de Utilização")
                util_resumo = (
                    df_pagamentos_campanha.group_by('UTILIZACAO')
                    .agg(pl.col('VALOR_PAGO').sum())
                    .sort('VALOR_PAGO', descending=True)
                ).to_pandas()

                fig_util = px.pie(
                    util_resumo, names='UTILIZACAO', values='VALOR_PAGO',
                    title='Distribuição por Utilização (Subcategoria)',
                    hole=0.4
                )
                st.plotly_chart(fig_util, use_container_width=True, key="fig_util_aba6")

elif executar_analise and not dados_prontos:
    if campanha_selecionada is None:
        st.warning("Selecione uma campanha antes de executar a análise.")
    elif df_pagamentos is None:
        st.warning("Base de pagamentos não disponível. Um administrador precisa fazer o upload.")
    elif df_envios is None:
        st.warning("Não foi possível carregar os envios da campanha selecionada.")
    elif df_clientes is None:
        st.warning("Não foi possível carregar os clientes da campanha selecionada.")

elif not executar_analise:
    if campanha_selecionada is None:
        st.info("👈 Selecione uma campanha na barra lateral para começar.")
    else:
        st.info("👈 Clique em **Executar Análise** na barra lateral para gerar os resultados.")

