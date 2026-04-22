import streamlit as st
import pandas as pd
from datetime import timedelta
import plotly.express as px
import io
import base64
import requests
import json
import uuid

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

def df_to_parquet_bytes(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)
    return buf.getvalue()

def parquet_bytes_to_df(content_bytes):
    if not content_bytes: return None
    try:
        buf = io.BytesIO(content_bytes)
        buf.seek(0)
        return pd.read_parquet(buf, engine='pyarrow')
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
    return pd.DataFrame(columns=['id', 'nome', 'criado_em', 'total_envios', 'total_clientes'])

def save_campanha(nome, df_envios, df_clientes):
    campanha_id = str(uuid.uuid4())[:8]
    ok_envios = save_file_to_github(f"data/campanhas/{campanha_id}_envios.parquet", df_to_parquet_bytes(df_envios), f"Campanha {nome}: envios")
    ok_clientes = save_file_to_github(f"data/campanhas/{campanha_id}_clientes.parquet", df_to_parquet_bytes(df_clientes), f"Campanha {nome}: clientes")

    if not ok_envios or not ok_clientes: return None, "Erro ao salvar arquivos da campanha."

    df_meta = load_campanhas_meta()
    nova = pd.DataFrame([{
        'id': campanha_id, 'nome': nome, 'criado_em': pd.Timestamp.now(),
        'total_envios': df_envios['TELEFONE_ENVIO'].nunique(), 'total_clientes': len(df_clientes)
    }])
    df_meta = pd.concat([df_meta, nova], ignore_index=True)
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} criada")
    return campanha_id, None

def update_campanha(campanha_id, nome, df_envios_novos=None, df_clientes_novos=None):
    df_meta = load_campanhas_meta()
    idx = df_meta.index[df_meta['id'] == campanha_id].tolist()
    if not idx: return False, "Campanha não encontrada."

    if df_envios_novos is not None:
        df_envios_existente = load_campanha_envios(campanha_id)
        df_envios_combined = pd.concat([df_envios_existente, df_envios_novos], ignore_index=True) if df_envios_existente is not None else df_envios_novos
        df_envios_combined = df_envios_combined.drop_duplicates(subset=['TELEFONE_ENVIO', 'DATA_ENVIO'], keep='last')
        save_file_to_github(f"data/campanhas/{campanha_id}_envios.parquet", df_to_parquet_bytes(df_envios_combined), f"Campanha {nome}: atualização envios")
        df_meta.at[idx[0], 'total_envios'] = df_envios_combined['TELEFONE_ENVIO'].nunique()

    if df_clientes_novos is not None:
        df_clientes_existente = load_campanha_clientes(campanha_id)
        df_clientes_combined = pd.concat([df_clientes_existente, df_clientes_novos], ignore_index=True) if df_clientes_existente is not None else df_clientes_novos
        df_clientes_combined = df_clientes_combined.drop_duplicates(subset=['TELEFONE_CLIENTE', 'MATRICULA_CLIENTE'], keep='last')
        save_file_to_github(f"data/campanhas/{campanha_id}_clientes.parquet", df_to_parquet_bytes(df_clientes_combined), f"Campanha {nome}: atualização clientes")
        df_meta.at[idx[0], 'total_clientes'] = len(df_clientes_combined)

    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} atualizada")
    return True, None

def load_campanha_envios(campanha_id):
    content, _ = get_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet")
    return parquet_bytes_to_df(content) if content else None

def load_campanha_clientes(campanha_id):
    content, _ = get_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet")
    return parquet_bytes_to_df(content) if content else None

def delete_campanha(campanha_id, nome):
    df_meta = load_campanhas_meta()
    df_meta = df_meta[df_meta['id'] != campanha_id]
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} removida")
    delete_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet", f"Removendo envios {nome}")
    delete_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet", f"Removendo clientes {nome}")

def load_pagamentos_github():
    content, _ = get_file_from_github(PAG_PATH)
    return parquet_bytes_to_df(content) if content else None

def update_pagamentos_github(df_novo):
    df_existente = load_pagamentos_github()
    if df_existente is not None and not df_existente.empty:
        df_combined = pd.concat([df_existente, df_novo], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO'], keep='last')
    else:
        df_combined = df_novo.copy()

    total_antes = len(df_existente) if df_existente is not None else 0
    novos = len(df_combined) - total_antes
    ok = save_file_to_github(PAG_PATH, df_to_parquet_bytes(df_combined), "Pagamentos: atualização")
    return ok, len(df_combined), novos

# ══════════════════════════════════════════════════════════════
# PROCESSAMENTO DE ARQUIVOS
# ══════════════════════════════════════════════════════════════

@st.cache_data
def load_and_process_envios(uploaded_file):
    try:
        df = pd.read_excel(uploaded_file)
        df_envios = df[['To', 'Send At']].copy()
        df_envios.rename(columns={'To': 'TELEFONE_ENVIO', 'Send At': 'DATA_ENVIO'}, inplace=True)
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
        if uploaded_file.name.endswith('.parquet'):
            df_pag = pd.read_parquet(uploaded_file, engine='pyarrow')
        elif uploaded_file.name.endswith('.csv'):
            df_pag = pd.read_csv(uploaded_file, sep=';', decimal=',', encoding='latin1', header=None)
        elif uploaded_file.name.endswith('.xlsx'):
            df_pag = pd.read_excel(uploaded_file, header=None)
        else:
            return None

        if df_pag.empty: return None

        # Se for CSV/Excel sem cabeçalho mapeado
        if not isinstance(df_pag.columns[0], str) or 'MATRICULA_PAGAMENTO' not in df_pag.columns:
            col_indices = [0, 5, 8]
            col_names = ['MATRICULA_PAGAMENTO', 'DATA_PAGAMENTO', 'VALOR_PAGO']
            if df_pag.shape[1] > 12:
                col_indices.append(12)
                col_names.append('TIPO_PAGAMENTO')

            df_temp = df_pag.iloc[:, col_indices].copy()
            df_temp.columns = col_names

            if df_pag.shape[1] > 4: df_temp['VENCIMENTO'] = df_pag.iloc[:, 4].values
            if df_pag.shape[1] > 11: df_temp['TIPO_FATURA'] = df_pag.iloc[:, 11].values
            if df_pag.shape[1] > 9: df_temp['UTILIZACAO'] = df_pag.iloc[:, 9].values
            df_pag = df_temp

        df_pag['MATRICULA_PAGAMENTO'] = df_pag['MATRICULA_PAGAMENTO'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df_pag['DATA_PAGAMENTO'] = pd.to_datetime(df_pag['DATA_PAGAMENTO'], errors='coerce', dayfirst=True)
        df_pag.dropna(subset=['DATA_PAGAMENTO'], inplace=True)

        df_pag['VALOR_PAGO'] = df_pag['VALOR_PAGO'].astype(str).str.replace('.', '').str.replace(',', '.')
        df_pag['VALOR_PAGO'] = pd.to_numeric(df_pag['VALOR_PAGO'], errors='coerce')
        df_pag.dropna(subset=['VALOR_PAGO'], inplace=True)

        if 'VENCIMENTO' in df_pag.columns:
            df_pag['VENCIMENTO'] = pd.to_datetime(df_pag['VENCIMENTO'], errors='coerce', dayfirst=True)
            df_pag['MES_ANO_FATURA'] = df_pag['VENCIMENTO'].dt.strftime('%m/%Y')
            df_pag['ANO_FATURA'] = df_pag['VENCIMENTO'].dt.year
            df_pag['MES_FATURA'] = df_pag['VENCIMENTO'].dt.month

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

st.title("📊 Análise de eficiência de campanha de cobrança via Whatsapp")

st.sidebar.markdown(f"👤 **{st.session_state['username']}**")
if st.sidebar.button("Sair"):
    st.session_state.clear()
    st.rerun()

st.sidebar.header("📋 Campanhas")
df_meta = load_campanhas_meta()
campanhas_disponiveis = df_meta['nome'].tolist() if not df_meta.empty else []

campanha_selecionada_nome = st.sidebar.selectbox("Selecionar campanha", ["(nenhuma)"] + campanhas_disponiveis)
campanha_selecionada = None

if campanha_selecionada_nome != "(nenhuma)":
    campanha_selecionada = df_meta[df_meta['nome'] == campanha_selecionada_nome].iloc[0]
    if is_admin() and st.sidebar.button("🗑️ Excluir esta campanha"):
        delete_campanha(campanha_selecionada['id'], campanha_selecionada_nome)
        st.rerun()

janela_dias = st.sidebar.slider("Janela de dias após o envio:", 0, 30, 7)
executar_analise = st.sidebar.button("▶️ Executar Análise")

if is_admin():
    st.sidebar.header("🔧 Administração")
    with st.sidebar.expander("➕ Nova Campanha"):
        nome_nova = st.text_input("Nome da campanha")
        up_env = st.file_uploader("Envios (.xlsx)", type=["xlsx"], key="n_env")
        up_cli = st.file_uploader("Clientes (.xlsx)", type=["xlsx"], key="n_cli")
        if st.button("Salvar campanha") and nome_nova and up_env and up_cli:
            save_campanha(nome_nova, load_and_process_envios(up_env), load_and_process_clientes(up_cli))
            st.success("Campanha salva!")
            st.rerun()

    with st.sidebar.expander("🔄 Atualizar Campanha"):
        if not df_meta.empty:
            camp_upd = st.selectbox("Campanha", df_meta['nome'].tolist())
            up_env_u = st.file_uploader("Novos Envios", type=["xlsx"], key="u_env")
            up_cli_u = st.file_uploader("Novos Clientes", type=["xlsx"], key="u_cli")
            if st.button("Atualizar") and (up_env_u or up_cli_u):
                cid = df_meta[df_meta['nome'] == camp_upd].iloc[0]['id']
                update_campanha(cid, camp_upd, load_and_process_envios(up_env_u) if up_env_u else None, load_and_process_clientes(up_cli_u) if up_cli_u else None)
                st.success("Campanha atualizada!")
                st.rerun()

    with st.sidebar.expander("💰 Base de Pagamentos"):
        up_pag = st.file_uploader("Pagamentos", type=["csv", "xlsx", "parquet"])
        if st.button("Enviar Pagamentos") and up_pag:
            ok, total, novos = update_pagamentos_github(load_and_process_pagamentos(up_pag))
            if ok: st.success(f"Pagamentos atualizados! Total: {total} | Novos: {novos}")

# ══════════════════════════════════════════════════════════════
# ANÁLISE E GRÁFICOS
# ══════════════════════════════════════════════════════════════

if executar_analise and campanha_selecionada is not None:
    with st.spinner("Carregando dados..."):
        df_envios = load_campanha_envios(campanha_selecionada['id'])
        df_clientes = load_campanha_clientes(campanha_selecionada['id'])
        df_pagamentos = load_pagamentos_github()

    if df_envios is not None and df_clientes is not None and df_pagamentos is not None:
        df_merge = pd.merge(df_envios, df_clientes, left_on='TELEFONE_ENVIO', right_on='TELEFONE_CLIENTE', how='inner')
        df_cruzado = pd.merge(df_merge, df_pagamentos, left_on='MATRICULA_CLIENTE', right_on='MATRICULA_PAGAMENTO', how='inner')

        df_cruzado['DIAS_APOS_ENVIO'] = (df_cruzado['DATA_PAGAMENTO'] - df_cruzado['DATA_ENVIO']).dt.days
        df_pagamentos_campanha = df_cruzado[(df_cruzado['DIAS_APOS_ENVIO'] >= 0) & (df_cruzado['DIAS_APOS_ENVIO'] <= janela_dias)].copy()
        df_pagamentos_campanha = df_pagamentos_campanha.drop_duplicates(subset=['MATRICULA_CLIENTE', 'DATA_PAGAMENTO', 'VALOR_PAGO'])

        aba1, aba2, aba3, aba4, aba5 = st.tabs(["📊 Visão Geral", "🏙️ Cidade/Diretoria", "📅 Faturas", "💳 Canal", "📋 Detalhes"])

        with aba1:
            st.subheader("Resultados da Campanha")
            col1, col2, col3 = st.columns(3)
            col1.metric("Clientes Notificados", f"{df_envios['TELEFONE_ENVIO'].nunique():,}")
            col2.metric("Pagantes na Janela", f"{df_pagamentos_campanha['MATRICULA_CLIENTE'].nunique():,}")
            col3.metric("Valor Arrecadado", fmt_brl(df_pagamentos_campanha['VALOR_PAGO'].sum()))

        with aba2:
            if 'CIDADE' in df_pagamentos_campanha.columns:
                st.subheader("Arrecadação por Cidade")
                fig_cid = px.bar(df_pagamentos_campanha.groupby('CIDADE')['VALOR_PAGO'].sum().reset_index(), x='CIDADE', y='VALOR_PAGO')
                st.plotly_chart(fig_cid, use_container_width=True)

        with aba3:
            if 'MES_ANO_FATURA' in df_pagamentos_campanha.columns:
                st.subheader("Arrecadação por Mês/Ano da Fatura")
                fig_mes = px.bar(df_pagamentos_campanha.groupby('MES_ANO_FATURA')['VALOR_PAGO'].sum().reset_index(), x='MES_ANO_FATURA', y='VALOR_PAGO')
                st.plotly_chart(fig_mes, use_container_width=True)

        with aba4:
            if 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
                st.subheader("Arrecadação por Canal")
                fig_canal = px.bar(df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['VALOR_PAGO'].sum().reset_index(), x='TIPO_PAGAMENTO', y='VALOR_PAGO')
                st.plotly_chart(fig_canal, use_container_width=True)

        with aba5:
            st.subheader("Detalhes dos Pagamentos")
            st.dataframe(df_pagamentos_campanha, use_container_width=True)
