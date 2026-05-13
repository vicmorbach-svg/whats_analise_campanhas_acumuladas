import streamlit as st
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

def parquet_bytes_to_df(content_bytes, colunas=None):
    if not content_bytes: return None
    try:
        buf = io.BytesIO(content_bytes)
        buf.seek(0)
        return pd.read_parquet(buf, engine='pyarrow', columns=colunas)
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
    df_meta = df_meta[df_meta['id'] != campanha_id]
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} removida")
    delete_file_from_github(f"data/campanhas/{campanha_id}_envios.parquet", f"Removendo envios {nome}")
    delete_file_from_github(f"data/campanhas/{campanha_id}_clientes.parquet", f"Removendo clientes {nome}")

@st.cache_data(ttl=900, max_entries=1)
def load_pagamentos_github():
    content, _ = get_file_from_github(PAG_PATH)
    if not content: return None

    # Passando as colunas para ler APENAS o necessário
    colunas_uteis = ["MATRICULA_PAGAMENTO", "DATA_PAGAMENTO", "VALOR_PAGO", "CIDADE", "TIPO_PAGAMENTO", "VENCIMENTO"]
    df = parquet_bytes_to_df(content, colunas=colunas_uteis)

    if df is not None:
        # Downcasting imediato (converte textos repetidos em categorias leves)
        colunas_categoricas = ['CIDADE', 'TIPO_PAGAMENTO']
        for col in colunas_categoricas:
            if col in df.columns:
                df[col] = df[col].astype('category')

        # Reduz o peso da coluna de valor
        if 'VALOR_PAGO' in df.columns:
            df['VALOR_PAGO'] = pd.to_numeric(df['VALOR_PAGO'], downcast='float')

    return df

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
    load_pagamentos_github.clear() 
    return ok, len(df_combined), novos

# ══════════════════════════════════════════════════════════════
# PROCESSAMENTO DE ARQUIVOS
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
            'Utilização (Sub. Categ.)': 'UTILIZACAO'
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
df_pag_geral = load_pagamentos_github()
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
        if not df_meta.empty:
            camp_upd = st.selectbox("Campanha", df_meta['nome'].tolist())
            up_env_u = st.file_uploader("Novos Envios", type=["xlsx", "parquet"], key="u_env")
            up_cli_u = st.file_uploader("Novos Clientes", type=["xlsx", "parquet"], key="u_cli")
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

    # Verifica se todos os 3 arquivos foram carregados com sucesso
    dados_prontos = (
        df_envios is not None and
        df_clientes is not None and
        df_pagamentos is not None
    )

# ══════════════════════════════════════════════════════════════
# ANÁLISE (VERSÃO OTIMIZADA PARA MEMÓRIA)
# ══════════════════════════════════════════════════════════════

if executar_analise and dados_prontos:

    # ── Cruzamento envios x clientes ──────────────────────────
    total_clientes_unicos_base_envios = df_envios['TELEFONE_ENVIO'].nunique()
    total_base_envio = df_envios['TELEFONE_ENVIO'].count()
    total_clientes_notificados = df_envios[df_envios['STATUS_ENVIO'] == 'DELIVERED_TO_HANDSET']['TELEFONE_ENVIO'].nunique()
    total_envios_rejeitados    = df_envios[df_envios['STATUS_ENVIO'] != 'DELIVERED_TO_HANDSET']['TELEFONE_ENVIO'].count()
    taxa_eficiencia_disparos   = (total_clientes_notificados / total_clientes_unicos_base_envios * 100) if total_clientes_unicos_base_envios > 0 else 0

    

    df_merge = pd.merge(
        df_envios,
        df_clientes,
        left_on='TELEFONE_ENVIO',
        right_on='TELEFONE_CLIENTE',
        how='inner'
    )

    if df_merge.empty:
        st.error("Nenhum cliente encontrado após cruzamento entre envios e clientes.")
        st.stop()

    

    # Garante tipo string nos campos de matrícula para o merge
    df_merge['MATRICULA_CLIENTE'] = df_merge['MATRICULA_CLIENTE'].astype(str).str.strip()
    df_pagamentos['MATRICULA_PAGAMENTO'] = df_pagamentos['MATRICULA_PAGAMENTO'].astype(str).str.strip()

    # ── Cálculos de Dívida (Removendo duplicatas por cliente) ──
    total_divida_base_envios = df_merge.drop_duplicates(subset=['MATRICULA_CLIENTE'])['SITUACAO'].sum()

    df_entregues = df_merge[df_merge['STATUS_ENVIO'] == 'DELIVERED_TO_HANDSET']
    total_divida_notificados = df_entregues.drop_duplicates(subset=['MATRICULA_CLIENTE'])['SITUACAO'].sum()

    # ── OTIMIZAÇÃO DE MEMÓRIA: Pré-filtragem ──────────────────
    # Descobre quais matrículas realmente importam para esta campanha
    matriculas_alvo = df_merge['MATRICULA_CLIENTE'].unique()

    # Filtra a base gigante de pagamentos ANTES de fazer o merge
    df_pagamentos_filtrado = df_pagamentos[df_pagamentos['MATRICULA_PAGAMENTO'].isin(matriculas_alvo)].copy()

    # Libera a memória da base gigante original (opcional, mas recomendado)
    del df_pagamentos
    load_pagamentos_github.clear()
    gc.collect() # Força a limpeza da RAM

    # ── Cruzamento final (agora muito mais leve) ──────────────
    df_cruzado = pd.merge(
        df_merge,
        df_pagamentos_filtrado,
        left_on='MATRICULA_CLIENTE',
        right_on='MATRICULA_PAGAMENTO',
        how='inner'
    )

    # Limpa variáveis temporárias que não serão mais usadas
    del df_merge
    del df_pagamentos_filtrado
    gc.collect()

    if df_cruzado.empty:
        st.error("Nenhum pagamento encontrado após cruzamento com a base de clientes.")
        st.stop()

    df_cruzado['DIAS_APOS_ENVIO'] = (
        df_cruzado['DATA_PAGAMENTO'] - df_cruzado['DATA_ENVIO']
    ).dt.days

    df_pagamentos_campanha = df_cruzado[
        (df_cruzado['DIAS_APOS_ENVIO'] >= 0) &
        (df_cruzado['DIAS_APOS_ENVIO'] <= janela_dias)
    ].copy()

    # Limpa o dataframe cruzado completo
    del df_cruzado
    gc.collect()

    df_pagamentos_campanha = df_pagamentos_campanha.drop_duplicates(
        subset=['MATRICULA_CLIENTE', 'DATA_PAGAMENTO', 'VALOR_PAGO'],
        keep='first'
    )

    df_pagamentos_campanha.rename(columns={'MATRICULA_CLIENTE': 'MATRICULA'}, inplace=True)


    # ── Métricas ──────────────────────────────────────────────
    clientes_que_pagaram_matriculas = df_pagamentos_campanha['MATRICULA'].nunique()
    valor_total_arrecadado          = df_pagamentos_campanha['VALOR_PAGO'].sum() if not df_pagamentos_campanha.empty else 0
    taxa_eficiencia_clientes_notificados        = (clientes_que_pagaram_matriculas / total_clientes_notificados * 100) if total_clientes_notificados > 0 else 0
    taxa_eficiencia_valor_notificados           = (valor_total_arrecadado / total_divida_notificados * 100) if total_divida_notificados > 0 else 0
    taxa_eficiencia_clientes_base_envios        = (clientes_que_pagaram_matriculas / total_clientes_unicos_base_envios * 100) if total_clientes_unicos_base_envios > 0 else 0
    taxa_eficiencia_valor_base           = (valor_total_arrecadado / total_divida_base_envios * 100) if total_divida_base_envios > 0 else 0
    ticket_medio                    = (valor_total_arrecadado / clientes_que_pagaram_matriculas) if clientes_que_pagaram_matriculas > 0 else 0
    custo_campanha                  = total_clientes_unicos_base_envios * 0.05
    roi                             = ((valor_total_arrecadado - custo_campanha) / custo_campanha * 100) if custo_campanha > 0 else 0
    # ── Abas ─────────────────────────────────────────────────
    aba1, aba2, aba3, aba4, aba5 = st.tabs([
        "📊 Visão Geral",
        "🏙️ Cidade e Diretoria",
        "📅 Análise das Faturas",
        "💳 Canal de Pagamento",
        "📋 Detalhes"
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
        col3.metric("Envios rejeitados", f"{total_envios_rejeitados:,}")
        col4.metric("Eficiência dos disparos", f"{taxa_eficiencia_disparos:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        st.markdown("##### 💰 Conversão e Arrecadação")
        col5, = st.columns(1)
        col5.metric("Clientes que pagaram",   f"{clientes_que_pagaram_matriculas:,}")

        col6, col7 = st.columns(2)
        col6.metric("Taxa de eficiência base envios",  f"{taxa_eficiencia_clientes_base_envios:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)
        col7.metric("Taxa de eficiência clientes notificados",  f"{taxa_eficiencia_clientes_notificados:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)

        col8, col9, col10 = st.columns(3)
        col8.metric("Dívida total da base", fmt_brl(total_divida_base_envios))
        col9.metric("Dívida dos notificados", fmt_brl(total_divida_notificados))
        col10.metric("Valor total arrecadado",          fmt_brl(valor_total_arrecadado))

        col11, col12 = st.columns(2)
        col11.metric("Taxa eficiênica dívida total", f"{taxa_eficiencia_valor_base:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)
        col12.metric("Taxa eficiênica dívida notificada", f"{taxa_eficiencia_valor_notificados:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."), border=True)

        col13, col14, col15, col16 = st.columns(4)  
        col13.metric("Ticket médio",   fmt_brl(ticket_medio))
        col14.metric("Total de disparos", f"{total_base_envio}")
        col15.metric("Custo da campanha", fmt_brl(custo_campanha))
        col16.metric("ROI",              f"{roi:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        if not df_pagamentos_campanha.empty:
            st.subheader(f"Pagamentos por Dia Após o Envio (Janela de {janela_dias} dias)")

            pagamentos_por_dia = df_pagamentos_campanha.groupby('DIAS_APOS_ENVIO')['VALOR_PAGO'].sum().reset_index()
            pagamentos_por_dia.rename(columns={'DIAS_APOS_ENVIO': 'Dias Após Envio', 'VALOR_PAGO': 'Valor Total Pago'}, inplace=True)

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
            tem_cidade    = 'CIDADE'    in df_pagamentos_campanha.columns
            tem_diretoria = 'DIRETORIA' in df_pagamentos_campanha.columns

            if tem_cidade:
                st.subheader("Análise por Cidade")
                cidade_resumo = df_pagamentos_campanha.groupby('CIDADE').agg(
                    Clientes_que_Pagaram=('MATRICULA', 'nunique'),
                    Valor_Arrecadado=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Arrecadado', ascending=False)

                fig_cidade_valor = px.bar(
                    cidade_resumo, x='CIDADE', y='Valor_Arrecadado',
                    title='Valor Arrecadado por Cidade',
                    labels={'CIDADE': 'Cidade', 'Valor_Arrecadado': 'Valor Arrecadado (R$)'}
                )
                st.plotly_chart(fig_cidade_valor, use_container_width=True, key="fig_cidade_valor")

            if tem_diretoria:
                st.subheader("Análise por Diretoria")
                diretoria_resumo = df_pagamentos_campanha.groupby('DIRETORIA').agg(
                    Clientes_que_Pagaram=('MATRICULA', 'nunique'),
                    Valor_Arrecadado=('VALOR_PAGO', 'sum')
                ).reset_index().sort_values('Valor_Arrecadado', ascending=False)

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
        if not df_pagamentos_campanha.empty:
            if 'VENCIMENTO' in df_pagamentos_campanha.columns:
                st.subheader("Antiguidade da Dívida Paga")
                df_pagamentos_campanha['ANTIGUIDADE_DIAS'] = (df_pagamentos_campanha['DATA_PAGAMENTO'] - df_pagamentos_campanha['VENCIMENTO']).dt.days

                def classificar_antiguidade(dias):
                    if pd.isna(dias): return 'Não informado'
                    elif dias <= 10:  return '0-10 dias'
                    elif dias <= 20:  return '11-20 dias'
                    elif dias <= 30:  return '21-30 dias'
                    elif dias <= 60:  return '31-60 dias'
                    else:             return 'Mais de 61 dias'

                df_pagamentos_campanha['FAIXA_ANTIGUIDADE'] = df_pagamentos_campanha['ANTIGUIDADE_DIAS'].apply(classificar_antiguidade)
                antiguidade_resumo = df_pagamentos_campanha.groupby('FAIXA_ANTIGUIDADE')['VALOR_PAGO'].sum().reset_index()

                fig_ant_valor = px.bar(
                    antiguidade_resumo, x='FAIXA_ANTIGUIDADE', y='VALOR_PAGO',
                    title='Valor Pago por Faixa de Antiguidade da Dívida',
                    labels={'FAIXA_ANTIGUIDADE': 'Faixa de Antiguidade', 'VALOR_PAGO': 'Valor Pago (R$)'}
                )
                st.plotly_chart(fig_ant_valor, use_container_width=True, key="fig_ant_valor")

            if 'MES_ANO_FATURA' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Mês/Ano da Fatura")
                mes_ano_resumo = df_pagamentos_campanha.groupby('MES_ANO_FATURA')['VALOR_PAGO'].sum().reset_index()
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
        if not df_pagamentos_campanha.empty and 'TIPO_PAGAMENTO' in df_pagamentos_campanha.columns:
            st.subheader("Valor Arrecadado por Canal de Pagamento")
            pagamentos_por_canal = df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['VALOR_PAGO'].sum().reset_index()
            pagamentos_por_canal = pagamentos_por_canal.sort_values('VALOR_PAGO', ascending=False)

            fig_canal_aba4 = px.bar(
                pagamentos_por_canal, x='TIPO_PAGAMENTO', y='VALOR_PAGO',
                title='Valor Arrecadado por Canal de Pagamento',
                labels={'TIPO_PAGAMENTO': 'Canal de Pagamento', 'VALOR_PAGO': 'Valor Total Pago (R$)'},
                color='TIPO_PAGAMENTO'
            )
            st.plotly_chart(fig_canal_aba4, use_container_width=True, key="fig_canal_aba4")

            st.subheader("Clientes que Pagaram por Canal")
            qtd_por_canal = df_pagamentos_campanha.groupby('TIPO_PAGAMENTO')['MATRICULA'].nunique().reset_index()
            qtd_por_canal.rename(columns={'MATRICULA': 'Clientes que Pagaram'}, inplace=True)
            qtd_por_canal = qtd_por_canal.sort_values('Clientes que Pagaram', ascending=False)

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
        else:
            st.info("Coluna 'TIPO_PAGAMENTO' não encontrada no arquivo de pagamentos.")

    # ══════════════════════════════════════════════════════════
    # ABA 5 — DETALHES
    # ══════════════════════════════════════════════════════════
    with aba5:
        if not df_pagamentos_campanha.empty:
            st.subheader("Detalhes dos Pagamentos Atribuídos à Campanha")

            colunas_possiveis = [
                'MATRICULA', 'CIDADE', 'DIRETORIA', 'TELEFONE_ENVIO',
                'DATA_ENVIO', 'DATA_PAGAMENTO', 'VENCIMENTO',
                'VALOR_PAGO', 'DIAS_APOS_ENVIO',
                'TIPO_FATURA', 'UTILIZACAO', 'TIPO_PAGAMENTO'
            ]
            colunas_exibicao = [c for c in colunas_possiveis if c in df_pagamentos_campanha.columns]
            df_detalhes = df_pagamentos_campanha[colunas_exibicao].drop_duplicates(
                subset=['MATRICULA', 'DATA_PAGAMENTO', 'VALOR_PAGO']
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
