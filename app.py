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

# Configurações iniciais
fuso_br = pytz.timezone('America/Sao_Paulo')
hora_atual = datetime.datetime.now(fuso_br).hour

if hora_atual < 8 or hora_atual >= 18:
    st.cache_data.clear()
    st.title("🌙 Sistema em Repouso")
    st.info("O painel de análise funciona apenas das 08h às 18h.")
    st.stop()

# SISTEMA DE LOGIN
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

# INTEGRAÇÃO COM GITHUB
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

# CAMPANHAS E PAGAMENTOS
META_PATH = "data/campanhas_meta.parquet"
PAG_PATH  = "data/pagamentos.par"

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

    if not ok_envioss or not ok_clientes: return None, "Erro ao salvar arquivos da campanha."

    df_meta = load_campanhas_meta()
    nova = pd.DataFrame([{
        'id': campanha_id, 'nome': nome, 'criado_em': pd.Timestamp.now(),
        'total_envios': df_envioss['TELEFONE_ENVIO'].nunique(), 'total_clientes': len(df_clientes)
    }])
    df_meta = pd.concat([df_meta, nova], ignore_index=True)
    save_file_to_github(META_PATH, df_to_parquet_bytes(df_meta), f"Meta: campanha {nome} criada")
    return campanha_id, None

def update_campanha(campanha_id, nome, df_envios_novos=None, df_clientes_novos=None):
    df_meta = load_campanhas_meta()
    idx = df_meta.index[df_meta['id'] == campanha_id].tolist()
    if not idx: return False, "Campanha não encontrada."

    if df_envioss_novos is not None:
        df_envioss_existente = load_campanha_envios(campanha_id)
        df_envioss_combined = pd.concat([df_envioss_existente, df_envioss_novos], ignore_index=True) if df_envioss_existente is not None else df_envioss_novos
        df_envioss_combined = df_envioss_combined.drop_duplicates(subset=['TELEFONE_ENVIO', 'DATA_ENVIO'], keep='last')
        save_file_to_github(f"data/campanhas/{campanha_id}_envios.parquet", df_to_parquet_bytes(df_envioss_combined), f"Campanha {nome}: atualização envios")
        df_meta.at[idx[0], 'total_envios'] = df_envioss_combined['TELEFONE_ENVIO'].nunique()

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
    colunas_cli = ['TELEFONE_CLIENTE', 'MATricula_cliente', 'situacao', 'cidade', 'diretoria']
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

    colunas_uteis = ["matricula_pagamento", "data_pagamento", "valor_pago", "cidade", "tipo_pagamento", "vencimento", "utilizacao", "tipo_fatura"]
    df = parquet_bytes_to_df(content, colunas=colunas_uteis)

    if df is not None:
        colunas_categoricas = ['cidade', 'tipo_pagamento']
        for col in colunas_categoricas:
            if col in df.columns:
                df[col] = df[col].astype('category')

        df['valor_pago'] = pd.to_numeric(df['valor_pago'], errors='coerce')

    return df

def fmt_brl(valor):
    try: return f"R$ {float(valor):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except: return "R$ 0,00"

# INTERFACE STREAMLIT
st.set_page_config(layout="wide", page_title="Análise de campanha de cobrança")

if not st.session_state.get("logged_in"):
    login_screen()
    st.stop()

st.sidebar.markdown(f"👤 **{st.session_state['username']}**")
if st.sidebar.button("Sair"):
    st.session_state.clear()
    st.rerun()

st.sidebar.markdown("---")

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

# CARREGAMENTO DOS DADOS
df_envios     = None
df_clientes   = None
df_pagamentos = None
dados_prontos = False

if campanha_selecionada is not None:
    with st.spinner("Carregando dados da campanha..."):
        df_envioss   = load_campanha_envioss(campanha_selecionada['id'])
        df_clientes = load_campanha_clientes(campanha_selecionada['id'])
        df_pagamentos = load_pagamentos_github()

    dados_pronto = (
        df_envios is not None and
        df_clientes is not None and
        df_pagamentos is not None
    )

# ANÁLISE
if executar_analise and dados_pronto:

    # Cruzamento envios x clientes
    total_clientes_unicos_base_envios = df_envios['TELEFONE_ENVIO'].nunique()
    total_base_envio = df_envios['TELEFONE_ENVIO'].count()
    df_merge = pd.merge(
        df_envios,
        df_clientes,
        left_on='TELEFONE_envio',
        right_on='TELEFONE_cliente',
        how='inner'
    )

    if df_merge.empty:
        st.error("Nenhum cliente encontrado após cruzamento entre envios e clientes.")
        st.stop()

    df_merge['matricula_cliente'] = df_merge['matricula_cliente'].astype(str).str.strip()
    df_pagamentos['matricula_pagamento'] = df_pagamentos['matricula_pagamento'].astype(str).str.strip()

    # Cálculo de dívida
    total_divida_base_envios = df_merge.drop_duplicates(subset=['matricula_cliente'])['situacao'].sum()

    df_entregues = df_merge[df_merge['status_envio'] == 'DELIVERED_to_HANDSET']
    total_divida_notificados = df_entregues.drop_duplicates(subset=['matricula_cliente'])['situacao'].sum()

    # Filtragem de pagamentos
    matricula_alvo = df_merge['matricula_cliente'].unique()
    df_pagamentos_filtrado = df_pagamentos[df_pagamentos['matricula_pagamento'].isin(matricula_alvo)].copy()

    df_cruzado = pd.merge(
        df_merge,
        df_pagamentos_filtrado,
        left_on='matricula_cliente',
        right_on='matricula_pagamento',
        how='inner'
    )

    df_cruzado['dias_apos_envio'] = (
        df_cruzado['data_pagamento'] - df_cruzado['data_envio']
    ).dt.days

    df_pagamentos_campanha = df_cruzado[
        (df_cruzado['dias_apos_envio'] >= 0) &
        (df_cruzado['dias_apos_envio'] <= janela_dias)
    ].copy()

    df_pagamentos_campanha = df_pagamentos_campanha.drop_duplicates(
        subset=['matricula_cliente', 'data_pagamento', 'valor_pago', 'vencimento'],
        keep='first'
    )
    df_pagamentos_campanha.rename(columns={'matricula_cliente': 'matricula'}, inplace=True)

    # Métricas
    clientes_unicos_que_pagaram_matricula = df_pagamentos_campanha['matricula'].nunique()
    qtd_pagamentos = df_pagamentos_campanha['matricula'].count()
    valor_total_arrecadado = df_pagamentos_campanha['valor_pago'].sum() if not df_pagamentos_campanha.empty else 0
    taxa_eficiencia_clientes_notificados = (clientes_unicos_que_pagaram_matricula / total_clientes_notificados * 100) if total_clientes_notificados > 0 else 0
    taxa_eficiencia_valor_notificados = (valor_total_arrecadado / total_divida_notificados * 100) if total_divida_notificados >0 else 0
    taxa_eficiencia_clientes_base_envios = (clientes_unicos_que_pagaram_matricula / total_clientes_unicos_base_envios * 100) if total_clientes_unicos_base_envios >0 else 0
    taxa_eficiencia_valor_base = (valor_total_arrecadado / total_divida_base_envios * 100) if total_divida_base_envios >0 else 0
    ticket_medio = (valor_total_arrecadado / clientes_unicos_que_pagaram_matricula) if clientes_unicos_que_pagaram_matricula >0 else 0
    custo_campanha = total_base_envio * 0.05
    roi = ((valor_total_arrecadado - custo_campanha) / custo_campanha * 100) if custo_campanha >0 else 0

    # Abas
    aba1, aba2, aba3, aba4, aba5, aba6 = st.tabs([
        "📊 Visão Geral",
        "🏙️ Cidade e Diretoria",
        "📅 Análise das Faturas",
        "💳 Canal de Pagamento",
        "📋 Detalhes",
        "🧪 Novas Visualizações"
    ])

    # ABA 1 — VISÃO GERAL
    with aba1:
        st.subheader("Resultados da Análise da Campanha")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Clientes na base de envios", f"{total_clientes_unicos_base_envios:,}")
        col2.metric("Clientes notificados", f"{total_clientes_notificados:,}")
        col3.metric("Envios rejeitados", f"{total_envios_rejeitados:,}")
        col4.metric("Eficiência dos disparos", f"{taxa_eficiencia_disparos:,.2f}%".replace(",", "X").replace(".", ",").replace("X", "."))

        col5, col6 = st.columns(2)
        col5.metric("Clientes que pagaram", f"{clientes_unicos_que_pagaram_matricula:,}")
        col6.metric("Quantidade de pagamentos", f"{qtd_pagamentos:,}")

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
            st.plotly_chart(fig_dias, use_container_width=True, key="fig_dias")

    # ABA2 — CIDADE E DIRETORIA
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

    # ABA3 — ANÁLISE DAS FAT
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
                    title='Valor Pago por Faixa de Antiguidade da Díida',
                    labels={'faixa_antiguidade': 'Faixa de Antiguidade', 'valor_pago': 'Valor Pago (R$)'}
                )
                st.plotly_chart(fig_ant_valor, use_container_width=True, key="fig_ant_valor")

            if 'mes_ano_fatura' in df_pagamentos_campanha.columns:
                st.subheader("Valor Pago por Mês/Anno da Fatura")
                mes_ano_res = df_pagamentos_campanha.groupby('mes_ano_fatura')['valor_pago'].sum().reset_index()
                fig_mes_ano = px.bar(
                    mes_ano_res, x='mes_ano_fatura', y='valor_pago',
                    title='Valorpago por Mês/Ano da Fatura',
                    labels={'mes_ano_fatura': 'Mês/Ano da Fatura', 'valor_pago': 'Valor Pago (R$)'}
                )
                st.plotly_chart(fig_mes_ano, use_container_width=True, key="fig_mes_ano")

    # ABA4 — CANAL DE PAGAMENTO
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

            if 'diretoria' in df_pagamentos_campanha.columns:
                st.subheader("Canal de Pagamento por Diretoria")
                canal_diretoria = df_pagamentos_campanha.groupby(['diretoria', 'tipo_pagamento'])['valor_pago'].sum().reset_index()
                fig_canal_dir = px.bar(
                    canal_diretoria, x='diretoria', y='valor_pago', color='tipo_pagamento',
                    title='Valor Arrecadado: Diretoria x Canal de Pagamento',
                    labels={'diretoria': 'Diretoria', 'valor_pago': 'Valor (R$)', 'tipo_pagamento': 'Canal'},
                    barmode='stack'
                )
                st.plotly_chart(fig_canal_dir, use_container_width=True, key="fig_canal_dir_aba4")

            if 'cidade' in df_pagamentos_campanha.columns:
                st.subheader("Canal de Pagamento por Cidade")
                canal_cidade = df_pagamentos_campanha.groupby(['cidade', 'tipo_pagamento'])['valor_pago'].sum().reset_index()
                ordem_cidades = canal_cidade.groupby('cidade')['valor_pago'].sum().sort_values(ascending=False).index
                fig_canal_cid = px.bar(
                    canal_cidade, x='cidade',y='valor_pago', color='tipo_pagamento',
                    title='Valor Arrecadado: Cidade x Canal de Pagamento',
                    labels={'cidade': 'Cidade', 'valor_pago': 'Valor (R$)', 'tipo_pagamento': 'Canal'},
                    barmode='stack',
                    category_orders={'cidade': ordem_cidades}
                )
                st.plotly_chart(fig_canal_cid, use_container_width=True, key="fig_canal_cid_aba4")            

        else:
            st.info("Coluna 'tipo_pagamento' não encontrada no arquivo de pagamentos.")

    # ABA5 — DETALHES
    with aba5:
        if not df_pagamentos_campanha.empty:
            st.sub("Detalhes dos Pagamentos Atribuídos à Campanha")

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
            st.st("Nenhum pagamento encontrado dentro da janela definida para a campanha.")

    # ABA6 — NOVAS VISUALIZAÇÕES
    with aba6:
        if not df_pagamentos_campanha.empty:
            st.header("Exploração de Novas Visualizações")
            st.markdown("Avalie estes gráficosos. Os que forem úteis podem ser movidos para as abas principais depois.")

            # Curva de Arrecadação Acumulada
            st.sub("📈 Curva de Arrecadação Acumulada")
            df_acumulado = df_pagamentos_campanha.groupby('dias_apos_envio')['valor_pago'].sum().reset_index()
            df_acumulado['valor_acumulado'] = df_acumulado['valor_pago'].cumsum()
            fig_acumulado = px.line(
                df_acumulado, x='dias_apos_envio',y='valor_acumulado',
                title='Evolução da Arrecadação (Acumulada ao longo dos dias)',
                labels={'dias_apos_envio': 'Dias Após o Envio', 'valor_acumulado': 'Valor Acumulado (R$)'},
                markers=True
            )
            st.plotly_chart(fig_acumulado, use_container_width=True, key="fig_acumulado_aba6")

            # Canal de Pagamento por Cidade (Gráfico Empilhado)
            if 'cidade' in df_pagamentos_campanha.columns and 'tipo_pagamento' in df_pagamentos_campanha.columns:
                st.st("🏙️ Canal de Pagamento por Cidade")
                canal_cidade = df_pagamentos_campanha.groupby(['cidade', 'tipo_pagamento'])['valor_pago'].sum().reset_index()
                ordem_cidades = canal_cidade.groupby('cidade')['valor_pago'].sum().sort_values(ascending=False).index
                fig_canal_cid = px.bar(
                    canal_cidade,x='cidade',y='valor_pago', color='tipo_pagamento',
                    title='Valor Arrecadado: Cidade x Canal de Pagamento',
                    labels={'cidade': 'Cidade', 'valor_pago': 'Valor (R$)', 'tipo_pagamento': 'Canal'},
                    barmode='stack',
                    category_orders={'cidade': ordem_cidades}
                )
                st.plotly_chart(fig_canal_cid, use_container_width=True, key="fig_canal_cid_aba6")

            # Ticket Médio por Cidade
            if 'cidade' in df_pagamentos_campanha.columns:
                st.st("🎫 Ticket Médio por Cidade")
                tm_cidade = df_pagamentos_campanha.groupby('cidade').agg(
                    Valor=('valor_pago', 'sum'),
                    Clientes=('matricula', 'nunique')
                ).reset_index()
                tm_cidade['Ticket_Medio'] = tm_cidade['Valor'] / tm_cidade['Clientes']
                tm_cidade = tm_cidade.sort_values('Ticket_Medio', ascending=False)
                fig_tm_cid = px.bar(
                    tm_cidade,x='cidade',y='Ticket_Medio',
                    title='Ticket Médio por Cidade',
                    labels={'cidade': 'Cidade', 'Ticket_Medio': 'Ticket Médio (R$)'},
                    text_auto='.2f'
                )
                st.plotly_chart(fig_tm_cid, use_container_width=True, key="fig_tm_cid_aba6")

            # Mapa de Calor: Dia do Pagamento x Canal
            if 'tipo_pagamento' in df_pagamentos_campanha.columns:
                st.st("🔥 Concentração: Tempo de Pagamento x Canal")
                heatmap_data = df_pagamentos_campanha.groupby(['tipo_pagamento', 'dias_apos_envio'])['valor_pago'].sum().reset_index()
                fig_heat = px.density_heatmap(
                    heatmap_data,x='dias_apos_envio',y='tipo_pagamento',z='valor_pago',
                    title='Mapa de Calor: Em quais dias cada canal arrecada mais?',
                    labels={'dias_apos_envio': 'Dias Após Envio', 'tipo_pagamento': 'Canal', 'valor_pago': 'Valor (R$)'},
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig_heat, use_container_width=True, key="fig_heat_aba6")

            # Utilização (Subcategoria)
            if 'utilizacao' in df_pagamentos_campanha.columns:
                st.st("💧 Arrecadação por Tipo de Utilização")
                util_resumo = df_pagamentos_campanha.groupby('utilizacao')['valor_pago'].sum().reset_index().sort_values('valor_pago', ascending=False)
                fig_util = px.pie(
                    util_resumo, names='utilizacao', values='valor_pago',
                    title='Distribuição por Utilização (Subcategoria)',
                    hole=0.4
                )
                st.plotly_chart(fig_util, use_container_width=True, key="fig_util_aba6")

elif executar_analise and not dados_pronto:
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
