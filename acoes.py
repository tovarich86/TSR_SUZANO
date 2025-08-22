import streamlit as st
import requests
import pandas as pd
import yfinance as yf
from base64 import b64encode
from datetime import datetime, timedelta
import json
import re
import time
from io import BytesIO

# Importa curl_cffi para criar sessão com fingerprint de navegador
from curl_cffi import requests as curl_requests
from requests.cookies import create_cookie
import yfinance.data as _data

# URL do arquivo no GitHub
URL_EMPRESAS = "https://github.com/tovarich86/ticker/raw/refs/heads/main/empresas_b3.xlsx"

@st.cache_data
def carregar_empresas():
    """Carrega e pré-processa o DataFrame de empresas a partir de um arquivo Excel."""
    try:
        df_empresas = pd.read_excel(URL_EMPRESAS)
        cols_to_process = ['Nome do Pregão', 'Tickers', 'CODE', 'typeStock']
        for col in cols_to_process:
            if col in df_empresas.columns:
                df_empresas[col] = df_empresas[col].astype(str).fillna('')
                df_empresas[col] = df_empresas[col].str.strip()
                if col == 'Nome do Pregão':
                    df_empresas[col] = df_empresas[col].str.replace(r'\s*S\.?A\.?/A?', ' S.A.', regex=True).str.upper().str.strip()
                if col == 'typeStock':
                    df_empresas[col] = df_empresas[col].str.upper()
        df_empresas = df_empresas[df_empresas['Tickers'] != '']
        df_empresas = df_empresas[df_empresas['Nome do Pregão'] != '']
        return df_empresas
    except Exception as e:
        st.error(f"Erro ao carregar ou processar a planilha de empresas: {e}")
        return pd.DataFrame()

def get_ticker_info(ticker, empresas_df):
    """
    Busca informações de um ticker (Nome do Pregão, CODE, typeStock) na planilha de empresas.
    Retorna um dicionário com as informações ou None se não encontrado.
    """
    ticker_upper = ticker.strip().upper()
    for index, row in empresas_df.iterrows():
        tickers_list = [t.strip().upper() for t in row['Tickers'].split(",") if t.strip()]
        if ticker_upper in tickers_list:
            return {
                'trading_name': row['Nome do Pregão'],
                'code': row['CODE'],
                'type_stock': row['typeStock']
            }
    return None

# --- Patch para cookies do yfinance (mantido do código original) ---
def _wrap_cookie(cookie, session):
    if isinstance(cookie, str):
        value = session.cookies.get(cookie)
        return create_cookie(name=cookie, value=value)
    return cookie

def patch_yfdata_cookie_basic():
    original = _data.YfData._get_cookie_basic
    def _patched(self, timeout=30):
        cookie = original(self, timeout)
        return _wrap_cookie(cookie, self._session)
    _data.YfData._get_cookie_basic = _patched

patch_yfdata_cookie_basic()


# --- Funções de Busca da B3 (mantidas do código original) ---
def buscar_dividendos_b3(ticker, empresas_df, data_inicio, data_fim):
    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info: return pd.DataFrame()
    trading_name = ticker_info['trading_name']
    desired_type_stock = ticker_info['type_stock']
    if not trading_name or not desired_type_stock: return pd.DataFrame()
    all_dividends = []
    current_page = 1
    total_pages = 1
    with st.spinner(f"Buscando dividendos B3 para {ticker}..."):
        while current_page <= total_pages:
            try:
                params = {"language": "pt-br", "pageNumber": str(current_page), "pageSize": "50", "tradingName": trading_name}
                params_json = json.dumps(params)
                params_encoded = b64encode(params_json.encode('utf-8')).decode('utf-8')
                url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/{params_encoded}'
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                response_json = response.json()
                if current_page == 1 and 'page' in response_json and 'totalPages' in response_json['page']:
                    total_pages = int(response_json['page']['totalPages'])
                if 'results' in response_json and response_json['results']:
                    all_dividends.extend(response_json['results'])
                else: break
                if total_pages > 1: time.sleep(0.5)
                current_page += 1
            except Exception as e:
                st.error(f"Erro ao buscar dividendos na B3 para {ticker} (página {current_page}): {e}")
                break
    if not all_dividends: return pd.DataFrame()
    df = pd.DataFrame(all_dividends)
    if 'typeStock' in df.columns:
        df['typeStock'] = df['typeStock'].str.strip().str.upper()
        df = df[df['typeStock'] == desired_type_stock].copy()
    if df.empty: return pd.DataFrame()
    df['Ticker'] = ticker
    if 'lastDatePriorEx' in df.columns:
        df['lastDatePriorEx_dt'] = pd.to_datetime(df['lastDatePriorEx'], format='%d/%m/%Y', errors='coerce')
        df = df.dropna(subset=['lastDatePriorEx_dt'])
        df = df[(df['lastDatePriorEx_dt'] >= data_inicio) & (df['lastDatePriorEx_dt'] <= data_fim)]
        df = df.drop(columns=['lastDatePriorEx_dt'])
        df['lastDatePriorEx'] = df['lastDatePriorEx'].astype(str)
    if df.empty: return pd.DataFrame()
    cols_to_keep = ['Ticker', 'paymentDate', 'typeStock', 'lastDatePriorEx', 'value', 'relatedToAction', 'label', 'ratio']
    existing_cols_to_keep = [col for col in cols_to_keep if col in df.columns]
    other_cols = [col for col in df.columns if col not in existing_cols_to_keep]
    return df[existing_cols_to_keep + other_cols]

def buscar_bonificacoes_b3(ticker, empresas_df, data_inicio, data_fim):
    ticker_info = get_ticker_info(ticker, empresas_df)
    if not ticker_info or not ticker_info.get('code'): return pd.DataFrame()
    code = ticker_info['code']
    try:
        with st.spinner(f"Buscando bonificações B3 para {ticker}..."):
            params_bonificacoes = {"issuingCompany": code, "language": "pt-br"}
            params_json = json.dumps(params_bonificacoes)
            params_encoded = b64encode(params_json.encode('utf-8')).decode('utf-8')
            url = f'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/{params_encoded}'
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            if not response.content or not response.text.strip(): return pd.DataFrame()
            data = response.json()
            if not isinstance(data, list) or not data or "stockDividends" not in data[0] or not data[0]["stockDividends"]:
                return pd.DataFrame()
            df = pd.DataFrame(data[0]["stockDividends"])
            if df.empty: return pd.DataFrame()
            df['Ticker'] = ticker
            if 'lastDatePrior' in df.columns:
                df['lastDatePrior_dt'] = pd.to_datetime(df['lastDatePrior'], format='%d/%m/%Y', errors='coerce')
                df = df.dropna(subset=['lastDatePrior_dt'])
                df = df[(df['lastDatePrior_dt'] >= data_inicio) & (df['lastDatePrior_dt'] <= data_fim)]
                df = df.drop(columns=['lastDatePrior_dt'])
                df['lastDatePrior'] = df['lastDatePrior'].astype(str)
            if df.empty: return pd.DataFrame()
            cols_to_keep = ['Ticker', 'label', 'lastDatePrior', 'factor', 'approvedIn', 'isinCode']
            existing_cols_to_keep = [col for col in cols_to_keep if col in df.columns]
            other_cols = [col for col in df.columns if col not in existing_cols_to_keep]
            return df[existing_cols_to_keep + other_cols]
    except Exception as e:
        st.error(f"Erro ao buscar bonificações na B3 para {ticker}: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False)
def buscar_dados_yfinance_completo(tickers_list, data_inicio_input, data_fim_input, empresas_df):
    precos_dict = {}
    dividends_dict = {}
    bonuses_dict = {}
    erros = []
    try:
        data_inicio_str = datetime.strptime(data_inicio_input, "%d/%m/%Y").strftime("%Y-%m-%d")
        data_fim_dt = datetime.strptime(data_fim_input, "%d/%m/%Y")
        data_fim_ajustada_str = (data_fim_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    except ValueError:
        return {}, {}, {}, ["Formato de data inválido. Use dd/mm/aaaa."]
    b3_tickers_set = set()
    if 'Tickers' in empresas_df.columns:
        for t_list in empresas_df['Tickers'].dropna().str.split(','):
            for t in t_list:
                if t.strip(): b3_tickers_set.add(t.strip().upper())
    tickers_yf = []
    for ticker in tickers_list:
        if ticker in b3_tickers_set:
            tickers_yf.append(ticker + '.SA')
        else:
            tickers_yf.append(ticker)
    session = curl_requests.Session(impersonate="chrome")
    with st.spinner(f"Buscando dados no Yahoo Finance para {', '.join(tickers_list)}..."):
        try:
            dados_completos = yf.download(
                tickers=tickers_yf, start=data_inicio_str, end=data_fim_ajustada_str, auto_adjust=False,
                progress=False, actions=True, session=session
            )
        except Exception as e:
            error_type = type(e).__name__
            erros.append(f"Erro ao baixar dados do Yahoo Finance: {error_type} - {e}")
            return {}, {}, {}, erros
    if dados_completos.empty:
        erros.append(f"Nenhum dado encontrado para os tickers.")
        return {}, {}, {}, erros
    for i, ticker_original in enumerate(tickers_list):
        ticker_yf = tickers_yf[i]
        try:
            if isinstance(dados_completos.columns, pd.MultiIndex):
                if ticker_yf not in dados_completos.columns.get_level_values(1):
                    erros.append(f"Nenhum dado encontrado para {ticker_original} ({ticker_yf}).")
                    continue
                dados_ticker = dados_completos.xs(key=ticker_yf, axis=1, level=1)
            else:
                if dados_completos.empty:
                    erros.append(f"Nenhum dado encontrado para {ticker_original} ({ticker_yf}).")
                    continue
                dados_ticker = dados_completos.copy()
            if not dados_ticker.empty:
                dados_ticker = dados_ticker.reset_index()
                dados_ticker = dados_ticker[dados_ticker['Date'] <= data_fim_dt]
                dados_ticker['Date'] = pd.to_datetime(dados_ticker['Date'])
                precos_df = dados_ticker.copy()
                precos_df['Ticker'] = ticker_original
                precos_df['Date'] = precos_df['Date'].dt.strftime('%d/%m/%Y')
                standard_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
                cols_order_start = ['Ticker', 'Date']
                existing_standard_cols = [col for col in standard_cols if col in precos_df.columns]
                other_cols = [col for col in precos_df.columns if col not in cols_order_start and col not in existing_standard_cols and 'Dividends' not in col and 'Stock Splits' not in col]
                final_cols_order = cols_order_start + existing_standard_cols + other_cols
                precos_dict[ticker_original] = precos_df[final_cols_order]
                dividends_df = dados_ticker[dados_ticker['Dividends'] > 0].copy()
                if not dividends_df.empty:
                    dividends_df = dividends_df.rename(columns={'Date': 'paymentDate', 'Dividends': 'value'})
                    dividends_df['typeStock'] = 'Dividendo'
                    dividends_df['relatedToAction'] = 'Yahoo Finance'
                    dividends_df['Ticker'] = ticker_original
                    dividends_dict[ticker_original] = dividends_df[['Ticker', 'paymentDate', 'value', 'typeStock', 'relatedToAction']]
                bonuses_df = dados_ticker[dados_ticker['Stock Splits'] > 0].copy()
                if not bonuses_df.empty:
                    bonuses_df = bonuses_df.rename(columns={'Date': 'lastDatePrior', 'Stock Splits': 'factor'})
                    bonuses_df['label'] = 'Bonificação (Stock Split)'
                    bonuses_df['Ticker'] = ticker_original
                    bonuses_dict[ticker_original] = bonuses_df[['Ticker', 'lastDatePrior', 'factor', 'label']]
            else:
                erros.append(f"Sem dados de preços, dividendos ou bonificações para {ticker_original} no período.")
        except Exception as e:
            error_type = type(e).__name__
            erros.append(f"Erro ao processar dados de {ticker_original}: {error_type} - {e}")
    return precos_dict, dividends_dict, bonuses_dict, erros

# ============================================
# Interface do Streamlit
# ============================================
st.set_page_config(layout="wide")
st.title('Consulta Tickers para TSR Suzano')

# --- Carrega o DataFrame de empresas B3 ---
df_empresas = carregar_empresas()
if df_empresas.empty:
    st.error("Não foi possível carregar a lista de empresas B3. A aplicação não pode continuar.")
    st.stop()
b3_tickers_set = set()
if 'Tickers' in df_empresas.columns:
    for t_list in df_empresas['Tickers'].dropna().str.split(','):
        for ticker in t_list:
            if ticker.strip(): b3_tickers_set.add(ticker.strip().upper())


# --- Entradas do Usuário ---
col1, col2 = st.columns(2)
with col1:
    tickers_predefinidos = ['SUZB3', 'KLBN11', 'IP', 'CMPC.SN', 'UPM.HE']
    tickers_selecionados = st.multiselect("Selecione os tickers para buscar:", tickers_predefinidos, default=tickers_predefinidos, key="selected_tickers")
with col2:
    tipos_dados_selecionados = st.multiselect("Selecione os dados que deseja buscar:", ["Preços Históricos", "Dividendos", "Bonificações"], default=["Preços Históricos", "Dividendos", "Bonificações"], key="data_types")

col3, col4 = st.columns(2)
with col3:
    data_inicio_input = st.text_input("Data de início (dd/mm/aaaa):", key="date_start", value="01/01/2023")
with col4:
    data_fim_input = st.text_input("Data de fim (dd/mm/aaaa):", key="date_end", value="31/12/2024")

# --- Mapeamento de fontes (IP agora é YF) ---
ticker_sources = {
    'SUZB3': 'B3', 'KLBN11': 'B3',
    'CMPC.SN': 'YF', 'UPM.HE': 'YF',
    'IP': 'YF' }

# --- Inicialização do Session State ---
if 'dados_buscados' not in st.session_state:
    st.session_state.dados_buscados = False
    st.session_state.todos_dados_acoes = {}
    st.session_state.todos_dados_dividendos = {}
    st.session_state.todos_dados_bonificacoes = {}
    st.session_state.erros_gerais = []

# --- Botão e Lógica Principal ---
if st.button('Buscar Dados', key="search_button"):
    st.session_state.dados_buscados = False
    st.session_state.todos_dados_acoes = {}
    st.session_state.todos_dados_dividendos = {}
    st.session_state.todos_dados_bonificacoes = {}
    st.session_state.erros_gerais = []

    if not tickers_selecionados or not tipos_dados_selecionados or not data_inicio_input or not data_fim_input:
        st.warning("Por favor, selecione ao menos um ticker, um tipo de dado e preencha as datas.")
        st.stop()
    
    try:
        data_inicio_dt = datetime.strptime(data_inicio_input, "%d/%m/%Y")
        data_fim_dt = datetime.strptime(data_fim_input, "%d/%m/%Y")
        if data_inicio_dt > data_fim_dt:
            st.error("A data de início não pode ser posterior à data de fim.")
            st.stop()
    except ValueError:
        st.error("Formato de data inválido. Use dd/mm/aaaa.")
        st.stop()
    
    tickers_por_fonte = {'B3': [], 'YF': []} # Remove 'AV'
    for ticker in tickers_selecionados:
        fonte = ticker_sources.get(ticker)
        if fonte: tickers_por_fonte[fonte].append(ticker)
    
    all_dividends_temp = []
    all_bonuses_temp = []
    
    with st.spinner('Buscando dados... Por favor, aguarde.'):
        # 1. Busca no Yahoo Finance (agora inclui B3 e IP)
        tickers_para_yf = tickers_por_fonte['YF'] + tickers_por_fonte['B3']
        if tickers_para_yf:
            precos_yf, div_yf, bon_yf, erros_yf = buscar_dados_yfinance_completo(
                tickers_para_yf, data_inicio_input, data_fim_input, df_empresas
            )
            st.session_state.erros_gerais.extend(erros_yf)
            
            if "Preços Históricos" in tipos_dados_selecionados:
                st.session_state.todos_dados_acoes.update(precos_yf)
            
            # Adiciona eventos do YF para os tickers YF (incluindo IP agora)
            if "Dividendos" in tipos_dados_selecionados:
                for t in tickers_por_fonte['YF']:
                    if t in div_yf: all_dividends_temp.append(div_yf[t])
            if "Bonificações" in tipos_dados_selecionados:
                for t in tickers_por_fonte['YF']:
                    if t in bon_yf: all_bonuses_temp.append(bon_yf[t])
        
        # 2. Busca na B3 (apenas eventos para tickers B3)
        if tickers_por_fonte['B3']:
            for ticker in tickers_por_fonte['B3']:
                if "Dividendos" in tipos_dados_selecionados:
                    df_dividendos_b3 = buscar_dividendos_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                    if not df_dividendos_b3.empty: all_dividends_temp.append(df_dividendos_b3)
                if "Bonificações" in tipos_dados_selecionados:
                    df_bonificacoes_b3 = buscar_bonificacoes_b3(ticker, df_empresas, data_inicio_dt, data_fim_dt)
                    if not df_bonificacoes_b3.empty: all_bonuses_temp.append(df_bonificacoes_b3)
        
        if all_dividends_temp: st.session_state.todos_dados_dividendos = {f"div_{i}": df for i, df in enumerate(all_dividends_temp)}
        if all_bonuses_temp: st.session_state.todos_dados_bonificacoes = {f"bon_{i}": df for i, df in enumerate(all_bonuses_temp)}

    st.session_state.dados_buscados = True
    st.rerun()

# --- EXIBIÇÃO E DOWNLOAD ---
if st.session_state.get('dados_buscados', False):
    if st.session_state.erros_gerais:
        for erro in st.session_state.erros_gerais:
            st.warning(erro)
    if st.session_state.todos_dados_acoes:
        st.subheader("1. Preços Históricos (Yahoo Finance)")
        df_acoes_agrupado = pd.concat(st.session_state.todos_dados_acoes.values(), ignore_index=True)
        st.dataframe(df_acoes_agrupado)
    if st.session_state.todos_dados_dividendos:
        st.subheader("2. Dividendos (B3 e Yahoo Finance)")
        df_dividendos_agrupado = pd.concat(st.session_state.todos_dados_dividendos.values(), ignore_index=True)
        st.dataframe(df_dividendos_agrupado)
    if st.session_state.todos_dados_bonificacoes:
        st.subheader("3. Bonificações (B3 e Yahoo Finance)")
        df_bonificacoes_agrupado = pd.concat(st.session_state.todos_dados_bonificacoes.values(), ignore_index=True)
        st.dataframe(df_bonificacoes_agrupado)
    if not st.session_state.todos_dados_acoes and not st.session_state.todos_dados_dividendos and not st.session_state.todos_dados_bonificacoes:
        st.info("Nenhum dado encontrado para os critérios selecionados.")
    else:
        st.subheader("📥 Download dos Dados em Excel")
        formato_excel = st.radio("Escolha o formato do arquivo Excel:", ("Agrupar por tipo de dado (uma aba para Preços, outra para Dividendos, etc.)", "Separar por ticker e tipo (ex: Precos_PETR4, Div_VALE3, etc.)"), key="excel_format")
        nome_arquivo = f"dados_mercado_{data_inicio_input.replace('/','')}_{data_fim_input.replace('/','')}_{datetime.now().strftime('%H%M')}.xlsx"
        try:
            output = BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                if formato_excel.startswith("Agrupar"):
                    if st.session_state.todos_dados_acoes: pd.concat(st.session_state.todos_dados_acoes.values(), ignore_index=True).to_excel(writer, sheet_name="Precos_Historicos", index=False)
                    if st.session_state.todos_dados_dividendos: pd.concat(st.session_state.todos_dados_dividendos.values(), ignore_index=True).to_excel(writer, sheet_name="Dividendos", index=False)
                    if st.session_state.todos_dados_bonificacoes: pd.concat(st.session_state.todos_dados_bonificacoes.values(), ignore_index=True).to_excel(writer, sheet_name="Bonificacoes", index=False)
                else:
                    if st.session_state.todos_dados_acoes:
                        for ticker, df in st.session_state.todos_dados_acoes.items(): df.to_excel(writer, sheet_name=f"Precos_{ticker[:25]}", index=False)
                    if st.session_state.todos_dados_dividendos:
                        all_div_df = pd.concat(st.session_state.todos_dados_dividendos.values())
                        for ticker in all_div_df['Ticker'].unique():
                            df = all_div_df[all_div_df['Ticker'] == ticker]
                            df.to_excel(writer, sheet_name=f"Div_{ticker[:25]}", index=False)
                    if st.session_state.todos_dados_bonificacoes:
                        all_bon_df = pd.concat(st.session_state.todos_dados_bonificacoes.values())
                        for ticker in all_bon_df['Ticker'].unique():
                            df = all_bon_df[all_bon_df['Ticker'] == ticker]
                            df.to_excel(writer, sheet_name=f"Bonif_{ticker[:25]}", index=False)
            excel_data = output.getvalue()
            st.download_button(label="Baixar arquivo Excel", data=excel_data, file_name=nome_arquivo, mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        except Exception as e:
            st.error(f"Erro ao gerar o arquivo Excel: {e}")
st.markdown("""
---
**Fontes dos dados:**
- Preços, Dividendos e Eventos societários: [API B3](https://www.b3.com.br) e [Yahoo Finance](https://finance.yahoo.com)
""")
