import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="COVID-19 - Brasil.io",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("<h1 style='text-align:center;color:#d7263d;'>Painel COVID-19 Brasil.io</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align:center;color:#555;'>Análise interativa dos dados da pandemia de COVID-19 no Brasil</p>", unsafe_allow_html=True)

uploaded_file = st.file_uploader("Faça upload do arquivo .csv.gz ou use o padrão", type=["csv","gz"])

@st.cache_data
def load_data(file):
    if file is not None:
        df = pd.read_csv(file, compression="gzip", parse_dates=["date"])
    else:
        try:
            df = pd.read_csv("DADOS/caso.csv.gz", compression="gzip", parse_dates=["date"])
        except Exception:
            st.error("Arquivo padrão não encontrado. Faça upload do arquivo.")
            return pd.DataFrame()
    return df

df = load_data(uploaded_file)

if df.empty:
    st.warning("Nenhum dado carregado.")
    st.stop()

# --------------------------
# Colunas de casos e óbitos
# --------------------------
col_cases = "confirmed"
col_deaths = "deaths"


with st.sidebar:
    st.title("Configurações")
    estados = sorted(df[df["place_type"] == "state"]["state"].unique())
    estado = st.selectbox("Escolha o estado", ["BRASIL"] + estados)
    indicador = st.radio("Indicador", ["Novos casos", "Novos óbitos"])

# --------------------------
# Tabs
# --------------------------
tab1, tab2 = st.tabs(["📈 Gráfico temporal", "🗺️ Insights regionais"])

# --------------------------
# Gráfico temporal
# --------------------------
if estado == "BRASIL":
    dfx = df[df["place_type"] == "state"].groupby("date")[[col_cases,col_deaths]].sum().reset_index()
else:
    dfx = df[(df["place_type"] == "state") & (df["state"]==estado)].copy()

ycol = col_cases if indicador=="Novos casos" else col_deaths
titulo = f"{indicador} - {estado}"
dfx = dfx.sort_values("date")
dfx["mm7"] = dfx[ycol].rolling(7, min_periods=1).mean()

with tab1:
    st.markdown(f"### {titulo}")
    fig = px.line(dfx, x="date", y="mm7", labels={"mm7":"Média móvel (7 dias)","date":"Data"}, template="plotly_white", color_discrete_sequence=["#d7263d"])
    st.plotly_chart(fig, use_container_width=True)

    # --------------------------
    # Ranking de UF (última data disponível)
    # --------------------------
    st.markdown("#### Ranking de UF (última data disponível)")
    df_ufs = df[df["place_type"] == "state"].copy()
    ultima_data = df_ufs["date"].max()
    df_ufs = df_ufs[df_ufs["date"] == ultima_data].copy()

    uf_col = col_cases if indicador == "Novos casos" else col_deaths
    df_ufs = df_ufs.sort_values(uf_col, ascending=False)
    top_n = st.slider("Número de UFs no ranking", 5, len(df_ufs), 10)

    fig_rank_uf = px.bar(
        df_ufs.head(top_n),
        x=uf_col,
        y="state",
        color=uf_col,
        orientation="h",
        text=uf_col,
        hover_data=["state", uf_col],
        labels={uf_col:indicador,"state":"UF"},
        template="plotly_white"
    )
    fig_rank_uf.update_layout(yaxis={'categoryorder':'total ascending'}, coloraxis_showscale=False)
    st.plotly_chart(fig_rank_uf, use_container_width=True)

# --------------------------
# Insights regionais
# --------------------------
with tab2:
    st.markdown("### Distribuição por estado (última data disponível)")
    df_last = df[df["place_type"]=="state"].copy()
    df_last = df_last[df_last["date"]==df_last["date"].max()]
    df_last["state"] = df_last["state"].str.upper()
    col = col_cases if indicador=="Novos casos" else col_deaths

    fig2 = px.choropleth(
        df_last,
        geojson="https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson",
        locations="state",
        featureidkey="properties.sigla",
        color=col,
        color_continuous_scale="Reds",
        scope="south america",
        title=f"{indicador} em {df_last['date'].max().date()}"
    )
    fig2.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig2, use_container_width=True)