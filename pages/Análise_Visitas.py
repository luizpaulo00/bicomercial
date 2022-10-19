import streamlit as st
import time
import pathlib
import numpy as np
from datetime import datetime
import pandas as pd
import seaborn as sns
from openpyxl import Workbook
from tqdm import tqdm
import matplotlib.pyplot as plt
import gspread
import mysql.connector 
import sqlalchemy   
from datetime import date
from scipy import stats
import mysql.connector
import pandas as pd
from st_aggrid import AgGrid, DataReturnMode, GridUpdateMode, GridOptionsBuilder, JsCode
import plotly.express as px
import psycopg2
import warnings
import pickle
from plotly import graph_objects as go
Data_Hoje = pd.to_datetime(date.today(),errors="coerce")
import streamlit.components.v1 as components
st.set_page_config(page_icon="https://7lm.com.br/wp-content/themes/7lm/build/img/icons/assinatura_7lm.png", layout="wide", page_title="GRUPO IMERGE | FERRAMENTA")

img = "assets/logo7lm.png"
st.sidebar.image(image=img, use_column_width=True,caption="Dashboard-Comercial")

c1, c2, c3 = st.columns((1,5,1))
c1.image(image=img, use_column_width=True, width="50px")
c2.markdown("# ANÁLISE | VISITAS | PROPOSTAS ")

def db_query(sql_query: str, db_conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """Execute SQL Query and get Dataframe with pandas"""
    with warnings.catch_warnings():
        # ignore warning for non-SQLAlchemy Connecton
        # see github.com/pandas-dev/pandas/issues/45660
        warnings.simplefilter('ignore', UserWarning)
        # create pandas DataFrame from database query
        df = pd.read_sql_query(sql_query, db_conn)
    return df


def Graf_barra_vertical(df,col_x,col_y):
    fig = px.bar(df, x=col_x, y=col_y, text_auto=True)
    fig.update_layout(template="plotly_white")
    return fig

def graf_leads(df,colunaX, colunaY, coluna_Cor):
    fig = px.bar(df, x=colunaX, y=colunaY, color=coluna_Cor, text=colunaY)
    fig.update_layout(barmode="relative")
    fig.update_layout(template="plotly_white", width=1500, height=500)
    return fig

def start_bd():
    db_connection = mysql.connector.connect(host="cvbidb.awservers.com.br",user="setelm_bi_ext",password="GrupoImerge7lm&Novk@2022",database="setelm_bi")
    df_leads = db_query('select * from leads;',db_connection)
    return df_leads

def start_bd1():
    db_connection = mysql.connector.connect(host="cvbidb.awservers.com.br",user="setelm_bi_ext",password="GrupoImerge7lm&Novk@2022",database="setelm_bi")
    df_leads_historicos_situacoes = db_query('select * from leads_historicos_situacoes;',db_connection)
    return df_leads_historicos_situacoes

def start_bd2():
    db_connection = mysql.connector.connect(host="cvbidb.awservers.com.br",user="setelm_bi_ext",password="GrupoImerge7lm&Novk@2022",database="setelm_bi")
    df_proposta = db_query('select * from precadastros;',db_connection)
    return df_proposta

def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

with st.sidebar.expander("BAIXAR BD"):
    DE = st.text_input("DATA_001","2022-01-01")
    PARA = st.text_input("DATA_002","2022-12-01")
    PDV_LOJA = st.selectbox(label="PDV_LOJA", options=["Equipe Própria | AGL", "Equipe Própria | FSA", "NOVKA"])
    FINALIZADOS = st.selectbox(label="FINALIZADOS?", options=["SIM", "NÃO"])
    BT_01 = st.button("ATUALIZAR")
if BT_01:
  pickle_out = open("leads.pickle","wb")
  pickle_out1 = open("funil.pickle","wb")
  pickle_out2 = open("proposta.pickle","wb")
  pickle.dump(start_bd(), pickle_out)
  pickle.dump(start_bd1(), pickle_out1)
  pickle.dump(start_bd2(), pickle_out2)
  pickle_out.close()  
  pickle_out1.close() 
  pickle_out2.close() 
  st.write("Pickle Criado!")

pickle_in = open("leads.pickle","rb")
pickle_in_001 = open("funil.pickle","rb")
pickle_in_002 = open("proposta.pickle","rb")
df_Leads_ = pickle.load(pickle_in)
df_Leads_Historico_ = pickle.load(pickle_in_001)
df_proposta_ = pickle.load(pickle_in_002)

def MOMENT_LEAD(df):
    MOMENT_LEAD = df.loc[:,["numero","situacao","nome_momento_lead","imobiliaria","gestor","corretor","empreendimento","data_ultima_interacao","data_cad","data_reativacao"]]
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "7LM Formosa","imobiliaria"] = "Equipe Própria | FSA"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "Formosa","imobiliaria"] = "Equipe Própria | FSA"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "Águas Lindas","imobiliaria"] = "Equipe Própria | AGL"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "Aguas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "Aguas Lindas 2","imobiliaria"] = "Equipe Própria | AGL"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "Águas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "Águas Lindas 2","imobiliaria"] = "Equipe Própria | AGL"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "NovKa Formosa","imobiliaria"] = "NOVKAL"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "NOVKA DF","imobiliaria"] = "NOVKA"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "Novka Pilotis","imobiliaria"] = "NOVKA"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "FSA - 7lm/Novka","imobiliaria"] = "NOVKA"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "NOVKAL","imobiliaria"] = "NOVKA"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "URBANA IMOVEIS","imobiliaria"] = "NOVKA"
    MOMENT_LEAD.loc[MOMENT_LEAD["imobiliaria"] == "BR HOUSE INTELIGENCIA IMOBILIARIA","imobiliaria"] = "NOVKA"
    MOMENT_LEAD.fillna(0, inplace=True)
    MOMENT_LEAD.rename(columns={"numero":"idlead"}, inplace=True)
    return MOMENT_LEAD

def VISITAS_TOTAL(df, df_ld ,dt01, dt02, LOJA):
    DATA_HOJE = pd.to_datetime(date.today(), errors="coerce")
    MOMENT_LEADS = MOMENT_LEAD(df_ld)
    ATENDIMENTO = df.copy()
    ATENDIMENTO = ATENDIMENTO.loc[(ATENDIMENTO["data_cad"] >= dt01) & (ATENDIMENTO["data_cad"] <= dt02)]
    ATENDIMENTO = ATENDIMENTO.loc[ATENDIMENTO["para_nome"].isin(["Visita"])]
    ATENDIMENTO = pd.merge(ATENDIMENTO, MOMENT_LEADS, on=["idlead"], how="left")
    ATENDIMENTO = ATENDIMENTO.loc[ATENDIMENTO["imobiliaria"] == LOJA]
    lts_dt = []
    ATENDIMENTO["DT_FINAL_CAD"] = 0
    for a, b in zip(ATENDIMENTO["data_reativacao"], ATENDIMENTO["data_cad_y"]):
        if a != 0:
            lts_dt.append(a)
        else:
            lts_dt.append(b)
    ATENDIMENTO["DT_FINAL_CAD"]  = lts_dt  
    ATENDIMENTO["DT_VIS_x_DT_CAD"] = (ATENDIMENTO["data_cad_x"] - ATENDIMENTO["DT_FINAL_CAD"]).dt.days
    ATENDIMENTO["TM_ATUALIZ"] = (DATA_HOJE - ATENDIMENTO["data_ultima_interacao"]).dt.days
    ATENDIMENTO = ATENDIMENTO.loc[:,["idlead","para_nome","gestor","corretor","data_cad_x","imobiliaria","empreendimento","nome_momento_lead","situacao",
                                     "DT_VIS_x_DT_CAD","TM_ATUALIZ"]]
    ATENDIMENTO.rename(columns={"data_cad_x":"dt_visita","para_nome":"Status"}, inplace=True)
    ATENDIMENTO["EMPREEND"] = 0
    for i in ATENDIMENTO["empreendimento"]:
        try:
            N = i.find(";")
            a = i[:N]
            ATENDIMENTO.loc[ATENDIMENTO["empreendimento"] == i,"EMPREEND"] = a
        except:
            ATENDIMENTO.loc[ATENDIMENTO["empreendimento"] == i,"EMPREEND"] = i
    ATENDIMENTO["MM-AA"] = ATENDIMENTO["dt_visita"].dt.strftime("01-%m-%Y")
    ATENDIMENTO = ATENDIMENTO.drop(columns=["empreendimento"])
    return ATENDIMENTO

def DADOS_VISITA_GRAD(df1, df2, dt01, dt02, pdv_loja, CRITERIO_S_N):
    d = VISITAS_TOTAL(df1, df2, dt01, dt02, pdv_loja)
    if CRITERIO_S_N == "SIM":
        d = d.loc[d["situacao"] == "Finalizado"]
    else:
        d = d.loc[d["situacao"] != "Finalizado"]
    d["count"] = 1
    d = pd.DataFrame(d.groupby(["nome_momento_lead"])["count"].sum()).reset_index().sort_values(by=["count"], ascending=False)
    d.loc[d["nome_momento_lead"] == 0,"nome_momento_lead"] = "Sem Status" 
    d["acum"] = d["count"].cumsum()
    d["acum_un"] = d["count"]/d["count"].sum()
    d["acum_%"] = d["acum"]/d["count"].sum()
    return d

st.title("# Resumo | Visitas")
A = DADOS_VISITA_GRAD(df_Leads_Historico_, df_Leads_, DE, PARA, PDV_LOJA, FINALIZADOS)
st.plotly_chart(Graf_barra_vertical(A,"nome_momento_lead","count"), use_container_width=True)













st.title("# Análise das Visitas | Geral")
base = VISITAS_TOTAL(df_Leads_Historico_, df_Leads_,"2022-01-01", "2022-12-01", PDV_LOJA)
gb = GridOptionsBuilder.from_dataframe(base)
gb.configure_default_column(groupable=True, enableValue=True, enableRowGroup=True,aggFunc="sum",editable=False)
gb.update_mode=GridUpdateMode.MANUAL
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
gb.configure_side_bar()
gridoptions = gb.build()
response = AgGrid(
    base,
    height=800,
    gridOptions=gridoptions,
    enable_enterprise_modules=True,
    header_checkbox_selection_filtered_only=True,
    use_checkbox=True, theme="blue")



csv = convert_df(base)
st.download_button(
label="Download Arquivo em CSV",
data=csv,
file_name='Visitas_7lm.csv',
mime='text/csv',) 








