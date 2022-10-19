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
c2.markdown("# SDR | PRÉ-VENDAS | BDR ")

tab1, tab2, tab3 = st.tabs(["Quantidade de Leads", "Analítico", "Em Desenvolvimento"])

st.title("#Quantidade de leads")

def db_query(sql_query: str, db_conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """Execute SQL Query and get Dataframe with pandas"""
    with warnings.catch_warnings():
        # ignore warning for non-SQLAlchemy Connecton
        # see github.com/pandas-dev/pandas/issues/45660
        warnings.simplefilter('ignore', UserWarning)
        # create pandas DataFrame from database query
        df = pd.read_sql_query(sql_query, db_conn)
    return df

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

with st.sidebar.expander("BAIXAR BD"):
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
    MOMENT_LEAD = df.loc[:,["numero","nome_momento_lead","imobiliaria","gestor","corretor","empreendimento","data_ultima_interacao","situacao","data_cad","data_reativacao"]]
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
    
    ATENDIMENTO = ATENDIMENTO.drop(columns=["empreendimento"])
    return ATENDIMENTO

#=================================================================================================================================================================

with tab1:
    def leads(df_inicio):
        LD = df_Leads_.copy()
        LD = LD.loc[:,["numero","situacao","motivo_cancelamento","nome","empreendimento","gestor","corretor","imobiliaria","origem","midia_original","data_cad","data_primeira_interacao_gestor",
                      "data_primeira_interacao_corretor","data_ultima_interacao","data_reativacao","data_cancelamento"]]
        LD.fillna(0, inplace=True)
        LD = LD.loc[LD["data_cad"] >= df_inicio]   
        LD["data_cad"] = pd.to_datetime(LD["data_cad"], errors="coerce") 
        LD["data_reativacao"] = pd.to_datetime(LD["data_reativacao"], errors="coerce") 
        LD = LD.sort_values(by=["data_cad"], ascending=True)
        LD["MM/AA"] = LD["data_cad"].dt.strftime('01-%m-%Y')
        LD.loc[LD["imobiliaria"] == "Aguas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
        LD.loc[LD["imobiliaria"] == "Águas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
        LD.loc[LD["imobiliaria"] == "Águas Lindas 2","imobiliaria"] = "Equipe Própria | AGL"
        LD.loc[LD["imobiliaria"] == "Formosa","imobiliaria"] = "7LM Formosa"

        LD.loc[LD["imobiliaria"] == "FSA - 7lm/Novka","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "NOVKA DF","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "BR HOUSE INTELIGENCIA IMOBILIARIA","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "URBANA IMOVEIS","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "Novka Pilotis","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "Imobiliárias | DF","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "Imobiliária Padrão","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "7LM Formosa","imobiliaria"] = "Equipe Própria | FSA"
        LD.loc[LD["imobiliaria"] == "NovKa Formosa","imobiliaria"] = "NOVKA"
        LD.fillna(0, inplace=True)
        LD["ANO"] = LD["data_cad"].dt.strftime('%Y')
        LD = pd.DataFrame(LD.groupby(["ANO","MM/AA"])["numero"].count()).reset_index()
        return LD

    def leads_002(dt_inicio):
        LD = df_Leads_.copy()
        LD = LD.loc[:,["numero","situacao","motivo_cancelamento","nome","empreendimento","gestor","corretor","imobiliaria","origem","midia_original","data_cad","data_primeira_interacao_gestor",
                      "data_primeira_interacao_corretor","data_ultima_interacao","data_reativacao","data_cancelamento"]]
        LD.fillna(0, inplace=True)
        LD = LD.loc[LD["data_cad"] >= dt_inicio]   
        LD["data_cad"] = pd.to_datetime(LD["data_cad"], errors="coerce") 
        LD["data_reativacao"] = pd.to_datetime(LD["data_reativacao"], errors="coerce") 
        LD = LD.sort_values(by=["data_cad"], ascending=True)
        LD["MM/AA"] = LD["data_cad"].dt.strftime('%m-%Y')
        LD.loc[LD["imobiliaria"] == "Aguas Lindas 1","imobiliaria"] = "AGL"
        LD.loc[LD["imobiliaria"] == "Águas Lindas 1","imobiliaria"] = "AGL"
        LD.loc[LD["imobiliaria"] == "Águas Lindas 2","imobiliaria"] = "AGL"
        LD.loc[LD["imobiliaria"] == "Formosa","imobiliaria"] = "FSA"
        LD.loc[LD["imobiliaria"] == "Equipe Própria | AGL","imobiliaria"] = "AGL"
        LD.loc[LD["imobiliaria"] == "Equipe Própria | FSA","imobiliaria"] = "FSA"
        LD.loc[LD["imobiliaria"] == "FSA - 7lm/Novka","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "NOVKA DF","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "BR HOUSE INTELIGENCIA IMOBILIARIA","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "URBANA IMOVEIS","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "Novka Pilotis","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "Imobiliárias | DF","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "Imobiliária Padrão","imobiliaria"] = "NOVKA"
        LD.loc[LD["imobiliaria"] == "7LM Formosa","imobiliaria"] = "FSA"
        LD.loc[LD["imobiliaria"] == "NovKa Formosa","imobiliaria"] = "NOVKA"
        LD.fillna(0, inplace=True)
        LD["ANO"] = LD["data_cad"].dt.strftime('%Y')
        LD = pd.DataFrame(LD.groupby(["ANO","imobiliaria","MM/AA"])["numero"].count()).reset_index().sort_values(by=["MM/AA"], ascending=True)
        return LD

    def propostas(df, dt_01, dt_02):
        PP = df.copy()
        PP = PP.loc[:,["data_cad","empreendimento","situacao"]]
        PP["MM/AA"] = PP["data_cad"].dt.strftime('01-%m-%Y')
        PP["count"]= 1
        PP = PP.loc[(PP["data_cad"]>=dt_01) & (PP["data_cad"]<=dt_02)]
        return len(PP)

    def funil_etapas_lead(df, dt1, dt2):
        LEADS = df.copy()
        LEADS = LEADS.loc[(LEADS["data_cad"] >= dt1) & (LEADS["data_cad"] <= dt2)]
        return len(LEADS)


    def funil_etapas(df, tipo_atend, dt1, dt2):
        ATENDIMENTO = df.copy()
        ATENDIMENTO = ATENDIMENTO.loc[(ATENDIMENTO["data_cad"] >= dt1) & (ATENDIMENTO["data_cad"] <= dt2)]
        ATENDIMENTO = ATENDIMENTO.loc[ATENDIMENTO["para_nome"].isin([tipo_atend])]
        return len(ATENDIMENTO)


    def funil(dt_01, dt_02):
        FUNIL_LEADS = funil_etapas_lead(df_Leads_,dt_01, dt_02) 
        FUNIL_AGENDAMENTO = funil_etapas(df_Leads_Historico_,"Agendamento",dt_01, dt_02) 
        FUNIL_VISITA = funil_etapas(df_Leads_Historico_,"Visita",dt_01, dt_02) 
        FUNIL_PROPOSTA = propostas(df_proposta_, dt_01, dt_02)
        list_resultado = [FUNIL_LEADS, FUNIL_AGENDAMENTO, FUNIL_VISITA,FUNIL_PROPOSTA,0,0,0,0,0]
        return list_resultado


    lts_meses = ["Status","01-2022","02-2022", "03-2022", "04-2022", "05-2022", "06-2022", "07-2022","08-2022", "09-2022"]
    lts_status = ["LEAD","AGENDAMENTO","VISITA", "PROPOSTA","VENDA","#######","LDxAG","AGxVIS","VISxPP"]
    funil_ = pd.DataFrame(columns=lts_meses)

    funil_["Status"] = lts_status
    funil_["01-2022"] = funil("2022-01-01", "2022-02-01")
    funil_["02-2022"] = funil("2022-02-01", "2022-03-01")
    funil_["03-2022"] = funil("2022-03-01", "2022-04-01")
    funil_["04-2022"] = funil("2022-04-01", "2022-05-01")
    funil_["05-2022"] = funil("2022-05-01", "2022-06-01")
    funil_["06-2022"] = funil("2022-06-01", "2022-07-01")
    funil_["07-2022"] = funil("2022-07-01", "2022-08-01")
    funil_["08-2022"] = funil("2022-08-01", "2022-09-01")
    funil_["09-2022"] = funil("2022-09-01", "2022-10-01")

    funil_["YTD"] = funil_.iloc[:,0:8].sum(axis=1)
    funil_.iloc[5,:] = ""

    n=0
    for i in range(1,11):
        funil_.iloc[6,i] = np.round(int(funil_.iloc[1,i]) / int(funil_.iloc[0,i]),2)
        funil_.iloc[7,i] = np.round(int(funil_.iloc[2,i]) / int(funil_.iloc[1,i]),2)
        funil_.iloc[8,i] = np.round(int(funil_.iloc[3,i]) / int(funil_.iloc[2,i]),2)



    st.plotly_chart(graf_leads(leads("2021-01-01"),"MM/AA", "numero", "ANO"))
    st.title("#Leads por cidade")
    st.plotly_chart(graf_leads(leads_002("2022-01-01"), "MM/AA","numero","imobiliaria"))
    st.title("#CONVERSÕES | PERFORMANCE")
    AgGrid(funil_, theme="blue", height=300)

    st.title("#Resumo / AGL")

    #LISTAS DAS VENDAS X OBJETIVO
    lts_objetivo_agl = [35,30,38,32,30,25,40, 25,28]
    lts_real_agl = [35,37,42,31,37,48,37, 32, 14]

    lts_objetivo_fsa = [18,18,20,20,25,25,22, 20, 33]
    lts_real_fsa = [20,31,33,32,30,24,23,14, 0]

    lts_objetivo_novka = [2,2,2,2,1,1,1,2]
    lts_real_novka = [1,3,2,3,0,5,2,0,0]

    lts_objetivo_sdr = [20,20,24,25,25,25,33,18, 28]
    lts_real_sdr = [21,21,24,28,39,42,36, 34, 12]
    #============================================================

    AGL25 = 'AGL 25 - Vila das Águas'
    AGL23 = 'AGL 23 - Vila do Sol'
    AGL27 =  'AGL 27 - Vila Azaleia - 7LM'
    AGL28 = 'AGL28 - Vila do Cerrado'
    FSA005 = 'FSA 05 - Vila das Orquídeas - 7LM'
    FSA006 = 'FSA 06 - Vila das Tulipas - 7LM'
    DF001 = 'DF 01 - Haus By Novka'
    FSA003 = 'FSA 03 -  Aurium Home'
    AGL_ = [AGL23, AGL25, AGL27, AGL28]
    FSA_ = [FSA005, FSA006]
    NOVKA_ = [DF001, FSA003]

    LOJA_AGL = "Equipe Própria | AGL"
    LOJA_FSA = "Equipe Própria | FSA"
    LOJA_NOVKA = "NOVKA"
    #============================================================
    def funil_etapas_lead(df, dt1, dt2, loja):
        LEADS = df.copy()
        LEADS = LEADS.loc[(LEADS["data_cad"] >= dt1) & (LEADS["data_cad"] <= dt2)]
        LEADS.loc[LEADS["imobiliaria"] == "Aguas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
        LEADS.loc[LEADS["imobiliaria"] == "Águas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
        LEADS.loc[LEADS["imobiliaria"] == "Águas Lindas 2","imobiliaria"] = "Equipe Própria | AGL"
        LEADS.loc[LEADS["imobiliaria"] == "Formosa","imobiliaria"] = "7LM Formosa"   
        LEADS.loc[LEADS["imobiliaria"] == "FSA - 7lm/Novka","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "NOVKA DF","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "BR HOUSE INTELIGENCIA IMOBILIARIA","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "URBANA IMOVEIS","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "Novka Pilotis","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "Imobiliárias | DF","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "Imobiliária Padrão","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "7LM Formosa","imobiliaria"] = "Equipe Própria | FSA"
        LEADS.loc[LEADS["imobiliaria"] == "NovKa Formosa","imobiliaria"] = "NOVKA"
        LEADS = LEADS.loc[LEADS["imobiliaria"].isin([loja])]
        return len(LEADS)

    def propostas_001(df, dt_01, dt_02, grupo_empreendimento):
        PP = df.copy()
        PP = PP.loc[:,["data_cad","empreendimento","situacao"]]
        PP["MM/AA"] = PP["data_cad"].dt.strftime('01-%m-%Y')
        PP["count"]= 1
        PP = PP.loc[PP["empreendimento"].isin(grupo_empreendimento)]
        PP = PP.loc[(PP["data_cad"]>=dt_01) & (PP["data_cad"]<=dt_02)]
        PP = pd.DataFrame(PP.groupby(["MM/AA"])["count"].sum()).reset_index()
        return list(PP["count"])[0]

    def funil_etapas_por_cidade(df, df_num_leads, tipo_atend, loja_cidade ,dt1, dt2):
        #df_leads_historicos_situacoes
        ATENDIMENTO = df.copy()
        LEADS = df_num_leads.copy()
        ATENDIMENTO = ATENDIMENTO.loc[(ATENDIMENTO["data_cad"] >= dt1) & (ATENDIMENTO["data_cad"] <= dt2)]
        LEADS.loc[LEADS["imobiliaria"] == "Aguas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
        LEADS.loc[LEADS["imobiliaria"] == "Águas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
        LEADS.loc[LEADS["imobiliaria"] == "Águas Lindas 2","imobiliaria"] = "Equipe Própria | AGL"
        LEADS.loc[LEADS["imobiliaria"] == "Formosa","imobiliaria"] = "7LM Formosa"   
        LEADS.loc[LEADS["imobiliaria"] == "FSA - 7lm/Novka","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "NOVKA DF","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "BR HOUSE INTELIGENCIA IMOBILIARIA","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "URBANA IMOVEIS","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "Novka Pilotis","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "Imobiliárias | DF","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "Imobiliária Padrão","imobiliaria"] = "NOVKA"
        LEADS.loc[LEADS["imobiliaria"] == "7LM Formosa","imobiliaria"] = "Equipe Própria | FSA"
        LEADS.loc[LEADS["imobiliaria"] == "NovKa Formosa","imobiliaria"] = "NOVKA"
        LEADS = LEADS.loc[:,["numero","imobiliaria"]]
        LEADS.rename(columns={"numero":"idlead"}, inplace=True)
        ATENDIMENTO = pd.merge(ATENDIMENTO, LEADS, on=["idlead"], how="left")
        ATENDIMENTO = ATENDIMENTO.loc[ATENDIMENTO["para_nome"].isin([tipo_atend])]
        ATENDIMENTO = ATENDIMENTO.loc[ATENDIMENTO["imobiliaria"].isin([loja_cidade])]
        return len(ATENDIMENTO)

    def performance_cidades(LOJA, GRUP_EMP, lts_vendas):
        df_leads = df_Leads_.copy()
        df_leads_historicos_situacoes = df_Leads_Historico_.copy()
        df_proposta = df_proposta_.copy()

        data_ = ["01-2022", "02-2022", "03-2022", "04-2022", "05-2022", "06-2022", "07-2022", "08-2022", "09-2022"]
        df_cidades_hit_rate = pd.DataFrame()
        df_cidades_hit_rate["data"] = data_
        df_cidades_hit_rate["leads"] = funil_etapas_lead(df_leads, "2022-01-01", "2022-02-01",LOJA)
        df_cidades_hit_rate.iloc[1,1] = funil_etapas_lead(df_leads, "2022-02-01", "2022-03-01",LOJA)
        df_cidades_hit_rate.iloc[2,1] = funil_etapas_lead(df_leads, "2022-03-01", "2022-04-01",LOJA)
        df_cidades_hit_rate.iloc[3,1] = funil_etapas_lead(df_leads, "2022-04-01", "2022-05-01",LOJA)
        df_cidades_hit_rate.iloc[4,1] = funil_etapas_lead(df_leads, "2022-05-01", "2022-06-01",LOJA)
        df_cidades_hit_rate.iloc[5,1] = funil_etapas_lead(df_leads, "2022-06-01", "2022-07-01",LOJA)
        df_cidades_hit_rate.iloc[6,1] = funil_etapas_lead(df_leads, "2022-07-01", "2022-08-01",LOJA)
        df_cidades_hit_rate.iloc[7,1] = funil_etapas_lead(df_leads, "2022-08-01", "2022-09-01",LOJA)
        df_cidades_hit_rate.iloc[8,1] = funil_etapas_lead(df_leads, "2022-09-01", "2022-10-01",LOJA)

        df_cidades_hit_rate["Agend"] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-01-01","2022-02-01")
        df_cidades_hit_rate.iloc[1,2] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-02-01","2022-03-01")
        df_cidades_hit_rate.iloc[2,2] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-03-01","2022-04-01")
        df_cidades_hit_rate.iloc[3,2] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-04-01","2022-05-01")
        df_cidades_hit_rate.iloc[4,2] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-05-01","2022-06-01")
        df_cidades_hit_rate.iloc[5,2] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-06-01","2022-07-01")
        df_cidades_hit_rate.iloc[6,2] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-07-01","2022-08-01")
        df_cidades_hit_rate.iloc[7,2] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-08-01","2022-09-01")
        df_cidades_hit_rate.iloc[8,2] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Agendamento", LOJA,"2022-09-01","2022-10-01")


        df_cidades_hit_rate["Visita"] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-01-01","2022-02-01")
        df_cidades_hit_rate.iloc[1,3] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-02-01","2022-03-01")
        df_cidades_hit_rate.iloc[2,3] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-03-01","2022-04-01")
        df_cidades_hit_rate.iloc[3,3] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-04-01","2022-05-01")
        df_cidades_hit_rate.iloc[4,3] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-05-01","2022-06-01")
        df_cidades_hit_rate.iloc[5,3] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-06-01","2022-07-01")
        df_cidades_hit_rate.iloc[6,3] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-07-01","2022-08-01")
        df_cidades_hit_rate.iloc[7,3] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-08-01","2022-09-01")
        df_cidades_hit_rate.iloc[8,3] = funil_etapas_por_cidade(df_leads_historicos_situacoes, df_leads, "Visita", LOJA,"2022-09-01","2022-10-01")


        df_cidades_hit_rate["Propost"] = propostas_001(df_proposta, "2022-01-01", "2022-02-01", GRUP_EMP)
        df_cidades_hit_rate.iloc[1,4] = propostas_001(df_proposta, "2022-02-01", "2022-03-01", GRUP_EMP)
        df_cidades_hit_rate.iloc[2,4] = propostas_001(df_proposta, "2022-03-01", "2022-04-01", GRUP_EMP)
        df_cidades_hit_rate.iloc[3,4] = propostas_001(df_proposta, "2022-04-01", "2022-05-01", GRUP_EMP)
        df_cidades_hit_rate.iloc[4,4] = propostas_001(df_proposta, "2022-05-01", "2022-06-01", GRUP_EMP)
        df_cidades_hit_rate.iloc[5,4] = propostas_001(df_proposta, "2022-06-01", "2022-07-01", GRUP_EMP)
        df_cidades_hit_rate.iloc[6,4] = propostas_001(df_proposta, "2022-07-01", "2022-08-01", GRUP_EMP)
        df_cidades_hit_rate.iloc[7,4] = propostas_001(df_proposta, "2022-08-01", "2022-09-01", GRUP_EMP)
        df_cidades_hit_rate.iloc[8,4] = propostas_001(df_proposta, "2022-09-01", "2022-10-01", GRUP_EMP)

        df_cidades_hit_rate["Venda"] = lts_vendas
        df_cidades_hit_rate["LD_x_AG"] = np.round(df_cidades_hit_rate["Agend"] / df_cidades_hit_rate["leads"],2)
        df_cidades_hit_rate["AG_x_VIS"] = np.round(df_cidades_hit_rate["Visita"] / df_cidades_hit_rate["Agend"],2)
        df_cidades_hit_rate["VIS_x_PP"] = np.round(df_cidades_hit_rate["Propost"] / df_cidades_hit_rate["Visita"],2)
        df_cidades_hit_rate["PP_x_VD"] = np.round(df_cidades_hit_rate["Venda"] / df_cidades_hit_rate["Propost"],2)

        return df_cidades_hit_rate



    import plotly.graph_objects as go
    data_ = ["01-2022", "02-2022", "03-2022", "04-2022", "05-2022", "06-2022", "07-2022", "08-2022", "09-2022"]
    y = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["leads"]
    y1 = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["Agend"]
    y2 = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["Visita"]
    y3 = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["Propost"]
    y4 = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["Venda"]

    fig = go.Figure(data=[
        go.Bar(name='Leads', x=data_, y=y, text=y),
        go.Bar(name='Agend', x=data_, y=y1, text=y1),
        go.Bar(name='Visita', x=data_, y=y2, text=y2),
        go.Bar(name='Propost', x=data_, y=y3, text=y3),
        go.Bar(name='Venda', x=data_, y=y4, text=y4)
    ])
    # Change the bar mode
    fig.update_layout(barmode='group')
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_layout(template="plotly_white", width=1500, height=500)
    st.plotly_chart(fig)



    #=========================================================================================

    y = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["LD_x_AG"]
    y1 = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["AG_x_VIS"]
    y2 = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["VIS_x_PP"]
    y3 = performance_cidades(LOJA_AGL, AGL_, lts_real_agl)["PP_x_VD"]
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=data_, y=y,
                        mode='lines+markers',
                        name='LD_x_AG'))
    fig1.add_trace(go.Scatter(x=data_, y=y1,
                        mode='lines+markers',
                        name='AG_x_VIS'))
    fig1.add_trace(go.Scatter(x=data_, y=y2,
                        mode='lines+markers', name='VIS_x_PP'))

    fig1.add_trace(go.Scatter(x=data_, y=y3,
                        mode='lines+markers', name='PP_x_VD'))

    fig1.update_layout(template="plotly_white", width=1500, height=500)
    st.plotly_chart(fig1)

    #=========================================================================================

    st.title("#Resumo / FSA")
    yy = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["leads"]
    yy1 = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["Agend"]
    yy2 = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["Visita"]
    yy3 = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["Propost"]
    yy4 = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["Venda"]

    fig3 = go.Figure(data=[
        go.Bar(name='Leads', x=data_, y=yy, text=yy),
        go.Bar(name='Agend', x=data_, y=yy1, text=yy1),
        go.Bar(name='Visita', x=data_, y=yy2, text=yy2),
        go.Bar(name='Propost', x=data_, y=yy3, text=yy3),
        go.Bar(name='Venda', x=data_, y=yy4, text=yy4)
    ])
    # Change the bar mode
    fig3.update_layout(barmode='group')
    fig3.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig3.update_layout(template="plotly_white", width=1500, height=500)
    st.plotly_chart(fig3)



    #=========================================================================================
    Y = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["LD_x_AG"]
    Y1 = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["AG_x_VIS"]
    Y2 = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["VIS_x_PP"]
    Y3 = performance_cidades(LOJA_FSA, FSA_, lts_real_fsa)["PP_x_VD"]
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=data_, y=Y,
                        mode='lines+markers',
                        name='LD_x_AG'))
    fig2.add_trace(go.Scatter(x=data_, y=Y1,
                        mode='lines+markers',
                        name='AG_x_VIS'))
    fig2.add_trace(go.Scatter(x=data_, y=Y2,
                        mode='lines+markers', name='VIS_x_PP'))

    fig2.add_trace(go.Scatter(x=data_, y=Y3,
                        mode='lines+markers', name='PP_x_VD'))

    fig2.update_layout(template="plotly_white", width=1500, height=500)
    st.plotly_chart(fig2)

   
    



        
        
        
        
        
        
        
        