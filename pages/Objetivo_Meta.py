import streamlit as st
import time
import pandas as pd
import numpy as np
from datetime import datetime
import pandas as pd
import seaborn as sns
import plotly.express as px
from openpyxl import Workbook
from st_aggrid import AgGrid, DataReturnMode, GridUpdateMode, GridOptionsBuilder, JsCode
from tqdm import tqdm
import matplotlib.pyplot as plt
import mysql.connector 
import sqlalchemy   
from datetime import date
from scipy import stats
import mysql.connector
import pandas as pd
import plotly.express as px
from plotly import graph_objects as go
import psycopg2
import warnings
import pickle
from deta import Deta
import json
Data_Hoje = pd.to_datetime(date.today(),errors="coerce")
import streamlit.components.v1 as components
st.set_page_config(page_icon="https://7lm.com.br/wp-content/themes/7lm/build/img/icons/assinatura_7lm.png", layout="wide", page_title="GRUPO IMERGE | FERRAMENTA")

img = "assets/logo7lm.png"
st.sidebar.image(image=img, use_column_width=True,caption="Dashboard-Comercial")

img = "assets/logo7lm.png"
img1 = "assets/login.png"
img2 = "assets/Resultado.png"
img3 = "assets/novos_negocios.png"
img4 = "assets/Imagem_001.png"

st.title("# DASHBOARD GERAL | OBJETIVO | META | OKR'S")


# BANCO DE DADOS ACESSO ================================================================================

ID_BD = "e0rabm84"
Key_name = "62167l"
Key = "62167l"
token = "e0rabm84_aacLarMdhxdia3DCcB8V3dPvMkEKrVyH"

# Banco de Dados Principal ======================================================
deta = Deta(token)
db = deta.Base(Key)

# Banco de Dados Fila de Aprovação ======================================================
token_ap = "e0z9t1y3_ADEg76UpAD3TMNasdzWqaENw6pqGU4hK"
Key_ap = "6crhes" 
deta_ap = Deta(token_ap)
db_ap = deta_ap.Base(Key_ap)


def salvar_bd(dic_emp, bd):
    n=0
    pbar = tqdm(total = len(dic_emp), position=0, leave = True)
    for i in range(0,len(dic_emp)):
        pbar.update()
        bd.put(dic_emp[n])
        n+=1
    return print("Script Finalizado")


def grid_dataframe_top(df, tamanho, tema, largura):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(groupable=True, enableValue=True, enableRowGroup=True,aggFunc="sum",editable=True)
    gb.update_mode=GridUpdateMode.MANUAL
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_side_bar()
    gridoptions = gb.build()
    response = AgGrid(
        df,
        height=tamanho,
        width=largura,
        gridOptions=gridoptions,
        enable_enterprise_modules=True,
        header_checkbox_selection_filtered_only=True,
        use_checkbox=True, theme=tema)
    return response

def conversor_moeda_brasil(my_value):
    a = '{:,.2f}'.format(float(my_value))
    b = a.replace(',','v')
    c = b.replace('.',',')
    return c.replace('v','.')

def baixa_bd(Banco_Dados, COL):
    res = Banco_Dados.fetch()
    all_items = res.items
    while res.last:
        res = Banco_Dados.fetch(last=res.last)
        all_items += res.items
    banco_de_dados = pd.DataFrame([all_items][0])
    banco_de_dados = banco_de_dados.loc[:, COL]
    return banco_de_dados

def delete_user(key):
    db.delete(key)
    return st.success("Dados Deletado")

def get_user(key):
    user = db.get(key)
    return user

def ATUALIZAR_BANCO_DADOS_PANDAS(Coluna, value, key):
    df1 = get_user(key)
    df1[Coluna] = value
    user = db.put(df1, key)
    return user


def db_query(sql_query: str, db_conn: psycopg2.extensions.connection) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        df = pd.read_sql_query(sql_query, db_conn)
    return df

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

def start_bd3():
    db_connection = mysql.connector.connect(host="cvbidb.awservers.com.br",user="setelm_bi_ext",password="GrupoImerge7lm&Novk@2022",database="setelm_bi")
    df_repasse = db_query('select * from repasses;',db_connection)
    return df_repasse

def INDICADOR(meta,text):
    fig = go.Figure()
    fig.add_trace(go.Indicator(
    mode = "number",
    title = {"text": text},
    value = meta,
    domain = {'row': 0, 'column': 1}))
    fig.update_layout(height=150, width=220,margin=dict(l=0, r=0, t=0 , b=0 ))
    fig.update_layout(showlegend=False, paper_bgcolor = 'rgba(0, 0, 0, 0)', plot_bgcolor = 'rgba(0, 0, 0, 0)')
    return fig

def BAIXAR_BANCO_DE_DADOS():
    #Cria o arquivo pickle
    criar_arquivo_leads = open("leads.pickle","wb")
    criar_arquivo_repasse = open("df_repasse.pickle","wb")
    criar_arquivo_propostas = open("df_proposta.pickle","wb")
    criar_arquivo_vendas = open("df_vendas.pickle","wb")
    criar_arquivo_distrato = open("df_distratos.pickle","wb")


    st.warning("Criado com sucesso")
    
    #Atribui ao pickle
    pickle.dump(start_bd(), criar_arquivo_leads)
    pickle.dump(start_bd3(), criar_arquivo_repasse)
    pickle.dump(start_bd2(), criar_arquivo_propostas)
    pickle.dump(start_bd4(), criar_arquivo_vendas)   
    pickle.dump(start_bd5(), criar_arquivo_vendas)   

    st.warning("Atribuido sucesso")

    #Fechar pickle
    criar_arquivo_leads.close()  
    criar_arquivo_repasse.close()    
    criar_arquivo_propostas.close()   
    criar_arquivo_vendas.close()   
    criar_arquivo_distrato.close()   
    return st.success("Arquivo fechado")

def ABRIR_LEADS():   
    #Abrir pickle
    leads_ = open("leads.pickle","rb")
    #Baixar pickle
    leads = pickle.load(leads_)
    return leads 

def ABRIR_REPASSE():  
    #Abrir pickle
    repasse_ = open("df_repasse.pickle","rb") 
    #Baixar pickle
    repasse = pickle.load(repasse_)
    return repasse     
    
    
def ABRIR_PROPOSTA(): 
    #Abrir pickle 
    propostas_ = open("df_proposta.pickle","rb") 
    #Baixar pickle
    propostas = pickle.load(propostas_)
    return propostas

def ABRIR_VENDAS():  
    #Abrir pickle 
    vendas_ = open("df_vendas.pickle","rb")
    #Baixar pickle 
    vendas = pickle.load(vendas_)
    return vendas        
           
def ABRIR_DISTRATO():  
    #Abrir pickle 
    distrato_ = open("df_distratos.pickle","rb")
    #Baixar pickle 
    distrato = pickle.load(distrato_)
    return distrato           
           

   
with st.sidebar.expander("FILTRO"):
    with st.form(key="7lm123"):
        FILTRO_ = st.selectbox("LOJA / PDV", options=["Todas","Equipe Própria | AGL","Equipe Própria | FSA", "NOVKA"])
        bt_001 = st.form_submit_button("BAIXAR_ARQUIVO")    
    
        
        
        
def dataframe_universal(df, cidade, lista_ano):
    GERAL_VENDAS = df.copy()
    GERAL_VENDAS["data"] = pd.to_datetime(GERAL_VENDAS["data"], errors="coerce")
    GERAL_VENDAS["MM_AA"] = GERAL_VENDAS["data"].dt.month.astype(str)+"-"+GERAL_VENDAS["data"].dt.year.astype(str)
    GERAL_VENDAS["ANO"] = GERAL_VENDAS["data"].dt.year
    GERAL_VENDAS["MES"] = GERAL_VENDAS["data"].dt.month
    GERAL_VENDAS["DIA"] = GERAL_VENDAS["data"].dt.day
    GERAL_VENDAS["COUNT"] = 1
    GERAL_VENDAS["EMP"] = GERAL_VENDAS["empreendimento"].str.slice(0,3)
    # Variável ANO no Objeto || FILTROS IMPORTANTES
    GERAL_VENDAS = GERAL_VENDAS.loc[GERAL_VENDAS["ANO"].isin(lista_ano)]
    GERAL_VENDAS = GERAL_VENDAS.loc[GERAL_VENDAS["EMP"].isin([cidade])]

    VENDAS_POR_ANO_CANCELADAS = pd.DataFrame(GERAL_VENDAS.groupby(["ANO","MES","MM_AA","situacao_atual"])["COUNT", "valor_contrato"].sum()).reset_index()
    VENDAS_POR_ANO_CANCELADAS = VENDAS_POR_ANO_CANCELADAS.loc[VENDAS_POR_ANO_CANCELADAS["situacao_atual"].isin(["Cancelada","Distrato"])]
    VENDAS_POR_ANO_CANCELADAS = pd.DataFrame(VENDAS_POR_ANO_CANCELADAS.groupby(["ANO","MES","MM_AA"])["COUNT", "valor_contrato"].sum()).reset_index()

    VENDAS_POR_ANO = pd.DataFrame(GERAL_VENDAS.groupby(["ANO","MES","MM_AA"])["COUNT","valor_contrato"].sum()).reset_index()

    a = VENDAS_POR_ANO #VENDAS BRUTAS
    b = VENDAS_POR_ANO_CANCELADAS # DISTRATO / CANCELAMENTOS
    b = b.loc[:,["MM_AA","COUNT"]]
    b.rename(columns={"COUNT":"CANC_DIST"}, inplace=True)
    a.rename(columns={"COUNT":"VENDAS_BRT"}, inplace=True)
    c = pd.merge(a, b, on=["MM_AA"], how="left")
    c.fillna(0, inplace=True)
    c["CANC_DIST"] = c["CANC_DIST"].astype(int)
    d = c.copy() 
    c = c.T
    return d        
    
        
        
        
            
GERAL_VENDAS = ABRIR_VENDAS().copy()
GERAL_VENDAS["data"] = pd.to_datetime(GERAL_VENDAS["data"], errors="coerce")
GERAL_VENDAS["MM_AA"] = GERAL_VENDAS["data"].dt.month.astype(str)+"-"+GERAL_VENDAS["data"].dt.year.astype(str)
GERAL_VENDAS["ANO"] = GERAL_VENDAS["data"].dt.year
GERAL_VENDAS["MES"] = GERAL_VENDAS["data"].dt.month
GERAL_VENDAS["DIA"] = GERAL_VENDAS["data"].dt.day
GERAL_VENDAS["COUNT"] = 1
GERAL_VENDAS["EMP"] = GERAL_VENDAS["empreendimento"].str.slice(0,3)

FORCA_VENDAS_AGL = ["Aguas Lindas 1","Imobiliárias | AGL", "Águas Lindas 2", "Águas Lindas 1"]
FORCA_VENDAS_FSA = ["Formosa","7LM Formosa","Imobiliárias | FSA", "Equipe Própria | FSA" ]
FORCA_VENDAS_DF =  ['Imobiliária Padrão',"URBANA IMOVEIS","NOVKA DF",'BR HOUSE INTELIGENCIA IMOBILIARIA',"NovKa Formosa", 'Novka Pilotis', 'Imobiliárias | DF' ]

GERAL_VENDAS.loc[GERAL_VENDAS["imobiliaria"].isin(FORCA_VENDAS_AGL),"imobiliaria"] = "Equipe Própria | AGL"
GERAL_VENDAS.loc[GERAL_VENDAS["imobiliaria"].isin(FORCA_VENDAS_FSA),"imobiliaria"] = "Equipe Própria | FSA"
GERAL_VENDAS.loc[GERAL_VENDAS["imobiliaria"].isin(FORCA_VENDAS_DF),"imobiliaria"] = "NOVKA"

# Variável ANO no Objeto || FILTROS IMPORTANTES
GERAL_VENDAS = GERAL_VENDAS.loc[GERAL_VENDAS["ANO"].isin([2021, 2022])]
VENDAS_POR_ANO_CANCELADAS = pd.DataFrame(GERAL_VENDAS.groupby(["ANO","MES","MM_AA","situacao_atual"])["COUNT","valor_contrato"].sum()).reset_index()
VENDAS_POR_ANO_CANCELADAS = VENDAS_POR_ANO_CANCELADAS.loc[VENDAS_POR_ANO_CANCELADAS["situacao_atual"].isin(["Cancelada","Distrato"])]
VENDAS_POR_ANO_CANCELADAS = pd.DataFrame(VENDAS_POR_ANO_CANCELADAS.groupby(["ANO","MES","MM_AA"])["COUNT","valor_contrato"].sum()).reset_index()
VENDAS_POR_ANO = pd.DataFrame(GERAL_VENDAS.groupby(["ANO","MES","MM_AA"])["COUNT","valor_contrato"].sum()).reset_index()

a = VENDAS_POR_ANO #VENDAS BRUTAS
b = VENDAS_POR_ANO_CANCELADAS # DISTRATO / CANCELAMENTOS
b = b.loc[:,["MM_AA","COUNT","valor_contrato"]]
b.rename(columns={"COUNT":"CANC_DIST","valor_contrato":"VGV_DIST"}, inplace=True)
a.rename(columns={"COUNT":"VENDAS_BRT", "valor_contrato":"VGV_VD"}, inplace=True)
c = pd.merge(a, b, on=["MM_AA"], how="left")
c.fillna(0, inplace=True)
c["CANC_DIST"] = c["CANC_DIST"].astype(int)
c = c.T

lst_ano = []
for i in VENDAS_POR_ANO["MM_AA"]:
    lst_ano.append(i)
    
VENDAS_POR_ANO = VENDAS_POR_ANO.loc[:,["MM_AA","VENDAS_BRT"]].T
VENDAS_POR_ANO_TRAT = pd.DataFrame(columns=lst_ano, index=range(0,3))
VENDAS_POR_ANO_TRAT.iloc[0,:] = VENDAS_POR_ANO.iloc[1,:]
VENDAS_POR_ANO_TRAT.insert(0,"STATUS_GERAL","VENDAS_BRUTAS",True)
VENDAS_POR_ANO_TRAT.iloc[1,0] = "DISTRATO_CANCELAMENTO"
VENDAS_POR_ANO_TRAT.iloc[2,0] = "VENDAS_LIQ"
VENDAS_POR_ANO_TRAT.iloc[1,1:] = c.iloc[5,:]
VENDAS_POR_ANO_TRAT.iloc[2,1:] = VENDAS_POR_ANO_TRAT.iloc[0,1:] - VENDAS_POR_ANO_TRAT.iloc[1,1:] 
coluna_data = ["STATUS_GERAL","1-2022", "2-2022", "3-2022", "4-2022", "5-2022", "5-2022", "6-2022", "7-2022", "8-2022", "9-2022", "10-2022"]
#st.write(GERAL_VENDAS)
c1,c2,c3,c4 = st.columns((4,4,4,5))
st.markdown("Base sintética | Vendas Brutas::")


with c1:
    df_vendas_trat = ABRIR_VENDAS()
    df_vendas_trat_0011 = dataframe_universal(df_vendas_trat,["DF "],[2022])
    df_vendas_trat_0012 = dataframe_universal(df_vendas_trat,["AGL"],[2022])
    df_vendas_trat_0013 = dataframe_universal(df_vendas_trat,["FSA"],[2022])
    #st.write(VENDAS_POR_ANO_TRAT.T.iloc[13:,0].sum())
    TAM = VENDAS_POR_ANO_TRAT.T.iloc[13:,0].sum()
    st.plotly_chart(INDICADOR(TAM,"Vendas Brutas"))
    #st.plotly_chart(INDICADOR(VENDAS_POR_ANO_TRAT.loc[:,coluna_data].iloc[0,1:].sum(),"Vendas Brutas"))
with c2:
    st.plotly_chart(INDICADOR(VENDAS_POR_ANO_TRAT.loc[:,coluna_data].iloc[1,1:].sum(),"Distratos"))
with c4:
    vgv = GERAL_VENDAS.loc[GERAL_VENDAS["data"]>="2022-01-01"]["valor_contrato"].sum()
    st.plotly_chart(INDICADOR(vgv,"VGV Brutas"))
with c3:
    tt1 = VENDAS_POR_ANO_TRAT.loc[:,coluna_data].iloc[0,1:].sum()
    tt2 = VENDAS_POR_ANO_TRAT.loc[:,coluna_data].iloc[1,1:].sum()
    st.plotly_chart(INDICADOR(TAM-tt2,"Vendas Líquidas"))






grid_dataframe_top(VENDAS_POR_ANO_TRAT.loc[:,coluna_data],150,"blue",50)

AGL = pd.DataFrame(columns=["Meta/Objetivo","Realizado","%_Conversão"], index=["1-2022", "2-2022", "3-2022", "4-2022", "5-2022", "5-2022", "6-2022", "7-2022", "8-2022", "9-2022", "10-2022"])
FSA = pd.DataFrame(columns=["Meta/Objetivo","Realizado","%_Conversão"], index=["1-2022", "2-2022", "3-2022", "4-2022", "5-2022", "5-2022", "6-2022", "7-2022", "8-2022", "9-2022", "10-2022"])
NOVKA_ = pd.DataFrame(columns=["Meta/Objetivo","Realizado","%_Conversão"], index=["1-2022", "2-2022", "3-2022", "4-2022", "5-2022", "5-2022", "6-2022", "7-2022", "8-2022", "9-2022", "10-2022"])


c1, c2, c3 = st.columns((5,5,5))

with c1:
    st.subheader("VENDAS_AGL::")
    
    df_vendas_trat = ABRIR_VENDAS()
    df_vendas_trat_001 = dataframe_universal(df_vendas_trat,"AGL",[2022])
    df_vendas_trat_001["META"] = [35,30,38, 32, 30,25,40, 25, 28,30]
    df_vendas_trat_001["%"] = np.round(df_vendas_trat_001["VENDAS_BRT"]/df_vendas_trat_001["META"],2)
    df_vendas_trat_001 = df_vendas_trat_001.loc[:,["MM_AA","META","VENDAS_BRT","%","valor_contrato"]]
    st.plotly_chart(INDICADOR(df_vendas_trat_001["VENDAS_BRT"].sum(),"Vendas brutas"))
    grid_dataframe_top(df_vendas_trat_001,300,"blue",50)


with c2:
    st.subheader("VENDAS_FSA::")
    
    df_vendas_trat = ABRIR_VENDAS()
    df_vendas_trat_001 = dataframe_universal(df_vendas_trat,"FSA",[2022])
    df_vendas_trat_001["META"] = [18,18,20,20,25,25,22, 20,12,12]
    df_vendas_trat_001["%"] = np.round(df_vendas_trat_001["VENDAS_BRT"]/df_vendas_trat_001["META"],2)
    df_vendas_trat_001 = df_vendas_trat_001.loc[:,["MM_AA","META","VENDAS_BRT","%","valor_contrato"]]
    st.plotly_chart(INDICADOR(df_vendas_trat_001["VENDAS_BRT"].sum(),"Vendas brutas"))
    grid_dataframe_top(df_vendas_trat_001,300,"blue",50)
    
with c3:
    st.subheader("VENDAS_NOVKA::")
    
    df_vendas_trat = ABRIR_VENDAS()
    df_vendas_trat_001 = dataframe_universal(df_vendas_trat,"DF ",[2022])
    df_vendas_trat_001["META"] = 1#[18,18,20,20,25,25,22, 20,12,12]
    df_vendas_trat_001["%"] = np.round(df_vendas_trat_001["VENDAS_BRT"]/df_vendas_trat_001["META"],2)
    df_vendas_trat_001 = df_vendas_trat_001.loc[:,["MM_AA","META","VENDAS_BRT","%","valor_contrato"]]
    st.plotly_chart(INDICADOR(df_vendas_trat_001["VENDAS_BRT"].sum(),"Vendas brutas"))
    grid_dataframe_top(df_vendas_trat_001,300,"blue",50)

















