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

st.title("# CURVA DE ESTOQUE | ESGOTAMENTO ")


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


def grid_dataframe_top(df, tamanho):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(groupable=True, enableValue=True, enableRowGroup=True,aggFunc="sum",editable=True)
    gb.update_mode=GridUpdateMode.MANUAL
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_side_bar()
    gridoptions = gb.build()
    response = AgGrid(
        df,
        height=tamanho,
        gridOptions=gridoptions,
        enable_enterprise_modules=True,
        header_checkbox_selection_filtered_only=True,
        use_checkbox=True, theme="blue")
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



    db_connection = mysql.connector.connect(host="cvbidb.awservers.com.br",user="setelm_bi_ext",password="GrupoImerge7lm&Novk@2022",database="setelm_bi")
    df_distratos = db_query('select * from distratos;',db_connection)
    return df_distratos


def BAIXAR_BANCO_DE_DADOS():
    #Cria o arquivo pickle
    criar_arquivo_distrato = open("df_distratos.pickle","wb")


    st.warning("Criado com sucesso")
    
    #Atribui ao pickle

    pickle.dump(start_bd5(), criar_arquivo_distrato)   

    st.warning("Atribuido sucesso")

    #Fechar pickle

    criar_arquivo_distrato.close()   


    return st.success("Arquivo fechado")



def start_bd5():
    db_connection = mysql.connector.connect(host="cvbidb.awservers.com.br",user="setelm_bi_ext",password="GrupoImerge7lm&Novk@2022",database="setelm_bi")
    df_distratos = db_query('select * from distratos;',db_connection)
    return df_distratos

base_de_distrato = start_bd5()

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



with st.sidebar.expander("ATUALIZAR BANCO DE DADOS"):
    with st.form(key="form001"):
        # incluir elementos
        bt_001 = st.form_submit_button("BAIXAR_ARQUIVO")
    if bt_001:
        BAIXAR_BANCO_DE_DADOS()


df_vendas_trat111 = ABRIR_VENDAS()



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

    VENDAS_POR_ANO = pd.DataFrame(GERAL_VENDAS.groupby(["ANO","MES","MM_AA","empreendimento"])["COUNT","valor_contrato"].sum()).reset_index()

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
    return GERAL_VENDAS   


a = dataframe_universal(df_vendas_trat111, "AGL", [2020,2021,2022])
b = a.groupby(["MM_AA","empreendimento"])["valor_contrato"].mean().reset_index()
st.write(pd.DataFrame(b))

  





  