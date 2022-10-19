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

st.title("# DISTRATOS | CANCELAMENTOS ")


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
merge_trat = df_vendas_trat111[["idreserva","data"]]
merge_trat.rename(columns={"data":"data_venda"}, inplace=True)

DISTRATO_TRAT = base_de_distrato.copy()

coluna_dist = ["idreserva","data","situacao_data","situacao_atual","empreendimento","cliente","corretor","imobiliaria","motivo_distrato",
              "valor_contrato"]
DISTRATO_TRAT_001 = pd.merge(DISTRATO_TRAT,merge_trat,on=["idreserva"], how="left")



DISTRATO_TRAT_001["data_venda"] = pd.to_datetime(DISTRATO_TRAT_001["data_venda"], errors="coerce")
DISTRATO_TRAT_001["data"] = pd.to_datetime(DISTRATO_TRAT_001["data"], errors="coerce")

FORCA_VENDAS_AGL = ["Aguas Lindas 1","Imobiliárias | AGL", "Águas Lindas 2", "Águas Lindas 1"]
FORCA_VENDAS_FSA = ["Formosa","7LM Formosa","Imobiliárias | FSA", "Equipe Própria | FSA" ]
FORCA_VENDAS_DF =  ['Imobiliária Padrão',"URBANA IMOVEIS","NOVKA DF",'BR HOUSE INTELIGENCIA IMOBILIARIA',"NovKa Formosa", 'Novka Pilotis', 'Imobiliárias | DF' ]

DISTRATO_TRAT_001.loc[DISTRATO_TRAT_001["imobiliaria"].isin(FORCA_VENDAS_AGL),"imobiliaria"] = "Equipe Própria | AGL"
DISTRATO_TRAT_001.loc[DISTRATO_TRAT_001["imobiliaria"].isin(FORCA_VENDAS_FSA),"imobiliaria"] = "Equipe Própria | FSA"
DISTRATO_TRAT_001.loc[DISTRATO_TRAT_001["imobiliaria"].isin(FORCA_VENDAS_DF),"imobiliaria"] = "NOVKA"
DISTRATO_TRAT_001["MM_AA_VD"] = DISTRATO_TRAT_001["data_venda"].dt.year.astype(str)+"-"+DISTRATO_TRAT_001["data_venda"].dt.month.astype(str)+"-"+str("01")
DISTRATO_TRAT_001["MM_AA_DIS"] = DISTRATO_TRAT_001["data"].dt.year.astype(str)+"-"+DISTRATO_TRAT_001["data"].dt.month.astype(str)+"-"+str("01")
DISTRATO_TRAT_001["Quantidade"] = 1
DISTRATO_TRAT_001["MM_AA_VD"] = pd.to_datetime(DISTRATO_TRAT_001["MM_AA_VD"], errors="coerce")
DISTRATO_TRAT_001["MM_AA_DIS"] = pd.to_datetime(DISTRATO_TRAT_001["MM_AA_DIS"], errors="coerce")

DISTRATO_TRAT_002 = DISTRATO_TRAT_001.loc[DISTRATO_TRAT_001["MM_AA_DIS"]>="2022-01-01"]
LOJA = "Todas"
if LOJA == "Todas":
    DISTRATO_TRAT_002 = DISTRATO_TRAT_002.copy()
else:
    DISTRATO_TRAT_002 = DISTRATO_TRAT_002.loc[DISTRATO_TRAT_002["imobiliaria"].isin([LOJA])]
DISTRATO_TRAT_003 = pd.DataFrame(DISTRATO_TRAT_002.groupby(["MM_AA_DIS"])["Quantidade"].sum())

DISTRATO_TRAT_0020 = DISTRATO_TRAT_001.loc[DISTRATO_TRAT_001["MM_AA_DIS"]>="2022-01-01"]
LOJA = "Equipe Própria | FSA"
if LOJA == "Todas":
    DISTRATO_TRAT_0020 = DISTRATO_TRAT_0020.copy()
else:
    DISTRATO_TRAT_0020 = DISTRATO_TRAT_0020.loc[DISTRATO_TRAT_0020["imobiliaria"].isin([LOJA])]
DISTRATO_TRAT_0030 = pd.DataFrame(DISTRATO_TRAT_0020.groupby(["MM_AA_DIS","MM_AA_VD"])["Quantidade"].sum()).reset_index()
DISTRATO_TRAT_0030["MM_AA_DIS"] = DISTRATO_TRAT_0030["MM_AA_DIS"].astype(str)
DISTRATO_TRAT_0030["MM_AA_VD"] = DISTRATO_TRAT_0030["MM_AA_VD"].astype(str)



c1, c2 = st.columns((3,5))  
with c1: 
    st.markdown("Distratos | Mês::")
    #grid_dataframe_top(DISTRATO_TRAT_001, 400,"blue", 50)
    st.write(DISTRATO_TRAT_0030)
with c2:
    st.markdown("Distratos | Mês | Graph::")
    fig111 = px.bar(DISTRATO_TRAT_0030, x="MM_AA_DIS", y="Quantidade", color="MM_AA_VD")
    fig111.update_layout(height=450, width=1000,margin=dict(l=0, r=0, t=0 , b=0 ))
    st.plotly_chart(fig111)
    