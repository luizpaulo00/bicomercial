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
import altair as alt
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

st.title("# MARKETING | GROWTH")
st.subheader("1ª Camada | Estratificação")


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

def PREÇO_LAUDO(df, escolha_bloco, unidade, empreendimento):
    Base_Preços = df.copy()
    lst_ = Base_Preços.loc[Base_Preços["CÓD"].isin([empreendimento])].loc[:,["CÓD","BLOCO","UNIDADE","ÁREA PRIVATIVA",
                                                                             "JARDIM","VALOR DE VENDA","VALOR DO LAUDO"]].loc[Base_Preços["BLOCO"].isin([escolha_bloco])]
    lst_preço = lst_.loc[lst_["UNIDADE"].isin([unidade])]["VALOR DE VENDA"]
    lst_laudo = lst_.loc[lst_["UNIDADE"].isin([unidade])]["VALOR DO LAUDO"]
    return lst_preço, lst_laudo  

def db_query(sql_query: str, db_conn: psycopg2.extensions.connection) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        df = pd.read_sql_query(sql_query, db_conn)
    return df

def graf_leads(df,colunaX, colunaY, coluna_Cor):
    fig = px.bar(df, x=colunaX, y=colunaY, color=coluna_Cor, text=colunaY)
    fig.update_layout(barmode="relative")
    fig.update_layout(template="plotly_white", width=1500, height=500)
    return fig

def ABRIR_LEADS():   
    #Abrir pickle
    leads_ = open("leads.pickle","rb")
    #Baixar pickle
    leads = pickle.load(leads_)
    return leads 
def ABRIR_VENDAS():  
    #Abrir pickle 
    vendas_ = open("df_vendas.pickle","rb")
    #Baixar pickle 
    vendas = pickle.load(vendas_)
    return vendas

with st.sidebar.expander("FILTROS DIVERSOS"):
    with st.form(key="form001"):
        DATA_001 = st.text_input("DATA", value="2022-01-01")
        bt_001 = st.form_submit_button("OK")
    if bt_001:
        st.warning("Atenção")
        

#st.line_chart(data=None, *, x=None, y=None, width=0, height=0, use_container_width=True)    
    
leads_tratados = ABRIR_LEADS().copy()
leads_tratados.fillna(0, inplace=True)
leads_colunas = ["numero","situacao", "nome_momento_lead","nome","empreendimento","imobiliaria","gestor","corretor","origem",
                "data_cad","data_cancelamento","data_ultima_interacao","data_reativacao","data_primeira_interacao_gestor",
                "data_primeira_interacao_corretor"]
if DATA_001:
    leads_tratados = leads_tratados.loc[leads_tratados["data_cad"] >=DATA_001]
else:
    leads_tratados = leads_tratados.loc[leads_tratados["data_cad"] >="2021-01-01"]


leads_tratados = leads_tratados.loc[:,leads_colunas]        
rede_social = ["Facebook","Instagram","InstaPage", "Mídia Paga","Busca Orgânica","Social"]
site_ = ["WebSite","Google", "Aplicativo", "Aplicativo","Email", "Referência","Busca Compartilhada"]
chat_ = ["ChatBot", "Chat Online"]
portais_ = ["Portais"]
prop_corretor = ["Painel Corretor","Módulo de Atendimento", "Painel PDV","Painel Cliente"]
prop_imob = ["Painel Imobiliária", "Painel Gestor"]
ligacao_ = ["Phonetrack","Ligação"]
outros_ = ["Outras publicidades","Outros", "Não Definido"]

leads_tratados.loc[leads_tratados["origem"].isin(rede_social), "origem"] = "Rede_Social"
leads_tratados.loc[leads_tratados["origem"].isin(site_), "origem"] = "Website"
leads_tratados.loc[leads_tratados["origem"].isin(prop_corretor), "origem"] = "Prop_Corretor"
leads_tratados.loc[leads_tratados["origem"].isin(outros_), "origem"] = "Outros"
leads_tratados.loc[leads_tratados["origem"].isin(prop_imob), "origem"] = "Imob"
leads_tratados.loc[leads_tratados["origem"].isin(ligacao_), "origem"] = "Ligação"
leads_tratados.loc[leads_tratados["origem"].isin(chat_), "origem"] = "BOT"

leads_tratados['gestor'] = leads_tratados['gestor'].replace(['Fabiana Leandro', 'Helena Roberta dos Santos'], ["Fabiana", "Helena"])
leads_tratados['gestor'] = leads_tratados['gestor'].replace(['Jessica Nogueira', 'ERIKA LUARA PEREIRA COLARES'], ["Jessica", "Erika"])
leads_tratados['gestor'] = leads_tratados['gestor'].replace(['JULIANA FRANCISCA DE SOUSA LOPES', 'Ingrid Lorrayne Carvalho de Morais'], ["Juliana", "Ingrid"])
leads_tratados['gestor'] = leads_tratados['gestor'].replace(['Thandara de Oliveira Delevedove', 'Sara Geovana de Sales Santos'], ["Thandara", "Sara"])
leads_tratados['gestor'] = leads_tratados['gestor'].replace([0], ["sem_sdr"])

leads_tratados["QUANTIDADE"] = 1
leads_tratados["ANO"] = leads_tratados['data_cad'].dt.year
leads_tratados["MES"] = leads_tratados['data_cad'].dt.month
leads_tratados["DIA"] = leads_tratados['data_cad'].dt.day
leads_tratados["MM_AA"] = leads_tratados['data_cad'].dt.year.astype(str)+"-"+leads_tratados['data_cad'].dt.month.astype(str)+"-"+str("01")
leads_tratados["MM_AA"] = pd.to_datetime(leads_tratados["data_cad"], errors="coerce").dt.strftime('01-%m-%Y')

FORCA_VENDAS_AGL = ["Aguas Lindas 1","Imobiliárias | AGL", "Águas Lindas 2", "Águas Lindas 1"]
FORCA_VENDAS_FSA = ["Formosa","7LM Formosa","Imobiliárias | FSA", "Equipe Própria | FSA" ]
FORCA_VENDAS_DF =  ['Imobiliária Padrão',"URBANA IMOVEIS","NOVKA DF",'BR HOUSE INTELIGENCIA IMOBILIARIA',"NovKa Formosa", 'Novka Pilotis', 'Imobiliárias | DF' ]

leads_tratados.loc[leads_tratados["imobiliaria"].isin(FORCA_VENDAS_AGL),"imobiliaria"] = "Equipe Própria | AGL"
leads_tratados.loc[leads_tratados["imobiliaria"].isin(FORCA_VENDAS_FSA),"imobiliaria"] = "Equipe Própria | FSA"
leads_tratados.loc[leads_tratados["imobiliaria"].isin(FORCA_VENDAS_DF),"imobiliaria"] = "NOVKA"

leads_tratados.loc[leads_tratados["empreendimento"] == "AGL28 - Vila do Cerrado","imobiliaria"] = 'Equipe Própria | AGL'
leads_tratados.loc[leads_tratados["empreendimento"] == "DF 01 - Haus By Novka","imobiliaria"] = 'NOVKA'
leads_tratados.loc[leads_tratados["empreendimento"] == "FSA 06 - Vila das Tulipas - 7LM","imobiliaria"] = 'Equipe Própria | FSA'
leads_tratados.loc[leads_tratados["empreendimento"] == "FSA07 - Vila Das Hortênsias 7LM","imobiliaria"] = 'Equipe Própria | FSA'

leads_tratados["Quantidade"] = 1
leads_tratados.loc[leads_tratados["empreendimento"] == "AGL28 - Vila do Cerrado","imobiliaria"] = 'Equipe Própria | AGL'
leads_tratados.loc[leads_tratados["empreendimento"] == "DF 01 - Haus By Novka","imobiliaria"] = 'NOVKA'
leads_tratados.loc[leads_tratados["empreendimento"] == "FSA 06 - Vila das Tulipas - 7LM","imobiliaria"] = 'Equipe Própria | FSA'
leads_tratados.loc[leads_tratados["empreendimento"] == "FSA07 - Vila Das Hortênsias 7LM","imobiliaria"] = 'Equipe Própria | FSA'

emp_agl = ['AGL 25 - Vila das Águas','AGL 27 - Vila Azaleia - 7LM','AGL 23 - Vila do Sol','AGL28 - Vila do Cerrad']
emp_fsa = ['FSA07 - Vila Das Hortências 7LM','FSA 05 - Vila das Orquídeas - 7LM','FSA 06 - Vila das Tulipas - 7LM']
emp_novka = ['FSA 03 -  Aurium Home','DF 01 - Haus By Novka']

leads_tratados.loc[leads_tratados["empreendimento"].isin(emp_agl),"imobiliaria"] = 'Equipe Própria | AGL'
leads_tratados.loc[leads_tratados["empreendimento"].isin(emp_fsa),"imobiliaria"] = 'Equipe Própria | FSA'
leads_tratados.loc[leads_tratados["empreendimento"].isin(emp_novka),"imobiliaria"] = 'NOVKA'
leads_tratados.loc[leads_tratados["imobiliaria"] == 0,"imobiliaria"] = "Sem_Imob"

ANALISE_RAPIDA_001 = pd.DataFrame(leads_tratados.groupby(["MES","ANO","MM_AA","imobiliaria"])["Quantidade"].sum()).reset_index()
ANALISE_RAPIDA_001 = ANALISE_RAPIDA_001.sort_values(by=["ANO","MES","Quantidade"], ascending=True)
ANALISE_RAPIDA_002 = ANALISE_RAPIDA_001.loc[~ANALISE_RAPIDA_001["imobiliaria"].isin(["Canal Virtual","Sem_Imob"])]

camada_001 = pd.DataFrame(leads_tratados.groupby(["ANO"])["QUANTIDADE"].sum()).reset_index()
camada_002 = pd.DataFrame(leads_tratados.groupby(["ANO","MES","MM_AA"])["QUANTIDADE"].sum()).reset_index()
camada_003 = pd.DataFrame(leads_tratados.groupby(["origem"])["QUANTIDADE"].sum()).reset_index()
camada_004 = ANALISE_RAPIDA_002.copy()
midia = leads_tratados.copy()
midia = pd.DataFrame(midia.groupby(["ANO","MES","MM_AA","origem"])["Quantidade"].sum()).reset_index().sort_values(by=["ANO","MES","Quantidade"], ascending=True)


#TRATAMENTO DE VENDAS =========================================================================================
colunas_reservas = ["idreserva","MM_AA","data","situacao_atual","situacao_data","empreendimento","cliente","renda","cidade","sexo","idade","estado_civil","valor_contrato","idlead","imobiliaria","corretor"]
VENDAS_YTD = ABRIR_VENDAS().copy()
VENDAS_YTD.fillna(0,inplace=True)
VENDAS_YTD["data"] = pd.to_datetime(VENDAS_YTD["data"], errors="coerce")
VENDAS_YTD["MM_AA"] = VENDAS_YTD["data"].dt.year.astype(str)+"-"+VENDAS_YTD["data"].dt.month.astype(str)+"-"+str("01")
VENDAS_YTD = VENDAS_YTD.loc[: ,colunas_reservas]
VENDAS_YTD["ANO"] = VENDAS_YTD["data"].dt.year
VENDAS_YTD["MES"] = VENDAS_YTD["data"].dt.month
VENDAS_YTD["DAY"] = VENDAS_YTD["data"].dt.day
VENDAS_YTD["Quantidade"] = 1 
VENDAS_YTD.loc[(VENDAS_YTD["idade"]>=18) & (VENDAS_YTD["idade"]<26),"fx_idade"] = "18_a_25"
VENDAS_YTD.loc[(VENDAS_YTD["idade"]>=26) & (VENDAS_YTD["idade"]<31),"fx_idade"] = "26_a_30"
VENDAS_YTD.loc[(VENDAS_YTD["idade"]>30) & (VENDAS_YTD["idade"]<=40),"fx_idade"] = "31_a_40"
VENDAS_YTD.loc[(VENDAS_YTD["idade"]>40) & (VENDAS_YTD["idade"]<=50),"fx_idade"] = "41_a_50"
VENDAS_YTD.loc[(VENDAS_YTD["idade"]>50),"fx_idade"] = "MAIOR_50"

de = ["Aguas Lindas 1", 'BR HOUSE INTELIGENCIA IMOBILIARIA',"NovKa Formosa",'Novka Pilotis','Imobiliárias | DF','Imobiliária Padrão',"Imobiliárias | AGL"]
para = ['Equipe Própria | AGL','NOVKA', 'NOVKA', 'NOVKA', 'NOVKA', 'NOVKA','Equipe Própria | AGL']
VENDAS_YTD["imobiliaria"] = VENDAS_YTD["imobiliaria"].replace(de,para)   
VENDAS_YTD.fillna(0,inplace=True)
VENDAS_YTD_001 = VENDAS_YTD.loc[VENDAS_YTD["MM_AA"]>=DATA_001]

vd = pd.DataFrame(VENDAS_YTD_001.groupby(["ANO","MES","MM_AA","imobiliaria","sexo"])["Quantidade"].sum()).reset_index()
vd0 = pd.DataFrame(vd.groupby(["ANO","MES","MM_AA","sexo"])["Quantidade"].sum()).reset_index()
vd1 = vd.loc[vd["imobiliaria"] =="Equipe Própria | AGL"]










c1, c2  = st.columns((3,5))  
with c1: 
    st.markdown("Leads Gerados::")
    grid_dataframe_top(camada_001,150,"blue", 50)
with c2:
    st.markdown("Leads Gerados | Graph::") 
    fig2 = px.pie(camada_001, values='QUANTIDADE', names='ANO', title='Quantidade de leads')
    fig2.update_layout(height=450, width=1000,margin=dict(l=0, r=0, t=0 , b=0 ))
    st.plotly_chart(fig2)



c1, c2 = st.columns((3,5))  
with c1: 
    st.markdown("Leads Gerados | Mês::")
    grid_dataframe_top(camada_002, 400,"blue", 50)
    
with c2:
    st.markdown("Leads Gerados | Mês | Graph::")
    fig1 = px.bar(camada_002, x="MM_AA", y="QUANTIDADE", color="MES", text_auto=True)
    fig1.update_layout(height=450, width=1000,margin=dict(l=0, r=0, t=0 , b=0 ))
    st.plotly_chart(fig1)
    

st.subheader("2ª Camada | Estratificação")
c1, c2 = st.columns((3,5))  
with c1: 
    st.markdown("Leads Gerados | Loja::")
    grid_dataframe_top(camada_004, 500,"blue", 50)
    
with c2:
    st.markdown("Leads Gerados | Loja | Graph::")
    fig = px.bar(ANALISE_RAPIDA_002, x="MM_AA", y="Quantidade", color="imobiliaria", text_auto=True)
    fig.update_layout(height=450, width=1200,margin=dict(l=0, r=0, t=0 , b=0 ))
    st.plotly_chart(fig)
    
    
c1, c2 = st.columns((3,5))  
with c1: 
    st.markdown("Leads Gerados | Mídia::")
    grid_dataframe_top(midia, 500,"blue", 50)
    
with c2:
    st.markdown("Leads Gerados | Mídia | Graph::")
    fig3 = px.scatter(midia, x="MM_AA", y="Quantidade",
         size="Quantidade", color="origem",
                 hover_name="origem", log_x=False, size_max=50)
    fig3.update_layout(height=450, width=1200,margin=dict(l=0, r=0, t=0 , b=0 ))
    st.plotly_chart(fig3)   


st.subheader("3ª Camada | Estratificação")
c1, c2 = st.columns((3,5))  
with c1: 
    st.markdown("Compradores | Sexo::")
    grid_dataframe_top(vd0, 500,"blue", 50)
    
with c2:
    st.markdown("Compradores | Sexo | Graph::")
    fig4 = go.Figure()
    fig4.add_trace(go.Bar(
        x=list(vd0.loc[vd0["sexo"]=="Feminino"]['MM_AA'].unique()),
        y=list(vd0.loc[vd0["sexo"]=="Feminino"]['Quantidade']),
        name="Feminino",
        orientation='v',
        marker=dict(
            color='rgba(246, 78, 139, 0.6)',
            line=dict(color='rgba(246, 78, 139, 1.0)', width=3)
        )
    ))
    fig4.add_trace(go.Bar(
        x=list(vd0.loc[vd0["sexo"]=="Masculino"]['MM_AA'].unique()),
        y=list(vd0.loc[vd0["sexo"]=="Masculino"]['Quantidade']),
        name='Masculino',
        orientation='v',
        marker=dict(
            color='rgba(58, 71, 80, 0.6)',
            line=dict(color='rgba(58, 71, 80, 1.0)', width=3)
        )
    ))

    fig4.update_layout(barmode='stack')
    fig4.update_layout(height=450, width=1200,margin=dict(l=0, r=0, t=0 , b=0 ))
    st.plotly_chart(fig4)   

def FAIXA_PRECO(df):    
    colunas_reservas = ["idreserva","MM_AA","data","situacao_atual","situacao_data","empreendimento","cliente","renda","cidade","sexo","idade","estado_civil","valor_contrato","idlead","imobiliaria","corretor"]
    VENDAS_YTD = df.copy()
    VENDAS_YTD.fillna(0,inplace=True)
    VENDAS_YTD["data"] = pd.to_datetime(VENDAS_YTD["data"], errors="coerce")
    VENDAS_YTD["MM_AA"] = VENDAS_YTD["data"].dt.year.astype(str)+"-"+VENDAS_YTD["data"].dt.month.astype(str)+"-"+str("01")
    VENDAS_YTD = VENDAS_YTD.loc[: ,colunas_reservas]
    VENDAS_YTD["ANO"] = VENDAS_YTD["data"].dt.year
    VENDAS_YTD["MES"] = VENDAS_YTD["data"].dt.month
    VENDAS_YTD["DAY"] = VENDAS_YTD["data"].dt.day
    VENDAS_YTD["Quantidade"] = 1 

    VENDAS_YTD.loc[(VENDAS_YTD["idade"]>=18) & (VENDAS_YTD["idade"]<26),"fx_idade"] = "18_a_25"
    VENDAS_YTD.loc[(VENDAS_YTD["idade"]>=26) & (VENDAS_YTD["idade"]<31),"fx_idade"] = "26_a_30"
    VENDAS_YTD.loc[(VENDAS_YTD["idade"]>30) & (VENDAS_YTD["idade"]<=40),"fx_idade"] = "31_a_40"
    VENDAS_YTD.loc[(VENDAS_YTD["idade"]>40) & (VENDAS_YTD["idade"]<=50),"fx_idade"] = "41_a_50"
    VENDAS_YTD.loc[(VENDAS_YTD["idade"]>50),"fx_idade"] = "MAIOR_50"

    VENDAS_YTD.loc[(VENDAS_YTD["renda"]>=1000) & (VENDAS_YTD["renda"]<1500),"fx_renda"] = "1.000_a_1.500"
    VENDAS_YTD.loc[(VENDAS_YTD["renda"]>=1500) & (VENDAS_YTD["renda"]<2000),"fx_renda"] = "1.501_a_2.000"
    VENDAS_YTD.loc[(VENDAS_YTD["renda"]>=2000) & (VENDAS_YTD["renda"]<2500),"fx_renda"] = "2.001_a_2.500"
    VENDAS_YTD.loc[(VENDAS_YTD["renda"]>=2500) & (VENDAS_YTD["renda"]<3000),"fx_renda"] = "2.501_a_3.000"
    VENDAS_YTD.loc[(VENDAS_YTD["renda"]>=3000) & (VENDAS_YTD["renda"]<4000),"fx_renda"] = "3.001_a_4.000"
    VENDAS_YTD.loc[(VENDAS_YTD["renda"]>=4000) & (VENDAS_YTD["renda"]<5000),"fx_renda"] = "4.001_a_5.000"
    VENDAS_YTD.loc[(VENDAS_YTD["renda"]>=5000),"fx_renda"] = "3.001_a_4.000"

    de = ["Aguas Lindas 1", 'BR HOUSE INTELIGENCIA IMOBILIARIA',"NovKa Formosa",'Novka Pilotis','Imobiliárias | DF','Imobiliária Padrão',"Imobiliárias | AGL"]
    para = ['Equipe Própria | AGL','NOVKA', 'NOVKA', 'NOVKA', 'NOVKA', 'NOVKA','Equipe Própria | AGL']
    VENDAS_YTD["imobiliaria"] = VENDAS_YTD["imobiliaria"].replace(de,para)   
    VENDAS_YTD.fillna(0,inplace=True)
    VENDAS_YTD_001 = VENDAS_YTD.loc[VENDAS_YTD["MM_AA"]>="2022-1-01"]

    VENDAS_YTD_001_FX_RENDA = pd.DataFrame(VENDAS_YTD_001.groupby(["ANO","MES","MM_AA","imobiliaria","fx_renda"])["Quantidade"].sum()).reset_index()
    return VENDAS_YTD_001_FX_RENDA











st.subheader("4ª Camada | Estratificação")
c1, c2 = st.columns((3,5))  
with c1: 
    st.markdown("Compradores | Renda::")
    VENDAS_YTD1 = ABRIR_VENDAS().copy()
    VENDAS_YTD1 = FAIXA_PRECO(VENDAS_YTD1)
    grid_dataframe_top(VENDAS_YTD1, 500,"blue", 50)
with c2: 
    st.markdown("Compradores | Renda | Graph::")
    fig5 = px.bar(VENDAS_YTD1, x="MM_AA", y="Quantidade", color="fx_renda")
    fig5.update_layout(height=450, width=1200,margin=dict(l=0, r=0, t=0 , b=0 ))
    st.plotly_chart(fig5)


#s


























