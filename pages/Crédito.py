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
#from oauth2client.service_account import ServiceAccountCredentials
import mysql.connector 
import sqlalchemy   
from datetime import date
#import xlsxwriter
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
DATA_HOJE = pd.to_datetime(date.today(), errors="coerce")
# =====================================================================
# TEXTO INICIO
# ======================================================================
#img = "assets/logo7lm.png"
imagem = "assets/logo7lm.png"
st.set_page_config(page_icon=imagem, layout="wide",page_title="GRUPO IMERGE | DASH GARENCIAL")
c1, c2, c3 = st.columns((1,5,1))
c1.image(image=imagem, use_column_width=True, width="200px")
c2.markdown("# CONTROLE DE REPASSE​")

img = "assets/logo7lm.png"
st.sidebar.image(image=img, use_column_width=True,caption="Dashboard-Comercial")

rep_status = pd.read_excel("df_status_repasse.xlsx")
list_emp = ["AGL25","AGL23","AGL27","AGL28","FSA005","FSA006","DF001","FSA003"]

html_model = """"
<!doctype html>

"""
st.markdown(html_model,unsafe_allow_html=True)

DIC_EMPREENDIMENTOS = {'AGL 25 - Vila das Águas':"AGL25", 'FSA 05 - Vila das Orquídeas - 7LM':"FSA005",
        'DF 01 - Haus By Novka':"DF001", 'AGL 23 - Vila do Sol':"AGL23",
       'FSA 06 - Vila das Tulipas - 7LM':"FSA006", 'AGL28 - Vila do Cerrado':"AGL28",
       'AGL 24 - Park Club Cidade Jardim':"AGL24", 'FSA 03 -  Aurium Home':"FSA003",
       'AGL 27 - Vila Azaleia - 7LM':"AGL27", 'FSA 04 - Vila da Serra':"FSA004","AGL 22 - Park Club Olinda":"AGL22",
        "FSA 01 - Park Prime Sul":"FSA001","FSA 02 - Park Prime Sul II":"FSA002",'AGL 20E1 - 7LM XX Etapa 1':"AGL20_MOD1", 
       'AGL 20E2 - Obra 20 Etapa 2':"AGL20_MOD2",'AGL 21E1 - Obra 21 Etapa 1':"AGL21_MOD1", 'AGL 21E2 - Obra 21 Etapa 2':"AGL21_MOD2",
       'AGL 21E3 - Obra 21 Etapa 3':"AGL21_MOD3", 'AGL 21E4 - Obra 21 Etapa 4':"AGL21_MOD4",'AGL24 - Park Club Cidade Jardim':"AGL24"}


DIC_MIDIA = {'': "ESTOQUE",'Ação Externa': "OFF",'Bing': "SITE",'Busca Orgânica': "SITE",'Busdoor/Backbus': "OFF",'CORRETOR INSTAGRAM': "INSTAGRAM",'CORRETOR MÍDIA PAGA': "INSTAGRAM",
 'Captação do Corretor': "PROSP. CORRETOR",'Chatbot': "CHAT",'Display': "SITE",'E-Mail': "E-MAIL",'Facebook': "FACEBOOK",'Facebook Ads': "FACEBOOK",'Feiras e Eventos': "OFF",
 'Google': "SITE",'Google AdWords':"SITE",'Indicação': "INDICAÇÃO",'Instagram': "INSTAGRAM",'Ligação': "LIGAÇÃO",'Mídia Paga':"SITE",'Outdoor / Mobiliário Urbano': "OFF",'Outros': "OUTROS",
 'Outros Sites': "SITE",'Painel Corretor': "PROSP. CORRETOR",'Painel Gestor': "PROSP. CORRETOR",'Painel Imobiliária': "PROSP. IMOB",'Panfleto': "PANFLETO",'Placas': "PLACAS",
 'Pré Atendimento': "PROSP. SDR",'Rádio': "RADIO",'Site': "SITE",'Telefone': "FONE",'Tenda / Stand Móvel': "OFF",'Twitter': "Twitter",'Visita Espontânea': "ESPONTÂNEO",'facebook_desktop_feed': "FACEBOOK",
 'facebook_stories': "FACEBOOK",'instagram_feed': "INSTAGRAM",'rdstation.com.br': "AUTOMAÇÃO",'whatsapp': "WHATSAPP", 'Social | Facebook':"FACEBOOK",'Indicação Amigo/Parente':"INDICAÇÃO",'Busca orgânica | Google':"SITE",
 'Referência | rdstation.com.br':"AUTOMAÇÃO",'Indicação Corretor':"PROSP. CORRETOR",'Social | Instagram':"INSTAGRAM",'Facebook Organic':"FACEBOOK","Televisão":"OFF",'ZAP / Viva Real':"PORTAL",
            "Não Definido":"OUTROS","Chat Online":"CHAT","Busca Compartilhada":"SITE","Tráfego Direto":"SITE","Referência":"OUTROS","Outras publicidades":"OUTROS","Portais":"PORTAIS","Email":"EMAIL","Painel PDV":"OFF","Social":"FACEBOOK",
            "Desconhecido":"OUTROS","WebSite":"SITE","ChatBot":"CHAT","InstaPage":"INSTAGRAM","Phonetrack":"TELEFONE"}


DIC_CREDITO = {'Não Repassado':"MP_ASSINATURA", 'Em Validação | SV | Registro':"MP_REGISTRO","Em Montagem Docs | Registro":"MP_REGISTRO",
 'Solicitação ITBI | Registro':"MP_REGISTRO", 'Protocolo Cartório | Registro':"MP_REGISTRO",
 'Exigência | Registro':"MP_REGISTRO", 'Contrato Registrado':"FINALIZADO", 'Distrato':"FORA_DA_CONTA","Fora da Fila":"FORA_DA_CONTA","Direto Construtora":"FINALIZADO",0:"VERIFICAR","Em Pgto de Tributos":"MP_REGISTRO"}

DIC_DATA = {'01-2022':1, '02-2022':2, '03-2022':3, '04-2022':4, '05-2022':5, '06-2022':6,
       '07-2022':7,'08-2022':8,'09-2022':9,'10-2022':10,'11-2022':11,'12-2022':12}

DIC_PLANO_PGTO = {'Entrada':"SINAL", 'Anuais':"PRO_SOLUTO", 'Mensais':"PRO_SOLUTO", 'Financiamento':"FIN", 'Semestrais':"PRO_SOLUTO",
       'Resíduo':"PRO_SOLUTO", 'Cartão de Crédito':"PRO_SOLUTO", 'Entrega das Chaves':"PRO_SOLUTO", 'FGTS':"FIN",
       'Mensais (Aditivo)':"PRO_SOLUTO", 'Assinatura Caixa':"PRO_SOLUTO", 'Bonus':"PRO_SOLUTO", 'Valor de Venda':"VGV",
       'Desconto':"DESC", 'Anuais (Aditivo)':"PRO_SOLUTO", 'Valor de Avaliação':"AVALIACAO",
       'Valor com Desconto':"VGV_LIQ", 'Cheque Moradia':"CH"}

def conversor_moeda_brasil(my_value):
    a = '{:,.2f}'.format(float(my_value))
    b = a.replace(',','v')
    c = b.replace('.',',')
    return c.replace('v','.')

def db_query(sql_query: str, db_conn: psycopg2.extensions.connection) -> pd.DataFrame:
    """Execute SQL Query and get Dataframe with pandas"""
    with warnings.catch_warnings():
        # ignore warning for non-SQLAlchemy Connecton
        # see github.com/pandas-dev/pandas/issues/45660
        warnings.simplefilter('ignore', UserWarning)
        # create pandas DataFrame from database query
        df = pd.read_sql_query(sql_query, db_conn)
    return df

def start_bd(Num_arq):
        db_connection = mysql.connector.connect(host="cvbidb.awservers.com.br",user="setelm_bi_ext",password="GrupoImerge7lm&Novk@2022",database="setelm_bi")
        df_repasse = db_query('select * from repasses;',db_connection)
        print("Tab001")
        df_vendas = db_query('select * from reservas;',db_connection)
        print("Tab002")
        df_pgto = db_query('select * from reservas_condicoes;',db_connection)
        print("Tab003")
        if Num_arq == 1:
            return df_repasse
        elif Num_arq == 2:
            return df_pgto
        else:
            return df_vendas     
        
def Num_Vendas(df,empreend):
    df = df.copy()
    df = df.loc[:,["empreendimento","bloco","unidade","situacao","cliente","data"]]
    lst_emp = []
    for i in df["empreendimento"]:
        try:
            lst_emp.append(DIC_EMPREENDIMENTOS[i])
        except:
            lst_emp.append("--")
    df["EMPREEND"] = lst_emp
    df = df.loc[df["EMPREEND"].isin([empreend])]
    df = df.loc[df["situacao"].isin(["Venda finalizada"])]
    return len(df)      


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


def ASSINADO_CEF(df, empreend):   
    df = df.copy()
    df["count"] = 1
    df = pd.merge(df, rep_status, on=["situacao"], how="left")
    lst_emp = []
    for i in df["empreendimento"]:
        try:
            lst_emp.append(DIC_EMPREENDIMENTOS[i])
        except:
            lst_emp.append("--")
    df["EMPREEND"] = lst_emp
    df = df.loc[df["EMPREEND"].isin([empreend])]
    df = df.loc[df["STATUS_REPASSE"].isin(['Direto Construtora / Avista','Contrato Assinado CEF'])]
    return len(df)

def GARANTIA_REC(df, empreend):   
    df = df.copy()
    df["count"] = 1
    df = pd.merge(df, rep_status, on=["situacao"], how="left")
    lst_emp = []
    for i in df["empreendimento"]:
        try:
            lst_emp.append(DIC_EMPREENDIMENTOS[i])
        except:
            lst_emp.append("--")
    df["EMPREEND"] = lst_emp
    df = df.loc[df["EMPREEND"].isin([empreend])]
    df = df.loc[df["STATUS_REGISTRO"].isin(['Contrato Registrado'])]
    return len(df)

with st.sidebar.expander("BAIXAR BD"):
    with st.form(key="7lm002"):
        st.warning("BAIXAR BANCO DE DADOS")
        BT_001 = st.form_submit_button("BAIXAR")
    if BT_001:
        st.warning("BAIXAR BANCO DE DADOS")
        pickle_out = open("repasse.pickle","wb")
        pickle_out1 = open("vendas.pickle","wb")
        pickle.dump(start_bd(1), pickle_out)
        pickle.dump(start_bd(3), pickle_out1)
        pickle_out.close()  
        pickle_out1.close() 
        st.write("Pickle Criado!")

try:
        pickle_in = open("repasse.pickle","rb")
        pickle_in_001 = open("vendas.pickle","rb")
        df_repasse_ = pickle.load(pickle_in)
        df_vendas_ = pickle.load(pickle_in_001)   
except:
        st.error("<--------------- FAVOR BAIXAR O BANCO DE DADOS!")


def card_universal1(nome):
  nome = f"""
          <!doctype html>
          <html lang="en">
            <head>
              <meta charset="utf-8">
              <meta name="viewport" content="width=device-width, initial-scale=1">
              <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.0/dist/css/bootstrap.min.css" rel="stylesheet">
              <link href="https://getbootstrap.com/docs/5.2/assets/css/docs.css" rel="stylesheet">
              <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.0/dist/js/bootstrap.bundle.min.js"></script>
            </head>
            <table class="table table-striped table-hover">
                <thead class="table-primary">
                    <tr>
                    <th scope="col">CIDADE:</th>
                    <th scope="col">EMPREEND:</th>
                    <th scope="col">CTO:</th>
                    <th scope="col">POC:</th>                    
                    <th scope="col">TOTAL_UH:</th>
                    <th scope="col">VENDIDOS:</th>
                    <th scope="col">ASS CEF+FINALZ:</th>
                    <th scope="col">GARANTIA REC:</th>
                    <th scope="col">ESTOQUE:</th>
                    <th scope="col">A REPASSAR:</th>
                    <th scope="col">OPORT_RECEITA:</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                    <th scope="row">AGL</th>
                    <td>AGL23</td>
                    <td>14.963.232,83</td>
                    <td>0</td>
                    <td>184</td>
                    <td>{Num_Vendas(df_vendas_,"AGL23")}</td>
                    <td>{ASSINADO_CEF(df_repasse_, "AGL23")}</td>   
                    <td>{GARANTIA_REC(df_repasse_, "AGL23")}</td>     
                    <td>{184 - Num_Vendas(df_vendas_,"AGL23")}</td>     
                    <td>{(Num_Vendas(df_vendas_,"AGL23")) - ASSINADO_CEF(df_repasse_, "AGL23")}</td>   
                    <td>{ASSINADO_CEF(df_repasse_, "AGL23") - GARANTIA_REC(df_repasse_, "AGL23")}</td>                                                              
                    </tr>
                    <tr>
                    <th scope="row">AGL</th>
                    <td>AGL25_MOD1</td>
                    <td>11.624.611,60</td>
                    <td>0</td>
                    <td>138</td>
                    <td>{Num_Vendas(df_vendas_,"AGL25")-144}</td> 
                    <td>138</td>
                    <td>138</td> 
                    <td>0</td> 
                    <td>0</td>     
                    <td>0</td>                                                                                                                     
                    </tr>
                    <tr>
                    <th scope="row">AGL</th>
                    <td>AGL25_MOD2</td>
                    <td>10.873.046,05</td>
                    <td>0</td>
                    <td>144</td>
                    <td>{Num_Vendas(df_vendas_,"AGL25")-138}</td> 
                    <td>{ASSINADO_CEF(df_repasse_, "AGL25")-138}</td> 
                    <td>{GARANTIA_REC(df_repasse_, "AGL25")-138}</td> 
                    <td>{(144+138) - Num_Vendas(df_vendas_,"AGL25")}</td>   
                    <td>{(Num_Vendas(df_vendas_,"AGL25")) - ASSINADO_CEF(df_repasse_, "AGL25")}</td>  
                    <td>{ASSINADO_CEF(df_repasse_, "AGL25") - GARANTIA_REC(df_repasse_, "AGL25")}</td>                                                                                                                      
                    </tr>
                    <tr>
                    <th scope="row">AGL</th>
                    <td>AGL27</td>
                    <td>10.451.266,46</td>
                    <td>0</td>
                    <td>112</td>
                    <td>{Num_Vendas(df_vendas_,"AGL27")}</td> 
                    <td>{ASSINADO_CEF(df_repasse_, "AGL27")}</td>   
                    <td>{GARANTIA_REC(df_repasse_, "AGL27")}</td>    
                    <td>{112 - Num_Vendas(df_vendas_,"AGL27")}</td>    
                    <td>{(Num_Vendas(df_vendas_,"AGL27")) - ASSINADO_CEF(df_repasse_, "AGL27")}</td>   
                    <td>{ASSINADO_CEF(df_repasse_, "AGL27") - GARANTIA_REC(df_repasse_, "AGL27")}</td>                                                                                                  
                    </tr>
                    <tr>
                    <th scope="row">FSA</th>
                    <td>FSA005</td>
                    <td>12.991.859,46</td>
                    <td>0</td>
                    <td>144</td>
                    <td>{Num_Vendas(df_vendas_,"FSA005")}</td> 
                    <td>{ASSINADO_CEF(df_repasse_, "FSA005")}</td>   
                    <td>{GARANTIA_REC(df_repasse_, "FSA005")}</td>    
                    <td>{144 - Num_Vendas(df_vendas_,"FSA005")}</td>   
                    <td>{(Num_Vendas(df_vendas_,"FSA005")) - ASSINADO_CEF(df_repasse_, "FSA005")}</td>   
                    <td>{ASSINADO_CEF(df_repasse_, "FSA005") - GARANTIA_REC(df_repasse_, "FSA005")}</td>                                                                                                   
                    </tr>
                    <tr> 
                    <th scope="row">FSA</th>
                    <td>FSA006</td>
                    <td>16.244.437,85</td>
                    <td>0</td>
                    <td>144</td>
                    <td>{Num_Vendas(df_vendas_,"FSA006")}</td> 
                    <td>{ASSINADO_CEF(df_repasse_, "FSA006")}</td>   
                    <td>{GARANTIA_REC(df_repasse_, "FSA006")}</td>    
                    <td>{144 - Num_Vendas(df_vendas_,"FSA006")}</td>    
                    <td>{(Num_Vendas(df_vendas_,"FSA006")) - ASSINADO_CEF(df_repasse_, "FSA006")}</td>  
                    <td>{ASSINADO_CEF(df_repasse_, "FSA006") - GARANTIA_REC(df_repasse_, "FSA006")}</td>                                                                                                   
                    </tr>
                    <tr>
                    <th scope="row">FSA</th>
                    <td>FSA003</td>
                    <td>19.095.874,44</td>
                    <td>0</td>
                    <td>59</td>
                    <td>{Num_Vendas(df_vendas_,"FSA003")}</td> 
                    <td>{ASSINADO_CEF(df_repasse_, "FSA003")}</td>   
                    <td>{GARANTIA_REC(df_repasse_, "FSA003")}</td>    
                    <td>{59 - Num_Vendas(df_vendas_,"FSA003")}</td>    
                    <td>{(Num_Vendas(df_vendas_,"FSA003")) - ASSINADO_CEF(df_repasse_, "FSA003")}</td>     
                    <td>{ASSINADO_CEF(df_repasse_, "FSA003") - GARANTIA_REC(df_repasse_, "FSA003")}</td>                                                                                                
                    </tr>
                    <tr>   
                    <th scope="row">DF</th>
                    <td>DF001</td>
                    <td>32.178.064,12</td>
                    <td>0</td>
                    <td>58</td>
                    <td>{Num_Vendas(df_vendas_,"DF001")}</td> 
                    <td>{ASSINADO_CEF(df_repasse_, "DF001")}</td>   
                    <td>{GARANTIA_REC(df_repasse_, "DF001")}</td>    
                    <td>{58 - Num_Vendas(df_vendas_,"DF001")}</td>    
                    <td>{(Num_Vendas(df_vendas_,"DF001")) - ASSINADO_CEF(df_repasse_, "DF001")}</td>   
                    <td>{ASSINADO_CEF(df_repasse_, "DF001") - GARANTIA_REC(df_repasse_, "DF001")}</td>                                                                                                  
                    </tr>
                    <tr>                                                          
                </tbody>
            </table>
          </html> 

          """
  return nome



def REPASSE_TRAT_MES(col, lts_empreendimento):   
    TESTE = df_repasse_.copy()
    VDS = df_vendas_.copy()
    VDS = VDS.loc[:,["idreserva","data","valor_contrato","imobiliaria","corretor","midia"]]
    VDS.rename(columns={"idreserva":"reserva"}, inplace=True)
    TESTE[col] = pd.to_datetime(TESTE[col], errors="coerce")
    TESTE.loc[(TESTE[col] >="2022-01-01") & (TESTE[col] <="2022-02-01"),"DT_REPASSE_TRAT"] = "01_Jan_22"
    TESTE.loc[(TESTE[col] >="2022-02-01") & (TESTE[col] <="2022-03-01"),"DT_REPASSE_TRAT"] = "02_Fev_22"
    TESTE.loc[(TESTE[col] >="2022-03-01") & (TESTE[col] <="2022-04-01"),"DT_REPASSE_TRAT"] = "03_Mar_22"
    TESTE.loc[(TESTE[col] >="2022-04-01") & (TESTE[col] <="2022-05-01"),"DT_REPASSE_TRAT"] = "04_Abr_22"
    TESTE.loc[(TESTE[col] >="2022-05-01") & (TESTE[col] <="2022-06-01"),"DT_REPASSE_TRAT"] = "05_Mai_22"
    TESTE.loc[(TESTE[col] >="2022-06-01") & (TESTE[col] <="2022-07-01"),"DT_REPASSE_TRAT"] = "06_Jun_22"
    TESTE.loc[(TESTE[col] >="2022-07-01") & (TESTE[col] <="2022-08-01"),"DT_REPASSE_TRAT"] = "07_Jul_22"
    TESTE.loc[(TESTE[col] >="2022-08-01") & (TESTE[col] <="2022-09-01"),"DT_REPASSE_TRAT"] = "08_Ago_22"
    TESTE.loc[(TESTE[col] >="2022-09-01") & (TESTE[col] <="2022-10-01"),"DT_REPASSE_TRAT"] = "09_Set_22"
    TESTE.loc[(TESTE[col] >="2022-10-01") & (TESTE[col] <="2022-11-01"),"DT_REPASSE_TRAT"] = "10_Out_22"
    TESTE.loc[(TESTE[col] >="2022-11-01") & (TESTE[col] <="2022-12-01"),"DT_REPASSE_TRAT"] = "11_Nov_22"
    TESTE.loc[(TESTE[col] >="2022-12-01") & (TESTE[col] <="2023-01-01"),"DT_REPASSE_TRAT"] = "12_Dez_22"
    TESTE.loc[(TESTE[col] < "2022-01-01"),"DT_REPASSE_TRAT"] = "< 2022"
    TESTE[col].fillna("00/00/0000", inplace=True)
    TESTE["DT_REPASSE_TRAT"].fillna(0, inplace=True)
    TESTE["count"] = 1
    TESTE = pd.merge(TESTE, rep_status, on=["situacao"], how="left")
    lst_emp = []
    for i in TESTE["empreendimento"]:
        try:
            lst_emp.append(DIC_EMPREENDIMENTOS[i])
        except:
            lst_emp.append("100%")
    TESTE["EMPREEND"] = lst_emp
    TESTE = TESTE.loc[TESTE["EMPREEND"].isin(lts_empreendimento)]
    TESTE = TESTE.loc[TESTE["DT_REPASSE_TRAT"] !=0]
    TESTE = pd.merge(TESTE, VDS, on=["reserva"], how="left")
    TESTE = TESTE.loc[TESTE["STATUS_REGISTRO"] !="Distrato"]
    return TESTE

def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


EMP = ["AGL23", "AGL25", "AGL27", "AGL28", "FSA005", "FSA006", "DF001", "FSA003","FSA007"]


try:
        st.subheader("Resumo da Carteira::")
        st.markdown(card_universal1("carlos"),unsafe_allow_html=True)
        st.subheader("Detalhado::")
        lts_valor1 = []
        lts_valor2 = []
        df_vendas_ = df_vendas_
        df_repasse_ = REPASSE_TRAT_MES("data_venda",EMP)
        df_repasse_ = df_repasse_.loc[:,["EMPREEND","bloco","unidade","cliente","reserva","idcontrato","imobiliaria","corretor","valor_contrato","valor_previsto","data_venda", 
                    "data_assinatura_de_contrato","data_registro","data_alteracao_status","STATUS_REPASSE","STATUS_REGISTRO","midia"]]

        df_repasse_.rename(columns={"data_assinatura_de_contrato":"Dt_Assinatura_CEF"}, inplace=True)
        for i in df_repasse_["valor_contrato"]:
                lts_valor1.append(f"R$ {conversor_moeda_brasil(i)}")
        df_repasse_["valor_contrato"] = lts_valor1 

        for a in df_repasse_["valor_previsto"]:
                lts_valor2.append(f"R$ {conversor_moeda_brasil(a)}")
        df_repasse_["valor_previsto"] = lts_valor2
        df_repasse_["data_venda"] = pd.to_datetime(df_repasse_["data_venda"], errors="coerce")
        df_repasse_.fillna(0,inplace=True)
        df_repasse_["RESUMO_STATUS"] = 0
        for i in df_repasse_["STATUS_REGISTRO"]:
                try:
                        df_repasse_.loc[df_repasse_["STATUS_REGISTRO"] == i,"RESUMO_STATUS"] = DIC_CREDITO[i]
                except:
                        df_repasse_.loc[df_repasse_["STATUS_REGISTRO"] == i,"RESUMO_STATUS"] = "Sem Status"
except:
        st.error("<--------------- FAVOR BAIXAR O BANCO DE DADOS!")


                
                
df_repasse_["MIDIA_TRAT"] = 0
for i in df_repasse_["midia"]:
        try:
                df_repasse_.loc[df_repasse_["midia"] == i,"MIDIA_TRAT"] = DIC_MIDIA[i]
        except:
                df_repasse_.loc[df_repasse_["midia"] == i,"MIDIA_TRAT"] = "OUTROS"               
                
                
                
                
                
df_repasse_.drop(columns=["midia"], inplace=True)               
                
df_repasse_["data_alteracao_status"] = pd.to_datetime(df_repasse_["data_alteracao_status"], errors="coerce")
df_repasse_["TM_ALTERAÇ"] = (Data_Hoje - df_repasse_["data_alteracao_status"]).dt.days
                
                
df_repasse_["Dt_Assinatura_CEF"] = pd.to_datetime(df_repasse_["Dt_Assinatura_CEF"], errors="coerce")
df_repasse_["Repasse_MM/AA"] = df_repasse_["Dt_Assinatura_CEF"].dt.strftime('%m-%Y')  
df_repasse_["Venda_MM/AA"] = df_repasse_["data_venda"].dt.strftime('%m-%Y')
        
df_repasse_["Dt_Assinatura_CEF"] = df_repasse_["Dt_Assinatura_CEF"].dt.strftime('%d-%m-%Y')
df_repasse_["data_venda"] = df_repasse_["data_venda"].dt.strftime('%d-%m-%Y')




# CLASSIFICAR IMOBS ==============================================================================================================
df_repasse_.loc[df_repasse_["imobiliaria"] == "7LM Formosa","imobiliaria"] = "Equipe Própria | FSA"
df_repasse_.loc[df_repasse_["imobiliaria"] == "Formosa","imobiliaria"] = "Equipe Própria | FSA"
df_repasse_.loc[df_repasse_["imobiliaria"] == "Águas Lindas","imobiliaria"] = "Equipe Própria | AGL"
df_repasse_.loc[df_repasse_["imobiliaria"] == "Aguas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
df_repasse_.loc[df_repasse_["imobiliaria"] == "Aguas Lindas 2","imobiliaria"] = "Equipe Própria | AGL"
df_repasse_.loc[df_repasse_["imobiliaria"] == "Águas Lindas 1","imobiliaria"] = "Equipe Própria | AGL"
df_repasse_.loc[df_repasse_["imobiliaria"] == "Águas Lindas 2","imobiliaria"] = "Equipe Própria | AGL"
df_repasse_.loc[df_repasse_["imobiliaria"] == "NovKa Formosa","imobiliaria"] = "NOVKAL"
df_repasse_.loc[df_repasse_["imobiliaria"] == "NOVKA DF","imobiliaria"] = "NOVKA"
df_repasse_.loc[df_repasse_["imobiliaria"] == "Novka Pilotis","imobiliaria"] = "NOVKA"
df_repasse_.loc[df_repasse_["imobiliaria"] == "FSA - 7lm/Novka","imobiliaria"] = "NOVKA"
df_repasse_.loc[df_repasse_["imobiliaria"] == "NOVKAL","imobiliaria"] = "NOVKA"
df_repasse_.loc[df_repasse_["imobiliaria"] == "URBANA IMOVEIS","imobiliaria"] = "NOVKA"
df_repasse_.loc[df_repasse_["imobiliaria"] == "BR HOUSE INTELIGENCIA IMOBILIARIA","imobiliaria"] = "NOVKA"        
# CLASSIFICAR IMOBS ==============================================================================================================

dt_ = ["01-2022", "02-2022", "03-2022", "04-2022", "05-2022", "06-2022", "07-2022", "08-2022", "09-2022", "10-2022", "11-2022", "12-2022"]

#RESUMO_BASE = df_repasse_.loc[df_repasse_["Dt_Assinatura_CEF"] >="2022-01-01"]
RESUMO_BASE = df_repasse_.loc[df_repasse_["Repasse_MM/AA"].isin(["01-2022", "02-2022", "03-2022", "04-2022", "05-2022", "06-2022", "07-2022", "08-2022", "09-2022","10-2022", "11-2022", "12-2022"])]
RESUMO_BASE = RESUMO_BASE.loc[RESUMO_BASE["STATUS_REPASSE"].isin(["Contrato Assinado CEF"])]
RESUMO_BASE["count"] = 1

RESUMO_BASE1 = pd.DataFrame(RESUMO_BASE.groupby(["Repasse_MM/AA"])["count"].sum()).reset_index()
RESUMO_BASE2 = pd.DataFrame(RESUMO_BASE.groupby(["Repasse_MM/AA","EMPREEND"])["count"].sum()).reset_index()

st.title("# REPASSE | TOTAL ")
#AgGrid(RESUMO_BASE1, theme="blue", height=300) 

#grid_dataframe_top()
#AgGrid(RESUMO_BASE2, theme="blue", height=300) 

# IMPRESSÃO DO DATAFRAME 
gb = GridOptionsBuilder.from_dataframe(RESUMO_BASE1)
gb.configure_default_column(groupable=True, enableValue=True, enableRowGroup=True,aggFunc="sum",editable=False)
gb.update_mode=GridUpdateMode.MANUAL
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
gb.configure_side_bar()
gridoptions = gb.build()
response = AgGrid(
    RESUMO_BASE1,
    height=300,
    gridOptions=gridoptions,
    enable_enterprise_modules=True,
    header_checkbox_selection_filtered_only=True,
    use_checkbox=True, theme="blue")

st.title("# REPASSE | EMPREEND ")
gb = GridOptionsBuilder.from_dataframe(RESUMO_BASE2)
gb.configure_default_column(groupable=True, enableValue=True, enableRowGroup=True,aggFunc="sum",editable=False)
gb.update_mode=GridUpdateMode.MANUAL
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
gb.configure_side_bar()
gridoptions = gb.build()
response = AgGrid(
    RESUMO_BASE2,
    height=300,
    gridOptions=gridoptions,
    enable_enterprise_modules=True,
    header_checkbox_selection_filtered_only=True,
    use_checkbox=True, theme="blue")






df_tm = df_repasse_.copy()
df_tm = df_tm.loc[df_tm["RESUMO_STATUS"].isin(["MP_ASSINATURA", "MP_REGISTRO"])]
df_tm = df_tm.loc[df_tm["TM_ALTERAÇ"] >30]
df_tm_emp = pd.DataFrame(df_tm.groupby(["EMPREEND","RESUMO_STATUS"])["TM_ALTERAÇ"].count()).reset_index()
df_tm_corretor = pd.DataFrame(df_tm.groupby(["EMPREEND","RESUMO_STATUS","corretor"])["TM_ALTERAÇ"].count()).reset_index()
df_tm = pd.DataFrame(df_tm.groupby(["EMPREEND","RESUMO_STATUS","cliente","corretor"])["TM_ALTERAÇ"].sum()).reset_index()


st.title("Monitoramento de Venda > 30dias")
AGL23_qtd_emp = df_tm_emp.loc[df_tm_emp["EMPREEND"].isin(["AGL23"])]["TM_ALTERAÇ"].sum()
AGL25_qtd_emp = df_tm_emp.loc[df_tm_emp["EMPREEND"].isin(["AGL25"])]["TM_ALTERAÇ"].sum()
AGL27_qtd_emp = df_tm_emp.loc[df_tm_emp["EMPREEND"].isin(["AGL27"])]["TM_ALTERAÇ"].sum()
AGL28_qtd_emp = df_tm_emp.loc[df_tm_emp["EMPREEND"].isin(["AGL28"])]["TM_ALTERAÇ"].sum()
FSA005_qtd_emp = df_tm_emp.loc[df_tm_emp["EMPREEND"].isin(["FSA005"])]["TM_ALTERAÇ"].sum()
FSA006_qtd_emp = df_tm_emp.loc[df_tm_emp["EMPREEND"].isin(["FSA006"])]["TM_ALTERAÇ"].sum()



c1, c2, c3, c4, c5, c6, c7 = st.columns((2,2,2,2,2,2,2))
c1.metric("AGL23", value=AGL23_qtd_emp)
c2.metric("AGL25", value=AGL25_qtd_emp)
c3.metric("AGL27", value=AGL27_qtd_emp)
c4.metric("AGL28", value=AGL28_qtd_emp)
c5.metric("FSA005", value=FSA005_qtd_emp)
c6.metric("FSA006", value=FSA006_qtd_emp)

st.subheader("Por Empreendimento")
df_tm_emp.rename(columns={"TM_ALTERAÇ":"Quantidade"},inplace=True)
grid_dataframe_top(df_tm_emp,200)

st.subheader("Por Corretor")
df_tm_corretor.rename(columns={"TM_ALTERAÇ":"Quantidade"},inplace=True)
grid_dataframe_top(df_tm_corretor,300)

st.subheader("Tempo Médio | Última Alteração")
grid_dataframe_top(df_tm,500)






st.subheader("Base Geral")
gb = GridOptionsBuilder.from_dataframe(df_repasse_)
gb.configure_default_column(groupable=True, enableValue=True, enableRowGroup=True,aggFunc="sum",editable=False)
gb.update_mode=GridUpdateMode.MANUAL
gb.configure_selection(selection_mode="multiple", use_checkbox=True)
gb.configure_side_bar()
gridoptions = gb.build()
response = AgGrid(
    df_repasse_,
    height=800,
    gridOptions=gridoptions,
    enable_enterprise_modules=True,
    header_checkbox_selection_filtered_only=True,
    use_checkbox=True, theme="blue")






csv = convert_df(df_repasse_)
st.download_button(
label="Download Arquivo em CSV",
data=csv,
file_name='Credito_7lm.csv',
mime='text/csv',) 


