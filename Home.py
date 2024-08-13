import streamlit as st
from st_files_connection import FilesConnection

import pandas as pd
import numpy as np

from functions.bene_sem_id import bene_sem_id_func
from functions.upper_outliers_nivel_provedor import proc_preco_nivel_provedor_func, upper_outliers_nivel_provedor_func
from functions.proc_diferentes import proc_diferentes_func
from functions.sem_tuss import sem_tuss_func
from functions.proc_duplicados import proc_duplicados_func
from functions.prest_sem_id import prest_sem_id_func
from functions.proc_sex import proc_male_func, proc_fem_func
from functions.sessoes_outliers import psico_func
from functions.sessoes_outliers import fono_func
from functions.proc_duplicados_por_provedor import proc_duplicados_por_provedor_func
from functions.quebra_recibo import quebra_recibo_jan_23_func, quebra_recibo_fev_23_func, quebra_recibo_mar_23_func, quebra_recibo_abr_23_func, quebra_recibo_mai_23_func, quebra_recibo_jun_23_func, quebra_recibo_jul_23_func, quebra_recibo_ago_23_func, quebra_recibo_set_23_func, quebra_recibo_out_23_func, quebra_recibo_nov_23_func, quebra_recibo_dez_23_func, quebra_recibo_jan_22_func, quebra_recibo_fev_22_func, quebra_recibo_mar_22_func, quebra_recibo_abr_22_func, quebra_recibo_mai_22_func, quebra_recibo_jun_22_func, quebra_recibo_jul_22_func, quebra_recibo_ago_22_func, quebra_recibo_set_22_func, quebra_recibo_out_22_func, quebra_recibo_nov_22_func, quebra_recibo_dez_22_func

import time
from PIL import Image

import datetime
from datetime import date

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

# import zipfile

st.set_page_config(
     page_title="Auditoria Beta",
     page_icon="🏗️",
 )

conn = st.connection('s3', type=FilesConnection)

@st.cache_data(ttl=3600, show_spinner="1/3 - Carregando base completa (142 MB)...") #Ler base com a classificação TUSS da ANS
def get_data_1():
    return conn.read("df-for-mvps/17/158/jun-2024/df_append_all.csv", input_format="csv")
#     return pd.read_csv("../dados/df_append_all.csv")

df_append_all = get_data_1()


@st.cache_data(ttl=3600, show_spinner="2/3 - Analisando procedimentos...") #Ler base com a classificação TUSS da ANS
def get_data_3():
    return conn.read("df-for-mvps/17/158/jun-2024/proc_describe.csv", input_format="csv")
#     return pd.read_csv('/Users/pedro/Documents/Blue/ds/df_sulamerica_describe.csv')
    
proc_describe = get_data_3()

# df_append_all = conn.read("df-for-mvps/17/158/jun-2024/df_append_all.csv", input_format="csv")
# df_append = conn.read("df-for-mvps/17/158/jun-2024/df_append.csv", input_format="csv")
# proc_describe = conn.read("df-for-mvps/17/158/jun-2024/proc_describe.csv", input_format="csv")

proc_describe = proc_describe.iloc[1:]

# zf = zipfile.ZipFile('cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv.zip') 

@st.cache_data(ttl=3600, show_spinner="3/3 - Carregando códigos TUSS...") #Ler base com a classificação TUSS da ANS
def get_data_4():
    return conn.read("df-for-mvps/tuss/cod_tuss_subgrupo_classe_2022_ponto_e_virgula 2.csv", input_format="csv")
	# return pd.read_csv(zf.open('cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv'))

df_subgrupo = get_data_4()

df_append_all = df_append_all.drop(df_append_all[df_append_all.cod_tuss == "0000MS2B"].index)
df_append_all["cod_tuss"] = df_append_all["cod_tuss"].astype(int).astype(str)

df_subgrupo["cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)
proc_describe["cod_tuss"] = proc_describe["cod_tuss"].astype(int).astype(str)

proc_describe["outlier_range"] = proc_describe["outlier_range"].round(decimals = 0)

df_append_all["dt_utilizacao"] = pd.to_datetime(df_append_all['dt_utilizacao'], dayfirst=False, errors='coerce')

df_append_all["mes_utilizacao"] = df_append_all["mes_utilizacao"].fillna(0).astype(int)
df_append_all["ano_utilizacao"] = df_append_all["ano_utilizacao"].fillna(0).astype(int)

df_append_all["valor_pago"] = df_append_all["valor_pago"].astype(float)

df_append_all['mes_utilizacao'] = df_append_all['mes_utilizacao'].fillna(0).astype(int).astype(str)
df_append_all['ano_utilizacao'] = df_append_all['ano_utilizacao'].fillna(0).astype(int)

df_append_all['operadora'] = df_append_all['operadora'].astype(str)

df_append_all['id_pessoa'] = df_append_all['id_pessoa'].astype(int).astype(str)


st.markdown("# 🏢 XP Investimentos")
st.markdown("### Classificação de gastos com o plano de saúde - Beta 🏗️")

st.markdown("A seguir, você encontrará alertas para possíveis inconsistências com o uso do plano de saúde. Na aba a direita, você conseguirá se aprofundar em cada um dos tópicos de atenção indicados abaixo. **Aproveite as classificações para fazer uma saúde diferente!**")

# Configurando sidebar

st.sidebar.markdown("# Classificação de gastos ")

# filter_date = st.sidebar.selectbox(label='Selecione o período', options=['2023', '2022', 'Período de reajuste', 'Últimos 12 meses', 'Mar/2023', 'Fev/2023', 'Jan/2023', 'Dez/2022', 'Nov/2022', 'Out/2022', 'Set/2022', 'Ago/2022', 'Jul/2022', 'Jun/2022', 'Mai/2022', 'Abr/2022', 'Mar/2022', 'Fev/2022', 'Jan/2022'])
filter_date = st.sidebar.selectbox(label='Selecione o período', options=['2024', '2023'])

if filter_date == 'Mar/2023':
    min_date = pd.to_datetime('2023-03-01')
    max_date = pd.to_datetime('2023-03-30')
elif filter_date == 'Fev/2023':
    min_date = pd.to_datetime('2023-02-01')
    max_date = pd.to_datetime('2023-02-28')
elif filter_date == 'Jan/2023':
    min_date = pd.to_datetime('2023-01-01')
    max_date = pd.to_datetime('2023-01-31')
elif filter_date == 'Dez/2022':
    min_date = pd.to_datetime('2022-12-01')
    max_date = pd.to_datetime('2022-12-31')
elif filter_date == 'Nov/2022':
    min_date = pd.to_datetime('2022-11-01', format='%Y-%m-%d')
    max_date = pd.to_datetime('2022-11-30', format='%Y-%m-%d')
elif filter_date == 'Out/2022':
    min_date = pd.to_datetime('2022-10-01')
    max_date = pd.to_datetime('2022-10-31')
elif filter_date == 'Set/2022':
    min_date = pd.to_datetime('2022-09-01')
    max_date = pd.to_datetime('2022-09-30')
elif filter_date == 'Ago/2022':
    min_date = pd.to_datetime('2022-08-01')
    max_date = pd.to_datetime('2022-08-31')
elif filter_date == 'Jul/2022':
    min_date = pd.to_datetime('2022-07-01')
    max_date = pd.to_datetime('2022-07-30')
elif filter_date == 'Jun/2022':
    min_date = pd.to_datetime('2022-06-01')
    max_date = pd.to_datetime('2022-06-30')
elif filter_date == 'Mai/2022':
    min_date = pd.to_datetime('2022-05-01')
    max_date = pd.to_datetime('2022-05-31')
elif filter_date == 'Abr/2022':
    min_date = pd.to_datetime('2022-04-01')
    max_date = pd.to_datetime('2022-04-30')
elif filter_date == 'Mar/2022':
    min_date = pd.to_datetime('2022-03-01')
    max_date = pd.to_datetime('2022-03-31')
elif filter_date == 'Fev/2022':
    min_date = pd.to_datetime('2022-02-01')
    max_date = pd.to_datetime('2022-02-28')
elif filter_date == 'Jan/2022':
    min_date = pd.to_datetime('2022-01-01')
    max_date = pd.to_datetime('2022-01-31')
elif filter_date == '2024':
    min_date = pd.to_datetime('2024-01-01')
    max_date = pd.to_datetime('2024-12-31')
elif filter_date == '2023':
    min_date = pd.to_datetime('2023-01-01')
    max_date = pd.to_datetime('2023-12-31')
elif filter_date == 'Período de reajuste':
    min_date = pd.to_datetime('2022-10-01')
    max_date = pd.to_datetime('2023-03-31') # Reajuste vai até outubro 2023-10-31
elif filter_date == 'Últimos 12 meses':
    min_date = pd.to_datetime('2022-04-01')
    max_date = pd.to_datetime('2023-03-31')
else:
    min_date = pd.to_datetime('2018-12-01')
    max_date = pd.to_datetime('2022-12-31')


insurance_option = df_append_all['operadora'].unique().tolist()

if len(insurance_option) > 1:
    filter_insurance = st.sidebar.selectbox(label='Selecione a Operadora', options=['Todas', insurance_option[0], insurance_option[1]])
else:
    filter_insurance = st.sidebar.selectbox(label='Selecione a Operadora', options=[insurance_option[0]])

image = conn.open("df-for-mvps/17/img/logo.png", input_format="png")
image = Image.open(image)
st.sidebar.image(image, width=70)

image = conn.open("df-for-mvps/17/158/img/logo.png", input_format="png")
image = Image.open(image)
st.sidebar.image(image, width=50)

st.sidebar.write('\n\n')
st.sidebar.write('\n\n')
st.sidebar.write('\n\n')
st.sidebar.write('\n\n')
st.sidebar.write('\n\n')
# st.sidebar.write('\n\n')
# st.sidebar.write('\n\n')
# st.sidebar.write('\n\n')
# st.sidebar.write('\n\n')
# st.sidebar.write('\n\n')
# st.sidebar.write('\n\n')


# st.sidebar.caption('Construído com 🧠 por Blue AI')

# 1 Beneficiário sem identificação
bene_sem_id = bene_sem_id_func(df_append_all, filter_insurance, min_date, max_date)


# 2 Procedimentos diferentes ou com descrição diferente da própria base, para o mesmo código
proc_diferentes = proc_diferentes_func(df_append_all, filter_insurance)


# 3 Códigos sem padrão TUSS ou sem identificação
sem_tuss = sem_tuss_func(df_append_all, filter_insurance, max_date, min_date)


# 4 Beneficiários com os mesmos procedimentos, para o mesmo prestador e no mesmo dia
proc_duplicados = proc_duplicados_func(df_append_all, proc_describe, filter_insurance, max_date, min_date)


# 5 Analisando o padrão do preço de procedimentos
proc_preco_nivel_provedor = proc_preco_nivel_provedor_func()
upper_outliers_nivel_provedor = upper_outliers_nivel_provedor_func(df_append_all.dropna(), df_subgrupo, proc_preco_nivel_provedor, filter_insurance, max_date, min_date)


# 6 Prestador sem identificação
prest_sem_id = prest_sem_id_func(df_append_all, filter_insurance, max_date, min_date)


# 7 Beneficiários que realizaram procedimentos indevidos para o seu sexo, no último ano
df_fem = proc_fem_func(df_append_all.dropna())
df_fem = df_fem.drop(df_fem[df_fem.id_pessoa == "802384001942"].index)
df_fem = df_fem.drop(df_fem[df_fem.id_pessoa == "801631264067"].index)
df_male = proc_male_func(df_append_all.dropna())


# 8 Quebra de recibo

# Criando média do valor pago por cada sinistro para identificar quebra do recibo dividia em 2 meses consecutivos
media_para_quebra = proc_preco_nivel_provedor[['cod_tuss', 'provedor', 'preço_limite']]
media_para_quebra = media_para_quebra.rename(columns={"preço_limite": "valor_medio"})

media_para_quebra.loc[:, "cod_tuss"] = media_para_quebra["cod_tuss"].astype(int).astype(str)

df_append_all_amb = df_append_all[df_append_all['cod_tuss'] == '10101012']
df_append_all_amb = df_append_all_amb.drop(df_append_all_amb[df_append_all_amb.id_pessoa == "8888848033281011"].index)


df_append_all_amb.loc[:, "mes_utilizacao"] = df_append_all_amb["mes_utilizacao"].astype(int).astype(str)

# Iniciando classificação de quebra de recibo para Janeiro, com primeira parcela em Dezembro do ano anterior
if filter_date == 'Jan/2023':
    quebra_recibos_jan_23 = quebra_recibo_jan_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jan_23['valor_pago'] = quebra_recibos_jan_23['valor_pago'].astype(float)
elif filter_date == 'Fev/2023':
    quebra_recibos_fev_23 = quebra_recibo_fev_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_fev_23['valor_pago'] = quebra_recibos_fev_23['valor_pago'].astype(float)
elif filter_date == 'Mar/2023':
    quebra_recibos_mar_23 = quebra_recibo_mar_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mar_23['valor_pago'] = quebra_recibos_mar_23['valor_pago'].astype(float)
elif filter_date == 'Jan/2022':
    quebra_recibos_jan = quebra_recibo_jan_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jan['valor_pago'] = quebra_recibos_jan['valor_pago'].astype(float)
elif filter_date == 'Fev/2022':
    quebra_recibos_fev = quebra_recibo_fev_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_fev['valor_pago'] = quebra_recibos_fev['valor_pago'].astype(float)
elif filter_date == 'Mar/2022':
    quebra_recibos_mar = quebra_recibo_mar_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mar['valor_pago'] = quebra_recibos_mar['valor_pago'].astype(float)
elif filter_date == 'Abr/2022':
    quebra_recibos_abr = quebra_recibo_abr_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_abr['valor_pago'] = quebra_recibos_abr['valor_pago'].astype(float)
elif filter_date == 'Mai/2022':
    quebra_recibos_mai = quebra_recibo_mai_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mai['valor_pago'] = quebra_recibos_mai['valor_pago'].astype(float)
elif filter_date == 'Jun/2022':
    quebra_recibos_jun = quebra_recibo_jun_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jun['valor_pago'] = quebra_recibos_jun['valor_pago'].astype(float)
elif filter_date == 'Jul/2022':
    quebra_recibos_jul = quebra_recibo_jul_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jul['valor_pago'] = quebra_recibos_jul['valor_pago'].astype(float)
elif filter_date == 'Ago/2022':
    quebra_recibos_ago = quebra_recibo_ago_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_ago['valor_pago'] = quebra_recibos_ago['valor_pago'].astype(float)
elif filter_date == 'Set/2022':
    quebra_recibos_set = quebra_recibo_set_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_set['valor_pago'] = quebra_recibos_set['valor_pago'].astype(float)
elif filter_date == 'Out/2022':
    quebra_recibos_out = quebra_recibo_out_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_out['valor_pago'] = quebra_recibos_out['valor_pago'].astype(float)
elif filter_date == 'Nov/2022':
    quebra_recibos_nov = quebra_recibo_nov_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_nov['valor_pago'] = quebra_recibos_nov['valor_pago'].astype(float)
elif filter_date == 'Dez/2022':
    quebra_recibos_dez = quebra_recibo_dez_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_dez['valor_pago'] = quebra_recibos_dez['valor_pago'].astype(float)
elif filter_date == '2024':
    quebra_recibos_jan_23 = quebra_recibo_jan_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_fev_23 = quebra_recibo_fev_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mar_23 = quebra_recibo_mar_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_abr_23 = quebra_recibo_abr_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mai_23 = quebra_recibo_mai_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jun_23 = quebra_recibo_jun_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jul_23 = quebra_recibo_jul_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_ago_23 = quebra_recibo_ago_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_set_23 = quebra_recibo_set_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_out_23 = quebra_recibo_out_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_nov_23 = quebra_recibo_nov_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_dez_23 = quebra_recibo_dez_23_func(media_para_quebra, df_append_all_amb)

    quebra_recibos_jan_23['valor_pago'] = quebra_recibos_jan_23['valor_pago'].astype(float)
    quebra_recibos_fev_23['valor_pago'] = quebra_recibos_fev_23['valor_pago'].astype(float)
    quebra_recibos_mar_23['valor_pago'] = quebra_recibos_mar_23['valor_pago'].astype(float)
    quebra_recibos_abr_23['valor_pago'] = quebra_recibos_abr_23['valor_pago'].astype(float)
    quebra_recibos_mai_23['valor_pago'] = quebra_recibos_mai_23['valor_pago'].astype(float)
    quebra_recibos_jun_23['valor_pago'] = quebra_recibos_jun_23['valor_pago'].astype(float)
    quebra_recibos_jul_23['valor_pago'] = quebra_recibos_jul_23['valor_pago'].astype(float)
    quebra_recibos_ago_23['valor_pago'] = quebra_recibos_ago_23['valor_pago'].astype(float)
    quebra_recibos_set_23['valor_pago'] = quebra_recibos_set_23['valor_pago'].astype(float)
    quebra_recibos_out_23['valor_pago'] = quebra_recibos_out_23['valor_pago'].astype(float)
    quebra_recibos_nov_23['valor_pago'] = quebra_recibos_nov_23['valor_pago'].astype(float)
    quebra_recibos_dez_23['valor_pago'] = quebra_recibos_dez_23['valor_pago'].astype(float)
elif filter_date == 'Período de reajuste':
    quebra_recibos_jan_23 = quebra_recibo_jan_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_fev_23 = quebra_recibo_fev_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mar_23 = quebra_recibo_mar_23_func(media_para_quebra, df_append_all_amb)

    quebra_recibos_out = quebra_recibo_out_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_nov = quebra_recibo_nov_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_dez = quebra_recibo_dez_22_func(media_para_quebra, df_append_all_amb)

    quebra_recibos_jan_23['valor_pago'] = quebra_recibos_jan_23['valor_pago'].astype(float)
    quebra_recibos_fev_23['valor_pago'] = quebra_recibos_fev_23['valor_pago'].astype(float)
    quebra_recibos_mar_23['valor_pago'] = quebra_recibos_mar_23['valor_pago'].astype(float)

    quebra_recibos_out['valor_pago'] = quebra_recibos_out['valor_pago'].astype(float)
    quebra_recibos_nov['valor_pago'] = quebra_recibos_nov['valor_pago'].astype(float)
    quebra_recibos_dez['valor_pago'] = quebra_recibos_dez['valor_pago'].astype(float)
elif filter_date == 'Últimos 12 meses':
    quebra_recibos_jan_23 = quebra_recibo_jan_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_fev_23 = quebra_recibo_fev_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mar_23 = quebra_recibo_mar_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_abr_23 = quebra_recibo_abr_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mai_23 = quebra_recibo_mai_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jun_23 = quebra_recibo_jun_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jul_23 = quebra_recibo_jul_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_ago_23 = quebra_recibo_ago_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_set_23 = quebra_recibo_set_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_out_23 = quebra_recibo_out_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_nov_23 = quebra_recibo_nov_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_dez_23 = quebra_recibo_dez_23_func(media_para_quebra, df_append_all_amb)

    quebra_recibos_abr = quebra_recibo_abr_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mai = quebra_recibo_mai_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jun = quebra_recibo_jun_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jul = quebra_recibo_jul_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_ago = quebra_recibo_ago_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_set = quebra_recibo_set_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_out = quebra_recibo_out_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_nov = quebra_recibo_nov_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_dez = quebra_recibo_dez_22_func(media_para_quebra, df_append_all_amb)

    quebra_recibos_jan_23['valor_pago'] = quebra_recibos_jan_23['valor_pago'].astype(float)
    quebra_recibos_fev_23['valor_pago'] = quebra_recibos_fev_23['valor_pago'].astype(float)
    quebra_recibos_mar_23['valor_pago'] = quebra_recibos_mar_23['valor_pago'].astype(float)
    quebra_recibos_abr_23['valor_pago'] = quebra_recibos_abr_23['valor_pago'].astype(float)
    quebra_recibos_mai_23['valor_pago'] = quebra_recibos_mai_23['valor_pago'].astype(float)
    quebra_recibos_jun_23['valor_pago'] = quebra_recibos_jun_23['valor_pago'].astype(float)
    quebra_recibos_jul_23['valor_pago'] = quebra_recibos_jul_23['valor_pago'].astype(float)
    quebra_recibos_ago_23['valor_pago'] = quebra_recibos_ago_23['valor_pago'].astype(float)
    quebra_recibos_set_23['valor_pago'] = quebra_recibos_set_23['valor_pago'].astype(float)
    quebra_recibos_out_23['valor_pago'] = quebra_recibos_out_23['valor_pago'].astype(float)
    quebra_recibos_nov_23['valor_pago'] = quebra_recibos_nov_23['valor_pago'].astype(float)
    quebra_recibos_dez_23['valor_pago'] = quebra_recibos_dez_23['valor_pago'].astype(float)

    quebra_recibos_abr['valor_pago'] = quebra_recibos_abr['valor_pago'].astype(float)
    quebra_recibos_mai['valor_pago'] = quebra_recibos_mai['valor_pago'].astype(float)
    quebra_recibos_jun['valor_pago'] = quebra_recibos_jun['valor_pago'].astype(float)
    quebra_recibos_jul['valor_pago'] = quebra_recibos_jul['valor_pago'].astype(float)
    quebra_recibos_ago['valor_pago'] = quebra_recibos_ago['valor_pago'].astype(float)
    quebra_recibos_set['valor_pago'] = quebra_recibos_set['valor_pago'].astype(float)
    quebra_recibos_out['valor_pago'] = quebra_recibos_out['valor_pago'].astype(float)
    quebra_recibos_nov['valor_pago'] = quebra_recibos_nov['valor_pago'].astype(float)
    quebra_recibos_dez['valor_pago'] = quebra_recibos_dez['valor_pago'].astype(float)
else:
    quebra_recibos_jan = quebra_recibo_jan_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_fev = quebra_recibo_fev_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mar = quebra_recibo_mar_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_abr = quebra_recibo_abr_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mai = quebra_recibo_mai_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jun = quebra_recibo_jun_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_jul = quebra_recibo_jul_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_ago = quebra_recibo_ago_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_set = quebra_recibo_set_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_out = quebra_recibo_out_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_nov = quebra_recibo_nov_22_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_dez = quebra_recibo_dez_22_func(media_para_quebra, df_append_all_amb)

    quebra_recibos_jan['valor_pago'] = quebra_recibos_jan['valor_pago'].astype(float)
    quebra_recibos_fev['valor_pago'] = quebra_recibos_fev['valor_pago'].astype(float)
    quebra_recibos_mar['valor_pago'] = quebra_recibos_mar['valor_pago'].astype(float)
    quebra_recibos_abr['valor_pago'] = quebra_recibos_abr['valor_pago'].astype(float)
    quebra_recibos_mai['valor_pago'] = quebra_recibos_mai['valor_pago'].astype(float)
    quebra_recibos_jun['valor_pago'] = quebra_recibos_jun['valor_pago'].astype(float)
    quebra_recibos_jul['valor_pago'] = quebra_recibos_jul['valor_pago'].astype(float)
    quebra_recibos_ago['valor_pago'] = quebra_recibos_ago['valor_pago'].astype(float)
    quebra_recibos_set['valor_pago'] = quebra_recibos_set['valor_pago'].astype(float)
    quebra_recibos_out['valor_pago'] = quebra_recibos_out['valor_pago'].astype(float)
    quebra_recibos_nov['valor_pago'] = quebra_recibos_nov['valor_pago'].astype(float)
    quebra_recibos_dez['valor_pago'] = quebra_recibos_dez['valor_pago'].astype(float)



# 9 Beneficiários com sessões acima do comum
# Filrando o número de repetições de sinistros de psicoterapia para maior que 48
psicoterapia_ultimo_ano = psico_func(df_append_all.dropna())

# Filrando o número de repetições de sinistros de fonoaudiologia para maior que 18
fonoaudiologia_ultimo_ano = fono_func(df_append_all.dropna())


# 10 Procedimentos, medicamentos, materiais, diárias ou taxas repetidos
# Agrupando sinistros para cálculo de reepetições

proc_repetido_b = proc_duplicados_por_provedor_func(df_append_all, df_subgrupo, proc_describe, filter_insurance)

# Filtro por tipo de classse do sinistro e data selecionada pelo usuário
proc = len(proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & ((proc_repetido_b['classe'] == 'Ambulatoriais')
                    | (proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Laboratoriais')
                    | (proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Hospitalizações'))
                    & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)])

med = len(proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Medicamentos') & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)])

mat = len(proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Materiais') & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)])

ted = len(proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Taxas e Diárias') & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)])

todos_sinistro_repetidos = proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)]


# Somando o total de alertas por tópico

f_1 = len(bene_sem_id)
f_1_cost = bene_sem_id['valor_pago'].sum()
f_1_cost_denied = 0
f_1_cost_aproved = 0
f_1_cost_left = f_1_cost - f_1_cost_denied - f_1_cost_aproved

f_2 = len(proc_diferentes)
f_2_cost = 0
f_2_cost_denied = 0
f_2_cost_aproved = 0
f_2_cost_left = f_2_cost - f_2_cost_denied - f_2_cost_aproved
# f_2_cost = proc_diferentes['valor_pago'].sum()

f_3 = len(sem_tuss)
f_3_cost = sem_tuss['valor_pago'].sum()
f_3_cost_denied = 0
f_3_cost_aproved = 0
f_3_cost_left = f_3_cost - f_3_cost_denied - f_3_cost_aproved

f_4 = len(proc_duplicados)
f_4_cost = proc_duplicados['valor_pago'].sum()
if filter_date == '2024':
    f_4_cost_denied = 0
    f_4_cost_aproved =  0
else:
    f_4_cost_denied = 0
    f_4_cost_aproved = 0
f_4_cost_left = f_4_cost - f_4_cost_denied - f_4_cost_aproved

f_5 = len(upper_outliers_nivel_provedor)
f_5_cost = upper_outliers_nivel_provedor['valor_pago'].sum()
f_5_cost_denied = 0
f_5_cost_aproved = 0
f_5_cost_left = f_5_cost 

f_6 = len(prest_sem_id)
f_6_cost = prest_sem_id['valor_pago'].sum()
if filter_date == '2024':
    f_6_cost_denied = 0
    f_6_cost_aproved = 0
else:
    f_6_cost_denied = 0
    f_6_cost_aproved = 0
f_6_cost_left = f_6_cost

f_7 = len(df_fem.dropna()) + len(df_male.dropna())
f_7_cost = df_fem['valor_pago'].sum() + df_male['valor_pago'].sum()
f_7_cost_denied = 0
f_7_cost_aproved = 0
f_7_cost_left = f_7_cost - f_7_cost_denied - f_7_cost_aproved


if filter_date == 'Dez/2022':
    f_8 = len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_dez['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Nov/2022':
    f_8 = len(quebra_recibos_nov)
    f_8_cost = quebra_recibos_nov['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Out/2022':
    f_8 = len(quebra_recibos_out)
    f_8_cost = quebra_recibos_out['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Set/2022':
    f_8 = len(quebra_recibos_set)
    f_8_cost = quebra_recibos_set['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Ago/2022':
    f_8 = len(quebra_recibos_ago)
    f_8_cost = quebra_recibos_ago['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Jul/2022':
    f_8 = len(quebra_recibos_jul)
    f_8_cost = quebra_recibos_jul['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Jun/2022':
    f_8 = len(quebra_recibos_jun)
    f_8_cost = quebra_recibos_jun['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Mai/2022':
    f_8 = len(quebra_recibos_mai)
    f_8_cost = quebra_recibos_mai['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Abr/2022':
    f_8 = len(quebra_recibos_abr)
    f_8_cost = quebra_recibos_abr['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Mar/2022':
    f_8 = len(quebra_recibos_mar)
    f_8_cost = quebra_recibos_mar['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Fev/2022':
    f_8 = len(quebra_recibos_fev)
    f_8_cost = quebra_recibos_fev['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Jan/2022':
    f_8 = len(quebra_recibos_jan)
    f_8_cost = quebra_recibos_jan['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Mar/2023':
    f_8 = len(quebra_recibos_mar_23)
    f_8_cost = quebra_recibos_mar_23['valor_pago'].sum()
    f_8_cost_denied = 1
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Fev/2023':
    f_8 = len(quebra_recibos_fev_23)
    f_8_cost = quebra_recibos_fev_23['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Jan/2023':
    f_8 = len(quebra_recibos_jan_23)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum()
    f_8_cost_denied = f_8_cost
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == '2023':
    f_8 = len(quebra_recibos_jan) + len(quebra_recibos_fev) + len(quebra_recibos_mar) + len(quebra_recibos_abr) + len(quebra_recibos_mai) + len(quebra_recibos_jun) + len(quebra_recibos_jul) + len(quebra_recibos_ago) + len(quebra_recibos_set) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_jan['valor_pago'].sum() + quebra_recibos_fev['valor_pago'].sum() + quebra_recibos_mar['valor_pago'].sum() + quebra_recibos_abr['valor_pago'].sum() + quebra_recibos_mai['valor_pago'].sum() + quebra_recibos_jun['valor_pago'].sum() + quebra_recibos_jul['valor_pago'].sum() + quebra_recibos_ago['valor_pago'].sum() + quebra_recibos_set['valor_pago'].sum() + quebra_recibos_out['valor_pago'].sum() + quebra_recibos_nov['valor_pago'].sum() + quebra_recibos_dez['valor_pago'].sum()
    f_8_cost_denied = 0
    f_8_cost_aproved = 0
    f_8_cost_left = 0
elif filter_date == '2024':
    f_8 = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_abr_23) + len(quebra_recibos_mai_23) + len(quebra_recibos_jun_23) + len(quebra_recibos_jul_23) + len(quebra_recibos_ago_23) + len(quebra_recibos_set_23) + len(quebra_recibos_out_23) + len(quebra_recibos_nov_23) + len(quebra_recibos_dez_23)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum() + quebra_recibos_fev_23['valor_pago'].sum() + quebra_recibos_mar_23['valor_pago'].sum() + quebra_recibos_abr_23['valor_pago'].sum() + quebra_recibos_mai_23['valor_pago'].sum() + quebra_recibos_jun_23['valor_pago'].sum() + quebra_recibos_jul_23['valor_pago'].sum() + quebra_recibos_ago_23['valor_pago'].sum() + quebra_recibos_set_23['valor_pago'].sum() + quebra_recibos_out_23['valor_pago'].sum() + quebra_recibos_nov_23['valor_pago'].sum() + quebra_recibos_dez_23['valor_pago'].sum()
    f_8_cost_denied = 0
    f_8_cost_aproved = 0
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Período de reajuste':
    f_8 = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum() + quebra_recibos_fev_23['valor_pago'].sum() + quebra_recibos_mar_23['valor_pago'].sum() + quebra_recibos_out['valor_pago'].sum() + quebra_recibos_nov['valor_pago'].sum() + quebra_recibos_dez['valor_pago'].sum()
    f_8_cost_denied = 0
    f_8_cost_aproved = 0
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
elif filter_date == 'Últimos 12 meses':
    f_8 = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_abr) + len(quebra_recibos_mai) + len(quebra_recibos_jun) + len(quebra_recibos_jul) + len(quebra_recibos_ago) + len(quebra_recibos_set) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum() + quebra_recibos_fev_23['valor_pago'].sum() + quebra_recibos_mar_23['valor_pago'].sum() + quebra_recibos_abr['valor_pago'].sum() + quebra_recibos_mai['valor_pago'].sum() + quebra_recibos_jun['valor_pago'].sum() + quebra_recibos_jul['valor_pago'].sum() + quebra_recibos_ago['valor_pago'].sum() + quebra_recibos_set['valor_pago'].sum() + quebra_recibos_out['valor_pago'].sum() + quebra_recibos_nov['valor_pago'].sum() + quebra_recibos_dez['valor_pago'].sum()
    f_8_cost_denied = 0
    f_8_cost_aproved = 0
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved
else:
    f_8 = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_jan) + len(quebra_recibos_fev) + len(quebra_recibos_mar) + len(quebra_recibos_abr) + len(quebra_recibos_mai) + len(quebra_recibos_jun) + len(quebra_recibos_jul) + len(quebra_recibos_ago) + len(quebra_recibos_set) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum() + quebra_recibos_fev_23['valor_pago'].sum() + quebra_recibos_mar_23['valor_pago'].sum() + quebra_recibos_jan['valor_pago'].sum() + quebra_recibos_fev['valor_pago'].sum() + quebra_recibos_mar['valor_pago'].sum() + quebra_recibos_abr['valor_pago'].sum() + quebra_recibos_mai['valor_pago'].sum() + quebra_recibos_jun['valor_pago'].sum() + quebra_recibos_jul['valor_pago'].sum() + quebra_recibos_ago['valor_pago'].sum() + quebra_recibos_set['valor_pago'].sum() + quebra_recibos_out['valor_pago'].sum() + quebra_recibos_nov['valor_pago'].sum() + quebra_recibos_dez['valor_pago'].sum()
    f_8_cost_denied = 0
    f_8_cost_aproved = 1
    f_8_cost_left = f_8_cost - f_8_cost_denied - f_8_cost_aproved

f_9 = len(psicoterapia_ultimo_ano) + len(fonoaudiologia_ultimo_ano)
f_9_cost = 0
f_9_cost_denied = 0
f_9_cost_aproved = 0
f_9_cost_left = f_9_cost - f_9_cost_denied - f_9_cost_aproved

# f_9_cost = df_fem['psicoterapia_ultimo_ano'].sum() + df_male['fonoaudiologia_ultimo_ano'].sum()
f_10 = len(todos_sinistro_repetidos)
f_10_cost = todos_sinistro_repetidos['valor_pago'].sum()

if filter_date == '2024':
    f_10_cost_denied = 0
    f_10_cost_aproved = 0
else:
    f_10_cost_denied = 0
    f_10_cost_aproved = 0
f_10_cost_left = f_10_cost - f_10_cost_denied - f_10_cost_aproved

if f_1_cost > 0:
    f_1_cost.round(0)
    f_1_cost_denied.round(0)
    f_1_cost_aproved.round(0)

if f_2_cost > 0:
    f_2_cost.round(0)
    f_2_cost_denied.round(0)
    f_2_cost_aproved.round(0)

if f_3_cost > 0:
    f_3_cost.round(0)
    # f_3_cost_denied.round(0)
    # f_3_cost_aproved.round(0)

if f_4_cost > 0:
    f_4_cost.round(0)
    # f_4_cost_denied.round(0)
    # f_4_cost_aproved.round(0)

if f_5_cost > 0:
    f_5_cost.round(0)
    # f_5_cost_denied.round(0)
    # f_5_cost_aproved.round(0)

if f_6_cost > 0:
    f_6_cost.round(0)
    # f_6_cost_denied.round(0)
    # f_6_cost_aproved.round(0)

if f_7_cost > 0:
    f_7_cost.round(0)
    # f_7_cost_denied.round(0)
    # f_7_cost_aproved.round(0)

if f_8_cost > 0:
    f_8_cost.round(0)
    # f_8_cost_denied.round(0)
    # f_8_cost_aproved.round(0)

if f_9_cost > 0:
    f_9_cost.round(0)
    # f_9_cost_denied.round(0)
    # f_9_cost_aproved.round(0)

if f_10_cost > 0:
    f_10_cost.round(0)
    # f_10_cost_denied.round(0)
    # f_10_cost_aproved.round(0)

total = f_1 + f_2 + f_3 + f_4 + f_5 + f_6 + f_7 + f_8 + f_9 + f_10

# Legenda introdutória sobra o total de alertas para cada tópico

if total > 0:
	'\n\n'
	st.error(f'**Atenção.** \n\n Foram encontradas **{total}** potenciais inconsistências com gastos em saúde, para **{filter_date}**. Os alertas que merecem a sua atenção estão divididos entre uso indevido do plano ou possível erro de classificação da operadora:', icon="🚨")
	'\n\n'
	'\n\n'
	# st.write('##### Foram encontradas', total, 'potenciais inconsistências com gastos em saúde, para', filter_date, '. Os alertas que merecem a sua atenção estão divididos entre uso indevido do plano ou possível erro de classificação da operadora:')
	# '\n\n'
	# '\n\n'

	# variable_costs = ['Gastos fora do padrão', 'Quebra de recibo', 'Sinistros repetidos', 'Sinistros e provedores repetidos', 'Sessões acima do comum', 'Procedimentos indevidos pelo sexo', 'Beneficiários sem ID', 'Códigos sem ID', 'Provedores sem ID']
	variable_costs = ['Gastos fora do padrão', 'Quebra de recibo', 'Sinistros repetidos', 'Sinistros e provedores repetidos', 'Sessões acima do comum', 'Procedimentos indevidos pelo sexo', 'Procedimentos sem ID', 'Beneficiários sem ID', 'Provedores sem ID']
	all_costs = [f_5_cost, f_8_cost, f_10_cost, f_4_cost, f_9_cost, f_7_cost, f_3_cost, f_1_cost, f_6_cost]
	all_denied_costs = [f_5_cost_denied, f_8_cost_denied, f_10_cost_denied, f_4_cost_denied, f_9_cost_denied, f_7_cost_denied, f_3_cost_denied, f_1_cost_denied, f_6_cost_denied]
	all_aproved_costs = [f_5_cost_aproved, f_8_cost_aproved, f_10_cost_aproved, f_4_cost_aproved, f_9_cost_aproved, f_7_cost_aproved, f_3_cost_aproved, f_1_cost_aproved, f_6_cost_aproved]
	all_left_costs = [f_5_cost_left, f_8_cost_left, f_10_cost_left, f_4_cost_left, f_9_cost_left, f_7_cost_left, f_3_cost_left, f_1_cost_left, f_6_cost_left]


	fig = go.Figure()
	fig = make_subplots(specs=[[{"secondary_y": True}]])
	fig.add_trace(go.Bar(x=variable_costs,
		    y=all_costs,
		    name='Gastos em R$',
		    marker_color='rgb(130, 107, 188)',
		    offsetgroup=1),
		    secondary_y=False
		    )
	fig.add_trace(go.Bar(x=variable_costs,
		    y=all_denied_costs,
		    name='Gastos justificados',
		    # marker_color='rgb(253, 129, 131)',
		    marker_color='rgb(255, 67, 134)',
		    offsetgroup=2),secondary_y=False)
	fig.add_trace(go.Bar(x=variable_costs,
		    y=all_aproved_costs,
		    name='Gastos corrigidos',
		    marker_color='rgb(0, 213, 99)',
		    # marker_color='rgb(253, 92, 94)',
		    offsetgroup=3),secondary_y=False)
	fig.add_trace(go.Bar(x=variable_costs,
		    y=all_left_costs,
		    name='Gastos em análises',
		    marker_color='rgb(26, 118, 255)',
		    offsetgroup=4),secondary_y=False)
	fig.add_trace(go.Scatter(x=variable_costs,
		    y=[f_5, f_8, f_10, f_4, f_9, f_7, f_3, f_1, f_6],
		    name='Quantidade de suspeitas',
		    # marker_color='rgb(130, 107, 188)',),
		    marker_color='rgb(254, 107, 87)',),
		    secondary_y=True
		    )

	fig.update_layout(
	xaxis_tickfont_size=14,
	hovermode="x unified",
	yaxis2=dict(overlaying="y"
	),
	yaxis=dict(
	title="R$",),
	legend=dict(
	    orientation="h",
	    x=-0.07,
	    y=1.2,
	    bgcolor='rgba(255, 255, 255, 0)',
	    bordercolor='rgba(255, 255, 255, 0)'
	),
	barmode='group',
	bargap=0.15, # gap between bars of adjacent location coordinates.
	bargroupgap=0.1, # gap between bars of the same location coordinate.
	autosize=False,
	width=800,
	height=400,
	margin=dict(l=0, r=20, t=20, b=20),
	)

	fig.update_yaxes(secondary_y=True)
	fig.update_traces(textfont_size=12, cliponaxis=False)
	
	if filter_date == '2024':
		if filter_insurance == 'SulAmérica':
			st.caption('Status das variáveis de auditoria em 2022')
			st.plotly_chart(fig, use_container_width=True)

	if filter_date == '2023':
		if filter_insurance == 'SulAmérica':
			st.caption('Status das variáveis de auditoria em 2023')
			st.plotly_chart(fig, use_container_width=True)

	# '\n\n'
	'\n\n'
	st.write('##### 1. Utilizações indevidas do plano de saúde')
	st.write('###### 1.1. Gastos fora do padrão de cada sinistro:', f_5)
	st.markdown('Cada sinistro, é avaliado pelo gasto médio e desvio padrão do valor pago para cada provedor. Dessa forma, são assinalados neste tópico sinistros que, apresentaram gastos maiores do que soma entre a média e desvio padrão do respectivo procedimento para o mesmo provedor.')
	'\n\n'
	st.write('###### 1.2. Quebra de recibo ou cobrança de retorno:', f_8)
	st.markdown('Consultas médicas que repetem o valor pago para o mesmo provedor, em dois meses consecutivos, sendo a soma dos dois valores maior que a média do valor pago por aquele procedimento na base, são alertados como suspeitas por quebras de recibo ou possível cobrança da consulta de retorno.')
	'\n\n'
	st.write('###### 1.3. Sinistros repetidos:', f_10)
	st.markdown('Neste tópico são classificados todos e quaisquer sinistros que tenham sido realizados pelo mesmo beneficiário, no mesmo dia e pelo mesmo valor, ou seja, que possam levantar suspeita do uso indevido pelo beneficiário em algum contexto.')
	'\n\n'
	st.write('###### 1.4. Mesmos procedimentos, para o mesmo provedor e no mesmo dia:', f_4)
	st.markdown('Além da suspeita do uso indevido, este segundo tópico de sinistros repetidos, acrescenta o provedor como ponto de atenção. Aqui, são alertados sinistros realizados pelo mesmo provedor, no mesmo dia e pelo mesmo valor, levantando suspeitas de possíveis cobranças duplicadas.')
	'\n\n'
	st.write('###### 1.5. Sessões acima do comum:', f_9)
	st.markdown('Quando o número de sessões de psicoterapia ou fonoaudiologia ultrapassar o limite de 48 sessões ou 18 sessões, respectivamente, no ano e para o mesmo beneficiário, você encontrará nesta aba.')
	'\n\n'
	st.write('###### 1.6. Procedimentos indevidos pelo sexo do beneficiário:', f_7)
	st.markdown('Procedimentos específicos do sexo feminino e masculino, se realizados pelo sexo oposto também serão indicados nesta classifição de uso indevido do plano de saúde.')
	'\n\n'
	'\n\n'
	st.write('##### 2. Possíveis falhas de classificação da operadora')
	st.write('###### 2.1. Beneficiários sem identificação:', f_1)
	st.markdown('Beneficiários sem identificação representam sinistros que não estão atrelados a um beneficiário, ou seja uma conta ou gasto sem o registro de qualquer usuário.')
	'\n\n'
	st.write('###### 2.2. Códigos de procedimentos sem identificação:', f_3)
	st.markdown('Sinistros sem identificação de algum código TUSS representam sinistros que não estão classificados de acordo com o padrão previsto pela ANS. Códigos que nem sequer receberam alguma outra identificação, também aparecem nessa classificação.')
	'\n\n'
	st.write('###### 2.3. Códigos com descrições diferentes:', f_2)
	st.markdown('Ainda, códigos que receberam mais de uma descrição diferente dentro da mesma base também fazem parte da classificação de possíveis falhas ao divulgar histórico dos sinistros.')
	'\n\n'
	st.write('###### 2.4. Provedores sem identificação:', f_6)
	st.markdown('Provedores sem qualquer identificação, como CNPJ ou CRM, os quais impossibilitam rastrear a fonte do serviço prestado são classificados neste tópico.')
	'\n\n'
else:
    st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. **A Blue te avisará se algo diferente acontecer.**', icon="🌟")
