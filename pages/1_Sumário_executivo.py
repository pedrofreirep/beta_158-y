import streamlit as st
from st_files_connection import FilesConnection

import pandas as pd
import numpy as np

from functions.bene_sem_id import bene_sem_id_func
from functions.upper_outliers_nivel_provedor import proc_preco_nivel_provedor_func
from functions.upper_outliers_nivel_provedor import upper_outliers_nivel_provedor_func
from functions.proc_diferentes import proc_diferentes_func
from functions.sem_tuss import sem_tuss_func
from functions.proc_duplicados import proc_duplicados_func
from functions.prest_sem_id import prest_sem_id_func
from functions.proc_sex import proc_male_func
from functions.proc_sex import proc_fem_func
from functions.sessoes_outliers import psico_func
from functions.sessoes_outliers import fono_func
from functions.proc_duplicados_por_provedor import proc_duplicados_por_provedor_func
from functions.quebra_recibo import quebra_recibo_jan_23_func, quebra_recibo_fev_23_func, quebra_recibo_mar_23_func, quebra_recibo_jan_22_func, quebra_recibo_fev_22_func, quebra_recibo_mar_22_func, quebra_recibo_abr_22_func, quebra_recibo_mai_22_func, quebra_recibo_jun_22_func, quebra_recibo_jul_22_func, quebra_recibo_ago_22_func, quebra_recibo_set_22_func, quebra_recibo_out_22_func, quebra_recibo_nov_22_func, quebra_recibo_dez_22_func

import time
from PIL import Image
import datetime
from datetime import date

import plotly.graph_objects as go
import plotly.express as px
# import zipfile

st.set_page_config(
     page_title="Auditoria Beta",
     page_icon="🏗️",
 )

# st.cache_resource.clear() 

conn = st.connection('s3', type=FilesConnection)

@st.cache_data(ttl=24*3600, show_spinner="1/3 - Carregando base completa...") #Ler base com a classificação TUSS da ANS
def get_data_1():
    return conn.read("df-for-mvps/17/158/jun-2024/df_append_all.csv", input_format="csv")
#     return pd.read_csv("../dados/df_append_all.csv")

df_append_all = get_data_1()

# @st.cache_data(ttl=24*3600, show_spinner="2/4 - Carregando histórico...") #Ler base com a classificação TUSS da ANS
# def get_data_2():
#     return conn.read("df-for-mvps/17/158/jun-2024/df_append.csv", input_format="csv")
# #     return pd.read_csv('../dados/df_append.csv')
    
# df_append = get_data_2()

# df_append = df_append_all.dropna()

@st.cache_data(ttl=24*3600, show_spinner="2/3 - Analisando procedimentos...") #Ler base com a classificação TUSS da ANS
def get_data_3():
    return conn.read("df-for-mvps/17/158/jun-2024/proc_describe.csv", input_format="csv")
#     return pd.read_csv('/Users/pedro/Documents/Blue/ds/df_sulamerica_describe.csv')
    
proc_describe = get_data_3()

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

# Configurando sidebar
st.sidebar.markdown("# Análise da sinistralidade")

# filter_date = st.sidebar.selectbox(label='Selecione o período', options=['2024', '2023'])
filter_date = st.sidebar.selectbox(label='Selecione o período', options=['2024', '2023'])

if filter_date == '2024':
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

# filter_insurance = 'Todas'

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

st.markdown("# 💰 Resumo do impacto nas despesas e possíveis economias, para " + filter_date)

st.markdown('A seguir, na ***seção 1*** são apresentados os principais tópicos sobre análise de gastos e utilizações indevidas com o plano de saúde que podem ser reduzidas por auditoria de gastos. Ainda, a ***seção 2*** mostra também as possíveis falhas de classificação que mais interferem na interpretação e análise dos dados. **Aproveite as recomendações para lidar melhor com cada contexto apresentado.**')


# 1 Beneficiário sem identificação
# bene_sem_id = bene_sem_id_func(df_append_all, filter_insurance, min_date, max_date)


# 2 Procedimentos diferentes ou com descrição diferente da própria base, para o mesmo código
# proc_diferentes = proc_diferentes_func(df_append_all, filter_insurance)


# 3 Códigos sem padrão TUSS ou sem identificação
sem_tuss = sem_tuss_func(df_append_all, filter_insurance, max_date, min_date)


# 4 Beneficiários com os mesmos procedimentos, para o mesmo prestador e no mesmo dia
# proc_duplicados = proc_duplicados_func(df_append_all, proc_describe, filter_insurance, max_date, min_date)


# 5 Analisando o padrão do preço de procedimentos
proc_preco_nivel_provedor = proc_preco_nivel_provedor_func()
upper_outliers_nivel_provedor = upper_outliers_nivel_provedor_func(df_append_all.dropna(), df_subgrupo, proc_preco_nivel_provedor, filter_insurance, max_date, min_date)


# 6 Prestador sem identificação
# prest_sem_id = prest_sem_id_func(df_append_all, filter_insurance, max_date, min_date)


# 7 Beneficiários que realizaram procedimentos indevidos para o seu sexo, no último ano
# df_fem = proc_fem_func(df_append_all.dropna())
# df_male = proc_male_func(df_append_all.dropna())


# 8 Quebra de recibo

# Criando média do valor pago por cada sinistro para identificar quebra do recibo dividia em 2 meses consecutivos
media_para_quebra = proc_preco_nivel_provedor[['cod_tuss', 'provedor', 'preço_limite']]
media_para_quebra = media_para_quebra.rename(columns={"preço_limite": "valor_medio"})

media_para_quebra.loc[:, "cod_tuss"] = media_para_quebra["cod_tuss"].astype(int).astype(str)

df_append_all_amb = df_append_all[df_append_all['cod_tuss'] == '10101012']

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
elif filter_date == '2023':
    quebra_recibos_jan_23 = quebra_recibo_jan_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_fev_23 = quebra_recibo_fev_23_func(media_para_quebra, df_append_all_amb)
    quebra_recibos_mar_23 = quebra_recibo_mar_23_func(media_para_quebra, df_append_all_amb)

    quebra_recibos_jan_23['valor_pago'] = quebra_recibos_jan_23['valor_pago'].astype(float)
    quebra_recibos_fev_23['valor_pago'] = quebra_recibos_fev_23['valor_pago'].astype(float)
    quebra_recibos_mar_23['valor_pago'] = quebra_recibos_mar_23['valor_pago'].astype(float)
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
# psicoterapia_ultimo_ano = psico_func(df_append_all.dropna())

# Filrando o número de repetições de sinistros de fonoaudiologia para maior que 18
# fonoaudiologia_ultimo_ano = fono_func(df_append_all.dropna())


# 10 Procedimentos, medicamentos, materiais, diárias ou taxas repetidos
# Agrupando sinistros para cálculo de reepetições

# proc_repetido_b = proc_duplicados_por_provedor_func(df_append_all, df_subgrupo, proc_describe, filter_insurance)

# # Filtro por tipo de classse do sinistro e data selecionada pelo usuário
# proc = len(proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & ((proc_repetido_b['classe'] == 'Ambulatoriais')
#                     | (proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Laboratoriais')
#                     | (proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Hospitalizações'))
#                     & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)])

# med = len(proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Medicamentos') & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)])

# mat = len(proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Materiais') & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)])

# ted = len(proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['classe'] == 'Taxas e Diárias') & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)])

# todos_sinistro_repetidos = proc_repetido_b[(proc_repetido_b['repeticoes'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['dt_utilizacao'] <= max_date) & (proc_repetido_b['dt_utilizacao'] >= min_date)]


# Somando o total de alertas por tópico

# f_1 = len(bene_sem_id)
# f_1_cost = bene_sem_id['valor_pago'].sum()
# f_2 = len(proc_diferentes)
# f_2_cost = proc_diferentes['valor_pago'].sum()
f_3 = len(sem_tuss)
f_3_cost = sem_tuss['valor_pago'].sum()
# f_4 = len(proc_duplicados)
# f_4_cost = proc_duplicados['valor_pago'].sum()
f_5 = len(upper_outliers_nivel_provedor)
f_5_cost = upper_outliers_nivel_provedor['valor_pago'].sum()
# f_6 = len(prest_sem_id)
# f_6_cost = prest_sem_id['valor_pago'].sum()
# f_7 = len(df_fem.dropna()) + len(df_male.dropna())
# f_7_cost = df_fem['valor_pago'].sum() + df_male['valor_pago'].sum()


if filter_date == 'Dez/2022':
    f_8 = len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_dez['valor_pago'].sum()
elif filter_date == 'Nov/2022':
    f_8 = len(quebra_recibos_nov)
    f_8_cost = quebra_recibos_nov['valor_pago'].sum()
elif filter_date == 'Out/2022':
    f_8 = len(quebra_recibos_out)
    f_8_cost = quebra_recibos_out['valor_pago'].sum()
elif filter_date == 'Set/2022':
    f_8 = len(quebra_recibos_set)
    f_8_cost = quebra_recibos_set['valor_pago'].sum()
elif filter_date == 'Ago/2022':
    f_8 = len(quebra_recibos_ago)
    f_8_cost = quebra_recibos_ago['valor_pago'].sum()
elif filter_date == 'Jul/2022':
    f_8 = len(quebra_recibos_jul)
    f_8_cost = quebra_recibos_jul['valor_pago'].sum()
elif filter_date == 'Jun/2022':
    f_8 = len(quebra_recibos_jun)
    f_8_cost = quebra_recibos_jun['valor_pago'].sum()
elif filter_date == 'Mai/2022':
    f_8 = len(quebra_recibos_mai)
    f_8_cost = quebra_recibos_mai['valor_pago'].sum()
elif filter_date == 'Abr/2022':
    f_8 = len(quebra_recibos_abr)
    f_8_cost = quebra_recibos_abr['valor_pago'].sum()
elif filter_date == 'Mar/2022':
    f_8 = len(quebra_recibos_mar)
    f_8_cost = quebra_recibos_mar['valor_pago'].sum()
elif filter_date == 'Fev/2022':
    f_8 = len(quebra_recibos_fev)
    f_8_cost = quebra_recibos_fev['valor_pago'].sum()
elif filter_date == 'Jan/2022':
    f_8 = len(quebra_recibos_jan)
    f_8_cost = quebra_recibos_jan['valor_pago'].sum()
elif filter_date == 'Mar/2023':
    f_8 = len(quebra_recibos_mar_23)
    f_8_cost = quebra_recibos_mar_23['valor_pago'].sum()
elif filter_date == 'Fev/2023':
    f_8 = len(quebra_recibos_fev_23)
    f_8_cost = quebra_recibos_fev_23['valor_pago'].sum()
elif filter_date == 'Jan/2023':
    f_8 = len(quebra_recibos_jan_23)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum()
elif filter_date == '2024':
    f_8 = len(quebra_recibos_jan) + len(quebra_recibos_fev) + len(quebra_recibos_mar) + len(quebra_recibos_abr) + len(quebra_recibos_mai) + len(quebra_recibos_jun) + len(quebra_recibos_jul) + len(quebra_recibos_ago) + len(quebra_recibos_set) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_jan['valor_pago'].sum() + quebra_recibos_fev['valor_pago'].sum() + quebra_recibos_mar['valor_pago'].sum() + quebra_recibos_abr['valor_pago'].sum() + quebra_recibos_mai['valor_pago'].sum() + quebra_recibos_jun['valor_pago'].sum() + quebra_recibos_jul['valor_pago'].sum() + quebra_recibos_ago['valor_pago'].sum() + quebra_recibos_set['valor_pago'].sum() + quebra_recibos_out['valor_pago'].sum() + quebra_recibos_nov['valor_pago'].sum() + quebra_recibos_dez['valor_pago'].sum()
elif filter_date == '2023':
    f_8 = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum() + quebra_recibos_fev_23['valor_pago'].sum() + quebra_recibos_mar_23['valor_pago'].sum()
elif filter_date == 'Período de reajuste':
    f_8 = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum() + quebra_recibos_fev_23['valor_pago'].sum() + quebra_recibos_mar_23['valor_pago'].sum() + quebra_recibos_out['valor_pago'].sum() + quebra_recibos_nov['valor_pago'].sum() + quebra_recibos_dez['valor_pago'].sum()
elif filter_date == 'Últimos 12 meses':
    f_8 = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_abr) + len(quebra_recibos_mai) + len(quebra_recibos_jun) + len(quebra_recibos_jul) + len(quebra_recibos_ago) + len(quebra_recibos_set) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum() + quebra_recibos_fev_23['valor_pago'].sum() + quebra_recibos_mar_23['valor_pago'].sum() + quebra_recibos_abr['valor_pago'].sum() + quebra_recibos_mai['valor_pago'].sum() + quebra_recibos_jun['valor_pago'].sum() + quebra_recibos_jul['valor_pago'].sum() + quebra_recibos_ago['valor_pago'].sum() + quebra_recibos_set['valor_pago'].sum() + quebra_recibos_out['valor_pago'].sum() + quebra_recibos_nov['valor_pago'].sum() + quebra_recibos_dez['valor_pago'].sum()
else:
    f_8 = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_jan) + len(quebra_recibos_fev) + len(quebra_recibos_mar) + len(quebra_recibos_abr) + len(quebra_recibos_mai) + len(quebra_recibos_jun) + len(quebra_recibos_jul) + len(quebra_recibos_ago) + len(quebra_recibos_set) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    f_8_cost = quebra_recibos_jan_23['valor_pago'].sum() + quebra_recibos_fev_23['valor_pago'].sum() + quebra_recibos_mar_23['valor_pago'].sum() + quebra_recibos_jan['valor_pago'].sum() + quebra_recibos_fev['valor_pago'].sum() + quebra_recibos_mar['valor_pago'].sum() + quebra_recibos_abr['valor_pago'].sum() + quebra_recibos_mai['valor_pago'].sum() + quebra_recibos_jun['valor_pago'].sum() + quebra_recibos_jul['valor_pago'].sum() + quebra_recibos_ago['valor_pago'].sum() + quebra_recibos_set['valor_pago'].sum() + quebra_recibos_out['valor_pago'].sum() + quebra_recibos_nov['valor_pago'].sum() + quebra_recibos_dez['valor_pago'].sum()

# f_9 = len(psicoterapia_ultimo_ano) + len(fonoaudiologia_ultimo_ano)
# f_9_cost = df_fem['psicoterapia_ultimo_ano'].sum() + df_male['fonoaudiologia_ultimo_ano'].sum()
# f_10 = len(todos_sinistro_repetidos)
# f_10_cost = todos_sinistro_repetidos['valor_pago'].sum()

total = f_3 + f_5 + f_8 

# Legenda introdutória sobra o total de alertas para cada tópico

if total > 0:
    '\n\n'
    st.write('##### 1.  Principais utilizações indevidas do plano de saúde')
    st.write('Os tópicos nesta seção mostram hábitos de utilização dos usuários (beneficiários e/ou empresas) que podem ser evitados, corrigidos ou re-planejados para reduzir despesas com o plano de saúde.')
    st.success('**Economia à vista.** \n\n Os tópicos abaixo somam, até então, aproximadamente **80% do total de despesas com utilizações indevidas** pelo plano de saúde.', icon="💰")
    st.warning('**Observação.** \n\n Os números abaixo são referentes as bases enviadas e após as respectivas atualizações. **Os feedbacks levantados serão considerados para atualizações constantes das informações**.', icon="⚠️")
    '\n\n'
    st.write('###### 1.1. Gastos fora do padrão de cada sinistro:')
    st.caption('**Definição:**')
    st.markdown('Cada sinistro, é avaliado pelo gasto médio e desvio padrão do valor pago para cada provedor. Dessa forma, são assinalados neste tópico sinistros que, apresentaram gastos maiores do que soma entre a média e desvio padrão do respectivo procedimento para o mesmo provedor.')
    st.caption('**Impacto:**')
    st.write('Para o período em análise,', f_5, 'sinistros ficaram acima do padrão de preço exercido, com isso R$',  f_5_cost.round(2), 'em despesas assistenciais pagas com o plano de saúde podem ser reduzidas se encaminhadas a provedores com preço de mercado.')
    st.caption('**Recomendações:**')
    st.markdown('Despesas com procedimentos acima do padrão de preço exercido pelo mercado, para aquele mesmo procedimeto, podem ser reduzidas direcionando os usuários para os provedores mais eficientes em preço e saúde. Apresentar para a empresa quais provedores oferecem serviços em saúde dentro no padrão de cobertura do plano, deve ser a atitude mais assertiva para evitar reajustes por gastos exorbitantes.')
    '\n\n'
    if f_8 > 0:
        st.write('###### 1.2. Quebras de recibo:')
        st.caption('**Definição:**')
        st.markdown('A avaliação de um alerta por quebra de recibo é definida pela repetição da conta paga em menos de um mês, quando também somadas ambas as contas o valor ultrapassa a média de gastoos do respectivo procedimento. Assim, procedimentos cobrados novamente no mês seguinte pelo mesmo provedor também podem estar nesse tópico.')
        st.caption('**Impacto:**')
        st.write('Quando provedores começam a ser um dos possíveis fatores de repeticões de contas,', f_8, 'sinistros foram realizados pelo mesmo beneficiário, em meses subsequentes. Com isso, R$', f_8_cost.round(2), 'representam os gastos com possíveis quebras de recibo.')
        st.caption('**Recomendações:**')
        st.markdown('Para este tópico, é necessário pesquisar com cuidado o motivo das contas duplicadas, em cada caso. Ainda, pode haver a necessidade de duplicidade para verificar e confirmar o estado de saúde do beneficiário, assim como pode haver divisão nos valores para que um procedimento possa ser coberto pelo plano. Comece investigando provedores com maiores repetições e busque formas eficientes de lidar com estas despesas.')
        '\n\n'


    st.write('##### 2. Principais possíveis falhas de classificação da operadora')
    st.write('A segunda seção de auditoria de sinistros levanta contas que deixam de entregar alguma informação relevante sobre o usuário, sinistro e/ou provedor. Trabalhar em conjunto com a operadora para alinhar tais informações pode trazer descobertas de potenciais economias.')
    st.error('**Atenção.** \n\n A principal falha de classificação de sinistros é representada por **Códigos sem identificação** que deixam de mostrar alguma informação relevante sobre o que foi utilizado e gasto.', icon="🚨")
    st.warning('**Observação.** \n\n Foram encontrados diversos formatos de datas na base de sinistros. Formatos de datas diferentes podem interferir nas análises e previsões. As datas já foram organizadas no mesmo formato e podem alterar os resultados nos próximos meses.', icon="⚠️")
    '\n\n'
    st.write('###### 2.2. Códigos sem identificação:')
    st.caption('**Definição:**')
    st.markdown('Procedimentos sem identificação de algum código TUSS ou representado pela terminologia da AMB representam sinistros que não estão classificados de acordo com o padrão previsto pela ANS, segundo o editorial da CBHPM. Códigos que nem sequer receberam alguma outra identificação, também aparecem nessa classificação.')
    st.caption('**Impacto:**')
    st.write(f_3, 'procedimentos não seguiram o padrão de identificação, a soma do valor médio desses procedimentos únicos é de R$', f_3_cost.round(2), 'em despesas assistenciais pagas com o planos de saúde, em ' + filter_date + '.') 
    st.caption('**Recomendações:**')
    st.markdown('Com a atual norma, pode ser complexo identificar sinistros que não seguem o padrão TUSS previsto pela ANS ou pela AMB, uma vez que existem outros procedimentos não classificados pela Agência Nacional de Saúde Suplementar (ANS), porém ainda sim precisam ser pagos pelos planos de saúde. Caso a recorrência de algum procedimento sem classificaçnao TUSS seja alta, é recomendado buscar a identificação do mesmo através da operadora e assim ser possível mapear a utlização do plano de saúde da empresa.')
    '\n\n'
    # st.write('###### 3. Códigos com descrições diferentes:', f_2)
    # st.markdown('Ainda, códigos que receberam mais de uma descrição diferente dentro da mesma base também fazem parte da classificação de possíveis falhas ao divulgar histórico dos sinistros.')
    # '\n\n'
    # st.write('###### 4. Provedores sem identificação:', f_6)
    # st.markdown('Provedores sem qualquer identificação, como CNPJ ou CRM, os quais impossibilitam rastrear a fonte do serviço prestado são classificados neste tópico.')
    # '\n\n'

    # st.write('###### 10. Procedimentos indevidos pelo sexo do beneficiário:', f_7)
    # st.markdown('Procedimentos específicos do sexo feminino e masculino, se realizados pelo sexo oposto também serão indicados nesta classifição de uso indevido do plano de saúde.')
    # '\n\n'
else:
    st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. **A Blue te avisará se algo diferente acontecer.**', icon="🌟")
