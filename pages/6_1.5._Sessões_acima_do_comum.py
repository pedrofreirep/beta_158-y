import streamlit as st
from st_files_connection import FilesConnection

import pandas as pd

from functions.sessoes_outliers import psico_func
from functions.sessoes_outliers import fono_func

from PIL import Image
# import zipfile

st.set_page_config(
     page_title="Auditoria Beta",
     page_icon="🏗️",
 )

# st.cache_resource.clear()

conn = st.connection('s3', type=FilesConnection)

@st.cache_data(ttl=3600, show_spinner="1/4 - Carregando base completa...") #Ler base com a classificação TUSS da ANS
def get_data_1():
    return conn.read("df-for-mvps/17/158/jun-2024/df_append_all.csv", input_format="csv")
#     return pd.read_csv("../dados/df_append_all.csv")

df_append_all = get_data_1()

# @st.cache_data(ttl=3600, show_spinner="2/4 - Carregando histórico...") #Ler base com a classificação TUSS da ANS
# def get_data_2():
#     return conn.read("df-for-mvps/17/158/jun-2024/df_append.csv", input_format="csv")
# #     return pd.read_csv('../dados/df_append.csv')
    
# df_append = get_data_2()

# df_append = df_append_all.dropna()

# zf = zipfile.ZipFile('cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv.zip') 

@st.cache_data(ttl=3600, show_spinner="4/4 - Carregando códigos TUSS...") #Ler base com a classificação TUSS da ANS
def get_data_4():
    return conn.read("df-for-mvps/tuss/cod_tuss_subgrupo_classe_2022_ponto_e_virgula 2.csv", input_format="csv")
	# return pd.read_csv(zf.open('cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv'))
    
df_subgrupo = get_data_4()

# df_subgrupo = pd.read_csv('../bases_de_terminologias/cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv', sep=';')
# df_append["cod_tuss"] = df_append["cod_tuss"].astype(int).astype(str)
df_append_all = df_append_all.drop(df_append_all[df_append_all.cod_tuss == "0000MS2B"].index)
df_append_all["cod_tuss"] = df_append_all["cod_tuss"].astype(int).astype(str)
df_subgrupo["cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)

# df_append["dt_utilizacao"] = pd.to_datetime(df_append['dt_utilizacao'], format='mixed')
df_append_all["dt_utilizacao"] = pd.to_datetime(df_append_all['dt_utilizacao'], dayfirst=False, errors='coerce')

# df_append["mes_utilizacao"] = df_append["mes_utilizacao"].fillna(0).astype(int)
# df_append["ano_utilizacao"] = df_append["ano_utilizacao"].fillna(0).astype(int)

df_append_all["mes_utilizacao"] = df_append_all["mes_utilizacao"].fillna(0).astype(int)
df_append_all["ano_utilizacao"] = df_append_all["ano_utilizacao"].fillna(0).astype(int)

df_append_all["valor_pago"] = df_append_all["valor_pago"].astype(float)

df_subgrupo["cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)

df_append_all['mes_utilizacao'] = df_append_all['mes_utilizacao'].fillna(0)
df_append_all['mes_utilizacao'] = df_append_all['mes_utilizacao'].astype(int).astype(str)
df_append_all['ano_utilizacao'] = df_append_all['ano_utilizacao'].fillna(0)
df_append_all['ano_utilizacao'] = df_append_all['ano_utilizacao'].astype(int)

st.markdown("# Sessões acima do comum")
st.sidebar.markdown("# Sessões acima do comum")

st.markdown('Quando o número de sessões de psicoterapia ou fonoaudiologia ultrapassar o limite de 48 sessões ou 18 sessões, respectivamente, no ano e para o mesmo beneficiário, você encontrará nesta aba.')

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
else:
    min_date = pd.to_datetime('2018-12-01')
    max_date = pd.to_datetime('2022-12-31')

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
# 9 Beneficiários com sessões acima do comum
# Filrando o número de repetições de sinistros de psicoterapia para maior que 48
psicoterapia_ultimo_ano = psico_func(df_append_all.dropna())

# Filrando o número de repetições de sinistros de fonoaudiologia para maior que 18
fonoaudiologia_ultimo_ano = fono_func(df_append_all.dropna())

total = len(psicoterapia_ultimo_ano) + len(fonoaudiologia_ultimo_ano)

if total == 1:
    st.write('###### Foi encontrado', total, 'beneficiário com sessões acima do comum.')

    psico = len(psicoterapia_ultimo_ano)
    st.write('1. Beneficiários de psicoterapia com mais de 48 sessões, no mesmo ano:', psico)
    psicoterapia_ultimo_ano = psicoterapia_ultimo_ano.rename(columns={"id_pessoa": "ID do usuário", "repeticoes": "Repetições"})
    psicoterapia_ultimo_ano

    fono = len(fonoaudiologia_ultimo_ano)
    st.write('2. Beneficiários de fonoaudiologia com mais de 18 sessões, no mesmo ano:', fono)
    fonoaudiologia_ultimo_ano = fonoaudiologia_ultimo_ano.rename(columns={"id_pessoa": "ID do usuário", "repeticoes": "Repetições"})
    fonoaudiologia_ultimo_ano

elif total > 1:
    st.write('###### Foram encontrados', total, 'beneficiários com sessões acima do comum.')

    psico = len(psicoterapia_ultimo_ano)
    st.write('1. Beneficiários de psicoterapia com mais de 48 sessões, no mesmo ano:', psico)
    psicoterapia_ultimo_ano = psicoterapia_ultimo_ano.rename(columns={"id_pessoa": "ID do usuário", "repeticoes": "Repetições"})
    psicoterapia_ultimo_ano

    fono = len(fonoaudiologia_ultimo_ano)
    st.write('2. Beneficiários de fonoaudiologia com mais de 18 sessões, no mesmo ano:', fono)
    fonoaudiologia_ultimo_ano = fonoaudiologia_ultimo_ano.rename(columns={"id_pessoa": "ID do usuário", "repeticoes": "Repetições"})
    fonoaudiologia_ultimo_ano

else:
    st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")
