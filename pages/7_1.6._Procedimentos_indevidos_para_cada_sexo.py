import streamlit as st
from st_files_connection import FilesConnection

import pandas as pd

from functions.proc_sex import proc_male_func
from functions.proc_sex import proc_fem_func

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
    return conn.read("df-for-mvps/17/158/mai-2024/df_append_all.csv", input_format="csv")
#     return pd.read_csv("../dados/df_append_all.csv")

df_append_all = get_data_1()

# @st.cache_data(ttl=3600, show_spinner="2/4 - Carregando histórico...") #Ler base com a classificação TUSS da ANS
# def get_data_2():
#     return conn.read("df-for-mvps/17/158/mai-2024/df_append.csv", input_format="csv")
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
df_subgrupo["cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)

st.markdown("# Beneficiários que realizaram procedimentos indevidos para o seu sexo")
st.sidebar.markdown("# Procedimentos indevidos para cada sexo")

st.markdown('Procedimentos específicos do sexo feminino e masculino, se realizados pelo sexo oposto também serão indicados nesta classifição de uso indevido do plano de saúde.')

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

# 7 Beneficiários que realizaram procedimentos indevidos para o seu sexo, no último ano
df_fem = proc_fem_func(df_append_all.dropna())
df_male = proc_male_func(df_append_all.dropna())

total = len(df_fem.dropna()) + len(df_male.dropna())

if total == 1:
    st.write('###### Foi encontrado', total, 'Beneficiários que realizou pelo menos 1 procedimento indevido para o seu sexo')
    df_fem.dropna()
    df_male.dropna()
elif total > 1:
    st.write('###### Foram encontrados', total, 'beneficiários que realizaram procedimentos indevidos para o seu sexo.')
    df_fem.dropna()
    df_fem
    df_male.dropna()
else:
    st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")
