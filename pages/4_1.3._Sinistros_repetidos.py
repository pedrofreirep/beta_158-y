import streamlit as st
from st_files_connection import FilesConnection

import pandas as pd
import numpy as np
from PIL import Image

from functions.proc_duplicados import proc_duplicados_func
from functions.proc_duplicados_por_provedor import proc_duplicados_por_provedor_func

# import zipfile
import xlsxwriter
from io import BytesIO

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

@st.cache_data(ttl=3600, show_spinner="3/4 - Analisando procedimentos...") #Ler base com a classificação TUSS da ANS
def get_data_3():
    return conn.read("df-for-mvps/17/158/jun-2024/proc_describe.csv", input_format="csv")
#     return pd.read_csv('/Users/pedro/Documents/Blue/ds/df_sulamerica_describe.csv')
    
proc_describe = get_data_3()
proc_describe = proc_describe.iloc[1:]


# zf = zipfile.ZipFile('cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv.zip') 

@st.cache_data(ttl=3600, show_spinner="4/4 - Carregando códigos TUSS...") #Ler base com a classificação TUSS da ANS
def get_data_4():
    return conn.read("df-for-mvps/tuss/cod_tuss_subgrupo_classe_2022_ponto_e_virgula 2.csv", input_format="csv")
	# return pd.read_csv(zf.open('cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv'))
    
df_subgrupo = get_data_4()

# df_append["cod_tuss"] = df_append["cod_tuss"].astype(int).astype(str)
df_append_all = df_append_all.drop(df_append_all[df_append_all.cod_tuss == "0000MS2B"].index)
df_append_all["cod_tuss"] = df_append_all["cod_tuss"].astype(int).astype(str)
df_subgrupo["cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)
proc_describe["cod_tuss"] = proc_describe["cod_tuss"].astype(int).astype(str)

proc_describe["outlier_range"] = proc_describe["outlier_range"].round(decimals = 0)

# df_append["dt_utilizacao"] = pd.to_datetime(df_append['dt_utilizacao'], format='mixed')
df_append_all["dt_utilizacao"] = pd.to_datetime(df_append_all['dt_utilizacao'], dayfirst=False, errors='coerce')

# df_append["mes_utilizacao"] = df_append["mes_utilizacao"].fillna(0).astype(int)
# df_append["ano_utilizacao"] = df_append["ano_utilizacao"].fillna(0).astype(int)

df_append_all["mes_utilizacao"] = df_append_all["mes_utilizacao"].fillna(0).astype(int)
df_append_all["ano_utilizacao"] = df_append_all["ano_utilizacao"].fillna(0).astype(int)

# df_subgrupo = pd.read_csv('../bases_de_terminologias/cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv', sep=';')
# df_subgrupo["cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)

st.markdown("# Sinistros repetidos")
st.sidebar.markdown("# Sinistros repetidos")

st.markdown('Neste tópico são classificados todos e quaisquer sinistros que tenham sido realizados pelo mesmo beneficiário, no mesmo dia e pelo mesmo valor, ou seja, que possam levantar suspeita do uso indevido pelo beneficiá em algum contexto.')

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

# 10 Procedimentos, medicamentos, materiais, diárias ou taxas repetidos
# Agrupando sinistros para cálculo de reepetições

proc_repetido_b = proc_duplicados_por_provedor_func(df_append_all, df_subgrupo, proc_describe, filter_insurance)

proc_repetido_b = proc_repetido_b.rename(columns={"id_pessoa": "ID do usuário", "cod_tuss": "TUSS", "dt_utilizacao": "Data", "valor_pago": "Valor pago", "repeticoes": "Repetições", "classe":"Classe", "proc_tuss":"Procedimento"})

total = len(proc_repetido_b[(proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]) 

if total == 1:
    st.write('###### Foi encontrado', total, 'procedimento, medicamento, material, diária ou taxa repetido no mesmo dia. Ou seja, quantidade de vezes em que o mesmo beneficiário fez o mesmo procedimento mais de uma vez no mesmo dia.')
    
    proc = len(proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & ((proc_repetido_b['Classe'] == 'Ambulatoriais')
                    | (proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Laboratoriais')
                    | (proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Hospitalizações'))
                    & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Classe'] >= min_date)])
    st.write('1. Procedimentos repetidos no mesmo dia:', proc)

    proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & ((proc_repetido_b['Classe'] == 'Ambulatoriais')
                    | (proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Laboratoriais')
                    | (proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Hospitalizações'))
                    & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    med = len(proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Medicamentos') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)])
    st.write('2. Medicamentos repetidos no mesmo dia:', med)
    proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Medicamentos') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    mat = len(proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Materiais') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)])
    st.write('3. Materiais repetidos no mesmo dia:', mat)
    proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Materiais') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    ted = len(proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Taxas e Diárias') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)])
    st.write('4. Taxas e diárias repetidas no mesmo dia:', ted)
    proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Classe'] == 'Taxas e Diárias') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]
	
    data_all = proc_repetido_b[(proc_repetido_b['Repetições'] > 1) & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    @st.cache_data
    def convert_df(data_all):
        return data_all.to_csv(index=False).encode('utf-8')


    csv = convert_df(data_all)

    st.download_button(
        "Baixar planilha",
        csv,
        "sinistros_repetidos.csv",
        "text/csv",
        key='download-csv'
    )

elif total > 1:
    st.write('###### Foram encontrados', total, 'procedimentos, medicamentos, materiais, diárias ou taxas repetidos no mesmo dia. Ou seja, quantidade de vezes em que o mesmo beneficiário fez o mesmo procedimento mais de uma vez no mesmo dia.')

    proc = len(proc_repetido_b[((proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Ambulatoriais')
                    | (proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Laboratoriais')
                    | (proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Hospitalizações'))
                    & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)])
    st.write('1. Procedimentos repetidos no mesmo dia:', proc)

    data_proc = proc_repetido_b[((proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Ambulatoriais')
                    | (proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Laboratoriais')
                    | (proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Hospitalizações'))
                    & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)][['ID do usuário', 'TUSS', 'Data', 'Procedimento', 'Classe', 'Repetições', 'Valor pago', 'operadora']].rename(columns = {'operadora': 'Operadora'})

    data_proc

    # st.markdown((data_proc['valor_pago']*data_proc['repeticoes']).sum())

    med = len(proc_repetido_b[(proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Medicamentos') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)])
    st.write('2. Medicamentos repetidos no mesmo dia:', med)
    proc_repetido_b[(proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Medicamentos') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    mat = len(proc_repetido_b[(proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Materiais') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)])
    st.write('3. Materiais repetidos no mesmo dia:', mat)
    proc_repetido_b[(proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Materiais') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    ted = len(proc_repetido_b[(proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Taxas e Diárias') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)])
    st.write('4. Taxas e diárias repetidas no mesmo dia:', ted)
    proc_repetido_b[(proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'] == 'Taxas e Diárias') & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    classe_nan = len(proc_repetido_b[((proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'].isna())
                    )
                    & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)])
    st.write('5. Sinistros repetidos sem classificação:', classe_nan)
    data_classe_nan = proc_repetido_b[((proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Classe'].isna())
                    )
                    & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    data_classe_nan


    data_all = proc_repetido_b[(proc_repetido_b['Repetições'] > proc_repetido_b['outlier_range']) & (proc_repetido_b['Data'] <= max_date) & (proc_repetido_b['Data'] >= min_date)]

    @st.cache_data
    def convert_df(data_all):
        return data_all.to_csv(index=False).encode('utf-8')


    csv = convert_df(data_all)

#     st.download_button(
#         "Baixar planilha",
#         csv,
#         "sinistros_repetidos.csv",
#         "text/csv",
#         key='download-csv'
#     )

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        data_all.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'num_format': '0.00'}) 
        worksheet.set_column('A:A', None, format1)  
        writer.close()
        processed_data = output.getvalue()
        return processed_data
    df_xlsx = to_excel(data_all)
    st.download_button(label='📥 Baixar Planilha',
                                    data=df_xlsx ,
                                    file_name= 'sinistros_repetidos.xlsx')

else:
    st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")


# df_append_all[(df_append_all['id_pessoa'] == 8888847523671011) & (df_append_all['dt_utilizacao'] == '2022-12-05T00:00:00')]

'''----'''

st.write('\n\n')

st.markdown('## 💡 Ações recomendadas e boas práticas')

st.markdown('Ao lidar com procedimentos pagos pelo plano de saúde com mais repetições do que o normal, é importante considerar as seguintes boas práticas:')

st.write('\n\n')

st.markdown('##### 1. Verificar a necessidade médica:')
st.markdown('Certifique-se de que as repetições do procedimento sejam clinicamente necessárias. Verifique com o médico ou especialista se existem razões válidas para a realização adicional do procedimento, levando em consideração as circunstâncias do beneficiário.')

st.markdown('##### 2. Contatar o plano de saúde:')
st.markdown('Entre em contato com o plano de saúde para esclarecer a situação e questionar o motivo pelo qual as repetições podem ser consideradas fora do normal. Explique detalhadamente a necessidade médica e forneça informações relevantes para apoiar seu caso, como o resumo de repetições por cada procedimento acima.')

st.markdown('##### 3. Obter apoio médico ou de profissinais de gestão em saúde:')
st.markdown('Busque o suporte para mais informações sobre o especialista responsável pelo procedimento. Essas informações podem ajudar a fornecer documentações e justificativas clínicas para a realização adicional do procedimento, como a especialidade do prestador, se for clinicamente necessário. Ter o respaldo de um profissional de saúde aumenta suas chances de obter uma reconsideração favorável.')

st.write('\n\n')

st.warning('Cada situação pode ser única e ter diferentes interpretações que vão além dos dados de utilização histórica. Manter registros detalhados de todas as comunicações, documentos e evidências relevantes é fundamental ao lidar com procedimentos pagos pelo plano de saúde com mais repetições do que o normal.')
