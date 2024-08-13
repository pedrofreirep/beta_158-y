import streamlit as st
from st_files_connection import FilesConnection

import pandas as pd
from PIL import Image

from functions.quebra_recibo import quebra_recibo_jan_23_func, quebra_recibo_fev_23_func, quebra_recibo_mar_23_func, quebra_recibo_abr_23_func, quebra_recibo_mai_23_func, quebra_recibo_jun_23_func, quebra_recibo_jul_23_func, quebra_recibo_ago_23_func, quebra_recibo_set_23_func, quebra_recibo_out_23_func, quebra_recibo_nov_23_func, quebra_recibo_dez_23_func, quebra_recibo_jan_22_func, quebra_recibo_fev_22_func, quebra_recibo_mar_22_func, quebra_recibo_abr_22_func, quebra_recibo_mai_22_func, quebra_recibo_jun_22_func, quebra_recibo_jul_22_func, quebra_recibo_ago_22_func, quebra_recibo_set_22_func, quebra_recibo_out_22_func, quebra_recibo_nov_22_func, quebra_recibo_dez_22_func

from functions.upper_outliers_nivel_provedor import proc_preco_nivel_provedor_func

# import zipfile
import xlsxwriter
from io import BytesIO

st.set_page_config(
     page_title="Auditoria Beta",
     page_icon="🏗️",
 )

# st.cache_resource.clear()

conn = st.connection('s3', type=FilesConnection)

@st.cache_data(ttl=3600, show_spinner="Carregando base completa...") #Ler base com a classificação TUSS da ANS
def get_data_1():
    return conn.read("df-for-mvps/17/158/mai-2024/df_append_all.csv", input_format="csv")
#     return pd.read_csv("../dados/df_append_all.csv")

df_append_all = get_data_1()

df_append_all = df_append_all.drop(df_append_all[df_append_all.cod_tuss == "0000MS2B"].index)
df_append_all["cod_tuss"] = df_append_all["cod_tuss"].astype(int).astype(str)
df_append_all['cod_tuss'] = df_append_all['cod_tuss'].replace(",",'', regex=True)

df_append_all["dt_utilizacao"] = pd.to_datetime(df_append_all['dt_utilizacao'], dayfirst=False, errors='coerce')

st.markdown("# Analisando possíveis quebras de recibo ou cobranças de retorno")
st.sidebar.markdown("# Quebras de recibo")

st.markdown('Consultas médicas que repetem o valor pago para o mesmo provedor, em dois meses consecutivos, sendo a soma dos dois valores maior que a média do valor pago por aquele procedimento na base, são alertados como suspeitas por quebras de recibo ou possível cobrança da consulta de retorno, contrária à [Resolução nº 1.958/2010](https://sistemas.cfm.org.br/normas/visualizar/resolucoes/BR/2010/1958) do Conselho Federal de Medicina (CFM) por eventualmente não configurar um novo ato profissional.')

filter_date = st.sidebar.selectbox(label='Selecione o período', options=['2024', '2023'])



df_append_all['mes_utilizacao'] = df_append_all['mes_utilizacao'].fillna(0).astype(int).astype(str)
df_append_all['ano_utilizacao'] = df_append_all['ano_utilizacao'].fillna(0).astype(int)

df_append_all['id_pessoa'] = df_append_all['id_pessoa'].astype(int).astype(str)

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


proc_preco_nivel_provedor = proc_preco_nivel_provedor_func()


# 8 Quebra de recibo

# Criando média do valor pago por cada sinistro para identificar quebra do recibo dividia em 2 meses consecutivos
media_para_quebra = proc_preco_nivel_provedor[['cod_tuss', 'provedor', 'preço_limite']]
media_para_quebra = media_para_quebra.rename(columns={"preço_limite": "valor_medio"})

media_para_quebra.loc[:, "cod_tuss"] = media_para_quebra["cod_tuss"].astype(int).astype(str)

df_append_all_amb = df_append_all[df_append_all['cod_tuss'] == '10101012']
df_append_all_amb = df_append_all_amb.drop(df_append_all_amb[df_append_all_amb.id_pessoa == "8888848033281011"].index)

df_append_all_amb.loc[:, "mes_utilizacao"] = df_append_all_amb["mes_utilizacao"].astype(int).astype(str)

# Iniciando classificação de quebra de recibo para Janeiro, com primeira parcela em Dezembro do ano anterior
# Iniciando classificação de quebra de recibo para Janeiro, com primeira parcela em Dezembro do ano anterior
if filter_date == 'Jan/2023':
    quebra_recibos_jan_23 = quebra_recibo_jan_23_func(media_para_quebra, df_append_all_amb)
    # st.write(quebra_recibos_jan_23['valor_pago'].sum())
elif filter_date == 'Fev/2023':
    quebra_recibos_fev_23 = quebra_recibo_fev_23_func(media_para_quebra, df_append_all_amb)
    # st.write(quebra_recibos_fev_23['valor_pago'].sum())
elif filter_date == 'Mar/2023':
    quebra_recibos_mar_23 = quebra_recibo_mar_23_func(media_para_quebra, df_append_all_amb)
    # st.write(quebra_recibos_mar_23['valor_pago'].sum())
elif filter_date == 'Jan/2022':
    quebra_recibos_jan = quebra_recibo_jan_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_jan['valor_pago'] = quebra_recibos_jan['valor_pago'].astype(float)
    # st.write(quebra_recibos_jan['valor_pago'].sum())
elif filter_date == 'Fev/2022':
    quebra_recibos_fev = quebra_recibo_fev_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_fev['valor_pago'] = quebra_recibos_fev['valor_pago'].astype(float)
    # st.write(quebra_recibos_fev['valor_pago'].sum())
elif filter_date == 'Mar/2022':
    quebra_recibos_mar = quebra_recibo_mar_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_mar['valor_pago'] = quebra_recibos_mar['valor_pago'].astype(float)
    # st.write(quebra_recibos_mar['valor_pago'].sum())
elif filter_date == 'Abr/2022':
    quebra_recibos_abr = quebra_recibo_abr_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_abr['valor_pago'] = quebra_recibos_abr['valor_pago'].astype(float)
    # st.write(quebra_recibos_abr['valor_pago'].sum())
elif filter_date == 'Mai/2022':
    quebra_recibos_mai = quebra_recibo_mai_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_mai['valor_pago'] = quebra_recibos_mai['valor_pago'].astype(float)
    # st.write(quebra_recibos_mai['valor_pago'].sum())
elif filter_date == 'Jun/2022':
    quebra_recibos_jun = quebra_recibo_jun_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_jun['valor_pago'] = quebra_recibos_jun['valor_pago'].astype(float)
    # st.write(quebra_recibos_jun['valor_pago'].sum())
elif filter_date == 'Jul/2022':
    quebra_recibos_jul = quebra_recibo_jul_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_jul['valor_pago'] = quebra_recibos_jul['valor_pago'].astype(float)
    # st.write(quebra_recibos_jul['valor_pago'].sum())
elif filter_date == 'Ago/2022':
    quebra_recibos_ago = quebra_recibo_ago_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_ago['valor_pago'] = quebra_recibos_ago['valor_pago'].astype(float)
    # st.write(quebra_recibos_ago['valor_pago'].sum())
elif filter_date == 'Set/2022':
    quebra_recibos_set = quebra_recibo_set_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_set['valor_pago'] = quebra_recibos_set['valor_pago'].astype(float)
    # st.write(quebra_recibos_set['valor_pago'].sum())
elif filter_date == 'Out/2022':
    quebra_recibos_out = quebra_recibo_out_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_out['valor_pago'] = quebra_recibos_out['valor_pago'].astype(float)
    # st.write(quebra_recibos_out['valor_pago'].sum())
elif filter_date == 'Nov/2022':
    quebra_recibos_nov = quebra_recibo_nov_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_nov['valor_pago'] = quebra_recibos_nov['valor_pago'].astype(float)
    # st.write(quebra_recibos_nov['valor_pago'].sum())
elif filter_date == 'Dez/2022':
    quebra_recibos_dez = quebra_recibo_dez_22_func(media_para_quebra, df_append_all_amb)
    # quebra_recibos_dez['valor_pago'] = quebra_recibos_dez['valor_pago'].astype(float)
    # st.write(quebra_recibos_dez['valor_pago'].sum())
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




if filter_date == 'Dez/2022':
    total = len(quebra_recibos_dez)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_dez = quebra_recibos_dez.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_dez

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_dez.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_dez)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_dez.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_dez = quebra_recibos_dez.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_dez

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_dez.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_dez)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_dez.xlsx')

    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Nov/2022':
    total = len(quebra_recibos_nov)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_nov = quebra_recibos_nov.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_nov

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_nov.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_nov)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_nov.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_nov = quebra_recibos_nov.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_nov

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_nov.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_nov)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_nov.xlsx')

    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Out/2022':
    total = len(quebra_recibos_out)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_out = quebra_recibos_out.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_out

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_out.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_out)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_out.xlsx')
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_out = quebra_recibos_out.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_out

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_out.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_out)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_out.xlsx')

    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Set/2022':
    total = len(quebra_recibos_set)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_set = quebra_recibos_set.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_set

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_set.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_set)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_set.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_set = quebra_recibos_set.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_set

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_set.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_set)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_set.xlsx')
	
    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Ago/2022':
    total = len(quebra_recibos_ago)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_ago = quebra_recibos_ago.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_ago

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_ago.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_ago)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_ago.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_ago = quebra_recibos_ago.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_ago

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_ago.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_ago)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_ago.xlsx')

    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Jul/2022':
    total = len(quebra_recibos_jul)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_jul = quebra_recibos_jul.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_jul

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_jul.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_jul)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_jul.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_jul = quebra_recibos_jul.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_jul

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_jul.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_jul)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_jul.xlsx')
	
    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Jun/2022':
    total = len(quebra_recibos_jun)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_jun = quebra_recibos_jun.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_jun

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_jun.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_jun)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_jun.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_jun = quebra_recibos_jun.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_jun

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_jun.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_jun)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_jun.xlsx')
	
    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Mai/2022':
    total = len(quebra_recibos_mai)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_mai = quebra_recibos_mai.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_mai

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_mai.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_mai)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_mai.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_mai = quebra_recibos_mai.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_mai

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_mai.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_mai)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_mai.xlsx')

    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Abr/2022':
    total = len(quebra_recibos_abr)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_abr = quebra_recibos_abr.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_abr

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_abr.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_abr)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_abr.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_abr = quebra_recibos_abr.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_abr
	
        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_abr.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_abr)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_abr.xlsx')
	
    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Mar/2022':
    total = len(quebra_recibos_mar)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_mar = quebra_recibos_mar.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_mar

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_mar.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_mar)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_mar.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_mar = quebra_recibos_mar.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_mar

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_mar.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_mar)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_mar.xlsx')
	
    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Fev/2022':
    total = len(quebra_recibos_fev)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_fev = quebra_recibos_fev.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_fev

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_fev.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_fev)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_fev.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_fev = quebra_recibos_fev.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_fev

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_fev.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_fev)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_fev.xlsx')

    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Jan/2022':
    total = len(quebra_recibos_jan)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_jan = quebra_recibos_jan.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_jan

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_jan.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_jan)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_jan.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_jan = quebra_recibos_jan.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_jan
	

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_jan.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_jan)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_jan.xlsx')
    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Mar/2023':
    total = len(quebra_recibos_mar_23)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_mar_23 = quebra_recibos_mar_23.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_mar_23
        # quebra_recibos_mar_23['Valor pago'] = quebra_recibos_mar_23['Valor pago'].astype(float)
        # st.write(quebra_recibos_mar_23['Valor pago'].sum())

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_mar_23.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_mar_23)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_mar.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_mar_23 = quebra_recibos_mar_23.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_mar_23
        # quebra_recibos_mar_23['Valor pago'] = quebra_recibos_mar_23['Valor pago'].astype(float)
        # st.write(quebra_recibos_mar_23['Valor pago'].sum())

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_mar_23.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_mar_23)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_mar.xlsx')
	
    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Fev/2023':
    total = len(quebra_recibos_fev_23)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_fev_23 = quebra_recibos_fev_23.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_fev_23

        # st.write(quebra_recibos_fev_23['Valor pago'].sum())

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_fev_23.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_fev_23)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_fev.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_fev_23 = quebra_recibos_fev_23.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_fev_23
        # quebra_recibos_fev_23['Valor pago'] = quebra_recibos_fev_23['Valor pago'].astype(float)
        # st.write(quebra_recibos_fev_23['Valor pago'].sum())

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_fev_23.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_fev_23)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_fev.xlsx')

    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == 'Jan/2023':
    total = len(quebra_recibos_jan_23)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        quebra_recibos_jan_23 = quebra_recibos_jan_23.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_jan_23
        # st.write(quebra_recibos_jan_23['Valor pago'].sum())

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_jan_23.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_jan_23)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_jan.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        quebra_recibos_jan_23 = quebra_recibos_jan_23.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        quebra_recibos_jan_23
        # quebra_recibos_jan_23['Valor pago'] = quebra_recibos_jan_23['Valor pago'].astype(float)
        # st.write(quebra_recibos_jan_23['Valor pago'].sum())
	

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             quebra_recibos_jan_23.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(quebra_recibos_jan_23)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_jan.xlsx')
    else:
        st.info('Nenhum alerta de potencial fraude foi encontrado para esse período. \n\n**A Blue te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == '2024':
    total = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_abr_23) + len(quebra_recibos_mai_23) + len(quebra_recibos_jun_23) + len(quebra_recibos_jul_23) + len(quebra_recibos_ago_23) + len(quebra_recibos_set_23) + len(quebra_recibos_out_23) + len(quebra_recibos_nov_23) + len(quebra_recibos_dez_23)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        concat = pd.concat([quebra_recibos_jan_23, quebra_recibos_fev_23, quebra_recibos_mar_23, quebra_recibos_abr_23, quebra_recibos_mai_23, quebra_recibos_jun_23, quebra_recibos_jul_23, quebra_recibos_ago_23, quebra_recibos_set_23, quebra_recibos_out_23, quebra_recibos_nov_23, quebra_recibos_dez_23])
        concat = concat.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        concat

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             concat.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(concat)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_2023.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        concat = pd.concat([quebra_recibos_jan_23, quebra_recibos_fev_23, quebra_recibos_mar_23, quebra_recibos_abr_23, quebra_recibos_mai_23, quebra_recibos_jun_23, quebra_recibos_jul_23, quebra_recibos_ago_23, quebra_recibos_set_23, quebra_recibos_out_23, quebra_recibos_nov_23, quebra_recibos_dez_23])
        concat = concat.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        concat

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             concat.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(concat)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_2023.xlsx')
	
    else:
        st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")

elif filter_date == '2023':
    total = len(quebra_recibos_jan) + len(quebra_recibos_fev) + len(quebra_recibos_mar) + len(quebra_recibos_abr) + len(quebra_recibos_mai) + len(quebra_recibos_jun) + len(quebra_recibos_jul) + len(quebra_recibos_ago) + len(quebra_recibos_set) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        concat = pd.concat([quebra_recibos_jan, quebra_recibos_fev, quebra_recibos_mar, quebra_recibos_abr, quebra_recibos_mai, quebra_recibos_jun, quebra_recibos_jul, quebra_recibos_ago, quebra_recibos_set, quebra_recibos_out, quebra_recibos_nov, quebra_recibos_dez])
        concat = concat.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        concat

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             concat.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(concat)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_2023.xlsx')
	
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        concat = pd.concat([quebra_recibos_jan, quebra_recibos_fev, quebra_recibos_mar, quebra_recibos_abr, quebra_recibos_mai, quebra_recibos_jun, quebra_recibos_jul, quebra_recibos_ago, quebra_recibos_set, quebra_recibos_out, quebra_recibos_nov, quebra_recibos_dez])
        concat = concat.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        concat

        def to_excel(df):
             output = BytesIO()
             writer = pd.ExcelWriter(output, engine='xlsxwriter')
             concat.to_excel(writer, index=False, sheet_name='Sheet1')
             workbook = writer.book
             worksheet = writer.sheets['Sheet1']
             format1 = workbook.add_format({'num_format': '0.00'}) 
             worksheet.set_column('A:A', None, format1)  
             writer.close()
             processed_data = output.getvalue()
             return processed_data
        df_xlsx = to_excel(concat)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_2023.xlsx')
	
    else:
        st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")

else:
    total = len(quebra_recibos_jan_23) + len(quebra_recibos_fev_23) + len(quebra_recibos_mar_23) + len(quebra_recibos_jan) + len(quebra_recibos_fev) + len(quebra_recibos_mar) + len(quebra_recibos_abr) + len(quebra_recibos_mai) + len(quebra_recibos_jun) + len(quebra_recibos_jul) + len(quebra_recibos_ago) + len(quebra_recibos_set) + len(quebra_recibos_out) + len(quebra_recibos_nov) + len(quebra_recibos_dez)
    if total == 1:
        st.write('###### Foi encontrado', total, 'possível quebra de recibo apenas.')
        concat = pd.concat([quebra_recibos_jan_23, quebra_recibos_fev_23, quebra_recibos_mar_23])
        concat = concat.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        concat

        def to_excel(df):
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            concat.to_excel(writer, index=False, sheet_name='Sheet1')
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            format1 = workbook.add_format({'num_format': '0.00'}) 
            worksheet.set_column('A:A', None, format1)  
            writer.close()
            processed_data = output.getvalue()
            return processed_data
        df_xlsx = to_excel(concat)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_2023.xlsx')
    
    elif total > 1:
        st.write('###### Foram encontrados', total, 'possíveis quebras de recibo.')
        concat = pd.concat([quebra_recibos_jan_23, quebra_recibos_fev_23, quebra_recibos_mar_23])
        concat = concat.rename(columns={"id_pessoa": "ID do usuário", "provedor": "Provedor", "cod_tuss": "TUSS", "valor_pago": "Valor pago", 'qtd_cobrancas': "Cobranças", "operadora": "Operadora"})
        concat

        def to_excel(df):
            output = BytesIO()
            writer = pd.ExcelWriter(output, engine='xlsxwriter')
            concat.to_excel(writer, index=False, sheet_name='Sheet1')
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            format1 = workbook.add_format({'num_format': '0.00'}) 
            worksheet.set_column('A:A', None, format1)  
            writer.close()
            processed_data = output.getvalue()
            return processed_data
        df_xlsx = to_excel(concat)
        st.download_button(label='📥 Baixar Planilha', data=df_xlsx, file_name= 'quebra_recibos_2023.xlsx')
    
    else:
        st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")

'''----'''

st.write('\n\n')

st.markdown('## 💡 Ações recomendadas e boas práticas')

st.markdown('Ao lidar com procedimentos pagos pelo plano de saúde em que possam ter ocorrido quebras de recibo ou cobranças de retorno, considere as seguintes boas práticas:')

st.write('\n\n')

st.markdown('##### 1. Revisar os registros de utilizações:')
st.markdown('Verifique cuidadosamente os registros e informações relacionadas aos procedimentos realizados. Priorize os alertas de maior gasto ou que mais vezes se repetem para um mesmo provedor ou beneficiário. Tente interpretar o contexto das utilizações levantadas acima na base de procedimentos do plano de saúde para identificar possíveis discrepâncias, erros, ou justificativas.')

st.markdown('##### 2. Saiba mais sobre o provedor de serviços médicos:')
st.markdown('Entre em contato com, ou pesquise mais sobre, o provedor de serviços médicos para interpretar os alertas identificados. Pesquise pelos preços cobrados pelas consultas por aquele provedor, em caso de divergência com os registros na base busque explicação detalhada sobre os motivos dessas ocorrências com a operadora.')

st.markdown('##### 3. Contatar o plano de saúde:')
st.markdown('Comunique-se também com o plano de saúde para informar sobre os alertas de possíveis quebras de recibo, cobranças de retorno, ou outras prováveis divergências. Forneça os detalhes específicos dos registros e descreva claramente o problema encontrado. Pergunte sobre o processo a ser seguido para investigar e contestar os alertas de possíveis cobranças indevidas que fugiram do padrão de utilização da população.')

st.markdown('##### 4. Solicitar uma investigação interna:')
st.markdown('Peça ao plano de saúde que conduzam uma investigação em conjunto sobre o assunto. Isso pode envolver revisar os registros, comparar comprovantes de pagamento e analisar a documentação pertinente para esclarecer as discrepâncias e encontrar uma solução adequada.')

st.markdown('##### 5. Construir iniciativas de economia em saúde:')
st.markdown('Construa e/ou comunique programas corporativos para educação de economia em saúde aos beneficiários, evidencie estimativas de economia sobre o impacto sistêmico gerado e o efeito de possíveis utilizações indevidas sobre o aumento dos gastos com plano de saúde da empresa. \n\n Mobilize ações de comunicação para propagar a informação entre os beneficiários de que a companhia está se protegendo contra utilizações indevidas, **deixe-os informados**. Demonstre as iniciativas criadas para a operadora, isso pode fortalecer a sua posição em uma negociação.')

st.markdown('##### 6. Manter registros e evidências:')
st.markdown('Certifique-se de manter cópias de todos os registros, recibos e faturas se fornecidos, e-mails e quaisquer outras comunicações relevantes relacionadas ao problema. Esses documentos serão essenciais para comprovar sua reclamação e auxiliar na resolução da situação.')

st.write('\n\n')

st.warning('Lembre-se de que cada situação pode ser única, é importante levantar mais informações sobre os alertas de procedimentos levantados. Mantenha um registro de todas as comunicações e tente resolver o problema buscando o apoio de profissionais especializados, se necessário.')


