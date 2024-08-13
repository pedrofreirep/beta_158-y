import streamlit as st
from st_files_connection import FilesConnection

import pandas as pd

from functions.sem_tuss import sem_tuss_func

from PIL import Image
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


# df_append["cod_tuss"] = df_append["cod_tuss"].astype(int).astype(str)
df_append_all = df_append_all.drop(df_append_all[df_append_all.cod_tuss == "0000MS2B"].index)
df_append_all["cod_tuss"] = df_append_all["cod_tuss"].astype(int).astype(str)

# df_append["dt_utilizacao"] = pd.to_datetime(df_append['dt_utilizacao'], format='mixed')
df_append_all["dt_utilizacao"] = pd.to_datetime(df_append_all['dt_utilizacao'], dayfirst=False, errors='coerce')

# df_append["mes_utilizacao"] = df_append["mes_utilizacao"].fillna(0).astype(int)
# df_append["ano_utilizacao"] = df_append["ano_utilizacao"].fillna(0).astype(int)

df_append_all["mes_utilizacao"] = df_append_all["mes_utilizacao"].fillna(0).astype(int)
df_append_all["ano_utilizacao"] = df_append_all["ano_utilizacao"].fillna(0).astype(int)

df_append_all["valor_pago"] = df_append_all["valor_pago"].astype(float)


df_append_all['mes_utilizacao'] = df_append_all['mes_utilizacao'].fillna(0)
df_append_all['mes_utilizacao'] = df_append_all['mes_utilizacao'].astype(int).astype(str)
df_append_all['ano_utilizacao'] = df_append_all['ano_utilizacao'].fillna(0)
df_append_all['ano_utilizacao'] = df_append_all['ano_utilizacao'].astype(int)

df_append_all['operadora'] = df_append_all['operadora'].astype(str)

st.markdown("# Códigos sem identificação")
st.sidebar.markdown("# Códigos sem identificação")

st.markdown('Sinistros sem identificação de algum código TUSS representam sinistros que não estão classificados de acordo com o padrão previsto pela ANS. Códigos que nem sequer receberam alguma outra identificação, também aparecem nessa classificação.')

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

# 3 Códigos sem padrão TUSS ou sem identificação
sem_tuss = sem_tuss_func(df_append_all, filter_insurance, max_date, min_date)

total = len(sem_tuss[((sem_tuss['dt_utilizacao'] <= max_date) & (sem_tuss['dt_utilizacao'] >= min_date))])

if total == 1:
    # st.error('**Atenção.** \n\n A Blue encontrou alguma(s) suspeita(s) de fraude com os gastos do plano de saúde, para o período selecionado. Confira abaixo quais são as suspeitas.', icon="🚨")
    st.write('###### Foi encontrado', total, 'código único sem padrão TUSS ou sem identificação.')
    # st.write('###### 1. Beneficiários sem identificação:', total)
    sem_tuss = sem_tuss.rename(columns={"id_pessoa": "ID do usuário", "sexo": "Sexo", "provedor": "Provedor", "cod_tuss": "TUSS", "proc_operadora": "Descrição", "dt_utilizacao": "Data", "valor_pago": "Valor pago"})
    sem_tuss = sem_tuss[((sem_tuss['Data'] <= max_date) & (sem_tuss['Data'] >= min_date)) | (sem_tuss['Data'] == '0')]
    sem_tuss
    
#     @st.experimental_memo
#     def convert_df(sem_tuss):
#         return sem_tuss.to_csv(index=False).encode('utf-8')


#     csv = convert_df(sem_tuss)

#     st.download_button(
#         "Baixar planilha",
#         csv,
#         "codigos_sem_identificacao.csv",
#         "text/csv",
#         key='download-csv'
#     )

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        sem_tuss.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'num_format': '0.00'}) 
        worksheet.set_column('A:A', None, format1)  
        writer.close()
        processed_data = output.getvalue()
        return processed_data
    df_xlsx = to_excel(sem_tuss)
    st.download_button(label='📥 Baixar Planilha',
                                    data=df_xlsx ,
                                    file_name= 'codigos_sem_identificacao.xlsx')
elif total > 1:
    # st.error('**Atenção.** \n\n A Blue encontrou alguma(s) suspeita(s) de fraude com os gastos do plano de saúde, para o período selecionado. Confira abaixo quais são as suspeitas.', icon="🚨")
    if filter_insurance == 'Paraná Clínicas':
        st.warning('**Observação.** \n\n A versão anterior, sem a correção de códigos, somava 1540 procedimentos sem classificação TUSS em 2022, totalizando mais de **200 mil reais**. Após a correção parcial restam 298 procedimentos sem classificação, os quais representam aproximadamente **103 mil reais**.', icon="⚠️")
    else:
        pass
    st.write('###### Foram encontrados', total, 'procedimentos sem padrão TUSS ou sem identificação.')
    # st.write('###### 1. Beneficiários sem identificação:', total)
    sem_tuss = sem_tuss.rename(columns={"id_pessoa": "ID do usuário", "sexo": "Sexo", "provedor": "Provedor", "cod_tuss": "Código do procedimento", "proc_operadora": "Descrição", "dt_utilizacao": "Data", "valor_pago": "Valor pago", "operadora": "Operadora"})
    sem_tuss = sem_tuss[['Código do procedimento', 'ID do usuário', 'Descrição', 'Provedor', 'Data', 'Valor pago', 'Operadora']]
    sem_tuss = sem_tuss[((sem_tuss['Data'] <= max_date) & (sem_tuss['Data'] >= min_date)) | (sem_tuss['Data'] == '0')]
    sem_tuss
    
#     @st.experimental_memo
#     def convert_df(sem_tuss):
#         return sem_tuss.to_csv(index=False).encode('utf-8')


#     csv = convert_df(sem_tuss)

#     st.download_button(
#         "Baixar planilha",
#         csv,
#         "codigos_sem_identificacao.csv",
#         "text/csv",
#         key='download-csv'
#     )

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        sem_tuss.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'num_format': '0.00'}) 
        worksheet.set_column('A:A', None, format1)  
        writer.close()
        processed_data = output.getvalue()
        return processed_data
    df_xlsx = to_excel(sem_tuss)
    st.download_button(label='📥 Baixar Planilha',
                                    data=df_xlsx ,
                                    file_name= 'codigos_sem_identificacao.xlsx')
    # st.write((sem_tuss[((sem_tuss['dt_utilizacao'] <= max_date) & (sem_tuss['dt_utilizacao'] >= min_date)) | (sem_tuss['dt_utilizacao'] == '0')]['valor_pago'].sum()) - 864423.24)
else:
    st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")

'''----'''

st.write('\n\n')

st.markdown('## 💡 Ações recomendadas e boas práticas')

st.markdown('Ao lidar com procedimentos pagos pelo plano de saúde sem a identificação segundo a Agência Nacional de Saúde Suplementar (ANS), aqui estão algumas boas práticas a serem consideradas:')

st.write('\n\n')

st.markdown('##### 1. Entrar em contato com o plano de saúde:')
st.markdown('Entre em contato com o plano de saúde para esclarecer a falta de identificação segundo a ANS nos procedimentos pagos. Explique a situação e questione o motivo da falta de identificação, solicitando informações sobre como o problema pode ser resolvido e os códigos traduzidos.')

st.markdown('##### 2. Pesquisar a tabela da ANS:')
st.markdown('Consulte a tabela de procedimentos da ANS disponível em seu site para identificar os códigos e descrições correspondentes aos procedimentos realizados. Essa pesquisa pode ajudar a ter uma referência clara dos procedimentos e auxiliar na comunicação com o plano de saúde. O objetivo dessa aplicação é automatizar essa busca e te mostrar os códigos não encontrados na tabela da ANS, com isso atualizações constantes podem acontecer.')

st.write('\n\n')

st.warning('Lembre-se de que cada situação pode ser única, e é importante seguir as regulamentações da ANS. Manter registros detalhados e buscar uma comunicação clara e documentada com todas as partes envolvidas é fundamental para resolver a falta de identificação dos procedimentos e conseguir a tradução de códigos.')
