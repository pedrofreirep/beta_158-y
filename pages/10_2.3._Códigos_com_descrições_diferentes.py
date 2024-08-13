import streamlit as st
from st_files_connection import FilesConnection

import pandas as pd

from functions.proc_diferentes import proc_diferentes_func

from PIL import Image
import xlsxwriter
from io import BytesIO

st.set_page_config(
     page_title="Auditoria Beta",
     page_icon="🏗️",
 )

# st.cache_resource.clear()

conn = st.experimental_connection('s3', type=FilesConnection)

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

# df_append["cod_tuss"] = df_append["cod_tuss"].astype(int).astype(str)
df_append_all = df_append_all.drop(df_append_all[df_append_all.cod_tuss == "0000MS2B"].index)
df_append_all["cod_tuss"] = df_append_all["cod_tuss"].astype(int).astype(str)

st.markdown("# Códigos com descrições diferentes")
st.sidebar.markdown("# Códigos com descrições diferentes")

st.markdown('Ainda, códigos que receberam mais de uma descrição diferente dentro da mesma base também fazem parte da classificação de possíveis falhas ao divulgar histórico dos sinistros.')

filter_date = st.sidebar.selectbox(label='Selecione o período', options=['Toda a base'])

if filter_date == 'Mar/2023':
    min_date = '2023-03-01'
    max_date = '2023-03-31'
elif filter_date == 'Fev/2023':
    min_date = '2023-02-01'
    max_date = '2023-02-31'
elif filter_date == 'Jan/2023':
    min_date = '2023-01-01'
    max_date = '2023-01-31'
elif filter_date == 'Dez/2022':
    min_date = '2022-12-01'
    max_date = '2022-12-31'
elif filter_date == 'Nov/2022':
    min_date = '2022-1-01'
    max_date = '2022-11-31'
elif filter_date == 'Out/2022':
    min_date = '2022-10-01'
    max_date = '2022-10-31'
elif filter_date == 'Set/2022':
    min_date = '2022-09-01'
    max_date = '2022-09-31'
elif filter_date == 'Ago/2022':
    min_date = '2022-08-01'
    max_date = '2022-08-31'
elif filter_date == 'Jul/2022':
    min_date = '2022-07-01'
    max_date = '2022-07-31'
elif filter_date == 'Jun/2022':
    min_date = '2022-06-01'
    max_date = '2022-06-31'
elif filter_date == 'Mai/2022':
    min_date = '2022-05-01'
    max_date = '2022-05-31'
elif filter_date == 'Abr/2022':
    min_date = '2022-04-01'
    max_date = '2022-04-31'
elif filter_date == 'Mar/2022':
    min_date = '2022-03-01'
    max_date = '2022-03-31'
elif filter_date == 'Fev/2022':
    min_date = '2022-02-01'
    max_date = '2022-02-31'
elif filter_date == 'Jan/2022':
    min_date = '2022-01-01'
    max_date = '2022-01-31'
elif filter_date == '2024':
    min_date = '2022-01-01'
    max_date = '2022-12-31'
elif filter_date == '2023':
    min_date = '2023-01-01'
    max_date = '2023-12-31'
else:
    min_date = '2018-12-01'
    max_date = '2022-11-30'

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

# 2 Procedimentos diferentes ou com descrição diferente da própria base, para o mesmo código
proc_diferentes = proc_diferentes_func(df_append_all, filter_insurance)
proc_diferentes = proc_diferentes[['cod_tuss', 'proc_operadora', 'qtd_proc_diferentes', 'repeticoes', 'operadora_y']]

total = len(proc_diferentes)

if total == 1:
    # st.error('**Atenção.** \n\n A Blue encontrou alguma(s) suspeita(s) de fraude com os gastos do plano de saúde, para o período selecionado. Confira abaixo quais são as suspeitas.', icon="🚨")
    st.write('###### Foi encontrado', total, 'procedimento diferente ou com descrição diferente da própria base, para o mesmo código, em todo o histórico.')

    proc_diferentes = proc_diferentes.rename(columns={"proc_operadora": "Descrição", "qtd_proc_diferentes": "Quantidade de descrições diferentes", "repeticoes": "Repetições", 'operadora_y': 'Operadora'})
    proc_diferentes = proc_diferentes[['cod_tuss', 'Descrição', 'Quantidade de descrições diferentes', 'Repetições', 'Operadora']]
    proc_diferentes
    


    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        proc_diferentes.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'num_format': '0.00'}) 
        worksheet.set_column('A:A', None, format1)  
        writer.close()
        processed_data = output.getvalue()
        return processed_data
    df_xlsx = to_excel(proc_diferentes)
    st.download_button(label='📥 Baixar Planilha',
                                    data=df_xlsx ,
                                    file_name= 'codigos_com_descricoes_diferentes.xlsx')
elif total > 1:
    # st.error('**Atenção.** \n\n A Blue encontrou alguma(s) suspeita(s) de fraude com os gastos do plano de saúde, para o período selecionado. Confira abaixo quais são as suspeitas.', icon="🚨")
    st.write('###### Foram encontrados', total, 'procedimentos diferentes ou com descrição diferente da própria base, para o mesmo código, em todo o histórico.')

    proc_diferentes = proc_diferentes.rename(columns={"proc_operadora": "Descrição", "qtd_proc_diferentes": "Quantidade de descrições diferentes", "repeticoes": "Repetições", 'operadora_y': 'Operadora'})
    proc_diferentes = proc_diferentes[['cod_tuss', 'Descrição', 'Quantidade de descrições diferentes', 'Repetições', 'Operadora']]
    proc_diferentes

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        proc_diferentes.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'num_format': '0.00'}) 
        worksheet.set_column('A:A', None, format1)  
        writer.close()
        processed_data = output.getvalue()
        return processed_data
    df_xlsx = to_excel(proc_diferentes)
    st.download_button(label='📥 Baixar Planilha',
                                    data=df_xlsx ,
                                    file_name= 'codigos_com_descricoes_diferentes.xlsx')

else:
    st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")

if total > 0:
    '''----'''

    st.write('\n\n')

    st.markdown('## 💡 Ações recomendadas e boas práticas')

    st.markdown('Ao lidar com procedimentos pagos pelo plano de saúde em que procedimentos com descrições diferentes recebem a mesma identificação, considere as seguintes boas práticas:')

    st.write('\n\n')

    st.markdown('##### 1. Documentar e evidenciar as diferenças:')
    st.markdown('Mantenha registros detalhados dos procedimentos realizados, incluindo descrições, datas e outros detalhes relevantes. O objetivo dessa aplicação é automatizar essa busca e te mostrar a quantidade de descrições diferentes encontradas para cada código, junto com a quantidade de repetições de uma mesma descrição para o respectivo código.')

    st.markdown('##### 2. Entrar em contato com o plano de saúde:')
    st.markdown('Informe o plano de saúde sobre a questão, explicando que diferentes procedimentos estão sendo identificados de maneira inadequada. Forneça exemplos específicos e peça ao plano para revisar e corrigir a identificação dos procedimentos na fatura.')

    st.markdown('##### 3. Pesquisar a tabela da ANS:')
    st.markdown('Caso o código levantado seja um código TUSS, consulte a tabela de procedimentos da ANS disponível em seu site para identificar o código em questão e suas devidas descrições correspondentes aos procedimentos realizados. Essa pesquisa pode ajudar a ter uma referência clara dos procedimentos e auxiliar na comunicação com o plano de saúde.')

    st.markdown('##### 4. Solicitar uma revisão e correção:')
    st.markdown('Formalize um pedido por escrito para que o plano de saúde ou o prestador de serviços médicos revisem e corrijam a identificação inadequada dos procedimentos. Inclua todos os detalhes relevantes e evidências documentais que comprovem a diferença entre os procedimentos.')

    st.write('\n\n')

    st.warning('Lembre-se de que cada situação pode ser única, e é importante manter uma comunicação clara e documentada com o o plano de saúde. Registrar todas as interações, manter cópias de documentos e buscar assistência profissional, se necessário, pode ser útil ao resolver esse problema.')
else:
    pass