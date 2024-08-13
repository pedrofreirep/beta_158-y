import streamlit as st
from st_files_connection import FilesConnection

from functions.upper_outliers_nivel_provedor import proc_preco_nivel_provedor_func
from functions.upper_outliers_nivel_provedor import upper_outliers_nivel_provedor_func

import pandas as pd
from PIL import Image

# import zipfile
import xlsxwriter
from io import BytesIO

st.set_page_config(
     page_title="Auditoria Beta",
     page_icon="🏗️",
 )

st.cache_resource.clear()

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

# df_append["id_pessoa"] = df_append["id_pessoa"].astype(int).astype(str)
df_append_all["id_pessoa"] = df_append_all["id_pessoa"].astype(int).astype(str)

# df_subgrupo = pd.read_csv('../bases_de_terminologias/cod_tuss_subgrupo_classe_2022_ponto_e_virgula.csv', sep=';')
df_subgrupo["cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)

df_append_all['operadora'] = df_append_all['operadora'].astype(str)

st.markdown("# Analisando o padrão do preço de procedimentos")
st.sidebar.markdown("# Analisando o padrão do preço de procedimentos")

st.markdown('Cada sinistro, é avaliado pelo gasto médio e desvio padrão do valor pago para cada provedor. Dessa forma, são assinalados neste tópico sinistros que, apresentaram gastos maiores do que soma entre a média e desvio padrão do respectivo procedimento para o mesmo provedor, multiplicado ainda pela VCMH acumulada do último ano.')

filter_date = st.sidebar.selectbox(label='Selecione o período', options=[ '2023', '2022'])

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

# 5 Analisando o padrão do preço de procedimentos

# 5 Analisando o padrão do preço de procedimentos
proc_preco_nivel_provedor = proc_preco_nivel_provedor_func()
upper_outliers_nivel_provedor = upper_outliers_nivel_provedor_func(df_append_all.dropna(), df_subgrupo, proc_preco_nivel_provedor, filter_insurance, max_date, min_date)

total = len(upper_outliers_nivel_provedor)

if total == 1:
    st.write('###### Foi encontrado', total, 'procedimento outlier por prestador:')

    upper_outliers_nivel_provedor = upper_outliers_nivel_provedor.rename(columns={"provedor": "Provedor", "cod_tuss": "TUSS", "proc_tuss": "Descrição", "valor_pago": "Valor pago", "preço_limite": "Preço limite", 'id_pessoa': 'ID do Usuário', "dt_utilizacao":"Data"})
    up = upper_outliers_nivel_provedor[(upper_outliers_nivel_provedor['Valor pago'] > upper_outliers_nivel_provedor['Preço limite']) & (upper_outliers_nivel_provedor['Data'] <= max_date) & (upper_outliers_nivel_provedor['Data'] >= min_date)][['ID do Usuário', 'TUSS', 'Descrição', 'Provedor', 'Data', 'Valor pago', 'Preço limite', 'operadora']]
    up.loc[:, 'Valor pago'] = up['Valor pago'].map('{:.2f}'.format)
    up.loc[:, 'Preço limite'] = up['Preço limite'].map('{:.2f}'.format)
    up

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        up.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'num_format': '0.00'}) 
        worksheet.set_column('A:A', None, format1)  
        writer.close()
        processed_data = output.getvalue()
        return processed_data
    df_xlsx = to_excel(up)
    st.download_button(label='📥 Baixar Planilha',
                                    data=df_xlsx ,
                                    file_name= 'procedimentos_fora_do_padrao.xlsx')
elif total > 1:
    st.write('###### Foram encontrados', total, 'procedimentos outliers por prestador:')

    upper_outliers_nivel_provedor = upper_outliers_nivel_provedor.rename(columns={"provedor": "Provedor", "cod_tuss": "TUSS", "proc_tuss": "Descrição", "valor_pago": "Valor pago", "preço_limite": "Preço limite", 'id_pessoa': 'ID do Usuário', "dt_utilizacao":"Data", "operadora":"Operadora"})
    up = upper_outliers_nivel_provedor[(upper_outliers_nivel_provedor['Valor pago'] > upper_outliers_nivel_provedor['Preço limite']) & (upper_outliers_nivel_provedor['Data'] <= max_date) & (upper_outliers_nivel_provedor['Data'] >= min_date)][['ID do Usuário', 'TUSS', 'Descrição', 'Provedor', 'Data', 'Valor pago', 'Preço limite', 'Operadora']]
    # up.loc[:, 'Valor pago'] = up['Valor pago'].map('{:.2f}'.format)
    up['Valor pago'] = up['Valor pago'].astype(float)
    # up.loc[:, 'Preço limite'] = up['Preço limite'].map('{:.2f}'.format)
    up['Preço limite'] = up['Preço limite'].astype(float)
    up

    def to_excel(df):
        output = BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        up.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        format1 = workbook.add_format({'num_format': '0.00'}) 
        worksheet.set_column('A:A', None, format1)  
        writer.close()
        processed_data = output.getvalue()
        return processed_data
    df_xlsx = to_excel(up)
    st.download_button(label='📥 Baixar Planilha',
                                    data=df_xlsx ,
                                    file_name= 'procedimentos_fora_do_padrao.xlsx')
else:
    st.info('Nenhum alerta de possível inconsistência foi encontrado para esse período. \n\n**Uma notificação te avisará se algo diferente acontecer.**', icon="🌟")

ver_padrao = st.checkbox('Ver padrão de preço por prestador:')

if ver_padrao:
    proc_preco_nivel_provedor = proc_preco_nivel_provedor.rename(columns={"provedor": "Provedor", "cod_tuss": "TUSS", "min": "Mínimo", "média": "Média", "max": "Máximo", "desvio_padrão": "Desvio Padrão", "IQR": "FIQ", "preço_limite": "Preço limite", "qtd_realizacoes":"Realizações", "q1":"Q1", "q2":"Q2", "q3":"Q3"})
    proc_preco_nivel_provedor[['TUSS', 'Provedor', 'Realizações', 'Média', 'Mínimo', 'Q1', 'Q2', 'Q3', 'Máximo', 'Desvio Padrão', 'FIQ', 'Preço limite']]

'''----'''

st.write('\n\n')

st.markdown('## 💡 Ações recomendadas e boas práticas')

st.markdown('Ao lidar com procedimentos pagos pelo plano de saúde acima do padrão de preço cobrado pelo mesmo procedimento, aqui estão algumas boas práticas que podem ser consideradas:')

st.write('\n\n')

st.markdown('##### 1. Pesquisar alternativas:')
st.markdown('Procure por opções alternativas de prestadores de serviços médicos que ofereçam o mesmo procedimento a preços mais acessíveis, ou que sigam a mediana do valor pago. Isso pode incluir hospitais, clínicas ou profissionais de saúde que cobrem taxas mais razoáveis, sem comprometer a qualidade do atendimento.')

st.markdown('##### 2. Contatar o plano de saúde:')
st.markdown('Entre em contato com o plano de saúde para expressar sua preocupação e questionar o motivo dos valores acima do padrão. Solicite uma justificativa clara e detalhada para a cobrança e peça uma revisão do valor. Explique o contexto e forneça evidências se possível.')

st.markdown('##### 3. Negociação e iniciativas de economia:')
st.markdown('Seja proativo na negociação com o plano de saúde. Apresente argumentos sólidos baseados em pesquisas e referências de preços praticados no mercado (como as análises fornecidas acima), além de iniciativas de economia de preço colocadas em prática. \n\n Independente dos questionamentos, é possível construir um **guia de provedores de saúde recomendados**, com aqueles que tendem a cobrar a mediana de preço com a mesma qualidade para um mesmo procedimento na maior parte das realizações, e comunicar constantemente aos beneficiários sobre o mesmo. Isso ajudará a embasar suas reivindicações e negociações.')

st.markdown('##### 4. Obter suporte profissional:')
st.markdown('Em situações mais complexas ou desafiadoras, é recomendável buscar orientação e apoio, se possível, de profissionais de saúde ou instituições relacionadas à gestão de saúde populacional. Eles podem fornecer insights de iniciativas e auxiliar nas apresentações com o plano de saúde.')

st.write('\n\n')

st.warning('Lembre-se de que cada situação pode ser única e os processos podem variar dependendo do contrato. É importante manter registros de todas as comunicações, documentos e evidências relevantes ao lidar com procedimentos pagos acima do padrão de preço cobrado pelo plano de saúde.')
