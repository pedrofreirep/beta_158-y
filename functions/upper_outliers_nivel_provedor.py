import streamlit as st
import pandas as pd
from st_files_connection import FilesConnection

@st.cache_resource(show_spinner="Identificando procedimentos acima do padrão de gastos")
def proc_preco_nivel_provedor_func():
    # 5 Analisando o padrão do preço de procedimentos
    conn = st.connection('s3', type=FilesConnection)
    proc_preco_nivel_provedor = conn.read("df-for-mvps/17/158/jun-2023/proc_preco_nivel_provedor.csv", input_format="csv").iloc[1:]
    proc_preco_nivel_provedor.columns = ["new_index", "provedor", "cod_tuss", "qtd_realizacoes", "média", "desvio_padrão", "min", "q1", "q2", "q3", "max"]

    # Classificando o tipo de cada variável
    proc_preco_nivel_provedor['média'] = proc_preco_nivel_provedor['média'].astype(float)
    proc_preco_nivel_provedor['q1'] = proc_preco_nivel_provedor['q1'].astype(float)
    proc_preco_nivel_provedor['q2'] = proc_preco_nivel_provedor['q2'].astype(float)
    proc_preco_nivel_provedor['q3'] = proc_preco_nivel_provedor['q3'].astype(float)
    proc_preco_nivel_provedor['desvio_padrão'] = proc_preco_nivel_provedor['desvio_padrão'].fillna(0)
    proc_preco_nivel_provedor['desvio_padrão'] = proc_preco_nivel_provedor['desvio_padrão'].astype(float)
    proc_preco_nivel_provedor['min'] = proc_preco_nivel_provedor['min'].astype(float)
    proc_preco_nivel_provedor['max'] = proc_preco_nivel_provedor['max'].astype(float)
    proc_preco_nivel_provedor["cod_tuss"] = proc_preco_nivel_provedor["cod_tuss"].astype(int).astype(str)
    
    proc_preco_nivel_provedor['IQR'] = ((proc_preco_nivel_provedor['q3']) - proc_preco_nivel_provedor['q1'])
    proc_preco_nivel_provedor['outilier_range'] = ((proc_preco_nivel_provedor['q3'] + (1.5*(proc_preco_nivel_provedor['IQR'])))*1.23).round(2)
    proc_preco_nivel_provedor.columns = ["new_index", "provedor", "cod_tuss", "qtd_realizacoes", "média", "desvio_padrão", "min", "q1", "q2", "q3", "max", "IQR", "preço_limite"]
    
    # proc_preco_nivel_provedor[(proc_preco_nivel_provedor['provedor'] == 'LUCARELLI E LOPES SERVICOS MEDICOS') & (proc_preco_nivel_provedor['cod_tuss'] == '10101012') & (proc_preco_nivel_provedor['cod_tuss'] == 'max') == 800]['preço_limite'] == 799

    return proc_preco_nivel_provedor

def upper_outliers_nivel_provedor_func(df_append, df_subgrupo, proc_preco_nivel_provedor, filter_insurance, max_date, min_date):

    # Contagem de repetições de procedimentos realizados pelo mesmo prestador com o mesmo preço e mesma data
    upper_outliers_nivel_provedor = df_append.groupby(['HashCliente', 'provedor', 'cod_tuss', 'valor_pago', 'dt_utilizacao', 'operadora', 'id_pessoa']).count().reset_index()
    # upper_outliers_nivel_provedor = upper_outliers_nivel_provedor.reset_index()
    upper_outliers_nivel_provedor = upper_outliers_nivel_provedor[['provedor', 'cod_tuss', 'valor_pago', 'id_pessoa', 'dt_utilizacao', 'operadora']].rename(columns={'HashCliente': "repeticoes"})

    upper_outliers_nivel_provedor["cod_tuss"] = upper_outliers_nivel_provedor["cod_tuss"].astype(int).astype(str)
    df_subgrupo["cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)

    # Classificação padrão TUSS
    upper_outliers_nivel_provedor = upper_outliers_nivel_provedor.set_index('cod_tuss').join(df_subgrupo[['cod_tuss', 'proc_tuss', 'classe']].set_index('cod_tuss'), how='left', on='cod_tuss').reset_index()

    # Combinação da quantidade de procedimentos realizados pelo prestador, com o preço limite de cada procedimento
    upper_outliers_nivel_provedor = upper_outliers_nivel_provedor.merge(proc_preco_nivel_provedor[['cod_tuss', 'provedor', 'preço_limite']], on=["cod_tuss", "provedor"])

    # if filter_insurance != 'Todas':
    #     upper_outliers_nivel_provedor = upper_outliers_nivel_provedor[(upper_outliers_nivel_provedor['operadora'] == filter_insurance) & ((upper_outliers_nivel_provedor['valor_pago'] > upper_outliers_nivel_provedor['preço_limite']) & (upper_outliers_nivel_provedor['dt_utilizacao'] <= max_date) & (upper_outliers_nivel_provedor['dt_utilizacao'] >= min_date))][['id_pessoa', 'cod_tuss', 'proc_tuss', 'provedor', 'dt_utilizacao', 'valor_pago', 'preço_limite', 'operadora']]
    # else:
    #     upper_outliers_nivel_provedor = upper_outliers_nivel_provedor[(upper_outliers_nivel_provedor['valor_pago'] > upper_outliers_nivel_provedor['preço_limite']) & (upper_outliers_nivel_provedor['dt_utilizacao'] <= max_date) & (upper_outliers_nivel_provedor['dt_utilizacao'] >= min_date)][['id_pessoa', 'cod_tuss', 'proc_tuss', 'provedor', 'dt_utilizacao', 'valor_pago', 'preço_limite', 'operadora']]


    # if filter_insurance == 'Todas':
    #     insurance_type_filter = upper_outliers_nivel_provedor
    # else:
    #     insurance_type_filter = upper_outliers_nivel_provedor[upper_outliers_nivel_provedor['operadora'] == filter_insurance]

    # upper_outliers_nivel_provedor = insurance_type_filter

    # Identificando procedimentos acima do valor limite de cada procedimento para cada prestador na data seelecionada pelo usuário
    upper_outliers_nivel_provedor = upper_outliers_nivel_provedor[(upper_outliers_nivel_provedor['valor_pago'] > upper_outliers_nivel_provedor['preço_limite']) & (upper_outliers_nivel_provedor['dt_utilizacao'] <= max_date) & (upper_outliers_nivel_provedor['dt_utilizacao'] >= min_date)][['id_pessoa', 'cod_tuss', 'proc_tuss', 'provedor', 'dt_utilizacao', 'valor_pago', 'preço_limite', 'operadora']]
    return upper_outliers_nivel_provedor
