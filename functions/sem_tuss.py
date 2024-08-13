import pandas as pd
import streamlit as st

@st.cache_resource(show_spinner="Identificando códigos de procedimentos sem padrão TUSS")
def sem_tuss_func(df_append_all, filter_insurance, max_date, min_date):
    # 3 Códigos sem padrão TUSS ou sem identificação

    # Filtrando apenas códigos sem uma classe padrão TUSS
    sem_tuss = df_append_all[df_append_all['classe'].isnull()][['id_pessoa', 'cod_tuss', 'proc_operadora', 'provedor', 'dt_utilizacao', 'valor_pago', 'operadora']]
    # sem_tuss = sem_tuss[['id_pessoa', 'cod_tuss', 'proc_operadora', 'provedor', 'dt_utilizacao', 'valor_pago', 'operadora']]

    df_amb = pd.read_csv('de-para-AMB-TUSS.csv', sep=';')
    df_amb = df_amb[['codigo_AMB_92', 'descricao_AMB_92']]
    df_amb = df_amb.rename(columns={"codigo_AMB_92": "cod_tuss", "descricao_AMB_92": "descricao_amb"}).drop_duplicates()
    # df_amb = df_amb.drop_duplicates()

    df_amb["cod_tuss"] = df_amb["cod_tuss"].astype(int).astype(str)
    sem_tuss["cod_tuss"] = sem_tuss["cod_tuss"].astype(int).astype(str)
    # sem_tuss['cod_tuss'] = sem_tuss['cod_tuss'].str.replace(".0",'', regex=True)

    sem_tuss = sem_tuss.set_index('cod_tuss').merge(df_amb.set_index('cod_tuss'), how='left', on='cod_tuss').reset_index()

    sem_tuss = sem_tuss[sem_tuss['descricao_amb'].isnull()]

    # sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "10101047"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "40403858"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "50000179"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "80071201"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "40314340"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "40324770"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "40314618"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "40324796"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "40324788"].index)
    sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "80019005"].index)
    # sem_tuss = sem_tuss.drop(sem_tuss[sem_tuss.cod_tuss == "80019013"].index)

    # Selecionando e classificando o tipo dos dados que serão usados

#     sem_tuss["cod_tuss"] = sem_tuss["cod_tuss"].astype(int).astype(str)
    sem_tuss["id_pessoa"] = sem_tuss["id_pessoa"].astype(int).astype(str)
    sem_tuss = sem_tuss.drop_duplicates(['cod_tuss', 'valor_pago'])

    if filter_insurance != 'Todas':
        sem_tuss = sem_tuss[(sem_tuss['operadora'] == filter_insurance) & ((sem_tuss['dt_utilizacao'] <= max_date) & (sem_tuss['dt_utilizacao'] >= min_date)) | (sem_tuss['dt_utilizacao'] == '0')]
    else:
        sem_tuss = sem_tuss[((sem_tuss['dt_utilizacao'] <= max_date) & (sem_tuss['dt_utilizacao'] >= min_date)) | (sem_tuss['dt_utilizacao'] == '0')]


    # if filter_insurance == 'Todas':
    #     insurance_type_filter = sem_tuss
    # else:
    #     insurance_type_filter = sem_tuss[sem_tuss['operadora'] == filter_insurance]

    # sem_tuss = insurance_type_filter

    # Filtro para identificar toda célula da coluna do cod_tuss sem um código padrão TUSS, dentro do período selecionado pelo usuário
    # sem_tuss = sem_tuss[((sem_tuss['dt_utilizacao'] <= max_date) & (sem_tuss['dt_utilizacao'] >= min_date)) | (sem_tuss['dt_utilizacao'] == '0')]
    return sem_tuss
