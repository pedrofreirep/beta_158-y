import pandas as pd
import streamlit as st

@st.cache_resource(show_spinner="Identificando beneficiários sem identificação")
def bene_sem_id_func(df_append_all, filter_insurance, min_date, max_date):
    bene_sem_id = df_append_all[['id_pessoa', 'sexo', 'provedor', 'cod_tuss', 'proc_operadora', 'dt_utilizacao', 'valor_pago', 'operadora']]
#     bene_sem_id["id_pessoa"] = bene_sem_id["id_pessoa"].astype(int).astype(str)
    bene_sem_id.loc[:, "id_pessoa"] = bene_sem_id["id_pessoa"].astype(int).astype(str)

    # Filtro para identificar toda célula da coluna do ID do beneficiário sem um código específico, ou apenas 0 ou nulo
    if filter_insurance != 'Todas':
        bene_sem_id = bene_sem_id[(bene_sem_id['operadora'] == filter_insurance) & ((bene_sem_id['id_pessoa'].isnull()) | (bene_sem_id['id_pessoa'] == '') | (bene_sem_id['id_pessoa'] == ' ') | (bene_sem_id['id_pessoa'] == '0') | (bene_sem_id['id_pessoa'] == 0)) & (bene_sem_id['dt_utilizacao'] >= min_date) & (bene_sem_id['dt_utilizacao'] <= max_date)]
    else:
        bene_sem_id = bene_sem_id[(bene_sem_id['id_pessoa'].isnull()) | (bene_sem_id['id_pessoa'] == '') | (bene_sem_id['id_pessoa'] == ' ') | (bene_sem_id['id_pessoa'] == '0') | (bene_sem_id['id_pessoa'] == 0) & (bene_sem_id['dt_utilizacao'] >= min_date) & (bene_sem_id['dt_utilizacao'] <= max_date)]

    # if filter_insurance == 'Todas':
    #     insurance_type_filter = bene_sem_id
    # else:
    #     insurance_type_filter = bene_sem_id[bene_sem_id['operadora'] == filter_insurance]

    # bene_sem_id = insurance_type_filter

    # bene_sem_id = bene_sem_id[(bene_sem_id['id_pessoa'].isnull()) | (bene_sem_id['id_pessoa'] == '') | (bene_sem_id['id_pessoa'] == ' ') | (bene_sem_id['id_pessoa'] == '0') | (bene_sem_id['id_pessoa'] == 0) & (bene_sem_id['dt_utilizacao'] >= min_date) & (bene_sem_id['dt_utilizacao'] <= max_date)]
    return bene_sem_id
    # st.write(bene_sem_id)
