import streamlit as st
import pandas as pd

@st.cache_resource(show_spinner="Identificando provedores sem identificação")
def prest_sem_id_func(df_append_all, filter_insurance, max_date, min_date):
    # 6 Prestador sem identificação

    prest_sem_id = df_append_all[['provedor', 'id_pessoa', 'sexo', 'cod_tuss', 'proc_operadora', 'dt_utilizacao', 'valor_pago', 'operadora']]
    prest_sem_id.loc[:, "id_pessoa"] = prest_sem_id["id_pessoa"].astype(int).astype(str)

    if filter_insurance != 'Todas':
        prest_sem_id = prest_sem_id[(prest_sem_id['operadora'] == filter_insurance) & ((prest_sem_id['provedor'].isnull()) | (prest_sem_id['provedor'] == '') | (prest_sem_id['provedor'] == ' ') | (prest_sem_id['provedor'] == '0') | (prest_sem_id['provedor'] == 0)) & (prest_sem_id['dt_utilizacao'] <= max_date) & (prest_sem_id['dt_utilizacao'] >= min_date)]
    else:
        prest_sem_id = prest_sem_id[(prest_sem_id['provedor'].isnull()) | (prest_sem_id['provedor'] == '') | (prest_sem_id['provedor'] == ' ') | (prest_sem_id['provedor'] == '0') | (prest_sem_id['provedor'] == 0) & (prest_sem_id['dt_utilizacao'] <= max_date) & (prest_sem_id['dt_utilizacao'] >= min_date)]

    prest_sem_id = pd.DataFrame(data=None, columns=['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora'])
    return prest_sem_id