import streamlit as st
import pandas as pd

@st.cache_resource(show_spinner="Identificando procedimentos com repetições acima do padrão de realização")
def proc_duplicados_func(df_append_all, proc_describe, filter_insurance, max_date, min_date):
    # 4 Beneficiários com os mesmos procedimentos, para o mesmo prestador e no mesmo dia

    proc_duplicados = df_append_all[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'sexo', 'operadora']]

    proc_duplicados = proc_duplicados.drop(proc_duplicados[proc_duplicados.cod_tuss == "99012324"].index)
    proc_duplicados = proc_duplicados.drop(proc_duplicados[proc_duplicados.cod_tuss == "99012499"].index)
    proc_duplicados = proc_duplicados.drop(proc_duplicados[proc_duplicados.cod_tuss == "99012340"].index)
    proc_duplicados = proc_duplicados.drop(proc_duplicados[proc_duplicados.cod_tuss == "10102019"].index)

    # Contando repetições de procedimentos para uma mesma pessoa, para o mesmos prestador, na mesma data e com o mesmo valor
    proc_duplicados = proc_duplicados.groupby(['id_pessoa', 'cod_tuss', 'provedor', 'dt_utilizacao', 'valor_pago', 'operadora']).count().reset_index().rename(columns={"sexo": "repeticoes"})
    # proc_duplicados = proc_duplicados.reset_index()
    # proc_duplicados = proc_duplicados.rename(columns={"sexo": "repeticoes"})
    
    proc_duplicados.loc[:, "cod_tuss"] = proc_duplicados["cod_tuss"].astype(int).astype(str)
    proc_describe.loc[:, "cod_tuss"] = proc_describe["cod_tuss"].astype(int).astype(str)

    proc_duplicados = proc_duplicados.set_index('cod_tuss').merge(proc_describe[['cod_tuss', 'IQR', 'outlier_range']].set_index('cod_tuss'), how='left', on='cod_tuss').reset_index()

    proc_duplicados["id_pessoa"] = proc_duplicados["id_pessoa"].astype(int).astype(str)

    if filter_insurance != 'Todas':
        proc_duplicados = proc_duplicados[(proc_duplicados['operadora'] == filter_insurance) & (proc_duplicados['repeticoes'] > proc_duplicados['outlier_range']) & ((proc_duplicados['dt_utilizacao'] <= max_date) & (proc_duplicados['dt_utilizacao'] >= min_date))]
    else:
        proc_duplicados = proc_duplicados[(proc_duplicados['repeticoes'] > proc_duplicados['outlier_range']) & (proc_duplicados['dt_utilizacao'] <= max_date) & (proc_duplicados['dt_utilizacao'] >= min_date)]

    # if filter_insurance == 'Todas':
    #     insurance_type_filter = proc_duplicados
    # else:
    #     insurance_type_filter = proc_duplicados[proc_duplicados['operadora'] == filter_insurance]

    # proc_duplicados = insurance_type_filter

    # Filtro para encontrar todos registros com repetição maior que 1 no período selecionado pelo usuéario
    # proc_duplicados = proc_duplicados[(proc_duplicados['repeticoes'] > proc_duplicados['outlier_range']) & (proc_duplicados['dt_utilizacao'] <= max_date) & (proc_duplicados['dt_utilizacao'] >= min_date)]
    return proc_duplicados
