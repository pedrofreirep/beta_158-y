import streamlit as st
import pandas as pd

@st.cache_resource(show_spinner="Identificando procedimentos com repetições acima do padrão de realização, por provedor")
def proc_duplicados_por_provedor_func(df_append_all, df_subgrupo, proc_describe, filter_insurance):
    # 10 Procedimentos, medicamentos, materiais, diárias ou taxas repetidos

    # Agrupando sinistros para cálculo de reepetições
    proc_repetido_b = df_append_all[['id_pessoa', 'dt_utilizacao', 'cod_tuss', 'HashCliente', 'valor_pago', 'operadora']]

    proc_repetido_b = proc_repetido_b.drop(proc_repetido_b[proc_repetido_b.cod_tuss == "99012324"].index)
    proc_repetido_b = proc_repetido_b.drop(proc_repetido_b[proc_repetido_b.cod_tuss == "99012499"].index)
    proc_repetido_b = proc_repetido_b.drop(proc_repetido_b[proc_repetido_b.cod_tuss == "99012340"].index)
    proc_repetido_b = proc_repetido_b.drop(proc_repetido_b[proc_repetido_b.cod_tuss == "10102019"].index)

    proc_repetido_b = proc_repetido_b.groupby(['id_pessoa', 'dt_utilizacao', 'cod_tuss', 'valor_pago', 'operadora']).count().reset_index().rename(columns={"HashCliente": "repeticoes"})
    # proc_repetido_b = proc_repetido_b.reset_index()

    # proc_repetido_b = proc_repetido_b.rename(columns={"HashCliente": "repeticoes"})
    proc_repetido_b.loc[:, "id_pessoa"] = proc_repetido_b["id_pessoa"].astype(int).astype(str)
    proc_repetido_b.loc[:, "cod_tuss"] = proc_repetido_b["cod_tuss"].astype(int).astype(str)
    df_subgrupo.loc[:, "cod_tuss"] = df_subgrupo["cod_tuss"].astype(int).astype(str)

    # Join para classificação e denominação padrão TUSS
    proc_repetido_b = proc_repetido_b.set_index('cod_tuss').merge(df_subgrupo[['cod_tuss', 'proc_tuss', 'classe']].set_index('cod_tuss'), how='left', on='cod_tuss').reset_index()
    proc_repetido_b = proc_repetido_b[['id_pessoa', 'dt_utilizacao', 'cod_tuss','proc_tuss', 'classe', 'repeticoes', 'valor_pago', 'operadora']].drop_duplicates()
    
    proc_describe.loc[:, "cod_tuss"] = proc_describe["cod_tuss"].astype(int).astype(str)

    # Merge para comparação com limite de outliers
    proc_repetido_b = proc_repetido_b.set_index('cod_tuss').merge(proc_describe[['cod_tuss', 'IQR', 'outlier_range']].set_index('cod_tuss'), how='left', on='cod_tuss').reset_index()

    if filter_insurance != 'Todas':
        proc_repetido_b = proc_repetido_b[proc_repetido_b['operadora'] == filter_insurance]
    else:
        pass
    
    return proc_repetido_b
