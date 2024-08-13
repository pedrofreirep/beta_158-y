import streamlit as st
import pandas as pd

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Janeiro")
def quebra_recibo_jan_23_func(media_para_quebra, df_append_all_amb):
    # Iniciando classificação de quebra de recibo para Janeiro, com primeira parcela em Dezembro do ano anterior
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    # quebra_recibos_jan_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '12')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '1'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_jan_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '12')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '1'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_jan_23 = quebra_recibos_jan_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_jan_23 = quebra_recibos_jan_23[(quebra_recibos_jan_23['dt_utilizacao'] > 1) & (quebra_recibos_jan_23['dt_utilizacao'] < 3)]
    quebra_recibos_jan_23["cod_tuss"] = quebra_recibos_jan_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_jan_23 = quebra_recibos_jan_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_jan_23 = quebra_recibos_jan_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_jan_23 = quebra_recibos_jan_23[(quebra_recibos_jan_23['valor_pago']*2) > (quebra_recibos_jan_23['valor_medio']*1.23)]
    quebra_recibos_jan_23 = quebra_recibos_jan_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_jan_23.loc[:, "valor_pago"] = quebra_recibos_jan_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_jan_23 = quebra_recibos_jan_23[(quebra_recibos_jan_23['valor_pago'] > 112)]

    quebra_recibos_jan_23 = quebra_recibos_jan_23.drop_duplicates()

    quebra_recibos_jan_23['cod_tuss'] = quebra_recibos_jan_23['cod_tuss'].astype(str)
    quebra_recibos_jan_23['cod_tuss'] = quebra_recibos_jan_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_jan_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Fevereiro")
def quebra_recibo_fev_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_fev_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max())) & (df_append_all_amb['mes_utilizacao'] == '1')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '2'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_fev_23 = quebra_recibos_fev_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_fev_23 = quebra_recibos_fev_23[(quebra_recibos_fev_23['dt_utilizacao'] > 1) & (quebra_recibos_fev_23['dt_utilizacao'] < 3)]
    quebra_recibos_fev_23["cod_tuss"] = quebra_recibos_fev_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_fev_23 = quebra_recibos_fev_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_fev_23 = quebra_recibos_fev_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_fev_23 = quebra_recibos_fev_23[(quebra_recibos_fev_23['valor_pago']*2) > (quebra_recibos_fev_23['valor_medio']*1.23)]
    quebra_recibos_fev_23 = quebra_recibos_fev_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_fev_23.loc[:, "valor_pago"] = quebra_recibos_fev_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_fev_23 = quebra_recibos_fev_23[(quebra_recibos_fev_23['valor_pago'] > 112)]

    quebra_recibos_fev_23 = quebra_recibos_fev_23.drop_duplicates()

    quebra_recibos_fev_23['cod_tuss'] = quebra_recibos_fev_23['cod_tuss'].astype(str)
    quebra_recibos_fev_23['cod_tuss'] = quebra_recibos_fev_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_fev_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Março")
def quebra_recibo_mar_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_mar_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '2')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '3'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_mar_23 = quebra_recibos_mar_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_mar_23 = quebra_recibos_mar_23[(quebra_recibos_mar_23['dt_utilizacao'] > 1) & (quebra_recibos_mar_23['dt_utilizacao'] < 3)]
    quebra_recibos_mar_23["cod_tuss"] = quebra_recibos_mar_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_mar_23 = quebra_recibos_mar_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_mar_23 = quebra_recibos_mar_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_mar_23 = quebra_recibos_mar_23[quebra_recibos_mar_23['valor_pago'] > (quebra_recibos_mar_23['valor_medio']*1.23)]
    quebra_recibos_mar_23 = quebra_recibos_mar_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_mar_23.loc[:, "valor_pago"] = quebra_recibos_mar_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_mar_23 = quebra_recibos_mar_23[(quebra_recibos_mar_23['valor_pago'] > 112)]

    quebra_recibos_mar_23 = quebra_recibos_mar_23.drop_duplicates()

    quebra_recibos_mar_23['cod_tuss'] = quebra_recibos_mar_23['cod_tuss'].astype(str)
    quebra_recibos_mar_23['cod_tuss'] = quebra_recibos_mar_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_mar_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Abril")
def quebra_recibo_abr_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_abr_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '3')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '4'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_abr_23 = quebra_recibos_abr_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_abr_23 = quebra_recibos_abr_23[(quebra_recibos_abr_23['dt_utilizacao'] > 1) & (quebra_recibos_abr_23['dt_utilizacao'] < 3)]
    quebra_recibos_abr_23["cod_tuss"] = quebra_recibos_abr_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_abr_23 = quebra_recibos_abr_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_abr_23 = quebra_recibos_abr_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_abr_23 = quebra_recibos_abr_23[quebra_recibos_abr_23['valor_pago'] > (quebra_recibos_abr_23['valor_medio']*1.23)]
    quebra_recibos_abr_23 = quebra_recibos_abr_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_abr_23.loc[:, "valor_pago"] = quebra_recibos_abr_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_abr_23 = quebra_recibos_abr_23[(quebra_recibos_abr_23['valor_pago'] > 112)]

    quebra_recibos_abr_23 = quebra_recibos_abr_23.drop_duplicates()

    quebra_recibos_abr_23['cod_tuss'] = quebra_recibos_abr_23['cod_tuss'].astype(str)
    quebra_recibos_abr_23['cod_tuss'] = quebra_recibos_abr_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_abr_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Maio")
def quebra_recibo_mai_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_mai_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '4')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '5'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_mai_23 = quebra_recibos_mai_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_mai_23 = quebra_recibos_mai_23[(quebra_recibos_mai_23['dt_utilizacao'] > 1) & (quebra_recibos_mai_23['dt_utilizacao'] < 3)]
    quebra_recibos_mai_23["cod_tuss"] = quebra_recibos_mai_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_mai_23 = quebra_recibos_mai_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_mai_23 = quebra_recibos_mai_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_mai_23 = quebra_recibos_mai_23[quebra_recibos_mai_23['valor_pago'] > (quebra_recibos_mai_23['valor_medio']*1.23)]
    quebra_recibos_mai_23 = quebra_recibos_mai_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_mai_23.loc[:, "valor_pago"] = quebra_recibos_mai_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_mai_23 = quebra_recibos_mai_23[(quebra_recibos_mai_23['valor_pago'] > 112)]

    quebra_recibos_mai_23 = quebra_recibos_mai_23.drop_duplicates()

    quebra_recibos_mai_23['cod_tuss'] = quebra_recibos_mai_23['cod_tuss'].astype(str)
    quebra_recibos_mai_23['cod_tuss'] = quebra_recibos_mai_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_mai_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Junho")
def quebra_recibo_jun_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_jun_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '5')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '6'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_jun_23 = quebra_recibos_jun_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_jun_23 = quebra_recibos_jun_23[(quebra_recibos_jun_23['dt_utilizacao'] > 1) & (quebra_recibos_jun_23['dt_utilizacao'] < 3)]
    quebra_recibos_jun_23["cod_tuss"] = quebra_recibos_jun_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_jun_23 = quebra_recibos_jun_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_jun_23 = quebra_recibos_jun_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_jun_23 = quebra_recibos_jun_23[quebra_recibos_jun_23['valor_pago'] > (quebra_recibos_jun_23['valor_medio']*1.23)]
    quebra_recibos_jun_23 = quebra_recibos_jun_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_jun_23.loc[:, "valor_pago"] = quebra_recibos_jun_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_jun_23 = quebra_recibos_jun_23[(quebra_recibos_jun_23['valor_pago'] > 112)]

    quebra_recibos_jun_23 = quebra_recibos_jun_23.drop_duplicates()

    quebra_recibos_jun_23['cod_tuss'] = quebra_recibos_jun_23['cod_tuss'].astype(str)
    quebra_recibos_jun_23['cod_tuss'] = quebra_recibos_jun_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_jun_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Julho")
def quebra_recibo_jul_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_jul_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '6')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '7'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_jul_23 = quebra_recibos_jul_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_jul_23 = quebra_recibos_jul_23[(quebra_recibos_jul_23['dt_utilizacao'] > 1) & (quebra_recibos_jul_23['dt_utilizacao'] < 3)]
    quebra_recibos_jul_23["cod_tuss"] = quebra_recibos_jul_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_jul_23 = quebra_recibos_jul_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_jul_23 = quebra_recibos_jul_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_jul_23 = quebra_recibos_jul_23[quebra_recibos_jul_23['valor_pago'] > (quebra_recibos_jul_23['valor_medio']*1.23)]
    quebra_recibos_jul_23 = quebra_recibos_jul_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_jul_23.loc[:, "valor_pago"] = quebra_recibos_jul_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_jul_23 = quebra_recibos_jul_23[(quebra_recibos_jul_23['valor_pago'] > 112)]

    quebra_recibos_jul_23 = quebra_recibos_jul_23.drop_duplicates()

    quebra_recibos_jul_23['cod_tuss'] = quebra_recibos_jul_23['cod_tuss'].astype(str)
    quebra_recibos_jul_23['cod_tuss'] = quebra_recibos_jul_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_jul_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Agosto")
def quebra_recibo_ago_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_ago_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '7')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '8'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_ago_23 = quebra_recibos_ago_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_ago_23 = quebra_recibos_ago_23[(quebra_recibos_ago_23['dt_utilizacao'] > 1) & (quebra_recibos_ago_23['dt_utilizacao'] < 3)]
    quebra_recibos_ago_23["cod_tuss"] = quebra_recibos_ago_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_ago_23 = quebra_recibos_ago_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_ago_23 = quebra_recibos_ago_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_ago_23 = quebra_recibos_ago_23[quebra_recibos_ago_23['valor_pago'] > (quebra_recibos_ago_23['valor_medio']*1.23)]
    quebra_recibos_ago_23 = quebra_recibos_ago_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_ago_23.loc[:, "valor_pago"] = quebra_recibos_ago_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_ago_23 = quebra_recibos_ago_23[(quebra_recibos_ago_23['valor_pago'] > 112)]

    quebra_recibos_ago_23 = quebra_recibos_ago_23.drop_duplicates()

    quebra_recibos_ago_23['cod_tuss'] = quebra_recibos_ago_23['cod_tuss'].astype(str)
    quebra_recibos_ago_23['cod_tuss'] = quebra_recibos_ago_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_ago_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Setembro")
def quebra_recibo_set_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_set_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '8')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '9'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_set_23 = quebra_recibos_set_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_set_23 = quebra_recibos_set_23[(quebra_recibos_set_23['dt_utilizacao'] > 1) & (quebra_recibos_set_23['dt_utilizacao'] < 3)]
    quebra_recibos_set_23["cod_tuss"] = quebra_recibos_set_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_set_23 = quebra_recibos_set_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_set_23 = quebra_recibos_set_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_set_23 = quebra_recibos_set_23[quebra_recibos_set_23['valor_pago'] > (quebra_recibos_set_23['valor_medio']*1.23)]
    quebra_recibos_set_23 = quebra_recibos_set_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_set_23.loc[:, "valor_pago"] = quebra_recibos_set_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_set_23 = quebra_recibos_set_23[(quebra_recibos_set_23['valor_pago'] > 112)]

    quebra_recibos_set_23 = quebra_recibos_set_23.drop_duplicates()

    quebra_recibos_set_23['cod_tuss'] = quebra_recibos_set_23['cod_tuss'].astype(str)
    quebra_recibos_set_23['cod_tuss'] = quebra_recibos_set_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_set_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Outubro")
def quebra_recibo_out_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_out_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '9')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '10'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_out_23 = quebra_recibos_out_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_out_23 = quebra_recibos_out_23[(quebra_recibos_out_23['dt_utilizacao'] > 1) & (quebra_recibos_out_23['dt_utilizacao'] < 3)]
    quebra_recibos_out_23["cod_tuss"] = quebra_recibos_out_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_out_23 = quebra_recibos_out_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_out_23 = quebra_recibos_out_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_out_23 = quebra_recibos_out_23[quebra_recibos_out_23['valor_pago'] > (quebra_recibos_out_23['valor_medio']*1.23)]
    quebra_recibos_out_23 = quebra_recibos_out_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_out_23.loc[:, "valor_pago"] = quebra_recibos_out_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_out_23 = quebra_recibos_out_23[(quebra_recibos_out_23['valor_pago'] > 112)]

    quebra_recibos_out_23 = quebra_recibos_out_23.drop_duplicates()

    quebra_recibos_out_23['cod_tuss'] = quebra_recibos_out_23['cod_tuss'].astype(str)
    quebra_recibos_out_23['cod_tuss'] = quebra_recibos_out_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_out_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Novembro")
def quebra_recibo_nov_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_nov_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '10')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '11'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_nov_23 = quebra_recibos_nov_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_nov_23 = quebra_recibos_nov_23[(quebra_recibos_nov_23['dt_utilizacao'] > 1) & (quebra_recibos_nov_23['dt_utilizacao'] < 3)]
    quebra_recibos_nov_23["cod_tuss"] = quebra_recibos_nov_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_nov_23 = quebra_recibos_nov_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_nov_23 = quebra_recibos_nov_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_nov_23 = quebra_recibos_nov_23[quebra_recibos_nov_23['valor_pago'] > (quebra_recibos_nov_23['valor_medio']*1.23)]
    quebra_recibos_nov_23 = quebra_recibos_nov_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_nov_23.loc[:, "valor_pago"] = quebra_recibos_nov_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_nov_23 = quebra_recibos_nov_23[(quebra_recibos_nov_23['valor_pago'] > 112)]

    quebra_recibos_nov_23 = quebra_recibos_nov_23.drop_duplicates()

    quebra_recibos_nov_23['cod_tuss'] = quebra_recibos_nov_23['cod_tuss'].astype(str)
    quebra_recibos_nov_23['cod_tuss'] = quebra_recibos_nov_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_nov_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Dezembro")
def quebra_recibo_dez_23_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_dez_23 = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '11')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '12'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_dez_23 = quebra_recibos_dez_23.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_dez_23 = quebra_recibos_dez_23[(quebra_recibos_dez_23['dt_utilizacao'] > 1) & (quebra_recibos_dez_23['dt_utilizacao'] < 3)]
    quebra_recibos_dez_23["cod_tuss"] = quebra_recibos_dez_23["cod_tuss"].astype(int).astype(str)
    quebra_recibos_dez_23 = quebra_recibos_dez_23.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_dez_23 = quebra_recibos_dez_23.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_dez_23 = quebra_recibos_dez_23[quebra_recibos_dez_23['valor_pago'] > (quebra_recibos_dez_23['valor_medio']*1.23)]
    quebra_recibos_dez_23 = quebra_recibos_dez_23[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_dez_23.loc[:, "valor_pago"] = quebra_recibos_dez_23["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_dez_23 = quebra_recibos_dez_23[(quebra_recibos_dez_23['valor_pago'] > 112)]

    quebra_recibos_dez_23 = quebra_recibos_dez_23.drop_duplicates()

    quebra_recibos_dez_23['cod_tuss'] = quebra_recibos_dez_23['cod_tuss'].astype(str)
    quebra_recibos_dez_23['cod_tuss'] = quebra_recibos_dez_23['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_dez_23

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Janeiro")
def quebra_recibo_jan_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_jan = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 2)) & (df_append_all_amb['mes_utilizacao'] == '12')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '1'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_jan = quebra_recibos_jan.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_jan = quebra_recibos_jan[(quebra_recibos_jan['dt_utilizacao'] > 1) & (quebra_recibos_jan['dt_utilizacao'] < 3)]
    quebra_recibos_jan["cod_tuss"] = quebra_recibos_jan["cod_tuss"].astype(int).astype(str)
    quebra_recibos_jan = quebra_recibos_jan.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_jan = quebra_recibos_jan.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_jan = quebra_recibos_jan[(quebra_recibos_jan['valor_pago']*2) > (quebra_recibos_jan['valor_medio']*1.23)]
    quebra_recibos_jan = quebra_recibos_jan[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_jan.loc[:, "valor_pago"] = quebra_recibos_jan["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_jan = quebra_recibos_jan[(quebra_recibos_jan['valor_pago'] > 112)]

    quebra_recibos_jan = quebra_recibos_jan.drop_duplicates()

    quebra_recibos_jan['cod_tuss'] = quebra_recibos_jan['cod_tuss'].astype(str)
    quebra_recibos_jan['cod_tuss'] = quebra_recibos_jan['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_jan

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Fevereiro")
def quebra_recibo_fev_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_fev = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '1')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '2'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_fev = quebra_recibos_fev.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_fev = quebra_recibos_fev[(quebra_recibos_fev['dt_utilizacao'] > 1) & (quebra_recibos_fev['dt_utilizacao'] < 3)]
    quebra_recibos_fev["cod_tuss"] = quebra_recibos_fev["cod_tuss"].astype(int).astype(str)
    quebra_recibos_fev = quebra_recibos_fev.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_fev = quebra_recibos_fev.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_fev = quebra_recibos_fev[(quebra_recibos_fev['valor_pago']*2) > (quebra_recibos_fev['valor_medio']*1.23)]
    quebra_recibos_fev = quebra_recibos_fev[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_fev.loc[:, "valor_pago"] = quebra_recibos_fev["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_fev = quebra_recibos_fev[(quebra_recibos_fev['valor_pago'] > 112)]

    quebra_recibos_fev = quebra_recibos_fev.drop_duplicates()

    quebra_recibos_fev['cod_tuss'] = quebra_recibos_fev['cod_tuss'].astype(str)
    quebra_recibos_fev['cod_tuss'] = quebra_recibos_fev['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_fev

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Março")
def quebra_recibo_mar_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_mar = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '2')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '3'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_mar = quebra_recibos_mar.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_mar = quebra_recibos_mar[(quebra_recibos_mar['dt_utilizacao'] > 1) & (quebra_recibos_mar['dt_utilizacao'] < 3)]
    quebra_recibos_mar["cod_tuss"] = quebra_recibos_mar["cod_tuss"].astype(int).astype(str)
    quebra_recibos_mar = quebra_recibos_mar.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_mar = quebra_recibos_mar.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_mar = quebra_recibos_mar[quebra_recibos_mar['valor_pago'] > (quebra_recibos_mar['valor_medio']*1.23)]
    quebra_recibos_mar = quebra_recibos_mar[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_mar.loc[:, "valor_pago"] = quebra_recibos_mar["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_mar = quebra_recibos_mar[(quebra_recibos_mar['valor_pago'] > 112)]

    quebra_recibos_mar = quebra_recibos_mar.drop_duplicates()

    quebra_recibos_mar['cod_tuss'] = quebra_recibos_mar['cod_tuss'].astype(str)
    quebra_recibos_mar['cod_tuss'] = quebra_recibos_mar['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_mar

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Abril")
def quebra_recibo_abr_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_abr = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '3')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '4'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_abr = quebra_recibos_abr.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_abr = quebra_recibos_abr[(quebra_recibos_abr['dt_utilizacao'] > 1) & (quebra_recibos_abr['dt_utilizacao'] < 3)]
    quebra_recibos_abr["cod_tuss"] = quebra_recibos_abr["cod_tuss"].astype(int).astype(str)
    quebra_recibos_abr = quebra_recibos_abr.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_abr = quebra_recibos_abr.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_abr = quebra_recibos_abr[(quebra_recibos_abr['valor_pago']*2) > (quebra_recibos_abr['valor_medio']*1.23)]
    quebra_recibos_abr = quebra_recibos_abr[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_abr.loc[:, "valor_pago"] = quebra_recibos_abr["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_abr = quebra_recibos_abr[(quebra_recibos_abr['valor_pago'] > 112)]

    quebra_recibos_abr = quebra_recibos_abr.drop_duplicates()

    quebra_recibos_abr['cod_tuss'] = quebra_recibos_abr['cod_tuss'].astype(str)
    quebra_recibos_abr['cod_tuss'] = quebra_recibos_abr['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_abr

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Maio")
def quebra_recibo_mai_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_mai = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '4')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '5'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_mai = quebra_recibos_mai.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_mai = quebra_recibos_mai[(quebra_recibos_mai['dt_utilizacao'] > 1) & (quebra_recibos_mai['dt_utilizacao'] < 3)]
    quebra_recibos_mai["cod_tuss"] = quebra_recibos_mai["cod_tuss"].astype(int).astype(str)
    quebra_recibos_mai = quebra_recibos_mai.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_mai = quebra_recibos_mai.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_mai = quebra_recibos_mai[(quebra_recibos_mai['valor_pago']*2) > (quebra_recibos_mai['valor_medio']*1.23)]
    quebra_recibos_mai = quebra_recibos_mai[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_mai.loc[:, "valor_pago"] = quebra_recibos_mai["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_mai = quebra_recibos_mai[(quebra_recibos_mai['valor_pago'] > 112)]

    quebra_recibos_mai = quebra_recibos_mai.drop_duplicates()

    quebra_recibos_mai['cod_tuss'] = quebra_recibos_mai['cod_tuss'].astype(str)
    quebra_recibos_mai['cod_tuss'] = quebra_recibos_mai['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_mai

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Junho")
def quebra_recibo_jun_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_jun = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '5')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '6'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_jun = quebra_recibos_jun.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_jun = quebra_recibos_jun[(quebra_recibos_jun['dt_utilizacao'] > 1) & (quebra_recibos_jun['dt_utilizacao'] < 3)]
    quebra_recibos_jun["cod_tuss"] = quebra_recibos_jun["cod_tuss"].astype(int).astype(str)
    quebra_recibos_jun = quebra_recibos_jun.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_jun = quebra_recibos_jun.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_jun = quebra_recibos_jun[(quebra_recibos_jun['valor_pago']*2) > (quebra_recibos_jun['valor_medio']*1.23)]
    quebra_recibos_jun = quebra_recibos_jun[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_jun.loc[:, "valor_pago"] = quebra_recibos_jun["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_jun = quebra_recibos_jun[(quebra_recibos_jun['valor_pago'] > 112)]

    quebra_recibos_jun['cod_tuss'] = quebra_recibos_jun['cod_tuss'].astype(str)
    quebra_recibos_jun['cod_tuss'] = quebra_recibos_jun['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_jun

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Julho")
def quebra_recibo_jul_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_jul = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '6')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '7'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_jul = quebra_recibos_jul.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_jul = quebra_recibos_jul[(quebra_recibos_jul['dt_utilizacao'] > 1) & (quebra_recibos_jul['dt_utilizacao'] < 3)]
    quebra_recibos_jul["cod_tuss"] = quebra_recibos_jul["cod_tuss"].astype(int).astype(str)
    quebra_recibos_jul = quebra_recibos_jul.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_jul = quebra_recibos_jul.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_jul = quebra_recibos_jul[(quebra_recibos_jul['valor_pago']*2) > (quebra_recibos_jul['valor_medio']*1.23)]
    quebra_recibos_jul = quebra_recibos_jul[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_jul.loc[:, "valor_pago"] = quebra_recibos_jul["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_jul = quebra_recibos_jul[(quebra_recibos_jul['valor_pago'] > 112)]

    quebra_recibos_jul = quebra_recibos_jul.drop_duplicates()

    quebra_recibos_jul['cod_tuss'] = quebra_recibos_jul['cod_tuss'].astype(str)
    quebra_recibos_jul['cod_tuss'] = quebra_recibos_jul['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_jul

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Agosto")
def quebra_recibo_ago_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_ago = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '7')) | ((df_append_all_amb['ano_utilizacao'] == df_append_all_amb['ano_utilizacao'].max()) & (df_append_all_amb['mes_utilizacao'] == '8'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_ago = quebra_recibos_ago.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_ago = quebra_recibos_ago[(quebra_recibos_ago['dt_utilizacao'] > 1) & (quebra_recibos_ago['dt_utilizacao'] < 3)]
    quebra_recibos_ago["cod_tuss"] = quebra_recibos_ago["cod_tuss"].astype(int).astype(str)
    quebra_recibos_ago = quebra_recibos_ago.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_ago = quebra_recibos_ago.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_ago = quebra_recibos_ago[(quebra_recibos_ago['valor_pago']*2) > (quebra_recibos_ago['valor_medio']*1.23)]
    quebra_recibos_ago = quebra_recibos_ago[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_ago.loc[:, "valor_pago"] = quebra_recibos_ago["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_ago = quebra_recibos_ago[(quebra_recibos_ago['valor_pago'] > 112)]

    quebra_recibos_ago = quebra_recibos_ago.drop_duplicates()

    quebra_recibos_ago['cod_tuss'] = quebra_recibos_ago['cod_tuss'].astype(str)
    quebra_recibos_ago['cod_tuss'] = quebra_recibos_ago['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_ago

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Setembro")
def quebra_recibo_set_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_set = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '8')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '9'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_set = quebra_recibos_set.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_set = quebra_recibos_set[(quebra_recibos_set['dt_utilizacao'] > 1) & (quebra_recibos_set['dt_utilizacao'] < 3)]
    quebra_recibos_set["cod_tuss"] = quebra_recibos_set["cod_tuss"].astype(int).astype(str)
    quebra_recibos_set = quebra_recibos_set.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_set = quebra_recibos_set.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_set = quebra_recibos_set[(quebra_recibos_set['valor_pago']*2) > (quebra_recibos_set['valor_medio']*1.23)]
    quebra_recibos_set = quebra_recibos_set[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_set.loc[:, "valor_pago"] = quebra_recibos_set["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_set = quebra_recibos_set[(quebra_recibos_set['valor_pago'] > 112)]

    quebra_recibos_set = quebra_recibos_set.drop_duplicates()

    quebra_recibos_set['cod_tuss'] = quebra_recibos_set['cod_tuss'].astype(str)
    quebra_recibos_set['cod_tuss'] = quebra_recibos_set['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_set

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Outubro")
def quebra_recibo_out_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_out = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '9')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '10'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_out = quebra_recibos_out.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_out = quebra_recibos_out[(quebra_recibos_out['dt_utilizacao'] > 1) & (quebra_recibos_out['dt_utilizacao'] < 3)]
    quebra_recibos_out["cod_tuss"] = quebra_recibos_out["cod_tuss"].astype(int).astype(str)
    quebra_recibos_out = quebra_recibos_out.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_out = quebra_recibos_out.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_out = quebra_recibos_out[(quebra_recibos_out['valor_pago']*2) > (quebra_recibos_out['valor_medio']*1.23)]
    quebra_recibos_out = quebra_recibos_out[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_out.loc[:, "valor_pago"] = quebra_recibos_out["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_out = quebra_recibos_out[(quebra_recibos_out['valor_pago'] > 112)]

    quebra_recibos_out = quebra_recibos_out.drop_duplicates()

    quebra_recibos_out['cod_tuss'] = quebra_recibos_out['cod_tuss'].astype(str)
    quebra_recibos_out['cod_tuss'] = quebra_recibos_out['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_out

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Novembro")
def quebra_recibo_nov_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_nov = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '10')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '11'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_nov = quebra_recibos_nov.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_nov = quebra_recibos_nov[(quebra_recibos_nov['dt_utilizacao'] > 1) & (quebra_recibos_nov['dt_utilizacao'] < 3)]
    quebra_recibos_nov["cod_tuss"] = quebra_recibos_nov["cod_tuss"].astype(int).astype(str)
    quebra_recibos_nov = quebra_recibos_nov.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_nov = quebra_recibos_nov.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_nov = quebra_recibos_nov[(quebra_recibos_nov['valor_pago']*2) > (quebra_recibos_nov['valor_medio']*1.23)]
    quebra_recibos_nov = quebra_recibos_nov[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_nov.loc[:, "valor_pago"] = quebra_recibos_nov["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_nov = quebra_recibos_nov[(quebra_recibos_nov['valor_pago'] > 112)]

    quebra_recibos_nov = quebra_recibos_nov.drop_duplicates()

    quebra_recibos_nov['cod_tuss'] = quebra_recibos_nov['cod_tuss'].astype(str)
    quebra_recibos_nov['cod_tuss'] = quebra_recibos_nov['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_nov

@st.cache_resource(show_spinner="Identificando procedimentos cobrados em Dezembro")
def quebra_recibo_dez_22_func(media_para_quebra, df_append_all_amb):
    # Filtro para sinistros realizados nos 2 meses consecutivos analisados e agrupamento para contagem dos sinistros
    quebra_recibos_dez = (df_append_all_amb[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'dt_utilizacao', 'operadora']][((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '11')) | ((df_append_all_amb['ano_utilizacao'] == (df_append_all_amb['ano_utilizacao'].max() - 1)) & (df_append_all_amb['mes_utilizacao'] == '12'))]).drop_duplicates().groupby(['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'operadora']).count()
    quebra_recibos_dez = quebra_recibos_dez.reset_index()

    # Filtro para considerar sinistros com pelo menos 2 cobranças dentro dos 2 meses realizados
    quebra_recibos_dez = quebra_recibos_dez[(quebra_recibos_dez['dt_utilizacao'] > 1) & (quebra_recibos_dez['dt_utilizacao'] < 3)]
    quebra_recibos_dez["cod_tuss"] = quebra_recibos_dez["cod_tuss"].astype(int).astype(str)
    quebra_recibos_dez = quebra_recibos_dez.merge(media_para_quebra, on=["cod_tuss", "provedor"]).reset_index()
    quebra_recibos_dez = quebra_recibos_dez.rename(columns={"dt_utilizacao": "qtd_cobrancas"})

    # Filtro para considerar sinistros com pelo menos 2 cobranças que, quando somadas, são maiores que o valor médio padrão daquele procedimento
    quebra_recibos_dez = quebra_recibos_dez[(quebra_recibos_dez['valor_pago']*2) > (quebra_recibos_dez['valor_medio']*1.23)]
    quebra_recibos_dez = quebra_recibos_dez[['id_pessoa', 'cod_tuss', 'provedor', 'valor_pago', 'qtd_cobrancas', 'operadora']]

    quebra_recibos_dez.loc[:, "valor_pago"] = quebra_recibos_dez["valor_pago"].astype(float).apply(lambda x:round(x,2))
    quebra_recibos_dez = quebra_recibos_dez[(quebra_recibos_dez['valor_pago'] > 112)]

    quebra_recibos_dez = quebra_recibos_dez.drop_duplicates()

    quebra_recibos_dez['cod_tuss'] = quebra_recibos_dez['cod_tuss'].astype(str)
    quebra_recibos_dez['cod_tuss'] = quebra_recibos_dez['cod_tuss'].apply(lambda x: x.replace(',',''))
    return quebra_recibos_dez
