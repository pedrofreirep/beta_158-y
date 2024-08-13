import streamlit as st
import pandas as pd

@st.cache_resource(show_spinner="Identificando sessões acima do comum")
def psico_func(df_append):
    # 9 Beneficiários com sessões acima do comum

    # Beneficiários de psicoterapia com mais de 48 sessões, no mesmo ano:

    # Separando códigos TUSS de psicoterapia
    psicoterapia_ultimo_ano = df_append[['cod_tuss', 'id_pessoa', 'sexo', 'proc_tuss', 'subgrupo_tuss', 'classe', 'valor_pago', 'HashCliente', 'provedor', 'dt_utilizacao', 'ano_mes_utilizacao', 'ano_utilizacao']][((df_append['cod_tuss'] == 20104197) | (df_append['cod_tuss'] == 20104200) 
                            | (df_append['cod_tuss'] == 20104219) | (df_append['cod_tuss'] == 20104227) 
                            | (df_append['cod_tuss'] == 50000470) | (df_append['cod_tuss'] == 50000489) 
                            | (df_append['cod_tuss'] == 50000497) | (df_append['cod_tuss'] == 50000500)) & (df_append['ano_utilizacao'] == df_append['ano_utilizacao'].max())]

    # Filrando sinistros de psicoterapia realizados no último ano
    # psicoterapia_ultimo_ano = psicoterapia[psicoterapia['ano_utilizacao'] == psicoterapia['ano_utilizacao'].max()]

    # Contando o número de repetições de sinistros de psicoterapia realizados no último ano para cada pessoa
    psicoterapia_ultimo_ano = psicoterapia_ultimo_ano[['id_pessoa', 'sexo', 'cod_tuss', 'proc_tuss', 'dt_utilizacao', 'valor_pago']].groupby('id_pessoa').count().reset_index().rename(columns={"sexo": "repeticoes"})
    # psicoterapia_ultimo_ano = psicoterapia_ultimo_ano.reset_index()
    # psicoterapia_ultimo_ano = psicoterapia_ultimo_ano.rename(columns={"sexo": "repeticoes"})

    # Filrando o número de repetições de sinistros de psicoterapia para maior que 48
    psicoterapia_ultimo_ano = psicoterapia_ultimo_ano[['id_pessoa', 'repeticoes']][psicoterapia_ultimo_ano['repeticoes'] > 48]

    return psicoterapia_ultimo_ano


def fono_func(df_append):
    # Beneficiários de fonoaudiologia com mais de 18 sessões, no mesmo ano:

    # Separando códigos TUSS de fonoaudiologia
    fonoaudiologia_ultimo_ano = df_append[['cod_tuss', 'id_pessoa', 'sexo', 'proc_tuss', 'subgrupo_tuss', 'classe', 'valor_pago', 'HashCliente', 'provedor', 'dt_utilizacao', 'ano_mes_utilizacao', 'ano_utilizacao']][((df_append['cod_tuss'] == 50000586) | (df_append['cod_tuss'] == 50000594) 
                            | (df_append['cod_tuss'] == 50000608) | (df_append['cod_tuss'] == 50000616) 
                            | (df_append['cod_tuss'] == 50000624) | (df_append['cod_tuss'] == 50000632) 
                            | (df_append['cod_tuss'] == 50000640)) & df_append['ano_utilizacao'] == df_append['ano_utilizacao'].max()]

    # Filrando sinistros de fonoaudiologia realizados no último ano
    # fonoaudiologia_ultimo_ano = fonoaudiologia[fonoaudiologia['ano_utilizacao'] == fonoaudiologia['ano_utilizacao'].max()]

    # Contando o número de repetições de sinistros de fonoaudiologia realizados no último ano para cada pessoa
    fonoaudiologia_ultimo_ano = fonoaudiologia_ultimo_ano[['id_pessoa', 'sexo', 'cod_tuss', 'proc_tuss', 'dt_utilizacao', 'valor_pago']].groupby('id_pessoa').count().reset_index().rename(columns={"sexo": "repeticoes"})
    # fonoaudiologia_ultimo_ano = fonoaudiologia_ultimo_ano.reset_index()
    # fonoaudiologia_ultimo_ano = fonoaudiologia_ultimo_ano.rename(columns={"sexo": "repeticoes"})

    # Filrando o número de repetições de sinistros de fonoaudiologia para maior que 18
    fonoaudiologia_ultimo_ano = fonoaudiologia_ultimo_ano[['id_pessoa', 'repeticoes']][fonoaudiologia_ultimo_ano['repeticoes'] > 18]

    return fonoaudiologia_ultimo_ano