import streamlit as st
import pandas as pd

@st.cache_resource(show_spinner="Identificando procedimentos indevidos para o sexo feminino")
def proc_fem_func(df_append):
    
    df_append = df_append.drop(df_append[df_append.id_pessoa == 802384001942].index)
    df_append = df_append.drop(df_append[df_append.id_pessoa == 801631264067].index)

    proc_male = pd.DataFrame(
            {
                "cod_tuss": [31203019,
    31203027,
    31203159,
    31203035,
    31203043,
    31203051,
    31203132,
    31203060,
    31203140,
    31203078,
    31203086,
    31203094,
    31203108,
    31203116,
    31203124,
    31206018,
    31206026,
    31206034,
    31206042,
    31206050,
    31206069,
    31206077,
    31206085,
    31206093,
    31206107,
    31206115,
    31206123,
    31206131,
    31206140,
    31206158,
    31206166,
    31206174,
    31206182,
    31206190,
    31206204,
    31206212,
    31206220,
    31206239,
    31206247,
    31206255,
    31206263],
            }
        )

    proc_male = proc_male['cod_tuss'].astype(int)
    proc_male = proc_male.reset_index()
    proc_male['sexo_proc'] = 'MALE'


    # Lendo procedimentos exclusivo entre usuários do sexo Feminino
    # proc_fem = pd.read_csv('proc_fem.csv')

    proc_fem = pd.DataFrame(
            {
            "cod_tuss": [30602017,
    30602335,
    30602025,
    30602033,
    30602122,
    30602041,
    30602050,
    30602068,
    30602076,
    30602084,
    30602092,
    30602106,
    30602114,
    30602130,
    30602343,
    30602149,
    30602157,
    30602165,
    30602173,
    30602181,
    30602203,
    30602190,
    30602262,
    30602211,
    30602238,
    30602246,
    30602254,
    30602289,
    30602297,
    30602300,
    30602319,
    30602327,
    31302017,
    31302130,
    31302025,
    31302033,
    31302041,
    31302050,
    31302068,
    31302076,
    31302084,
    31302092,
    31302106,
    31302114,
    31302122,
    31303013,
    31303021,
    31303030,
    31303196,
    31303056,
    31303064,
    31303072,
    31303285,
    31303080,
    31303200,
    31303102,
    31303110,
    31303129,
    31303218,
    31303226,
    31303234,
    31303170,
    31303188,
    31303293,
    31303269,
    31303137,
    31303242,
    31303145,
    31303250,
    31303153,
    31303161,
    31305032,
    31305016,
    31305024],
            }
        )

    proc_fem = proc_fem['cod_tuss'].astype(int)
    proc_fem = proc_fem.reset_index()
    proc_fem['sexo_proc'] = 'FEM'

    # Filtrando para usuários do sexo Feminino com sinistros no último ano
    df_fem = df_append[['id_pessoa', 'sexo', 'provedor', 'dt_utilizacao', 'cod_tuss', 'proc_tuss', 'subgrupo_tuss', 'classe', 'valor_pago']][(df_append['sexo'] == '0') & (df_append['ano_utilizacao'] == df_append['ano_utilizacao'].max())].reset_index()
    df_fem["cod_tuss"] = df_fem["cod_tuss"].astype(int)

    df_fem = df_fem.drop(df_fem[df_fem.id_pessoa == 802384001942].index)
    df_fem = df_fem.drop(df_fem[df_fem.id_pessoa == 801631264067].index)

    # Fazendo join com a lista de procedimentos masculinos, para classificar se há algum código masculino entre usuários do sexo feminino
    df_fem = df_fem[['id_pessoa', 'sexo', 'provedor', 'dt_utilizacao', 'cod_tuss', 'proc_tuss', 'subgrupo_tuss', 'classe', 'valor_pago']].set_index('cod_tuss').join(proc_male[['cod_tuss', 'sexo_proc']].set_index('cod_tuss'), how='left', on='cod_tuss').reset_index().dropna()

    # Deletando todos os sinistros entre mulheres que são do sexo feminino
    # df_fem = df_fem.dropna()

    return df_fem

@st.cache_data(show_spinner="Identificando procedimentos indevidos para o sexo masculino")
def proc_male_func(df_append):

    df_append = df_append.drop(df_append[df_append.id_pessoa == 802384001942].index)
    df_append = df_append.drop(df_append[df_append.id_pessoa == 801631264067].index)

    proc_fem = pd.DataFrame(
            {
            "cod_tuss": [30602017,
    30602335,
    30602025,
    30602033,
    30602122,
    30602041,
    30602050,
    30602068,
    30602076,
    30602084,
    30602092,
    30602106,
    30602114,
    30602130,
    30602343,
    30602149,
    30602157,
    30602165,
    30602173,
    30602181,
    30602203,
    30602190,
    30602262,
    30602211,
    30602238,
    30602246,
    30602254,
    30602289,
    30602297,
    30602300,
    30602319,
    30602327,
    31302017,
    31302130,
    31302025,
    31302033,
    31302041,
    31302050,
    31302068,
    31302076,
    31302084,
    31302092,
    31302106,
    31302114,
    31302122,
    31303013,
    31303021,
    31303030,
    31303196,
    31303056,
    31303064,
    31303072,
    31303285,
    31303080,
    31303200,
    31303102,
    31303110,
    31303129,
    31303218,
    31303226,
    31303234,
    31303170,
    31303188,
    31303293,
    31303269,
    31303137,
    31303242,
    31303145,
    31303250,
    31303153,
    31303161,
    31305032,
    31305016,
    31305024],
            }
        )

    proc_fem = proc_fem['cod_tuss'].astype(int)
    proc_fem = proc_fem.reset_index()
    proc_fem['sexo_proc'] = 'FEM'
    
    # Lendo procedimentos exclusivo entre usuários do sexo Masculino
    # proc_male = pd.read_csv('proc_male.csv')

    proc_male = pd.DataFrame(
            {
                "cod_tuss": [31203019,
    31203027,
    31203159,
    31203035,
    31203043,
    31203051,
    31203132,
    31203060,
    31203140,
    31203078,
    31203086,
    31203094,
    31203108,
    31203116,
    31203124,
    31206018,
    31206026,
    31206034,
    31206042,
    31206050,
    31206069,
    31206077,
    31206085,
    31206093,
    31206107,
    31206115,
    31206123,
    31206131,
    31206140,
    31206158,
    31206166,
    31206174,
    31206182,
    31206190,
    31206204,
    31206212,
    31206220,
    31206239,
    31206247,
    31206255,
    31206263],
            }
        )

    proc_male = proc_male['cod_tuss'].astype(int)
    proc_male = proc_male.reset_index()
    proc_male['sexo_proc'] = 'MALE'

    # Filtrando para usuários do sexo Masculino com sinistros no último ano
    df_male = df_append[['id_pessoa', 'sexo', 'provedor', 'dt_utilizacao', 'cod_tuss', 'proc_tuss', 'subgrupo_tuss', 'classe', 'valor_pago']][(df_append['sexo'] == '1') & (df_append['ano_utilizacao'] == df_append['ano_utilizacao'].max())].reset_index()
    df_male["cod_tuss"] = df_male["cod_tuss"].astype(int)

    # Fazendo join com a lista de procedimentos femininos, para classificar se há algum código feminino entre usuários do sexo masculino
    df_male = df_male[['id_pessoa', 'sexo', 'provedor', 'dt_utilizacao', 'cod_tuss', 'proc_tuss', 'subgrupo_tuss', 'classe', 'valor_pago']].set_index('cod_tuss').join(proc_fem[['cod_tuss', 'sexo_proc']].set_index('cod_tuss'), how='left', on='cod_tuss').reset_index().dropna()

    # Deletando todos os sinistros entre homens que são do sexo masculino
    # df_male = df_male.dropna()

    return df_male