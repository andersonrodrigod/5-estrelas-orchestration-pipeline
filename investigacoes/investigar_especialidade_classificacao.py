# -*- coding: utf-8 -*-
from pathlib import Path

import pandas as pd

arquivo_base = Path('data/5_estrelas_fevereiro_tratado.csv')
pasta_saida = Path('saida_investigacao')

pasta_saida.mkdir(exist_ok=True)

grupos_local = [
    {
        'nome_grupo': 'CRED_ATEND EMERGENCIA',
        'nome_lista': 'lista_cred_atend_emergencia',
        'palavra_filtro': None,
        'tipo_igual': 1,
        'tipo_diferente': None,
        'contratacao_igual': 'rede credenciada',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 1',
        'descricao_contratacao': "CONTRATACAO igual a 'rede credenciada'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'HOSPITALAR',
        'nome_lista': 'lista_hospitalar',
        'palavra_filtro': None,
        'tipo_igual': 1,
        'tipo_diferente': None,
        'contratacao_igual': 'rede propria',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 1',
        'descricao_contratacao': "CONTRATACAO igual a 'rede propria'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'QUALIVIDA',
        'nome_lista': 'lista_qualivida',
        'palavra_filtro': 'QUALIVIDA|QUALIVITA',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': 'qualivida|qualivita',
        'filtro_local_diferente': None,
        'filtro_especialidade': 'qualivida|qualivita',
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'QUALIVIDA' ou 'QUALIVITA'",
        'descricao_especialidade': "ESPECIALIDADE contem 'QUALIVIDA' ou 'QUALIVITA'",
    },
    {
        'nome_grupo': 'QUALIVIDA',
        'nome_lista': 'lista_qualivida',
        'palavra_filtro': 'SINTA-SE|SINTASE|SINTA SE|VIVER BEM',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': 'sinta-se|sintase|sinta se|viver bem',
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'SINTA-SE', 'SINTASE', 'SINTA SE' ou 'VIVER BEM'",
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'NASCER BEM',
        'nome_lista': 'lista_nascer_bem',
        'palavra_filtro': 'NB|RISCO',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': r'\bnb\b|risco',
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'NB' ou 'RISCO'",
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'TELEMEDICINA',
        'nome_lista': 'lista_telemedicina',
        'palavra_filtro': 'TELEM',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': 'telemed|telemedicina|telem',
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'TELEM'",
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'PRODUTO COORDENADO',
        'nome_lista': 'lista_produto_coordenado',
        'palavra_filtro': 'NOSSO MEDICO|NOTRELIFE|NUCLEO MF|PROD. COORD|PROD.COORD',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': 'nosso medico|notrelife|nucleo mf|prod. coord|prod.coord',
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'NOSSO MEDICO', 'NOTRELIFE', 'NUCLEO MF', 'PROD. COORD' ou 'PROD.COORD'",
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'CASE',
        'nome_lista': 'lista_case',
        'palavra_filtro': 'CASE',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': 'case',
        'filtro_local_diferente': None,
        'filtro_especialidade': 'case',
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'case'",
        'descricao_especialidade': "ESPECIALIDADE contem 'case'",
    },
    {
        'nome_grupo': 'TRANSPLANTE',
        'nome_lista': 'lista_transplante',
        'palavra_filtro': 'TRANSPL',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': 'transpl',
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "sem filtro em LOCAL",
        'descricao_especialidade': "ESPECIALIDADE contem 'transpl'",
    },
    {
        'nome_grupo': 'TEA',
        'nome_lista': 'lista_tea',
        'palavra_filtro': 'TEA',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': 'tea',
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'tea'",
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'GESTAR BEM',
        'nome_lista': 'lista_gestar_bem',
        'palavra_filtro': 'GESTAR|GESTAR BEM|PGS',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': 'gestar|gestar bem|pgs',
        'filtro_local_diferente': None,
        'filtro_especialidade': 'gestar|gestar bem',
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'gestar', 'gestar bem' ou 'pgs'",
        'descricao_especialidade': "ESPECIALIDADE contem 'gestar', 'gestar bem' ou 'pgs'",
    },
    {
        'nome_grupo': 'TELECONSULTA',
        'nome_lista': 'lista_teleconsulta',
        'palavra_filtro': 'PGC|TRANSPLANTE|CASE',
        'tipo_igual': 6,
        'tipo_diferente': None,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': 'pgc|transplante|case',
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "ESPECIALIDADE contem 'pgc', 'transplante' ou 'case'",
    },
    {
        'nome_grupo': 'TELECONSULTA ELETIVA',
        'nome_lista': 'lista_teleconsulta_eletiva',
        'palavra_filtro': None,
        'tipo_igual': 6,
        'tipo_diferente': None,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': 'pgc|transplante|case',
        'descricao_tipo': 'TIPO igual a 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "ESPECIALIDADE diferente de 'pgc', 'transplante' ou 'case'",
    },
    {
        'nome_grupo': 'TELECONSULTA URGENCIA', # ADICIONAR ACENTO NA PLANILHA
        'nome_lista': 'lista_teleconsulta_urgencia',
        'palavra_filtro': None,
        'tipo_igual': 7,
        'tipo_diferente': None,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 7',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'MED PREV', 
        'nome_lista': 'lista_med_prev',
        'palavra_filtro': None,
        'tipo_igual': 4,
        'tipo_diferente': None,
        'contratacao_igual': 'rede propria',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 4',
        'descricao_contratacao': "CONTRATACAO igual a 'rede propria'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'CRED_TRATAMENTO',
        'nome_lista': 'lista_cred_tratamento',
        'palavra_filtro': None,
        'tipo_igual': 4,
        'tipo_diferente': None,
        'contratacao_igual': 'rede credenciada',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 4',
        'descricao_contratacao': "CONTRATACAO igual a 'rede credenciada'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'MED PREV', 
        'nome_lista': 'lista_med_prev_tipo_2',
        'palavra_filtro': 'MED PREV',
        'tipo_igual': 2,
        'tipo_diferente': None,
        'contratacao_igual': 'rede propria',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': 'med prev',
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 2',
        'descricao_contratacao': "CONTRATACAO igual a 'rede propria'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'HAPCLINICA', 
        'nome_lista': 'lista_hapclinica',
        'palavra_filtro': None,
        'tipo_igual': 2,
        'tipo_diferente': None,
        'contratacao_igual': 'rede propria',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': 'med prev',
        'descricao_tipo': 'TIPO igual a 2',
        'descricao_contratacao': "CONTRATACAO igual a 'rede propria'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'ODONTOLOGIA', 
        'nome_lista': 'lista_odontologia',
        'palavra_filtro': None,
        'tipo_igual': 5,
        'tipo_diferente': None,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 5',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'ODONTOLOGIA', 
        'nome_lista': 'lista_odontologia_2',
        'palavra_filtro': 'ODONT',
        'tipo_igual': None,
        'tipo_diferente': None,
        'contratacao_igual': None,
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': 'odont',
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a None',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'CRED_ATEND ELETIVO', 
        'nome_lista': 'lista_cred_atend_eletivo',
        'palavra_filtro': 'ODONT',
        'tipo_igual': 3,
        'tipo_diferente': None,
        'contratacao_igual': 'rede credenciada',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 3',
        'descricao_contratacao': "CONTRATACAO igual a 'rede credenciada'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'CRED_ATEND ELETIVO', 
        'nome_lista': 'lista_cred_atend_eletivo',
        'palavra_filtro': 'ODONT',
        'tipo_igual': 3,
        'tipo_diferente': None,
        'contratacao_igual': 'rede credenciada',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 3',
        'descricao_contratacao': "CONTRATACAO igual a 'rede credenciada'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'LABORATORIO', # AQUI TEM ACENTO NA PLANILHA 
        'nome_lista': 'lista_laboratorio',
        'palavra_filtro': None,
        'tipo_igual': 8,
        'tipo_diferente': None,
        'contratacao_igual': 'rede propria',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 8',
        'descricao_contratacao': "CONTRATACAO igual a 'rede propria'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'CRED_LABORATORIO',  # AQUI TEM ACENTO NA PLANILHA 
        'nome_lista': 'lista_cred_laboratorio',
        'palavra_filtro': None,
        'tipo_igual': 8,
        'tipo_diferente': None,
        'contratacao_igual': 'rede credenciada',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 8',
        'descricao_contratacao': "CONTRATACAO igual a 'rede credenciada'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'VIDA IMAGEM',  # AQUI TEM ACENTO NA PLANILHA 
        'nome_lista': 'lista_vida_imagem',
        'palavra_filtro': None,
        'tipo_igual': 9,
        'tipo_diferente': None,
        'contratacao_igual': 'rede propria',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 9',
        'descricao_contratacao': "CONTRATACAO igual a 'rede propria'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
    {
        'nome_grupo': 'CRED_EXAMES',  # AQUI TEM ACENTO NA PLANILHA 
        'nome_lista': 'lista_cred_exames',
        'palavra_filtro': None,
        'tipo_igual': 9,
        'tipo_diferente': None,
        'contratacao_igual': 'rede credenciada',
        'contratacao_diferente': None,
        'filtro_local': None,
        'filtro_local_diferente': None,
        'filtro_especialidade': None,
        'filtro_especialidade_diferente': None,
        'descricao_tipo': 'TIPO igual a 9',
        'descricao_contratacao': "CONTRATACAO igual a 'rede credenciada'",
        'descricao_local': 'sem filtro em LOCAL',
        'descricao_especialidade': "sem filtro em ESPECIALIDADE'",
    },
]

df = pd.read_csv(arquivo_base, low_memory=False)
df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
df['CONTRATACAO'] = df['CONTRATACAO'].astype('string').str.strip().str.lower()
df['LOCAL'] = df['LOCAL'].astype('string').str.strip()
df['ESPECIALIDADE'] = df['ESPECIALIDADE'].astype('string').str.strip()


def montar_contagem_coluna(dataframe, nome_coluna):
    contagem_especialidade = (
        dataframe[nome_coluna]
        .fillna('VAZIO')
        .value_counts()
        .reset_index()
    )
    contagem_especialidade.columns = [nome_coluna, 'QUANTIDADE']

    if not contagem_especialidade.empty:
        contagem_especialidade = contagem_especialidade.sort_values(
            by=['QUANTIDADE', nome_coluna],
            ascending=[False, True]
        ).reset_index(drop=True)

    return contagem_especialidade


def montar_lista_valores(dataframe, nome_coluna):
    contagem_coluna = montar_contagem_coluna(dataframe, nome_coluna)
    return contagem_coluna[nome_coluna].dropna().astype(str).tolist()


todas_as_contagens = []
linhas_txt = [
    'INVESTIGACAO DE ESPECIALIDADE PARA CLASSIFICACAO',
    '',
    f'Arquivo analisado: {arquivo_base}',
    f'Total de linhas da base: {len(df)}',
    '',
]

for grupo in grupos_local:
    df_tipo_filtrado = df.copy()

    if grupo['tipo_igual'] is not None:
        df_tipo_filtrado = df_tipo_filtrado[df_tipo_filtrado['TIPO'] == grupo['tipo_igual']].copy()

    if grupo['tipo_diferente'] is not None:
        df_tipo_filtrado = df_tipo_filtrado[df_tipo_filtrado['TIPO'] != grupo['tipo_diferente']].copy()

    df_contratacao_filtrado = df_tipo_filtrado.copy()

    if grupo['contratacao_igual']:
        df_contratacao_filtrado = df_contratacao_filtrado[
            df_contratacao_filtrado['CONTRATACAO'] == grupo['contratacao_igual']
        ].copy()

    if grupo['contratacao_diferente']:
        df_contratacao_filtrado = df_contratacao_filtrado[
            df_contratacao_filtrado['CONTRATACAO'] != grupo['contratacao_diferente']
        ].copy()

    df_local_filtrado = df_contratacao_filtrado.copy()

    if grupo['filtro_local']:
        filtro_local = df_local_filtrado['LOCAL'].str.contains(
            grupo['filtro_local'],
            case=False,
            na=False,
            regex=True
        )
        df_local_filtrado = df_local_filtrado[filtro_local].copy()

    if grupo['filtro_local_diferente']:
        filtro_local_diferente = df_local_filtrado['LOCAL'].str.contains(
            grupo['filtro_local_diferente'],
            case=False,
            na=False,
            regex=True
        )
        df_local_filtrado = df_local_filtrado[~filtro_local_diferente].copy()

    if grupo['filtro_especialidade']:
        filtro_especialidade = df_local_filtrado['ESPECIALIDADE'].str.contains(
            grupo['filtro_especialidade'],
            case=False,
            na=False,
            regex=True
        )
        df_final_filtrado = df_local_filtrado[filtro_especialidade].copy()
    else:
        df_final_filtrado = df_local_filtrado.copy()

    if grupo['filtro_especialidade_diferente']:
        filtro_especialidade_diferente = df_final_filtrado['ESPECIALIDADE'].str.contains(
            grupo['filtro_especialidade_diferente'],
            case=False,
            na=False,
            regex=True
        )
        df_final_filtrado = df_final_filtrado[~filtro_especialidade_diferente].copy()

    contagem_especialidade = montar_contagem_coluna(df_final_filtrado, 'ESPECIALIDADE')
    contagem_especialidade['GRUPO'] = grupo['nome_grupo']
    contagem_especialidade['NOME_LISTA'] = grupo['nome_lista']
    contagem_especialidade['PALAVRA_FILTRO'] = grupo['palavra_filtro']
    contagem_especialidade['TIPO_FILTRO'] = grupo['descricao_tipo']
    contagem_especialidade['CONTRATACAO_FILTRO'] = grupo['descricao_contratacao']
    todas_as_contagens.append(contagem_especialidade)

    lista_local = montar_lista_valores(df_local_filtrado, 'LOCAL')
    lista_especialidade = montar_lista_valores(df_final_filtrado, 'ESPECIALIDADE')

    linhas_txt.append(f"GRUPO: {grupo['nome_grupo']}")
    linhas_txt.append(f"PALAVRA_FILTRO: {grupo['palavra_filtro']}")
    linhas_txt.append(f"- filtro TIPO: {grupo['descricao_tipo']}")
    linhas_txt.append(f"- filtro CONTRATACAO: {grupo['descricao_contratacao']}")
    linhas_txt.append(f"- filtro LOCAL: {grupo['descricao_local']}")
    linhas_txt.append(f"- filtro ESPECIALIDADE: {grupo['descricao_especialidade']}")
    linhas_txt.append(f"- total apos filtro TIPO: {len(df_tipo_filtrado)}")
    linhas_txt.append(f"- total apos filtro CONTRATACAO: {len(df_contratacao_filtrado)}")
    linhas_txt.append(f"- total apos filtro LOCAL: {len(df_local_filtrado)}")
    linhas_txt.append(f"- total final do grupo: {len(df_final_filtrado)}")
    linhas_txt.append('')
    
    if grupo['filtro_local']:
        linhas_txt.append(f"{grupo['nome_lista']}_filtro_local = [")

        for valor in lista_local:
            linhas_txt.append(f"    '{valor}',")

        linhas_txt.append(']')
        linhas_txt.append('')

    if grupo['filtro_especialidade']:
        linhas_txt.append(f"{grupo['nome_lista']}_filtro_especialidade = [")

        for valor in lista_especialidade:
            linhas_txt.append(f"    '{valor}',")

        linhas_txt.append(']')
        linhas_txt.append('')

    if not grupo['filtro_local'] and not grupo['filtro_especialidade']:
        linhas_txt.append(f"{grupo['nome_lista']} = [")

        for valor in lista_especialidade:
            linhas_txt.append(f"    '{valor}',")

        linhas_txt.append(']')
        linhas_txt.append('')

arquivo_txt = pasta_saida / 'investigacao_especialidade_classificacao.txt'
arquivo_csv = pasta_saida / 'investigacao_especialidade_classificacao.csv'

with open(arquivo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

if todas_as_contagens:
    df_saida = pd.concat(todas_as_contagens, ignore_index=True)
else:
    df_saida = pd.DataFrame(
        columns=[
            'ESPECIALIDADE',
            'QUANTIDADE',
            'GRUPO',
            'NOME_LISTA',
            'PALAVRA_FILTRO',
            'TIPO_FILTRO',
            'CONTRATACAO_FILTRO'
        ]
    )

df_saida.to_csv(arquivo_csv, index=False, encoding='utf-8-sig')
