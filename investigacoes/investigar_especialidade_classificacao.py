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
        'filtro_local': None,
        'filtro_especialidade': None,
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
        'filtro_local': None,
        'filtro_especialidade': None,
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
        'filtro_local': 'qualivida|qualivita',
        'filtro_especialidade': 'qualivida|qualivita',
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
        'filtro_local': 'sinta-se|sintase|sinta se|viver bem',
        'filtro_especialidade': None,
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
        'filtro_local': r'\bnb\b|risco',
        'filtro_especialidade': None,
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
        'filtro_local': 'telemed|telemedicina|telem',
        'filtro_especialidade': None,
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
        'filtro_local': 'nosso medico|notrelife|nucleo mf|prod. coord|prod.coord',
        'filtro_especialidade': None,
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
        'filtro_local': 'case',
        'filtro_especialidade': 'case',
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'case'",
        'descricao_especialidade': "ESPECIALIDADE contem 'QUALIVIDA'",
    },
    {
        'nome_grupo': 'TRANSPLANTE',
        'nome_lista': 'lista_transplante',
        'palavra_filtro': 'TRANSPL',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'filtro_local': None,
        'filtro_especialidade': 'transpl',
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "sem filtro em LOCAL",
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'TEA',
        'nome_lista': 'lista_tea',
        'palavra_filtro': 'TEA',
        'tipo_igual': None,
        'tipo_diferente': 6,
        'contratacao_igual': None,
        'filtro_local': 'tea',
        'filtro_especialidade': None,
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
        'filtro_local': 'gestar|gestar bem|pgs',
        'filtro_especialidade': 'gestar|gestar bem',
        'descricao_tipo': 'TIPO diferente de 6',
        'descricao_contratacao': 'sem filtro em CONTRATACAO',
        'descricao_local': "LOCAL contem 'gestar', 'gestar bem' ou 'pgs'",
        'descricao_especialidade': "ESPECIALIDADE contem 'gestar', 'gestar bem' ou 'pgs'",
    },
]

df = pd.read_csv(arquivo_base, low_memory=False)
df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
df['CONTRATACAO'] = df['CONTRATACAO'].astype('string').str.strip().str.lower()
df['LOCAL'] = df['LOCAL'].astype('string').str.strip()
df['ESPECIALIDADE'] = df['ESPECIALIDADE'].astype('string').str.strip()

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

    df_local_filtrado = df_contratacao_filtrado.copy()

    if grupo['filtro_local']:
        filtro_local = df_local_filtrado['LOCAL'].str.contains(
            grupo['filtro_local'],
            case=False,
            na=False,
            regex=True
        )
        df_local_filtrado = df_local_filtrado[filtro_local].copy()

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

    contagem_especialidade = (
        df_final_filtrado['ESPECIALIDADE']
        .fillna('VAZIO')
        .value_counts()
        .reset_index()
    )
    contagem_especialidade.columns = ['ESPECIALIDADE', 'QUANTIDADE']

    if not contagem_especialidade.empty:
        contagem_especialidade = contagem_especialidade.sort_values(
            by=['QUANTIDADE', 'ESPECIALIDADE'],
            ascending=[False, True]
        ).reset_index(drop=True)

    contagem_especialidade['GRUPO'] = grupo['nome_grupo']
    contagem_especialidade['PALAVRA_FILTRO'] = grupo['palavra_filtro']
    contagem_especialidade['TIPO_FILTRO'] = grupo['descricao_tipo']
    contagem_especialidade['CONTRATACAO_FILTRO'] = grupo['descricao_contratacao']
    todas_as_contagens.append(contagem_especialidade)

    lista_valores = contagem_especialidade['ESPECIALIDADE'].dropna().astype(str).tolist()

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
    linhas_txt.append(f"{grupo['nome_lista']} = [")

    for valor in lista_valores:
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
        columns=['ESPECIALIDADE', 'QUANTIDADE', 'GRUPO', 'PALAVRA_FILTRO', 'TIPO_FILTRO', 'CONTRATACAO_FILTRO']
    )

df_saida.to_csv(arquivo_csv, index=False, encoding='utf-8-sig')
