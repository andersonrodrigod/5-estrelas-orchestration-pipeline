# -*- coding: utf-8 -*-
from pathlib import Path

import pandas as pd

arquivo_base = Path('data/5_estrelas_fevereiro_tratado.csv')
pasta_saida = Path('saida_investigacao')

pasta_saida.mkdir(exist_ok=True)

grupos_local = [
    {
        'nome_grupo': 'QUALIVIDA',
        'nome_lista': 'lista_qualivida',
        'palavra_filtro': 'qualivida|qualivita',
        'filtro_local': 'qualivida|qualivita',
        'filtro_especialidade': 'qualivida|qualivita',
        'descricao_local': "LOCAL contem 'qualivida' ou 'qualivita'",
        'descricao_especialidade': "ESPECIALIDADE contem 'qualivida' ou 'qualivita'",
    },
    {
        'nome_grupo': 'QUALIVIDA',
        'nome_lista': 'lista_qualivida',
        'palavra_filtro': 'sinta-se|sintase',
        'filtro_local': 'sinta-se|sintase|sinta se',
        'filtro_especialidade': None,
        'descricao_local': "LOCAL contem 'sinta-se' ou 'sintase' ou 'sinta se'",
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
    {
        'nome_grupo': 'QUALIVIDA',
        'nome_lista': 'lista_qualivida',
        'palavra_filtro': 'viver bem',
        'filtro_local': 'viver bem',
        'filtro_especialidade': None,
        'descricao_local': "LOCAL contem 'viver bem'",
        'descricao_especialidade': 'sem filtro em ESPECIALIDADE',
    },
]

df = pd.read_csv(arquivo_base, low_memory=False)
df['LOCAL'] = df['LOCAL'].astype('string').str.strip()
df['ESPECIALIDADE'] = df['ESPECIALIDADE'].astype('string').str.strip()

todas_as_contagens = []
linhas_txt = [
    'INVESTIGACAO DE ESPECIALIDADE PARA CLASSIFICACAO',
    '',
    f'Arquivo analisado: {arquivo_base}',
    f'Total de linhas da base: {len(df)}',
    '',
    'GRUPO REAL: QUALIVIDA',
    '',
]

for grupo in grupos_local:
    filtro_local = df['LOCAL'].str.contains(
        grupo['filtro_local'],
        case=False,
        na=False,
        regex=True
    )
    df_local_filtrado = df[filtro_local].copy()

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
    todas_as_contagens.append(contagem_especialidade)

    lista_valores = contagem_especialidade['ESPECIALIDADE'].dropna().astype(str).tolist()

    linhas_txt.append(f"PALAVRA_FILTRO: {grupo['palavra_filtro']}")
    linhas_txt.append(f"- filtro LOCAL: {grupo['descricao_local']}")
    linhas_txt.append(f"- filtro ESPECIALIDADE: {grupo['descricao_especialidade']}")
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
    df_saida = pd.DataFrame(columns=['ESPECIALIDADE', 'QUANTIDADE', 'GRUPO', 'PALAVRA_FILTRO'])

df_saida.to_csv(arquivo_csv, index=False, encoding='utf-8-sig')
