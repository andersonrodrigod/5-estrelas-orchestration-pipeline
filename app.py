# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path

print('Lendo o arquivo CSV...')
df = pd.read_csv('data/5_estrelas_fevereiro.csv')

print('Quantidade total de linhas:')
print(len(df))

colunas = [
    'Nota geral',
    'Contratacao',
    'CLASSIFICACAO',
    'Operadora',
    'Local editado',
    'Meta',
    'Resultado da unidade',
    'Status unidade'
]

print('Verificando colunas obrigatorias...')

for coluna in colunas:
    if coluna not in df.columns:
        df[coluna] = None
        print(f'Coluna criada: {coluna}')

print('Calculando a media de NOTA1 ate NOTA5...')

df['NOTA1'] = pd.to_numeric(df['NOTA1'], errors='coerce')
df['NOTA2'] = pd.to_numeric(df['NOTA2'], errors='coerce')
df['NOTA3'] = pd.to_numeric(df['NOTA3'], errors='coerce')
df['NOTA4'] = pd.to_numeric(df['NOTA4'], errors='coerce')
df['NOTA5'] = pd.to_numeric(df['NOTA5'], errors='coerce')

df['Quantidade notas validas'] = (
    df[['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']]
    .notna()
    .sum(axis=1)
)

df['Nota geral'] = (
    df[['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']]
    .mean(axis=1)
)

df['Nota geral'] = df['Nota geral'].round(2)

print('Separando somente as linhas que possuem pelo menos uma nota numerica valida...')

total_linhas_original = len(df)

df_valido = df[df['Quantidade notas validas'] > 0].copy()

total_linhas_validas = len(df_valido)
total_linhas_removidas = total_linhas_original - total_linhas_validas

print('Resumo da filtragem das notas:')
print(f'Total de linhas original: {total_linhas_original}')
print(f'Total de linhas validas para calcular Nota geral: {total_linhas_validas}')
print(f'Total de linhas removidas da analise: {total_linhas_removidas}')

print('Mostrando as colunas usadas no calculo, a quantidade de notas validas e o resultado da Nota geral:')
print(
    df_valido[
        [
            'NOTA1',
            'NOTA2',
            'NOTA3',
            'NOTA4',
            'NOTA5',
            'Quantidade notas validas',
            'Nota geral'
        ]
    ].head(20)
)

print('Fim da etapa de calculo da Nota geral.')

print('Iniciando a etapa de Contratacao...')

pasta_insumos = Path('utils/insumos')
arquivo_insumos = pasta_insumos / 'insumos 5 estrelas.xlsx'

if not arquivo_insumos.exists():
    print('Arquivo de insumos nao encontrado.')
else:
    print(f'Arquivo auxiliar encontrado: {arquivo_insumos}')

    df_contratacao = pd.read_excel(arquivo_insumos, sheet_name='insumos')

    print('Limpando espacos extras das colunas de local...')
    df_valido['LOCAL'] = df_valido['LOCAL'].astype('string').str.strip()
    df_contratacao['Local'] = df_contratacao['Local'].astype('string').str.strip()
    df_contratacao['contratacao'] = df_contratacao['contratacao'].astype('string').str.strip().str.lower()

    print('Padronizando maiusculo e minusculo somente para comparacao...')
    df_valido['LOCAL_COMPARACAO'] = df_valido['LOCAL'].str.lower()
    df_contratacao['LOCAL_COMPARACAO'] = df_contratacao['Local'].str.lower()

    print('Fazendo o cruzamento da coluna LOCAL com a coluna Local...')
    df_contratacao = df_contratacao.drop_duplicates(subset=['LOCAL_COMPARACAO', 'contratacao'])
    df_contratacao = df_contratacao.drop_duplicates(subset=['LOCAL_COMPARACAO'], keep='first')

    mapa_contratacao = df_contratacao.set_index('LOCAL_COMPARACAO')['contratacao']
    df_valido['Contratacao'] = df_valido['LOCAL_COMPARACAO'].map(mapa_contratacao)

    df_valido['Contratacao'] = df_valido['Contratacao'].astype('string').str.strip()
    df_valido['Contratacao'] = df_valido['Contratacao'].str.lower()

    total_rede_propria = (df_valido['Contratacao'] == 'rede propria').sum()
    total_rede_credenciada = (df_valido['Contratacao'] == 'rede credenciada').sum()
    total_nao_encontrado = df_valido['Contratacao'].isna().sum()

    print('Resumo da etapa de Contratacao:')
    print(f'Total analisado: {len(df_valido)}')
    print(f'Total rede propria: {int(total_rede_propria)}')
    print(f'Total rede credenciada: {int(total_rede_credenciada)}')
    print(f'Total nao encontrado: {int(total_nao_encontrado)}')
