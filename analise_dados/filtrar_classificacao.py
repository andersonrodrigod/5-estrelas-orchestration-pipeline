# -*- coding: utf-8 -*-
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/avaliacoes/04_base_com_classificacao.csv')
arquivo_saida = Path('analise_dados/resultado_filtro_classificacao.csv')

# Preencha com o valor desejado ou deixe None para nao filtrar a coluna.
filtro_tipo = None
filtro_contratacao = None
filtro_local = 'qualivida'
filtro_especialidade = None
filtro_classificacao = None

# Preencha com o valor que deve ser excluido ou deixe None para nao aplicar.
filtro_tipo_diferente = None
filtro_contratacao_diferente = 6
filtro_local_diferente = None
filtro_especialidade_diferente = None
filtro_classificacao_diferente = None

colunas_saida = [
    'TIPO',
    'CONTRATACAO',
    'LOCAL',
    'ESPECIALIDADE',
    'CLASSIFICACAO',
]


def normalizar_texto(serie):
    return serie.astype('string').str.strip().str.lower()


print(f'Lendo arquivo: {arquivo_entrada}')

df = pd.read_csv(arquivo_entrada, usecols=colunas_saida, low_memory=False)
df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')

mascara = pd.Series(True, index=df.index)

if filtro_tipo is not None:
    mascara = mascara & (df['TIPO'] == filtro_tipo)

if filtro_tipo_diferente is not None:
    mascara = mascara & (df['TIPO'] != filtro_tipo_diferente)

if filtro_contratacao is not None:
    mascara = mascara & (normalizar_texto(df['CONTRATACAO']) == str(filtro_contratacao).strip().lower())

if filtro_contratacao_diferente is not None:
    mascara = mascara & (normalizar_texto(df['CONTRATACAO']) != str(filtro_contratacao_diferente).strip().lower())

if filtro_local is not None:
    mascara = mascara & normalizar_texto(df['LOCAL']).str.contains(
        str(filtro_local).strip().lower(),
        na=False,
        regex=False
    )

if filtro_local_diferente is not None:
    mascara = mascara & ~normalizar_texto(df['LOCAL']).str.contains(
        str(filtro_local_diferente).strip().lower(),
        na=False,
        regex=False
    )

if filtro_especialidade is not None:
    mascara = mascara & normalizar_texto(df['ESPECIALIDADE']).str.contains(
        str(filtro_especialidade).strip().lower(),
        na=False,
        regex=False
    )

if filtro_especialidade_diferente is not None:
    mascara = mascara & ~normalizar_texto(df['ESPECIALIDADE']).str.contains(
        str(filtro_especialidade_diferente).strip().lower(),
        na=False,
        regex=False
    )

if filtro_classificacao is not None:
    mascara = mascara & (normalizar_texto(df['CLASSIFICACAO']) == str(filtro_classificacao).strip().lower())

if filtro_classificacao_diferente is not None:
    mascara = mascara & (normalizar_texto(df['CLASSIFICACAO']) != str(filtro_classificacao_diferente).strip().lower())

df_filtrado = df.loc[mascara, colunas_saida].copy()

arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
df_filtrado.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')

print(f'Total encontrado: {len(df_filtrado)}')
print(f'Arquivo gerado: {arquivo_saida}')
