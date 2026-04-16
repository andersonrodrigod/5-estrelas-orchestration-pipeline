# -*- coding: utf-8 -*-
from pathlib import Path

import pandas as pd

arquivo_base = Path('data/5_estrelas_fevereiro_tratado.csv')
pasta_saida = Path('saida_inspecao')

pasta_saida.mkdir(exist_ok=True)

df = pd.read_csv(arquivo_base, low_memory=False)

df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
df['CONTRATACAO'] = df['CONTRATACAO'].astype('string').str.strip().str.lower()
df['ESPECIALIDADE'] = df['ESPECIALIDADE'].astype('string').str.strip()

df_tipo_1 = df[df['TIPO'] == 1].copy()
df_tipo_6 = df[df['TIPO'] == 6].copy()

resultado_tipos = [
    {
        'grupo': 'TIPO 1',
        'total_registros': int(len(df_tipo_1)),
        'total_rede_credenciada': int((df_tipo_1['CONTRATACAO'] == 'rede credenciada').sum()),
        'total_rede_propria': int((df_tipo_1['CONTRATACAO'] == 'rede propria').sum()),
        'total_sem_contratacao': int(df_tipo_1['CONTRATACAO'].isna().sum())
    },
    {
        'grupo': 'TIPO 6',
        'total_registros': int(len(df_tipo_6)),
        'total_rede_credenciada': int((df_tipo_6['CONTRATACAO'] == 'rede credenciada').sum()),
        'total_rede_propria': int((df_tipo_6['CONTRATACAO'] == 'rede propria').sum()),
        'total_sem_contratacao': int(df_tipo_6['CONTRATACAO'].isna().sum())
    }
]

contagem_especialidade = (
    df['ESPECIALIDADE']
    .fillna('VAZIO')
    .value_counts()
    .reset_index()
)
contagem_especialidade.columns = ['ESPECIALIDADE', 'QUANTIDADE']

arquivo_txt_tipos = pasta_saida / 'inspecao_tipos.txt'
arquivo_csv_global = pasta_saida / 'controle_global_inspecao.csv'
arquivo_txt_especialidade = pasta_saida / 'inspecao_especialidade.txt'
arquivo_csv_especialidade = pasta_saida / 'inspecao_especialidade.csv'

linhas_txt = [
    'INSPECAO DE TIPOS',
    '',
    f'Arquivo analisado: {arquivo_base}',
    ''
]

for item in resultado_tipos:
    linhas_txt.append(f"{item['grupo']}:")
    linhas_txt.append(f"- total de registros: {item['total_registros']}")
    linhas_txt.append(f"- total rede credenciada: {item['total_rede_credenciada']}")
    linhas_txt.append(f"- total rede propria: {item['total_rede_propria']}")
    linhas_txt.append(f"- total sem contratacao: {item['total_sem_contratacao']}")
    linhas_txt.append('')

with open(arquivo_txt_tipos, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

df_controle_global = pd.DataFrame(resultado_tipos)
df_controle_global.to_csv(arquivo_csv_global, index=False, encoding='utf-8-sig')

linhas_txt_especialidade = [
    'INSPECAO DE ESPECIALIDADE',
    '',
    f'Arquivo analisado: {arquivo_base}',
    f'Total de registros da base: {len(df)}',
    '',
    'CONTAGEM DE TODOS OS VALORES DA COLUNA ESPECIALIDADE:'
]

for _, linha in contagem_especialidade.iterrows():
    linhas_txt_especialidade.append(f"- {linha['ESPECIALIDADE']}: {int(linha['QUANTIDADE'])}")

with open(arquivo_txt_especialidade, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt_especialidade))

contagem_especialidade.to_csv(arquivo_csv_especialidade, index=False, encoding='utf-8-sig')
