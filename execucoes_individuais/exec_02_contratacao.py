# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/01_base_limpa.csv')
arquivo_insumos = Path('utils/insumos/insumos 5 estrelas.xlsx')
arquivo_saida = Path('data_exec_indiv/02_base_com_contratacao.csv')
pasta_resumo = Path('saida_resumo')
arquivo_resumo_json = pasta_resumo / 'exec_02_contratacao_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_02_contratacao_resumo.txt'

print('Iniciando execucao 02 - contratacao...')
print(f'Lendo arquivo da execucao 01: {arquivo_entrada}')

df = pd.read_csv(arquivo_entrada, low_memory=False)
df['LOCAL'] = df['LOCAL'].astype('string').str.strip()
df['LOCAL_COMPARACAO'] = df['LOCAL'].str.lower()

print(f'Total de linhas recebidas: {len(df)}')
print(f'Lendo arquivo de insumos: {arquivo_insumos}')

df_insumos = pd.read_excel(arquivo_insumos, sheet_name='insumos')
df_insumos['Local'] = df_insumos['Local'].astype('string').str.strip()
df_insumos['contratacao'] = df_insumos['contratacao'].astype('string').str.strip().str.lower()
df_insumos['LOCAL_COMPARACAO'] = df_insumos['Local'].str.lower()

df_insumos = df_insumos.drop_duplicates(subset=['LOCAL_COMPARACAO', 'contratacao'])
df_insumos = df_insumos.drop_duplicates(subset=['LOCAL_COMPARACAO'], keep='first')

mapa_contratacao = df_insumos.set_index('LOCAL_COMPARACAO')['contratacao']

df['CONTRATACAO'] = df['LOCAL_COMPARACAO'].map(mapa_contratacao)
df['CONTRATACAO'] = df['CONTRATACAO'].astype('string').str.strip().str.lower()

total_rede_propria = int((df['CONTRATACAO'] == 'rede propria').sum())
total_rede_credenciada = int((df['CONTRATACAO'] == 'rede credenciada').sum())
total_nao_encontrado = int(df['CONTRATACAO'].isna().sum())
locais_sem_contratacao = (
    df[df['CONTRATACAO'].isna()]['LOCAL']
    .fillna('VAZIO')
    .value_counts()
    .to_dict()
)

print(f'Total rede propria: {total_rede_propria}')
print(f'Total rede credenciada: {total_rede_credenciada}')
print(f'Total sem contratacao encontrada: {total_nao_encontrado}')
print(f'Gravando arquivo da execucao 02: {arquivo_saida}')

df = df.drop(columns=['LOCAL_COMPARACAO'])
arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(exist_ok=True)
df.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')

resumo = {
    'execucao': 'exec_02_contratacao',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_insumos': str(arquivo_insumos),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_rede_propria': total_rede_propria,
    'total_rede_credenciada': total_rede_credenciada,
    'total_sem_contratacao': total_nao_encontrado,
    'locais_sem_contratacao': locais_sem_contratacao
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 02 - CONTRATACAO',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de insumos: {resumo['arquivo_insumos']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total rede propria: {resumo['total_rede_propria']}",
    f"Total rede credenciada: {resumo['total_rede_credenciada']}",
    f"Total sem contratacao: {resumo['total_sem_contratacao']}",
    '',
    'Locais sem contratacao:'
]

if resumo['locais_sem_contratacao']:
    for local, quantidade in resumo['locais_sem_contratacao'].items():
        linhas_txt.append(f'- {local}: {quantidade}')
else:
    linhas_txt.append('- Nenhum local sem contratacao')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print('Execucao 02 finalizada.')
