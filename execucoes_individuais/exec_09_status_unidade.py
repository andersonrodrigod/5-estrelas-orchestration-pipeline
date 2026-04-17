# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/08_base_com_resultado_unidade.csv')
arquivo_saida = Path('data_exec_indiv/09_base_com_status_unidade.csv')

pasta_resumo = Path('saida_resumo') / 'exec_09_status_unidade'
arquivo_resumo_json = pasta_resumo / 'exec_09_status_unidade_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_09_status_unidade_resumo.txt'
arquivo_status_csv = pasta_resumo / 'exec_09_status_unidade_status.csv'


print('Iniciando execucao 09 - status unidade...')
print(f'Lendo arquivo da execucao 08: {arquivo_entrada}')

df = pd.read_csv(arquivo_entrada, low_memory=False)

df['RESULTADO DA UNIDADE'] = pd.to_numeric(df['RESULTADO DA UNIDADE'], errors='coerce')
df['META'] = pd.to_numeric(df['META'], errors='coerce')
df['STATUS UNIDADE'] = df['STATUS UNIDADE'].astype('object')

mascara_sem_status = df['RESULTADO DA UNIDADE'].isna() | df['META'].isna()
mascara_fora_meta = (~mascara_sem_status) & (df['RESULTADO DA UNIDADE'] < df['META'])
mascara_dentro_meta = (~mascara_sem_status) & (df['RESULTADO DA UNIDADE'] >= df['META'])

df.loc[mascara_fora_meta, 'STATUS UNIDADE'] = 'fora da meta'
df.loc[mascara_dentro_meta, 'STATUS UNIDADE'] = 'dentro da meta'
df.loc[mascara_sem_status, 'STATUS UNIDADE'] = None

total_dentro_meta = int(mascara_dentro_meta.sum())
total_fora_meta = int(mascara_fora_meta.sum())
total_sem_status = int(mascara_sem_status.sum())

resumo_status = (
    df['STATUS UNIDADE']
    .fillna('sem status')
    .value_counts()
    .reset_index()
)
resumo_status.columns = ['STATUS UNIDADE', 'QUANTIDADE']

print(f'Total de linhas recebidas: {len(df)}')
print(f'Total dentro da meta: {total_dentro_meta}')
print(f'Total fora da meta: {total_fora_meta}')
print(f'Total sem status: {total_sem_status}')
print(f'Gravando arquivo da execucao 09: {arquivo_saida}')

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
df.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')
resumo_status.to_csv(arquivo_status_csv, index=False, encoding='utf-8-sig')

resumo = {
    'execucao': 'exec_09_status_unidade',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_saida': str(arquivo_saida),
    'arquivo_status_csv': str(arquivo_status_csv),
    'total_linhas_entrada': int(len(df)),
    'total_dentro_meta': total_dentro_meta,
    'total_fora_meta': total_fora_meta,
    'total_sem_status': total_sem_status
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 09 - STATUS UNIDADE',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    f"Arquivo de status: {resumo['arquivo_status_csv']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total dentro da meta: {resumo['total_dentro_meta']}",
    f"Total fora da meta: {resumo['total_fora_meta']}",
    f"Total sem status: {resumo['total_sem_status']}"
]

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print('Execucao 09 finalizada.')
