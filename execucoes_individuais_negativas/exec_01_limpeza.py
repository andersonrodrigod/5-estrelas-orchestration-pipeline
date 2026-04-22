# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data/5_estrelas_negativo_marco.csv')
arquivo_saida = Path('data_exec_indiv/negativas/01_base_limpa.csv')
pasta_resumo = Path('saida_resumo_negativas') / 'exec_01_limpeza'
arquivo_resumo_json = pasta_resumo / 'exec_01_limpeza_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_01_limpeza_resumo.txt'

renomear_colunas = {
    'Contratacao': 'CONTRATACAO',
    'Local editado': 'LOCAL EDITADO'
}

colunas_obrigatorias = [
    'CONTRATACAO',
    'CLASSIFICACAO',
    'LOCAL EDITADO'
]

print('Iniciando execucao 01 - limpeza negativas...')
print(f'Lendo arquivo original: {arquivo_entrada}')

df = pd.read_csv(arquivo_entrada, low_memory=False)
df = df.rename(columns=renomear_colunas)

colunas_criadas = []
for coluna in colunas_obrigatorias:
    if coluna not in df.columns:
        df[coluna] = None
        colunas_criadas.append(coluna)

print(f'Total de linhas recebidas: {len(df)}')
print(f'Gravando arquivo da execucao 01: {arquivo_saida}')

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
df.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')

resumo = {
    'execucao': 'exec_01_limpeza_negativas',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_linhas_saida': int(len(df)),
    'total_colunas_saida': int(len(df.columns)),
    'colunas_criadas': colunas_criadas,
    'colunas_obrigatorias': colunas_obrigatorias
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 01 - LIMPEZA NEGATIVAS',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total de linhas na saida: {resumo['total_linhas_saida']}",
    f"Total de colunas na saida: {resumo['total_colunas_saida']}",
    '',
    'COLUNAS OBRIGATORIAS:',
]

for coluna in resumo['colunas_obrigatorias']:
    linhas_txt.append(f'- {coluna}')

linhas_txt.append('')
linhas_txt.append('COLUNAS CRIADAS:')

if resumo['colunas_criadas']:
    for coluna in resumo['colunas_criadas']:
        linhas_txt.append(f'- {coluna}')
else:
    linhas_txt.append('- Nenhuma coluna criada')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print('Execucao 01 negativas finalizada.')

