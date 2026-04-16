# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/02_base_com_contratacao.csv')
arquivo_saida = Path('data_exec_indiv/03_base_com_nota.csv')
pasta_resumo = Path('saida_resumo')
arquivo_resumo_json = pasta_resumo / 'exec_03_nota_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_03_nota_resumo.txt'

colunas_notas = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']

print('Iniciando execucao 03 - nota...')
print(f'Lendo arquivo da execucao 02: {arquivo_entrada}')

df = pd.read_csv(arquivo_entrada, low_memory=False)

# Garante que as colunas de nota estejam no formato numerico.
for coluna in colunas_notas:
    df[coluna] = pd.to_numeric(df[coluna], errors='coerce')

# Conta quantas notas validas existem por linha.
df['Quantidade notas validas'] = df[colunas_notas].notna().sum(axis=1)

# Calcula a media com duas casas decimais.
df['NOTA GERAL'] = df[colunas_notas].mean(axis=1).round(2)

total_com_nota_geral = int(df['NOTA GERAL'].notna().sum())
total_sem_nota_geral = int(df['NOTA GERAL'].isna().sum())
distribuicao_notas_validas = (
    df['Quantidade notas validas']
    .value_counts()
    .sort_index()
    .to_dict()
)

print(f'Total de linhas recebidas: {len(df)}')
print(f'Total com nota geral calculada: {total_com_nota_geral}')
print(f'Total sem nota geral: {total_sem_nota_geral}')
print(f'Gravando arquivo da execucao 03: {arquivo_saida}')

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(exist_ok=True)
df.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')

resumo = {
    'execucao': 'exec_03_nota',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_com_nota_geral': total_com_nota_geral,
    'total_sem_nota_geral': total_sem_nota_geral,
    'distribuicao_quantidade_notas_validas': {
        str(chave): int(valor)
        for chave, valor in distribuicao_notas_validas.items()
    }
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 03 - NOTA',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total com nota geral calculada: {resumo['total_com_nota_geral']}",
    f"Total sem nota geral: {resumo['total_sem_nota_geral']}",
    '',
    'Quantidade de notas validas por linha:'
]

for quantidade, total in resumo['distribuicao_quantidade_notas_validas'].items():
    linhas_txt.append(f'- {quantidade} nota(s) valida(s): {total}')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print('Execucao 03 finalizada.')
