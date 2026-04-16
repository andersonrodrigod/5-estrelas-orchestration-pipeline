# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data/5_estrelas_marco.csv')
arquivo_saida = Path('data_exec_indiv/01_base_limpa.csv')
pasta_resumo = Path('saida_resumo')
arquivo_resumo_json = pasta_resumo / 'exec_01_limpeza_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_01_limpeza_resumo.txt'

renomear_colunas = {
    'Nota geral': 'NOTA GERAL',
    'Contratacao': 'CONTRATACAO',
    'Operadora': 'OPERADORA',
    'Local editado': 'LOCAL EDITADO',
    'Meta': 'META',
    'Resultado da unidade': 'RESULTADO DA UNIDADE',
    'Status unidade': 'STATUS UNIDADE'
}

colunas_obrigatorias = [
    'NOTA GERAL',
    'CONTRATACAO',
    'CLASSIFICACAO',
    'OPERADORA',
    'LOCAL EDITADO',
    'META',
    'RESULTADO DA UNIDADE',
    'STATUS UNIDADE'
]

colunas_notas = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']

print('Iniciando execucao 01 - limpeza...')
print(f'Lendo arquivo original: {arquivo_entrada}')

df = pd.read_csv(arquivo_entrada, low_memory=False)
df = df.rename(columns=renomear_colunas)

for coluna in colunas_obrigatorias:
    if coluna not in df.columns:
        df[coluna] = None

print(f'Total de linhas antes da limpeza: {len(df)}')

for coluna in colunas_notas:
    df[coluna] = pd.to_numeric(df[coluna], errors='coerce')

df['Quantidade notas validas'] = df[colunas_notas].notna().sum(axis=1)
df_limpo = df[df['Quantidade notas validas'] > 0].copy()
df_limpo['NOTA GERAL'] = None

print(f'Total de linhas apos tirar IGN, NQA e sem nota valida: {len(df_limpo)}')
print(f'Total de linhas removidas: {len(df) - len(df_limpo)}')
print(f'Gravando arquivo da execucao 01: {arquivo_saida}')

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(exist_ok=True)
df_limpo.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')

resumo = {
    'execucao': 'exec_01_limpeza',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_linhas_saida': int(len(df_limpo)),
    'total_linhas_removidas': int(len(df) - len(df_limpo)),
    'total_linhas_com_alguma_nota_valida': int((df['Quantidade notas validas'] > 0).sum()),
    'total_linhas_sem_nota_valida': int((df['Quantidade notas validas'] == 0).sum())
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 01 - LIMPEZA',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total de linhas na saida: {resumo['total_linhas_saida']}",
    f"Total de linhas removidas: {resumo['total_linhas_removidas']}",
    f"Linhas com alguma nota valida: {resumo['total_linhas_com_alguma_nota_valida']}",
    f"Linhas sem nota valida: {resumo['total_linhas_sem_nota_valida']}",
]

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print('Execucao 01 finalizada.')
