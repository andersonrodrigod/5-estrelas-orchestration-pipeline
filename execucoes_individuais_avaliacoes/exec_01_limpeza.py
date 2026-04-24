# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data/5_estrelas_marco.csv')
arquivo_saida = Path('data_exec_indiv/avaliacoes/01_base_limpa.csv')
arquivo_auditoria_classificacao = Path('data/base_auditoria_classificacao.csv')
pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_01_limpeza'
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

df = ler_csv_padronizado(arquivo_entrada)
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
print(f'Gravando base de auditoria da classificacao: {arquivo_auditoria_classificacao}')

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
salvar_csv_padronizado(df_limpo, arquivo_saida)

colunas_auditoria_classificacao = [
    'CDUSUARIO',
    'MES',
    'DIA',
    'ANO',
    'TIPO',
    'CONTRATACAO',
    'LOCAL',
    'ESPECIALIDADE'
]

df_auditoria_classificacao = df_limpo[colunas_auditoria_classificacao].copy()
salvar_csv_padronizado(df_auditoria_classificacao, arquivo_auditoria_classificacao)

resumo = {
    'execucao': 'exec_01_limpeza',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_saida': str(arquivo_saida),
    'arquivo_auditoria_classificacao': str(arquivo_auditoria_classificacao),
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
    f"Arquivo de auditoria da classificacao: {resumo['arquivo_auditoria_classificacao']}",
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

