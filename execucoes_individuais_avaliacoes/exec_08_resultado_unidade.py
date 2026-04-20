# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/avaliacoes/07_base_com_meta.csv')
arquivo_saida = Path('data_exec_indiv/avaliacoes/08_base_com_resultado_unidade.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_08_resultado_unidade'
arquivo_resumo_json = pasta_resumo / 'exec_08_resultado_unidade_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_08_resultado_unidade_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_08_resultado_unidade_resumo.csv'
arquivo_inspecao_grupos_csv = pasta_resumo / 'exec_08_resultado_unidade_inspecao_grupos.csv'
arquivo_linhas_chave_vazia_csv = pasta_resumo / 'exec_08_resultado_unidade_linhas_chave_vazia.csv'

colunas_grupo = ['CLASSIFICACAO', 'LOCAL EDITADO', 'UF']


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip()


def identificar_colunas_vazias(linha):
    colunas_vazias = []

    for coluna in colunas_grupo:
        valor = linha[coluna]

        if pd.isna(valor) or valor == '':
            colunas_vazias.append(coluna)

    return ', '.join(colunas_vazias)


print('Iniciando execucao 08 - resultado da unidade...')
print(f'Lendo arquivo da execucao 07: {arquivo_entrada}')

df = pd.read_csv(arquivo_entrada, low_memory=False)

# Padroniza as chaves do agrupamento.
for coluna in colunas_grupo:
    df[coluna] = normalizar_texto(df[coluna])

# Garante que a nota geral esteja numerica para o calculo da media.
df['NOTA GERAL'] = pd.to_numeric(df['NOTA GERAL'], errors='coerce')

# Calcula a media por grupo e devolve o valor para cada linha.
df['RESULTADO DA UNIDADE'] = (
    df.groupby(colunas_grupo, dropna=False)['NOTA GERAL']
    .transform('mean')
    .round(2)
)

# Gera uma visao resumida para inspecao posterior.
df_inspecao_grupos = (
    df.groupby(colunas_grupo, dropna=False)
    .agg(
        QUANTIDADE_LINHAS=('NOTA GERAL', 'size'),
        RESULTADO_DA_UNIDADE=('RESULTADO DA UNIDADE', 'first'),
        NOTA_GERAL_MIN=('NOTA GERAL', 'min'),
        NOTA_GERAL_MAX=('NOTA GERAL', 'max'),
        META=('META', 'first'),
        OPERADORA=('OPERADORA', 'first')
    )
    .reset_index()
    .sort_values('QUANTIDADE_LINHAS', ascending=False)
)

total_grupos = int(len(df_inspecao_grupos))
total_linhas = int(len(df))
total_linhas_com_resultado = int(df['RESULTADO DA UNIDADE'].notna().sum())
total_linhas_sem_resultado = int(df['RESULTADO DA UNIDADE'].isna().sum())
mascara_grupos_chave_vazia = (
    df_inspecao_grupos['CLASSIFICACAO'].isna() |
    (df_inspecao_grupos['CLASSIFICACAO'] == '') |
    df_inspecao_grupos['LOCAL EDITADO'].isna() |
    (df_inspecao_grupos['LOCAL EDITADO'] == '') |
    df_inspecao_grupos['UF'].isna() |
    (df_inspecao_grupos['UF'] == '')
)
total_grupos_chave_vazia = int(mascara_grupos_chave_vazia.sum())

mascara_linhas_chave_vazia = (
    df['CLASSIFICACAO'].isna() |
    (df['CLASSIFICACAO'] == '') |
    df['LOCAL EDITADO'].isna() |
    (df['LOCAL EDITADO'] == '') |
    df['UF'].isna() |
    (df['UF'] == '')
)
df_linhas_chave_vazia = df.loc[mascara_linhas_chave_vazia].copy()
df_linhas_chave_vazia['COLUNA_VAZIA'] = df_linhas_chave_vazia.apply(identificar_colunas_vazias, axis=1)
total_linhas_chave_vazia = int(len(df_linhas_chave_vazia))

print(f'Total de linhas recebidas: {total_linhas}')
print(f'Total de grupos encontrados: {total_grupos}')
print(f'Total de linhas com resultado da unidade: {total_linhas_com_resultado}')
print(f'Total de linhas sem resultado da unidade: {total_linhas_sem_resultado}')
print(f'Gravando arquivo da execucao 08: {arquivo_saida}')

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
df.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')
df_inspecao_grupos.to_csv(arquivo_inspecao_grupos_csv, index=False, encoding='utf-8-sig')
df_linhas_chave_vazia.to_csv(arquivo_linhas_chave_vazia_csv, index=False, encoding='utf-8-sig')

resumo = {
    'execucao': 'exec_08_resultado_unidade',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_saida': str(arquivo_saida),
    'arquivo_inspecao_grupos': str(arquivo_inspecao_grupos_csv),
    'arquivo_linhas_chave_vazia': str(arquivo_linhas_chave_vazia_csv),
    'total_linhas_entrada': total_linhas,
    'total_grupos': total_grupos,
    'total_linhas_com_resultado': total_linhas_com_resultado,
    'total_linhas_sem_resultado': total_linhas_sem_resultado,
    'total_grupos_chave_vazia': total_grupos_chave_vazia,
    'total_linhas_chave_vazia': total_linhas_chave_vazia
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 08 - RESULTADO DA UNIDADE',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    f"Arquivo de inspecao: {resumo['arquivo_inspecao_grupos']}",
    f"Arquivo de linhas com chave vazia: {resumo['arquivo_linhas_chave_vazia']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total de grupos: {resumo['total_grupos']}",
    f"Total de linhas com resultado da unidade: {resumo['total_linhas_com_resultado']}",
    f"Total de linhas sem resultado da unidade: {resumo['total_linhas_sem_resultado']}",
    f"Total de grupos com alguma chave vazia: {resumo['total_grupos_chave_vazia']}",
    f"Total de linhas com alguma chave vazia: {resumo['total_linhas_chave_vazia']}"
]

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

pd.DataFrame([{
    'EXECUCAO': resumo['execucao'],
    'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
    'ARQUIVO_SAIDA': resumo['arquivo_saida'],
    'ARQUIVO_INSPECAO': resumo['arquivo_inspecao_grupos'],
    'ARQUIVO_LINHAS_CHAVE_VAZIA': resumo['arquivo_linhas_chave_vazia'],
    'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
    'TOTAL_GRUPOS': resumo['total_grupos'],
    'TOTAL_LINHAS_COM_RESULTADO': resumo['total_linhas_com_resultado'],
    'TOTAL_LINHAS_SEM_RESULTADO': resumo['total_linhas_sem_resultado'],
    'TOTAL_GRUPOS_CHAVE_VAZIA': resumo['total_grupos_chave_vazia'],
    'TOTAL_LINHAS_CHAVE_VAZIA': resumo['total_linhas_chave_vazia']
}]).to_csv(arquivo_resumo_csv, index=False, encoding='utf-8-sig')

print('Execucao 08 finalizada.')

