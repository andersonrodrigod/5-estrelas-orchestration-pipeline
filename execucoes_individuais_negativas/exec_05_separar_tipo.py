# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/negativas/04_base_com_local_editado.csv')
arquivo_saida_tipo_1_a_7 = Path('data_exec_indiv/negativas/05_base_tipo_1_a_7.csv')
arquivo_saida_tipo_8_ou_mais = Path('data_exec_indiv/negativas/05_base_tipo_8_ou_mais.csv')

pasta_resumo = Path('saida_resumo_negativas') / 'exec_05_separar_tipo'
arquivo_resumo_json = pasta_resumo / 'exec_05_separar_tipo_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_05_separar_tipo_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_05_separar_tipo_resumo.csv'


def remover_decimal_zero_identificador(serie):
    texto = serie.astype('string').str.strip()
    return texto.str.replace(r'\.0$', '', regex=True)


print('Iniciando execucao 05 - separar por tipo negativas...')
print(f'Lendo arquivo da execucao 04: {arquivo_entrada}')

df = pd.read_csv(arquivo_entrada, low_memory=False)
df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')

# FEATURE TEMPORARIA: remover ".0" que vem em identificadores lidos como numero.
for coluna in ['NUM_BENEFICIARIO', 'TELEFONE']:
    df[coluna] = remover_decimal_zero_identificador(df[coluna])

mascara_tipo_1_a_7 = df['TIPO'].between(1, 7, inclusive='both')
mascara_tipo_8_ou_mais = df['TIPO'] >= 8
mascara_tipo_fora_recorte = ~(mascara_tipo_1_a_7 | mascara_tipo_8_ou_mais)

total_linhas = int(len(df))
total_tipo_1_a_7 = int(mascara_tipo_1_a_7.sum())
total_tipo_8_ou_mais = int(mascara_tipo_8_ou_mais.sum())
total_tipo_fora_recorte = int(mascara_tipo_fora_recorte.sum())

print(f'Total de linhas recebidas: {total_linhas}')
print(f'Total TIPO 1 a 7: {total_tipo_1_a_7}')
print(f'Total TIPO 8 ou mais: {total_tipo_8_ou_mais}')
print(f'Total fora do recorte: {total_tipo_fora_recorte}')
print(f'Gravando arquivo TIPO 1 a 7: {arquivo_saida_tipo_1_a_7}')
print(f'Gravando arquivo TIPO 8 ou mais: {arquivo_saida_tipo_8_ou_mais}')

arquivo_saida_tipo_1_a_7.parent.mkdir(parents=True, exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)

df.loc[mascara_tipo_1_a_7].to_csv(arquivo_saida_tipo_1_a_7, index=False, encoding='utf-8-sig')
df.loc[mascara_tipo_8_ou_mais].to_csv(arquivo_saida_tipo_8_ou_mais, index=False, encoding='utf-8-sig')

resumo = {
    'execucao': 'exec_05_separar_tipo_negativas',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_saida_tipo_1_a_7': str(arquivo_saida_tipo_1_a_7),
    'arquivo_saida_tipo_8_ou_mais': str(arquivo_saida_tipo_8_ou_mais),
    'total_linhas_entrada': total_linhas,
    'total_tipo_1_a_7': total_tipo_1_a_7,
    'total_tipo_8_ou_mais': total_tipo_8_ou_mais,
    'total_tipo_fora_recorte': total_tipo_fora_recorte
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 05 - SEPARAR POR TIPO NEGATIVAS',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo TIPO 1 a 7: {resumo['arquivo_saida_tipo_1_a_7']}",
    f"Arquivo TIPO 8 ou mais: {resumo['arquivo_saida_tipo_8_ou_mais']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total TIPO 1 a 7: {resumo['total_tipo_1_a_7']}",
    f"Total TIPO 8 ou mais: {resumo['total_tipo_8_ou_mais']}",
    f"Total fora do recorte: {resumo['total_tipo_fora_recorte']}"
]

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

pd.DataFrame([{
    'EXECUCAO': resumo['execucao'],
    'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
    'ARQUIVO_TIPO_1_A_7': resumo['arquivo_saida_tipo_1_a_7'],
    'ARQUIVO_TIPO_8_OU_MAIS': resumo['arquivo_saida_tipo_8_ou_mais'],
    'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
    'TOTAL_TIPO_1_A_7': resumo['total_tipo_1_a_7'],
    'TOTAL_TIPO_8_OU_MAIS': resumo['total_tipo_8_ou_mais'],
    'TOTAL_TIPO_FORA_RECORTE': resumo['total_tipo_fora_recorte']
}]).to_csv(arquivo_resumo_csv, index=False, encoding='utf-8-sig')

print('Execucao 05 negativas finalizada.')
