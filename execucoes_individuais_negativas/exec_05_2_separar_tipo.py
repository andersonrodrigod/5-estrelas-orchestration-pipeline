# -*- coding: utf-8 -*-
import json
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/negativas/04_base_com_local_editado.csv')
arquivo_nomes_classificacao = Path('data/nomes_classificacao.json')
arquivo_saida_tipo_1_a_3 = Path('data_exec_indiv/negativas/05_2_base_tipo_1_a_3.csv')
arquivo_saida_tipo_4_a_7 = Path('data_exec_indiv/negativas/05_2_base_tipo_4_a_7.csv')
arquivo_saida_tipo_8_ou_mais = Path('data_exec_indiv/negativas/05_2_base_tipo_8_ou_mais.csv')

pasta_resumo = Path('saida_resumo_negativas') / 'exec_05_2_separar_tipo'
arquivo_resumo_json = pasta_resumo / 'exec_05_2_separar_tipo_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_05_2_separar_tipo_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_05_2_separar_tipo_resumo.csv'


def remover_decimal_zero_identificador(serie):
    texto = serie.astype('string').str.strip()
    return texto.str.replace(r'\.0$', '', regex=True)


def normalizar_texto(valor):
    if pd.isna(valor):
        return ''

    texto = str(valor).strip().upper()
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(caractere for caractere in texto if not unicodedata.combining(caractere))
    return ' '.join(texto.split())


def carregar_json(caminho):
    with open(caminho, 'r', encoding='utf-8-sig') as arquivo:
        return json.load(arquivo)


def criar_mapa_nomes_envio(nomes_classificacao):
    mapa = {}

    for chave, nome in nomes_classificacao.items():
        mapa[normalizar_texto(chave)] = nome
        mapa[normalizar_texto(nome)] = nome

    return mapa


def aplicar_nomes_envio(df, mapa_nomes_envio):
    classificacao_normalizada = df['CLASSIFICACAO'].apply(normalizar_texto)
    nomes_envio = classificacao_normalizada.map(mapa_nomes_envio)
    df['CLASSIFICACAO'] = nomes_envio.fillna(df['CLASSIFICACAO'])


print('Iniciando execucao 05.2 - separar por tipo negativas...')
print(f'Lendo arquivo da execucao 04: {arquivo_entrada}')
print(f'Lendo nomes de classificacao para envio: {arquivo_nomes_classificacao}')

df = ler_csv_padronizado(arquivo_entrada)
nomes_classificacao = carregar_json(arquivo_nomes_classificacao)
mapa_nomes_envio = criar_mapa_nomes_envio(nomes_classificacao)
df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')

# FEATURE TEMPORARIA: remover ".0" que vem em identificadores lidos como numero.
for coluna in ['NUM_BENEFICIARIO', 'TELEFONE']:
    df[coluna] = remover_decimal_zero_identificador(df[coluna])

aplicar_nomes_envio(df, mapa_nomes_envio)

mascara_tipo_1_a_3 = df['TIPO'].between(1, 3, inclusive='both')
mascara_tipo_4_a_7 = df['TIPO'].between(4, 7, inclusive='both')
mascara_tipo_8_ou_mais = df['TIPO'] >= 8
mascara_tipo_fora_recorte = ~(mascara_tipo_1_a_3 | mascara_tipo_4_a_7 | mascara_tipo_8_ou_mais)

total_linhas = int(len(df))
total_tipo_1_a_3 = int(mascara_tipo_1_a_3.sum())
total_tipo_4_a_7 = int(mascara_tipo_4_a_7.sum())
total_tipo_8_ou_mais = int(mascara_tipo_8_ou_mais.sum())
total_tipo_fora_recorte = int(mascara_tipo_fora_recorte.sum())

print(f'Total de linhas recebidas: {total_linhas}')
print(f'Total TIPO 1 a 3: {total_tipo_1_a_3}')
print(f'Total TIPO 4 a 7: {total_tipo_4_a_7}')
print(f'Total TIPO 8 ou mais: {total_tipo_8_ou_mais}')
print(f'Total fora do recorte: {total_tipo_fora_recorte}')
print(f'Gravando arquivo TIPO 1 a 3: {arquivo_saida_tipo_1_a_3}')
print(f'Gravando arquivo TIPO 4 a 7: {arquivo_saida_tipo_4_a_7}')
print(f'Gravando arquivo TIPO 8 ou mais: {arquivo_saida_tipo_8_ou_mais}')

arquivo_saida_tipo_1_a_3.parent.mkdir(parents=True, exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)

salvar_csv_padronizado(df.loc[mascara_tipo_1_a_3], arquivo_saida_tipo_1_a_3)
salvar_csv_padronizado(df.loc[mascara_tipo_4_a_7], arquivo_saida_tipo_4_a_7)
salvar_csv_padronizado(df.loc[mascara_tipo_8_ou_mais], arquivo_saida_tipo_8_ou_mais)

resumo = {
    'execucao': 'exec_05_2_separar_tipo_negativas',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_nomes_classificacao': str(arquivo_nomes_classificacao),
    'arquivo_saida_tipo_1_a_3': str(arquivo_saida_tipo_1_a_3),
    'arquivo_saida_tipo_4_a_7': str(arquivo_saida_tipo_4_a_7),
    'arquivo_saida_tipo_8_ou_mais': str(arquivo_saida_tipo_8_ou_mais),
    'total_linhas_entrada': total_linhas,
    'total_tipo_1_a_3': total_tipo_1_a_3,
    'total_tipo_4_a_7': total_tipo_4_a_7,
    'total_tipo_8_ou_mais': total_tipo_8_ou_mais,
    'total_tipo_fora_recorte': total_tipo_fora_recorte
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 05.2 - SEPARAR POR TIPO NEGATIVAS',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo TIPO 1 a 3: {resumo['arquivo_saida_tipo_1_a_3']}",
    f"Arquivo TIPO 4 a 7: {resumo['arquivo_saida_tipo_4_a_7']}",
    f"Arquivo TIPO 8 ou mais: {resumo['arquivo_saida_tipo_8_ou_mais']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total TIPO 1 a 3: {resumo['total_tipo_1_a_3']}",
    f"Total TIPO 4 a 7: {resumo['total_tipo_4_a_7']}",
    f"Total TIPO 8 ou mais: {resumo['total_tipo_8_ou_mais']}",
    f"Total fora do recorte: {resumo['total_tipo_fora_recorte']}"
]

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

salvar_csv_padronizado(pd.DataFrame([{
    'EXECUCAO': resumo['execucao'],
    'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
    'ARQUIVO_TIPO_1_A_3': resumo['arquivo_saida_tipo_1_a_3'],
    'ARQUIVO_TIPO_4_A_7': resumo['arquivo_saida_tipo_4_a_7'],
    'ARQUIVO_TIPO_8_OU_MAIS': resumo['arquivo_saida_tipo_8_ou_mais'],
    'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
    'TOTAL_TIPO_1_A_3': resumo['total_tipo_1_a_3'],
    'TOTAL_TIPO_4_A_7': resumo['total_tipo_4_a_7'],
    'TOTAL_TIPO_8_OU_MAIS': resumo['total_tipo_8_ou_mais'],
    'TOTAL_TIPO_FORA_RECORTE': resumo['total_tipo_fora_recorte']
}]), arquivo_resumo_csv)

print('Execucao 05.2 negativas finalizada.')
