# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd
from listas_classificacao import lista_qualivida

arquivo_base = Path('data/5_estrelas_fevereiro_tratado.csv')
pasta_saida = Path('saida_inspecao')

pasta_saida.mkdir(exist_ok=True)

df = pd.read_csv(arquivo_base, low_memory=False)

df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
df['CONTRATACAO'] = df['CONTRATACAO'].astype('string').str.strip().str.lower()
df['ESPECIALIDADE'] = df['ESPECIALIDADE'].astype('string').str.strip()
df['ESPECIALIDADE_COMPARACAO'] = df['ESPECIALIDADE'].str.upper()

df_tipo_1 = df[df['TIPO'] == 1].copy()
df_tipo_6 = df[df['TIPO'] == 6].copy()
df_tipo_diferente_6 = df[df['TIPO'] != 6].copy()
df_tipo_diferente_6_rede_propria = df_tipo_diferente_6[df_tipo_diferente_6['CONTRATACAO'] == 'rede propria'].copy()

lista_qualivida_comparacao = [valor.strip().upper() for valor in lista_qualivida]

df_qualivida = df_tipo_diferente_6_rede_propria[
    df_tipo_diferente_6_rede_propria['ESPECIALIDADE_COMPARACAO'].isin(lista_qualivida_comparacao)
].copy()

contagem_qualivida = (
    df_qualivida['ESPECIALIDADE']
    .fillna('VAZIO')
    .value_counts()
    .to_dict()
)

resultado = {
    'arquivo_analisado': str(arquivo_base),
    'tipo_1': {
        'total_registros': int(len(df_tipo_1)),
        'total_rede_credenciada': int((df_tipo_1['CONTRATACAO'] == 'rede credenciada').sum()),
        'total_rede_propria': int((df_tipo_1['CONTRATACAO'] == 'rede propria').sum()),
        'total_sem_contratacao': int(df_tipo_1['CONTRATACAO'].isna().sum())
    },
    'tipo_6': {
        'total_registros': int(len(df_tipo_6)),
        'total_rede_credenciada': int((df_tipo_6['CONTRATACAO'] == 'rede credenciada').sum()),
        'total_rede_propria': int((df_tipo_6['CONTRATACAO'] == 'rede propria').sum()),
        'total_sem_contratacao': int(df_tipo_6['CONTRATACAO'].isna().sum())
    },
    'qualivida': {
        'lista_usada': lista_qualivida,
        'total_tipo_diferente_6': int(len(df_tipo_diferente_6)),
        'total_tipo_diferente_6_rede_propria': int(len(df_tipo_diferente_6_rede_propria)),
        'total_encontrado_na_lista': int(len(df_qualivida)),
        'contagem_por_valor': contagem_qualivida
    }
}

arquivo_txt = pasta_saida / 'inspecao_tipos.txt'
arquivo_json_global = pasta_saida / 'controle_global_inspecao.json'
arquivo_json_especialidade = pasta_saida / 'inspecao_especialidade.json'
arquivo_txt_especialidade = pasta_saida / 'inspecao_especialidade.txt'

especialidades_total = (
    df['ESPECIALIDADE']
    .fillna('VAZIO')
    .value_counts()
    .to_dict()
)

resultado_especialidade = {
    'arquivo_analisado': str(arquivo_base),
    'total_registros_base': int(len(df)),
    'especialidades': especialidades_total
}

linhas_txt = [
    'INSPECAO DE TIPOS',
    '',
    f"Arquivo analisado: {arquivo_base}",
    '',
    'TIPO 1:',
    f"- total de registros: {resultado['tipo_1']['total_registros']}",
    f"- total rede credenciada: {resultado['tipo_1']['total_rede_credenciada']}",
    f"- total rede propria: {resultado['tipo_1']['total_rede_propria']}",
    f"- total sem contratacao: {resultado['tipo_1']['total_sem_contratacao']}",
    '',
    'TIPO 6:',
    f"- total de registros: {resultado['tipo_6']['total_registros']}",
    f"- total rede credenciada: {resultado['tipo_6']['total_rede_credenciada']}",
    f"- total rede propria: {resultado['tipo_6']['total_rede_propria']}",
    f"- total sem contratacao: {resultado['tipo_6']['total_sem_contratacao']}",
    '',
    'QUALIVIDA - TIPO DIFERENTE DE 6 E REDE PROPRIA:',
    f"- total de registros com TIPO diferente de 6: {resultado['qualivida']['total_tipo_diferente_6']}",
    f"- total de registros com TIPO diferente de 6 e rede propria: {resultado['qualivida']['total_tipo_diferente_6_rede_propria']}",
    f"- total encontrado na lista_qualivida: {resultado['qualivida']['total_encontrado_na_lista']}",
    '',
    'LISTA USADA:',
]

for valor in resultado['qualivida']['lista_usada']:
    linhas_txt.append(f"- {valor}")

linhas_txt.append('')
linhas_txt.append('CONTAGEM POR VALOR ENCONTRADO:')

if resultado['qualivida']['contagem_por_valor']:
    for valor, quantidade in resultado['qualivida']['contagem_por_valor'].items():
        linhas_txt.append(f'- {valor}: {quantidade}')
else:
    linhas_txt.append('- Nenhum valor da lista foi encontrado')

with open(arquivo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

with open(arquivo_txt_especialidade, 'w', encoding='utf-8') as arquivo:
    linhas_txt_especialidade = [
        'INSPECAO DE ESPECIALIDADE',
        '',
        f"Arquivo analisado: {arquivo_base}",
        f"Total de registros da base: {resultado_especialidade['total_registros_base']}",
        '',
        'CONTAGEM DE TODOS OS VALORES DA COLUNA ESPECIALIDADE:'
    ]

    for especialidade, quantidade in resultado_especialidade['especialidades'].items():
        linhas_txt_especialidade.append(f'- {especialidade}: {quantidade}')

    arquivo.write('\n'.join(linhas_txt_especialidade))

with open(arquivo_json_especialidade, 'w', encoding='utf-8') as arquivo:
    json.dump(resultado_especialidade, arquivo, ensure_ascii=False, indent=4)

controle_global = {
    'arquivo_base': str(arquivo_base),
    'inspecoes': {}
}

if arquivo_json_global.exists():
    with open(arquivo_json_global, 'r', encoding='utf-8') as arquivo:
        controle_global = json.load(arquivo)

if 'inspecoes' not in controle_global:
    controle_global['inspecoes'] = {}

controle_global['inspecoes']['tipos'] = resultado

with open(arquivo_json_global, 'w', encoding='utf-8') as arquivo:
    json.dump(controle_global, arquivo, ensure_ascii=False, indent=4)
