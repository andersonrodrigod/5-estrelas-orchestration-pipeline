# -*- coding: utf-8 -*-
import json
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/06_base_com_operadora.csv')
arquivo_insumos = Path('utils/insumos/insumos 5 estrelas.xlsx')
arquivo_saida = Path('data_exec_indiv/avaliacoes/07_base_com_meta.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_07_meta'
arquivo_resumo_json = pasta_resumo / 'exec_07_meta_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_07_meta_resumo.txt'


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.fillna('')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    texto = texto.str.strip()
    return texto.apply(remover_acentos)


def remover_acentos(valor):
    if valor is None:
        return ''

    valor = str(valor)
    valor = unicodedata.normalize('NFKD', valor)
    valor = ''.join(caractere for caractere in valor if not unicodedata.combining(caractere))
    return valor


def transformar_em_lista_registros(df_base, colunas):
    if df_base.empty:
        return []

    registros = []

    for _, linha in df_base.iterrows():
        item = {}

        for coluna in colunas:
            valor = linha[coluna]

            if pd.isna(valor):
                item[coluna] = None
            else:
                item[coluna] = str(valor)

        registros.append(item)

    return registros


print('Iniciando execucao 07 - meta...')
print(f'Lendo arquivo da execucao 06: {arquivo_entrada}')
print(f'Lendo arquivo de insumos: {arquivo_insumos}')

df = ler_csv_padronizado(arquivo_entrada)
df_insumos = pd.read_excel(arquivo_insumos, sheet_name='insumos')

df['CLASSIFICACAO'] = df['CLASSIFICACAO'].astype('string').str.strip()
df['CHAVE_META'] = normalizar_texto(df['CLASSIFICACAO']).str.upper()

df_insumos['grupo'] = df_insumos['grupo'].astype('string').str.strip()
df_insumos['CHAVE_META'] = normalizar_texto(df_insumos['grupo']).str.upper()
df_insumos = df_insumos.dropna(subset=['CHAVE_META'])
df_insumos = df_insumos[df_insumos['CHAVE_META'] != '']
df_insumos = df_insumos.drop_duplicates(subset=['CHAVE_META'], keep='first')

mapa_meta = df_insumos.set_index('CHAVE_META')['meta']
mapa_grupo_original = df_insumos.set_index('CHAVE_META')['grupo']

df['META'] = df['CHAVE_META'].map(mapa_meta)
df['GRUPO_ENCONTRADO'] = df['CHAVE_META'].map(mapa_grupo_original)

mascara_encontrados = df['GRUPO_ENCONTRADO'].notna() & (df['GRUPO_ENCONTRADO'] != '')
mascara_nao_encontrados = ~mascara_encontrados

resumo_grupos_encontrados = (
    df.loc[mascara_encontrados, ['GRUPO_ENCONTRADO']]
    .value_counts()
    .reset_index(name='QUANTIDADE')
    .sort_values(['QUANTIDADE', 'GRUPO_ENCONTRADO'], ascending=[False, True])
)
resumo_grupos_encontrados.columns = ['GRUPO', 'QUANTIDADE']

resumo_classificacoes_nao_encontradas = (
    df.loc[mascara_nao_encontrados, ['CLASSIFICACAO']]
    .fillna('VAZIO')
    .value_counts()
    .reset_index(name='QUANTIDADE')
    .sort_values(['QUANTIDADE', 'CLASSIFICACAO'], ascending=[False, True])
)

total_encontrados = int(mascara_encontrados.sum())
total_nao_encontrados = int(mascara_nao_encontrados.sum())

print(f'Total de linhas recebidas: {len(df)}')
print(f'Total de metas encontradas: {total_encontrados}')
print(f'Total sem grupo correspondente: {total_nao_encontrados}')
print(f'Gravando arquivo da execucao 07: {arquivo_saida}')

df_saida = df.drop(columns=['CHAVE_META', 'GRUPO_ENCONTRADO'])
arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
salvar_csv_padronizado(df_saida, arquivo_saida)

resumo = {
    'execucao': 'exec_07_meta',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_insumos': str(arquivo_insumos),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_encontrados': total_encontrados,
    'total_nao_encontrados': total_nao_encontrados,
    'grupos_encontrados': transformar_em_lista_registros(
        resumo_grupos_encontrados,
        ['GRUPO', 'QUANTIDADE']
    ),
    'classificacoes_nao_encontradas': transformar_em_lista_registros(
        resumo_classificacoes_nao_encontradas,
        ['CLASSIFICACAO', 'QUANTIDADE']
    )
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 07 - META',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de insumos: {resumo['arquivo_insumos']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total encontrados: {resumo['total_encontrados']}",
    f"Total nao encontrados: {resumo['total_nao_encontrados']}",
    '',
    'GRUPOS ENCONTRADOS:'
]

if resumo['grupos_encontrados']:
    for item in resumo['grupos_encontrados']:
        linhas_txt.append(f"- {item['GRUPO']}: {item['QUANTIDADE']}")
else:
    linhas_txt.append('- Nenhum grupo encontrado')

linhas_txt.append('')
linhas_txt.append('CLASSIFICACOES SEM CORRESPONDENCIA:')

if resumo['classificacoes_nao_encontradas']:
    for item in resumo['classificacoes_nao_encontradas']:
        linhas_txt.append(f"- {item['CLASSIFICACAO']}: {item['QUANTIDADE']}")
else:
    linhas_txt.append('- Nenhuma classificacao sem correspondencia')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print('Execucao 07 finalizada.')

