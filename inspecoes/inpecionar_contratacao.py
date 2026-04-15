# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

print('Procurando o arquivo de insumos...')

arquivo_insumos = Path('utils/insumos/insumos 5 estrelas.xlsx')

if not arquivo_insumos.exists():
    print('Arquivo de insumos nao encontrado.')
    raise SystemExit

print(f'Arquivo encontrado: {arquivo_insumos}')
print('Lendo a aba insumos...')

df_insumos = pd.read_excel(arquivo_insumos, sheet_name='insumos')
df_base = pd.read_csv('data/5_estrelas_fevereiro.csv', low_memory=False)

print('Preparando as colunas de nota da base principal...')
colunas_notas = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']

for coluna in colunas_notas:
    df_base[coluna] = pd.to_numeric(df_base[coluna], errors='coerce')

df_base['QUANTIDADE_NOTAS_VALIDAS'] = df_base[colunas_notas].notna().sum(axis=1)

print('Limpando espacos extras...')
df_insumos['Local'] = df_insumos['Local'].astype('string').str.strip()
df_insumos['contratacao'] = df_insumos['contratacao'].astype('string').str.strip().str.lower()
df_base['LOCAL'] = df_base['LOCAL'].astype('string').str.strip()

print('Padronizando maiusculo e minusculo somente para comparacao...')
df_insumos['LOCAL_COMPARACAO'] = df_insumos['Local'].str.lower()
df_base['LOCAL_COMPARACAO'] = df_base['LOCAL'].str.lower()

print('Filtrando somente linhas com pelo menos uma nota numerica valida...')
total_linhas_base_original = len(df_base)
df_base = df_base[df_base['QUANTIDADE_NOTAS_VALIDAS'] > 0].copy()
total_linhas_sem_nota_valida = total_linhas_base_original - len(df_base)

print('Ignorando NQA, IGN e valores vazios na base principal...')
mascara_local_valido = (
    df_base['LOCAL_COMPARACAO'].notna()
    & df_base['LOCAL_COMPARACAO'].ne('')
    & df_base['LOCAL_COMPARACAO'].ne('nqa')
    & df_base['LOCAL_COMPARACAO'].ne('ign')
)

total_linhas_ignoradas = int((~mascara_local_valido).sum())
df_base = df_base[mascara_local_valido].copy()

print('Procurando duplicados na coluna Local...')
df_duplicados = df_insumos[df_insumos['LOCAL_COMPARACAO'].duplicated(keep=False)].copy()

duplicados_iguais = []
duplicados_conflitantes = []

for local_comparacao, grupo in df_duplicados.groupby('LOCAL_COMPARACAO', dropna=False):
    locais_originais = grupo['Local'].dropna().astype(str).unique().tolist()
    contratacoes = grupo['contratacao'].dropna().astype(str).unique().tolist()

    item = {
        'local_comparacao': 'VAZIO' if pd.isna(local_comparacao) else str(local_comparacao),
        'locais_originais': locais_originais if locais_originais else ['VAZIO'],
        'quantidade_linhas': int(len(grupo)),
        'contratacoes_encontradas': contratacoes if contratacoes else ['VAZIO']
    }

    if len(item['contratacoes_encontradas']) <= 1:
        duplicados_iguais.append(item)
    else:
        duplicados_conflitantes.append(item)

resultado = {
    'arquivo_analisado': str(arquivo_insumos),
    'aba_analisada': 'insumos',
    'coluna_chave': 'Local',
    'coluna_retorno': 'contratacao',
    'comparacao_case_insensitive': True,
    'total_linhas': int(len(df_insumos)),
    'total_linhas_duplicadas': int(len(df_duplicados)),
    'total_locais_duplicados': int(df_duplicados['LOCAL_COMPARACAO'].nunique()),
    'total_duplicados_iguais': int(len(duplicados_iguais)),
    'total_duplicados_conflitantes': int(len(duplicados_conflitantes)),
    'duplicados_iguais': duplicados_iguais,
    'duplicados_conflitantes': duplicados_conflitantes
}

print('Agora vou inspecionar se os valores de LOCAL da base principal existem em Local...')

df_mapa = df_insumos.drop_duplicates(subset=['LOCAL_COMPARACAO', 'contratacao']).copy()
df_mapa = df_mapa.drop_duplicates(subset=['LOCAL_COMPARACAO'], keep='first')

mapa_contratacao = df_mapa.set_index('LOCAL_COMPARACAO')['contratacao']
df_base['CONTRATACAO ENCONTRADA'] = df_base['LOCAL_COMPARACAO'].map(mapa_contratacao)

locais_nao_encontrados = (
    df_base[df_base['CONTRATACAO ENCONTRADA'].isna()]['LOCAL']
    .fillna('VAZIO')
    .value_counts()
    .head(50)
    .to_dict()
)

resultado['inspecao_cruzamento_base_principal'] = {
    'coluna_base_principal': 'LOCAL',
    'coluna_base_auxiliar': 'Local',
    'comparacao_case_insensitive': True,
    'total_linhas_base_principal_original': int(total_linhas_base_original),
    'total_linhas_sem_nota_numerica_valida': int(total_linhas_sem_nota_valida),
    'total_linhas_base_principal': int(len(df_base)),
    'total_linhas_ignoradas_por_nqa_ign_nan': int(total_linhas_ignoradas),
    'total_encontrado': int(df_base['CONTRATACAO ENCONTRADA'].notna().sum()),
    'total_nao_encontrado': int(df_base['CONTRATACAO ENCONTRADA'].isna().sum()),
    'total_rede_propria': int((df_base['CONTRATACAO ENCONTRADA'] == 'rede propria').sum()),
    'total_rede_credenciada': int((df_base['CONTRATACAO ENCONTRADA'] == 'rede credenciada').sum()),
    'principais_locais_nao_encontrados': locais_nao_encontrados
}

print('Gravando os relatorios...')
pasta_saida = Path('saida_inspecao')
pasta_saida.mkdir(exist_ok=True)

arquivo_json = pasta_saida / 'inspecao_contratacao.json'
arquivo_txt = pasta_saida / 'inspecao_contratacao.txt'

with open(arquivo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resultado, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'INSPECAO DE CONTRATACAO',
    '',
    f'Arquivo analisado: {arquivo_insumos}',
    'Aba analisada: insumos',
    'Chave usada: Local',
    'Valor retornado: CONTRATACAO',
    'Comparacao em minusculo: sim',
    f"Total de linhas: {resultado['total_linhas']}",
    f"Total de linhas duplicadas: {resultado['total_linhas_duplicadas']}",
    f"Total de locais duplicados: {resultado['total_locais_duplicados']}",
    f"Total de duplicados iguais: {resultado['total_duplicados_iguais']}",
    f"Total de duplicados conflitantes: {resultado['total_duplicados_conflitantes']}",
    '',
    'DUPLICADOS IGUAIS:'
]

if duplicados_iguais:
    for item in duplicados_iguais:
        linhas_txt.append(
            f"- Local comparacao: {item['local_comparacao']} | Locais originais: {item['locais_originais']} | Quantidade: {item['quantidade_linhas']} | Contratacoes: {item['contratacoes_encontradas']}"
        )
else:
    linhas_txt.append('- Nenhum encontrado')

linhas_txt.append('')
linhas_txt.append('DUPLICADOS CONFLITANTES:')

if duplicados_conflitantes:
    for item in duplicados_conflitantes:
        linhas_txt.append(
            f"- Local comparacao: {item['local_comparacao']} | Locais originais: {item['locais_originais']} | Quantidade: {item['quantidade_linhas']} | Contratacoes: {item['contratacoes_encontradas']}"
        )
else:
    linhas_txt.append('- Nenhum encontrado')

cruzamento = resultado['inspecao_cruzamento_base_principal']

linhas_txt.extend([
    '',
    'INSPECAO DO CRUZAMENTO COM A BASE PRINCIPAL:',
    'Coluna da base principal: LOCAL',
    'Coluna da base auxiliar: Local',
    'Comparacao em minusculo: sim',
    'Considerando somente linhas com pelo menos uma nota numerica valida',
    'Ignorando na base principal: NQA, IGN e vazio/NaN',
    f"Total de linhas da base principal original: {cruzamento['total_linhas_base_principal_original']}",
    f"Total de linhas sem nota numerica valida: {cruzamento['total_linhas_sem_nota_numerica_valida']}",
    f"Total de linhas da base principal: {cruzamento['total_linhas_base_principal']}",
    f"Total de linhas ignoradas: {cruzamento['total_linhas_ignoradas_por_nqa_ign_nan']}",
    f"Total encontrado na planilha auxiliar: {cruzamento['total_encontrado']}",
    f"Total nao encontrado na planilha auxiliar: {cruzamento['total_nao_encontrado']}",
    f"Total rede propria: {cruzamento['total_rede_propria']}",
    f"Total rede credenciada: {cruzamento['total_rede_credenciada']}",
    '',
    'Principais locais nao encontrados:'
])

if cruzamento['principais_locais_nao_encontrados']:
    for local, quantidade in cruzamento['principais_locais_nao_encontrados'].items():
        linhas_txt.append(f'- {local}: {quantidade}')
else:
    linhas_txt.append('- Nenhum local ficou de fora')

with open(arquivo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print('Inspecao concluida.')
print(f'Relatorio JSON: {arquivo_json}')
print(f'Relatorio TXT: {arquivo_txt}')
