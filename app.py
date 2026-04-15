# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_base = Path('data/5_estrelas_fevereiro.csv')
arquivo_insumos = Path('utils/insumos/insumos 5 estrelas.xlsx')
pasta_saida = Path('saida_resumo')

renomear_colunas = {
    'Nota geral': 'NOTA GERAL',
    'Contratacao': 'CONTRATACAO',
    'Operadora': 'OPERADORA',
    'Local editado': 'LOCAL EDITADO',
    'Meta': 'META',
    'Resultado da unidade': 'RESULTADO DA UNIDADE',
    'Status unidade': 'STATUS UNIDADE'
}

# Garante a pasta onde os resumos finais serao gravados.
pasta_saida.mkdir(exist_ok=True)

# Le a base principal que sera tratada.
df = pd.read_csv(arquivo_base, low_memory=False)
df = df.rename(columns=renomear_colunas)

colunas = [
    'NOTA GERAL',
    'CONTRATACAO',
    'CLASSIFICACAO',
    'OPERADORA',
    'LOCAL EDITADO',
    'META',
    'RESULTADO DA UNIDADE',
    'STATUS UNIDADE'
]

colunas_criadas = []

# Garante as colunas obrigatorias da automacao.
for coluna in colunas:
    if coluna not in df.columns:
        df[coluna] = None
        colunas_criadas.append(coluna)

colunas_notas = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']

# Converte as notas para numero e calcula a Nota geral.
for coluna in colunas_notas:
    df[coluna] = pd.to_numeric(df[coluna], errors='coerce')

df['Quantidade notas validas'] = df[colunas_notas].notna().sum(axis=1)
df['NOTA GERAL'] = df[colunas_notas].mean(axis=1).round(2)

total_linhas_original = len(df)
df_valido = df[df['Quantidade notas validas'] > 0].copy()
total_linhas_validas = len(df_valido)
total_linhas_sem_nota = total_linhas_original - total_linhas_validas

# Prepara a coluna LOCAL para o cruzamento com os insumos.
df_valido['LOCAL'] = df_valido['LOCAL'].astype('string').str.strip()
df_valido['LOCAL_COMPARACAO'] = df_valido['LOCAL'].str.lower()

total_rede_propria = 0
total_rede_credenciada = 0
total_nao_encontrado = 0
principais_locais_nao_encontrados = {}

# Se o arquivo de insumos existir, faz o cruzamento da contratacao.
if arquivo_insumos.exists():
    df_insumos = pd.read_excel(arquivo_insumos, sheet_name='insumos')
    df_insumos['Local'] = df_insumos['Local'].astype('string').str.strip()
    df_insumos['contratacao'] = df_insumos['contratacao'].astype('string').str.strip().str.lower()
    df_insumos['LOCAL_COMPARACAO'] = df_insumos['Local'].str.lower()

    # Remove duplicidades antes de montar o mapa de busca.
    df_insumos = df_insumos.drop_duplicates(subset=['LOCAL_COMPARACAO', 'contratacao'])
    df_insumos = df_insumos.drop_duplicates(subset=['LOCAL_COMPARACAO'], keep='first')

    mapa_contratacao = df_insumos.set_index('LOCAL_COMPARACAO')['contratacao']
    df_valido['CONTRATACAO'] = df_valido['LOCAL_COMPARACAO'].map(mapa_contratacao)
    df_valido['CONTRATACAO'] = df_valido['CONTRATACAO'].astype('string').str.strip().str.lower()

    total_rede_propria = int((df_valido['CONTRATACAO'] == 'rede propria').sum())
    total_rede_credenciada = int((df_valido['CONTRATACAO'] == 'rede credenciada').sum())
    total_nao_encontrado = int(df_valido['CONTRATACAO'].isna().sum())

    principais_locais_nao_encontrados = (
        df_valido[df_valido['CONTRATACAO'].isna()]['LOCAL']
        .fillna('VAZIO')
        .value_counts()
        .head(50)
        .to_dict()
    )

# Monta o resumo final da execucao.
resumo = {
    'arquivo_base': str(arquivo_base),
    'arquivo_insumos': str(arquivo_insumos),
    'total_linhas_original': int(total_linhas_original),
    'total_linhas_com_nota_valida': int(total_linhas_validas),
    'total_linhas_sem_nota_valida': int(total_linhas_sem_nota),
    'colunas_criadas': colunas_criadas,
    'nota_geral': {
        'linhas_com_nota_geral': int(df['NOTA GERAL'].notna().sum()),
        'linhas_sem_nota_geral': int(df['NOTA GERAL'].isna().sum())
    },
    'contratacao': {
        'arquivo_insumos_encontrado': arquivo_insumos.exists(),
        'total_rede_propria': total_rede_propria,
        'total_rede_credenciada': total_rede_credenciada,
        'total_nao_encontrado': total_nao_encontrado,
        'principais_locais_nao_encontrados': principais_locais_nao_encontrados
    }
}

arquivo_json = pasta_saida / 'resumo_execucao.json'
arquivo_txt = pasta_saida / 'resumo_execucao.txt'

# Grava o resumo estruturado em JSON.
with open(arquivo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO',
    '',
    f"Arquivo base: {resumo['arquivo_base']}",
    f"Arquivo de insumos: {resumo['arquivo_insumos']}",
    '',
    f"Total de linhas original: {resumo['total_linhas_original']}",
    f"Total de linhas com nota valida: {resumo['total_linhas_com_nota_valida']}",
    f"Total de linhas sem nota valida: {resumo['total_linhas_sem_nota_valida']}",
    '',
    'COLUNAS CRIADAS:'
]

if resumo['colunas_criadas']:
    for coluna in resumo['colunas_criadas']:
        linhas_txt.append(f'- {coluna}')
else:
    linhas_txt.append('- Nenhuma coluna criada')

linhas_txt.extend([
    '',
    'NOTA GERAL:',
    f"Linhas com nota geral: {resumo['nota_geral']['linhas_com_nota_geral']}",
    f"Linhas sem nota geral: {resumo['nota_geral']['linhas_sem_nota_geral']}",
    '',
    'CONTRATACAO:',
    f"Arquivo de insumos encontrado: {resumo['contratacao']['arquivo_insumos_encontrado']}",
    f"Total rede propria: {resumo['contratacao']['total_rede_propria']}",
    f"Total rede credenciada: {resumo['contratacao']['total_rede_credenciada']}",
    f"Total nao encontrado: {resumo['contratacao']['total_nao_encontrado']}",
    '',
    'Principais locais nao encontrados:'
])

if resumo['contratacao']['principais_locais_nao_encontrados']:
    for local, quantidade in resumo['contratacao']['principais_locais_nao_encontrados'].items():
        linhas_txt.append(f'- {local}: {quantidade}')
else:
    linhas_txt.append('- Nenhum local ficou de fora')

# Grava uma versao resumida e facil de ler em TXT.
with open(arquivo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

# BLOCO OPCIONAL - GRAVAR BASE TRATADA
# Descomente as linhas abaixo quando quiser salvar a base com
# NOTA GERAL e CONTRATACAO ja preenchidas.

arquivo_base_tratada = Path('data/5_estrelas_fevereiro_tratado.csv')
df_saida = df.copy()
df_saida.loc[df_valido.index, 'CONTRATACAO'] = df_valido['CONTRATACAO']
df_saida.to_csv(arquivo_base_tratada, index=False, encoding='utf-8-sig')
