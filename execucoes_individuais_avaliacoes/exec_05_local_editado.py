# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/04_base_com_classificacao.csv')
arquivo_insumos = Path('utils/insumos/insumos 5 estrelas.xlsx')
arquivo_saida = Path('data_exec_indiv/avaliacoes/05_base_com_local_editado.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_05_local_editado'
arquivo_resumo_json = pasta_resumo / 'exec_05_local_editado_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_05_local_editado_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_05_local_editado_resumo.csv'
arquivo_atualizados_csv = pasta_resumo / 'exec_05_local_editado_atualizados.csv'
arquivo_nao_encontrados_csv = pasta_resumo / 'exec_05_local_editado_nao_encontrados.csv'


def normalizar_texto(serie):
    return serie.astype('string').str.strip()


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


print('Iniciando execucao 05 - local editado...')
print(f'Lendo arquivo da execucao 04: {arquivo_entrada}')
print(f'Lendo arquivo de insumos: {arquivo_insumos}')

df = ler_csv_padronizado(arquivo_entrada)
df_insumos = pd.read_excel(arquivo_insumos, sheet_name='insumos')

df['LOCAL'] = normalizar_texto(df['LOCAL'])
df['LOCAL EDITADO'] = normalizar_texto(df['LOCAL EDITADO'])
df['LOCAL_COMPARACAO'] = df['LOCAL'].str.lower()

df_insumos['Local'] = normalizar_texto(df_insumos['Local'])
df_insumos['local editado'] = normalizar_texto(df_insumos['local editado'])
df_insumos['LOCAL_COMPARACAO'] = df_insumos['Local'].str.lower()

df_insumos = df_insumos.dropna(subset=['LOCAL_COMPARACAO'])
df_insumos = df_insumos.drop_duplicates(subset=['LOCAL_COMPARACAO'], keep='first')

mapa_local_editado = df_insumos.set_index('LOCAL_COMPARACAO')['local editado']

local_editado_antes = df['LOCAL EDITADO'].copy()
local_editado_novo = df['LOCAL_COMPARACAO'].map(mapa_local_editado)

encontrados = local_editado_novo.notna() & (local_editado_novo != '')
nao_encontrados = ~encontrados
nao_alterados = encontrados & (local_editado_antes.fillna('') == local_editado_novo.fillna(''))
atualizados = encontrados & ~nao_alterados

df.loc[atualizados, 'LOCAL EDITADO'] = local_editado_novo.loc[atualizados]
df = df.drop(columns=['LOCAL_COMPARACAO'])

resumo_atualizados = (
    df.loc[atualizados, ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO']]
    .value_counts()
    .reset_index(name='QUANTIDADE')
    .sort_values(['QUANTIDADE', 'LOCAL'], ascending=[False, True])
)

resumo_nao_alterados = (
    df.loc[nao_alterados, ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO']]
    .value_counts()
    .reset_index(name='QUANTIDADE')
    .sort_values(['QUANTIDADE', 'LOCAL'], ascending=[False, True])
)

resumo_nao_encontrados = (
    df.loc[nao_encontrados, ['LOCAL']]
    .fillna('VAZIO')
    .value_counts()
    .reset_index(name='QUANTIDADE')
    .sort_values(['QUANTIDADE', 'LOCAL'], ascending=[False, True])
)

total_atualizados = int(atualizados.sum())
total_nao_alterados = int(nao_alterados.sum())
total_nao_encontrados = int(nao_encontrados.sum())

print(f'Total de linhas recebidas: {len(df)}')
print(f'Total atualizadas: {total_atualizados}')
print(f'Total nao alteradas: {total_nao_alterados}')
print(f'Total nao encontradas: {total_nao_encontrados}')
print(f'Gravando arquivo da execucao 05: {arquivo_saida}')

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
salvar_csv_padronizado(df, arquivo_saida)

resumo = {
    'execucao': 'exec_05_local_editado',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_insumos': str(arquivo_insumos),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_atualizados': total_atualizados,
    'total_nao_alterados': total_nao_alterados,
    'total_nao_encontrados': total_nao_encontrados,
    'valores_atualizados': transformar_em_lista_registros(
        resumo_atualizados,
        ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO', 'QUANTIDADE']
    ),
    'valores_nao_alterados': transformar_em_lista_registros(
        resumo_nao_alterados,
        ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO', 'QUANTIDADE']
    ),
    'valores_nao_encontrados': transformar_em_lista_registros(
        resumo_nao_encontrados,
        ['LOCAL', 'QUANTIDADE']
    )
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 05 - LOCAL EDITADO',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de insumos: {resumo['arquivo_insumos']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Registros atualizados: {resumo['total_atualizados']}",
    f"Registros nao alterados: {resumo['total_nao_alterados']}",
    f"Registros nao encontrados: {resumo['total_nao_encontrados']}",
    '',
    'PRINCIPAIS LOCAIS NAO ENCONTRADOS:'
]

if resumo['valores_nao_encontrados']:
    for item in resumo['valores_nao_encontrados'][:50]:
        linhas_txt.append(f"- {item['LOCAL']}: {item['QUANTIDADE']}")
else:
    linhas_txt.append('- Nenhum local sem correspondencia')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

df_resumo_csv = pd.DataFrame([
    {
        'EXECUCAO': resumo['execucao'],
        'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
        'ARQUIVO_INSUMOS': resumo['arquivo_insumos'],
        'ARQUIVO_SAIDA': resumo['arquivo_saida'],
        'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
        'TOTAL_ATUALIZADOS': resumo['total_atualizados'],
        'TOTAL_NAO_ALTERADOS': resumo['total_nao_alterados'],
        'TOTAL_NAO_ENCONTRADOS': resumo['total_nao_encontrados']
    }
])
salvar_csv_padronizado(df_resumo_csv, arquivo_resumo_csv)

salvar_csv_padronizado(resumo_atualizados, arquivo_atualizados_csv)
salvar_csv_padronizado(resumo_nao_encontrados, arquivo_nao_encontrados_csv)

print('Execucao 05 finalizada.')

