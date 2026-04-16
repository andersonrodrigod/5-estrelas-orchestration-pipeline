# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/02_base_com_contratacao.csv')
arquivo_grupos = Path('data/grupos_classificacao.json')
arquivo_nomes = Path('data/nomes_classificacao.json')
arquivo_saida = Path('data_exec_indiv/03_base_com_classificacao.csv')

pasta_resumo = Path('saida_resumo')
arquivo_resumo_json = pasta_resumo / 'exec_03_classificacao_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_03_classificacao_resumo.txt'
arquivo_auditoria_csv = pasta_resumo / 'exec_03_classificacao_auditoria.csv'
arquivo_sobrescritas_csv = pasta_resumo / 'exec_03_classificacao_sobrescritas.csv'
arquivo_nao_classificados_csv = pasta_resumo / 'exec_03_classificacao_nao_classificados.csv'
arquivo_nao_classificados_detalhado_csv = pasta_resumo / 'exec_03_classificacao_nao_classificados_detalhado.csv'


def carregar_json(caminho):
    with open(caminho, 'r', encoding='utf-8-sig') as arquivo:
        return json.load(arquivo)


def normalizar_valor_ou_lista(valor):
    if valor is None:
        return None

    if isinstance(valor, list):
        return valor

    return [valor]


def obter_nome_classificacao(chave_grupo, mapa_nomes):
    return mapa_nomes.get(chave_grupo, chave_grupo.replace('_', ' '))


def descrever_regra(grupo):
    partes = []

    if grupo['tipo_igual'] is not None:
        partes.append(f"TIPO igual a {grupo['tipo_igual']}")

    if grupo['tipo_diferente'] is not None:
        partes.append(f"TIPO diferente de {grupo['tipo_diferente']}")

    if grupo['contratacao_igual']:
        partes.append(f"CONTRATACAO igual a {grupo['contratacao_igual']}")

    if grupo['contratacao_diferente']:
        partes.append(f"CONTRATACAO diferente de {grupo['contratacao_diferente']}")

    if grupo['filtro_local']:
        partes.append(f"LOCAL contem {grupo['filtro_local']}")

    if grupo['filtro_local_diferente']:
        partes.append(f"LOCAL diferente de {grupo['filtro_local_diferente']}")

    if grupo['filtro_especialidade']:
        partes.append(f"ESPECIALIDADE contem {grupo['filtro_especialidade']}")

    if grupo['filtro_especialidade_diferente']:
        partes.append(f"ESPECIALIDADE diferente de {grupo['filtro_especialidade_diferente']}")

    if not partes:
        return 'sem filtros'

    return ' | '.join(str(parte) for parte in partes)


print('Iniciando execucao 03 - classificacao...')
print(f'Lendo arquivo da execucao 02: {arquivo_entrada}')
print(f'Lendo arquivo de grupos: {arquivo_grupos}')
print(f'Lendo arquivo de nomes: {arquivo_nomes}')

grupos = carregar_json(arquivo_grupos)
nomes_classificacao = carregar_json(arquivo_nomes)

df = pd.read_csv(arquivo_entrada, low_memory=False)
df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
df['CONTRATACAO'] = df['CONTRATACAO'].astype('string').str.strip().str.lower()
df['LOCAL'] = df['LOCAL'].astype('string').str.strip()
df['ESPECIALIDADE'] = df['ESPECIALIDADE'].astype('string').str.strip()

if 'CLASSIFICACAO' not in df.columns:
    df['CLASSIFICACAO'] = None

df['CLASSIFICACAO'] = df['CLASSIFICACAO'].astype('string').str.strip()

auditoria_regras = []
sobrescritas = []

for ordem, grupo in enumerate(grupos, start=1):
    nome_classificacao = obter_nome_classificacao(grupo['grupo_classificacao'], nomes_classificacao)
    tipos_iguais = normalizar_valor_ou_lista(grupo['tipo_igual'])
    tipos_diferentes = normalizar_valor_ou_lista(grupo['tipo_diferente'])

    filtro = pd.Series(True, index=df.index)

    if tipos_iguais is not None:
        filtro = filtro & df['TIPO'].isin(tipos_iguais)

    if tipos_diferentes is not None:
        filtro = filtro & ~df['TIPO'].isin(tipos_diferentes)

    if grupo['contratacao_igual']:
        filtro = filtro & (df['CONTRATACAO'] == grupo['contratacao_igual'])

    if grupo['contratacao_diferente']:
        filtro = filtro & (df['CONTRATACAO'] != grupo['contratacao_diferente'])

    if grupo['filtro_local']:
        filtro = filtro & df['LOCAL'].str.contains(
            grupo['filtro_local'],
            case=False,
            na=False,
            regex=True
        )

    if grupo['filtro_local_diferente']:
        filtro = filtro & ~df['LOCAL'].str.contains(
            grupo['filtro_local_diferente'],
            case=False,
            na=False,
            regex=True
        )

    if grupo['filtro_especialidade']:
        filtro = filtro & df['ESPECIALIDADE'].str.contains(
            grupo['filtro_especialidade'],
            case=False,
            na=False,
            regex=True
        )

    if grupo['filtro_especialidade_diferente']:
        filtro = filtro & ~df['ESPECIALIDADE'].str.contains(
            grupo['filtro_especialidade_diferente'],
            case=False,
            na=False,
            regex=True
        )

    total_atingidas = int(filtro.sum())
    classificacao_antes = df.loc[filtro, 'CLASSIFICACAO'].copy()
    vazias_antes = classificacao_antes.isna() | (classificacao_antes == '')
    total_vazias_antes = int(vazias_antes.sum())
    total_ja_classificadas = int((~vazias_antes).sum())

    mascara_sobrescritas = pd.Series(False, index=df.index)
    mascara_sobrescritas.loc[classificacao_antes.index] = ~vazias_antes
    sobrescritas_regra = df.loc[mascara_sobrescritas].copy()
    total_sobrescritas = int(len(sobrescritas_regra))

    if total_sobrescritas > 0:
        resumo_sobrescritas = (
            sobrescritas_regra['CLASSIFICACAO']
            .fillna('VAZIO')
            .value_counts()
            .reset_index()
        )
        resumo_sobrescritas.columns = ['CLASSIFICACAO_ANTERIOR', 'QUANTIDADE']

        for _, linha in resumo_sobrescritas.iterrows():
            sobrescritas.append({
                'ORDEM_REGRA': ordem,
                'NOME_LISTA': grupo['nome_lista'],
                'CHAVE_GRUPO': grupo['grupo_classificacao'],
                'CLASSIFICACAO_NOVA': nome_classificacao,
                'CLASSIFICACAO_ANTERIOR': linha['CLASSIFICACAO_ANTERIOR'],
                'QUANTIDADE': int(linha['QUANTIDADE'])
            })

    df.loc[filtro, 'CLASSIFICACAO'] = nome_classificacao

    auditoria_regras.append({
        'ORDEM_REGRA': ordem,
        'CHAVE_GRUPO': grupo['grupo_classificacao'],
        'CLASSIFICACAO': nome_classificacao,
        'NOME_LISTA': grupo['nome_lista'],
        'PALAVRA_FILTRO': grupo['palavra_filtro'],
        'REGRA': descrever_regra(grupo),
        'TOTAL_ATINGIDAS': total_atingidas,
        'TOTAL_CLASSIFICADAS_VAZIAS': total_vazias_antes,
        'TOTAL_JA_CLASSIFICADAS': total_ja_classificadas,
        'TOTAL_SOBRESCRITAS': total_sobrescritas
    })

df_saida = df.copy()
arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(exist_ok=True)
df_saida.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')

filtro_nao_classificados = df_saida['CLASSIFICACAO'].isna() | (df_saida['CLASSIFICACAO'] == '')
df_nao_classificados = df_saida[filtro_nao_classificados].copy()

resumo_nao_classificados = (
    df_nao_classificados['LOCAL']
    .fillna('VAZIO')
    .value_counts()
    .reset_index()
)
resumo_nao_classificados.columns = ['LOCAL', 'QUANTIDADE']
df_nao_classificados_detalhado = df_nao_classificados[['TIPO', 'CONTRATACAO', 'LOCAL', 'ESPECIALIDADE']].copy()

total_classificadas = int((~filtro_nao_classificados).sum())
total_nao_classificadas = int(filtro_nao_classificados.sum())
total_sobrescritas_geral = int(sum(item['QUANTIDADE'] for item in sobrescritas))

resumo = {
    'execucao': 'exec_03_classificacao',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_grupos': str(arquivo_grupos),
    'arquivo_nomes': str(arquivo_nomes),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_classificadas': total_classificadas,
    'total_nao_classificadas': total_nao_classificadas,
    'total_sobrescritas': total_sobrescritas_geral,
    'total_regras': len(grupos)
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 03 - CLASSIFICACAO',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de grupos: {resumo['arquivo_grupos']}",
    f"Arquivo de nomes: {resumo['arquivo_nomes']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total classificadas: {resumo['total_classificadas']}",
    f"Total nao classificadas: {resumo['total_nao_classificadas']}",
    f"Total sobrescritas: {resumo['total_sobrescritas']}",
    f"Total de regras aplicadas: {resumo['total_regras']}",
    '',
    'REGRAS APLICADAS:'
]

for item in auditoria_regras:
    linhas_txt.append(
        f"- ordem={item['ORDEM_REGRA']} | lista={item['NOME_LISTA']} | classificacao={item['CLASSIFICACAO']} | "
        f"atingidas={item['TOTAL_ATINGIDAS']} | vazias={item['TOTAL_CLASSIFICADAS_VAZIAS']} | "
        f"ja_classificadas={item['TOTAL_JA_CLASSIFICADAS']} | sobrescritas={item['TOTAL_SOBRESCRITAS']}"
    )

linhas_txt.append('')
linhas_txt.append('SOBRESCRITAS:')

if sobrescritas:
    for item in sobrescritas:
        linhas_txt.append(
            f"- ordem={item['ORDEM_REGRA']} | lista={item['NOME_LISTA']} | "
            f"{item['CLASSIFICACAO_ANTERIOR']} -> {item['CLASSIFICACAO_NOVA']}: {item['QUANTIDADE']}"
        )
else:
    linhas_txt.append('- Nenhuma sobrescrita encontrada')

linhas_txt.append('')
linhas_txt.append('NAO CLASSIFICADAS - PRINCIPAIS LOCAIS:')

if not resumo_nao_classificados.empty:
    for _, linha in resumo_nao_classificados.head(50).iterrows():
        linhas_txt.append(f"- {linha['LOCAL']}: {int(linha['QUANTIDADE'])}")
else:
    linhas_txt.append('- Nenhuma linha sem classificacao')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

df_auditoria = pd.DataFrame(auditoria_regras)
df_auditoria.to_csv(arquivo_auditoria_csv, index=False, encoding='utf-8-sig')

df_sobrescritas = pd.DataFrame(sobrescritas)
df_sobrescritas.to_csv(arquivo_sobrescritas_csv, index=False, encoding='utf-8-sig')

resumo_nao_classificados.to_csv(arquivo_nao_classificados_csv, index=False, encoding='utf-8-sig')
df_nao_classificados_detalhado.to_csv(arquivo_nao_classificados_detalhado_csv, index=False, encoding='utf-8-sig')

print('Execucao 03 finalizada.')
