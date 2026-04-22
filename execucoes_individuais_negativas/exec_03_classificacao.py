# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/negativas/02_base_com_contratacao.csv')
arquivo_grupos = Path('data/grupos_classificacao.json')
arquivo_nomes = Path('data/nomes_classificacao.json')
arquivo_saida = Path('data_exec_indiv/negativas/03_base_com_classificacao.csv')

pasta_resumo = Path('saida_resumo_negativas') / 'exec_03_classificacao'
arquivo_resumo_json = pasta_resumo / 'exec_03_classificacao_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_03_classificacao_resumo.txt'
arquivo_auditoria_csv = pasta_resumo / 'exec_03_classificacao_auditoria.csv'
arquivo_sobrescritas_csv = pasta_resumo / 'exec_03_classificacao_sobrescritas.csv'
arquivo_nao_classificados_detalhado_csv = pasta_resumo / 'exec_03_classificacao_nao_classificados_detalhado.csv'
arquivo_regras_explicadas_txt = Path('data/grupos_classificacao_explicado.txt')

coluna_ordem_regra = '_ORDEM_REGRA_CLASSIFICACAO'
coluna_nome_lista = '_NOME_LISTA_CLASSIFICACAO'
coluna_chave_grupo = '_CHAVE_GRUPO_CLASSIFICACAO'


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

    if grupo.get('aplicar_somente_vazios'):
        partes.append("CLASSIFICACAO vazia")

    if not partes:
        return 'sem filtros'

    return ' | '.join(str(parte) for parte in partes)


print('Iniciando execucao 03 - classificacao negativas...')
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

df[coluna_ordem_regra] = pd.NA
df[coluna_nome_lista] = pd.NA
df[coluna_chave_grupo] = pd.NA

classificadas_antes_execucao = df['CLASSIFICACAO'].notna() & (df['CLASSIFICACAO'] != '')
df.loc[classificadas_antes_execucao, coluna_ordem_regra] = 'ORIGINAL'
df.loc[classificadas_antes_execucao, coluna_nome_lista] = 'BASE DE ENTRADA'
df.loc[classificadas_antes_execucao, coluna_chave_grupo] = 'CLASSIFICACAO_PRE_EXISTENTE'

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
    aplicar_somente_vazios = bool(grupo.get('aplicar_somente_vazios'))
    filtro_aplicacao = filtro

    if aplicar_somente_vazios:
        filtro_aplicacao = filtro & (
            df['CLASSIFICACAO'].isna() | (df['CLASSIFICACAO'] == '')
        )

    mascara_sobrescritas = pd.Series(False, index=df.index)
    classificacao_antes_aplicacao = df.loc[filtro_aplicacao, 'CLASSIFICACAO'].copy()
    vazias_antes_aplicacao = (
        classificacao_antes_aplicacao.isna() | (classificacao_antes_aplicacao == '')
    )
    mascara_sobrescritas.loc[classificacao_antes_aplicacao.index] = ~vazias_antes_aplicacao
    sobrescritas_regra = df.loc[mascara_sobrescritas].copy()
    total_sobrescritas = int(len(sobrescritas_regra))

    if total_sobrescritas > 0:
        resumo_sobrescritas = (
            sobrescritas_regra
            .assign(
                ORDEM_REGRA_ANTERIOR=sobrescritas_regra[coluna_ordem_regra].astype('string').fillna('DESCONHECIDA'),
                ORDEM_REGRA_NOVA=ordem,
                NOME_LISTA_ANTERIOR=sobrescritas_regra[coluna_nome_lista].astype('string').fillna('DESCONHECIDA'),
                NOME_LISTA_NOVA=grupo['nome_lista'],
                CHAVE_GRUPO_ANTERIOR=sobrescritas_regra[coluna_chave_grupo].astype('string').fillna('DESCONHECIDA'),
                CHAVE_GRUPO_NOVA=grupo['grupo_classificacao'],
                CLASSIFICACAO_ANTERIOR=sobrescritas_regra['CLASSIFICACAO'].fillna('VAZIO'),
                CLASSIFICACAO_NOVA=nome_classificacao
            )
            .groupby([
                'ORDEM_REGRA_ANTERIOR',
                'ORDEM_REGRA_NOVA',
                'NOME_LISTA_ANTERIOR',
                'NOME_LISTA_NOVA',
                'CHAVE_GRUPO_ANTERIOR',
                'CHAVE_GRUPO_NOVA',
                'CLASSIFICACAO_ANTERIOR',
                'CLASSIFICACAO_NOVA'
            ], dropna=False)
            .size()
            .reset_index(name='QUANTIDADE')
        )

        for _, linha in resumo_sobrescritas.iterrows():
            sobrescritas.append({
                'ORDEM_REGRA_ANTERIOR': linha['ORDEM_REGRA_ANTERIOR'],
                'ORDEM_REGRA_NOVA': int(linha['ORDEM_REGRA_NOVA']),
                'NOME_LISTA_ANTERIOR': linha['NOME_LISTA_ANTERIOR'],
                'NOME_LISTA_NOVA': linha['NOME_LISTA_NOVA'],
                'CHAVE_GRUPO_ANTERIOR': linha['CHAVE_GRUPO_ANTERIOR'],
                'CHAVE_GRUPO_NOVA': linha['CHAVE_GRUPO_NOVA'],
                'CLASSIFICACAO_ANTERIOR': linha['CLASSIFICACAO_ANTERIOR'],
                'CLASSIFICACAO_NOVA': linha['CLASSIFICACAO_NOVA'],
                'QUANTIDADE': int(linha['QUANTIDADE'])
            })

    df.loc[filtro_aplicacao, 'CLASSIFICACAO'] = nome_classificacao
    df.loc[filtro_aplicacao, coluna_ordem_regra] = ordem
    df.loc[filtro_aplicacao, coluna_nome_lista] = grupo['nome_lista']
    df.loc[filtro_aplicacao, coluna_chave_grupo] = grupo['grupo_classificacao']

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

df_saida = df.drop(columns=[
    coluna_ordem_regra,
    coluna_nome_lista,
    coluna_chave_grupo
]).copy()
arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
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
colunas_nao_classificados_detalhado = [
    'CDUSUARIO',
    'UF',
    'MES',
    'DIA',
    'ANO',
    'TIPO',
    'CONTRATACAO',
    'LOCAL',
    'ESPECIALIDADE'
]

df_nao_classificados_detalhado = df_nao_classificados[colunas_nao_classificados_detalhado].copy()

total_classificadas = int((~filtro_nao_classificados).sum())
total_nao_classificadas = int(filtro_nao_classificados.sum())
total_sobrescritas_geral = int(sum(item['QUANTIDADE'] for item in sobrescritas))

resumo = {
    'execucao': 'exec_03_classificacao_negativas',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_grupos': str(arquivo_grupos),
    'arquivo_nomes': str(arquivo_nomes),
    'arquivo_saida': str(arquivo_saida),
    'arquivo_regras_explicadas': str(arquivo_regras_explicadas_txt),
    'arquivo_auditoria': str(arquivo_auditoria_csv),
    'arquivo_sobrescritas': str(arquivo_sobrescritas_csv),
    'arquivo_nao_classificados_detalhado': str(arquivo_nao_classificados_detalhado_csv),
    'total_linhas_entrada': int(len(df)),
    'total_classificadas': total_classificadas,
    'total_nao_classificadas': total_nao_classificadas,
    'total_sobrescritas': total_sobrescritas_geral,
    'total_regras': len(grupos)
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 03 - CLASSIFICACAO NEGATIVAS',
    '',
    'ARQUIVOS:',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    f"Regras explicadas: {resumo['arquivo_regras_explicadas']}",
    f"Auditoria por regra: {resumo['arquivo_auditoria']}",
    f"Sobrescritas detalhadas: {resumo['arquivo_sobrescritas']}",
    f"Nao classificados detalhado: {resumo['arquivo_nao_classificados_detalhado']}",
    '',
    'TOTAIS:',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total classificadas: {resumo['total_classificadas']}",
    f"Total nao classificadas: {resumo['total_nao_classificadas']}",
    f"Total sobrescritas: {resumo['total_sobrescritas']}",
    f"Total de regras aplicadas: {resumo['total_regras']}",
    '',
    'REGRAS:',
    f"As regras completas estao documentadas em: {resumo['arquivo_regras_explicadas']}",
    f"Nesta execucao foram aplicadas {resumo['total_regras']} regras.",
    '',
    'REGRAS COM MAIS LINHAS ATINGIDAS:'
]

for item in sorted(auditoria_regras, key=lambda regra: regra['TOTAL_ATINGIDAS'], reverse=True)[:10]:
    linhas_txt.append(
        f"- Regra {item['ORDEM_REGRA']} - {item['CLASSIFICACAO']}: "
        f"{item['TOTAL_ATINGIDAS']} linhas atingidas "
        f"({item['TOTAL_SOBRESCRITAS']} sobrescritas)"
    )

linhas_txt.append('')
linhas_txt.append('SOBRESCRITAS:')

if sobrescritas:
    linhas_txt.append(f"Total de linhas sobrescritas: {resumo['total_sobrescritas']}")
    linhas_txt.append(f"Detalhamento completo em: {resumo['arquivo_sobrescritas']}")
    linhas_txt.append('')
    linhas_txt.append('Principais sobrescritas:')

    for item in sorted(sobrescritas, key=lambda sobrescrita: sobrescrita['QUANTIDADE'], reverse=True)[:20]:
        linhas_txt.extend([
            f"[{item['QUANTIDADE']} linhas] Regra {item['ORDEM_REGRA_NOVA']} substituiu Regra {item['ORDEM_REGRA_ANTERIOR']}",
            f"De: {item['CLASSIFICACAO_ANTERIOR']}",
            f"Para: {item['CLASSIFICACAO_NOVA']}",
            f"Lista anterior: {item['NOME_LISTA_ANTERIOR']}",
            f"Lista nova: {item['NOME_LISTA_NOVA']}",
            f"Chave anterior: {item['CHAVE_GRUPO_ANTERIOR']}",
            f"Chave nova: {item['CHAVE_GRUPO_NOVA']}",
            ''
        ])
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

colunas_sobrescritas = [
    'ORDEM_REGRA_ANTERIOR',
    'ORDEM_REGRA_NOVA',
    'NOME_LISTA_ANTERIOR',
    'NOME_LISTA_NOVA',
    'CHAVE_GRUPO_ANTERIOR',
    'CHAVE_GRUPO_NOVA',
    'CLASSIFICACAO_ANTERIOR',
    'CLASSIFICACAO_NOVA',
    'QUANTIDADE'
]
df_sobrescritas = pd.DataFrame(sobrescritas, columns=colunas_sobrescritas)
if not df_sobrescritas.empty:
    df_sobrescritas = df_sobrescritas.sort_values(
        ['QUANTIDADE', 'ORDEM_REGRA_NOVA'],
        ascending=[False, True]
    )
df_sobrescritas.to_csv(arquivo_sobrescritas_csv, index=False, encoding='utf-8-sig')

df_nao_classificados_detalhado.to_csv(arquivo_nao_classificados_detalhado_csv, index=False, encoding='utf-8-sig')

print('Execucao 03 negativas finalizada.')
