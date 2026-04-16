# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_base = Path('data/5_estrelas_fevereiro_tratado.csv')
arquivo_grupos = Path('investigacoes/grupos_classificacao.json')
pasta_saida = Path('saida_investigacao')

pasta_saida.mkdir(exist_ok=True)

# Ajuste aqui os nomes finais da classificacao sem mexer no json de filtros.
nomes_grupo_personalizados = {
    'CRED_ATEND_EMERGENCIA': 'CRED_ATEND EMERGENCIA',
    'HOSPITALAR': 'HOSPITALAR',
    'QUALIVIDA': 'QUALIVIDA',
    'NASCER_BEM': 'NASCER BEM',
    'TELEMEDICINA': 'TELEMEDICINA',
    'PRODUTO_COORDENADO': 'PRODUTO COORDENADO',
    'CASE': 'CASE',
    'TRANSPLANTE': 'TRANSPLANTE',
    'TEA': 'TEA',
    'GESTAR_BEM': 'GESTAR BEM',
    'TELECONSULTA': 'TELECONSULTA',
    'TELECONSULTA_ELETIVA': 'TELECONSULTA ELETIVA',
    'TELECONSULTA_URGENCIA': 'TELECONSULTA URGENCIA',
    'MED_PREV': 'MED PREV',
    'CRED_TRATAMENTO': 'CRED_TRATAMENTO',
    'HAPCLINICA': 'HAPCLINICA',
    'ODONTOLOGIA': 'ODONTOLOGIA',
    'CRED_ATEND_ELETIVO': 'CRED_ATEND ELETIVO',
    'LABORATORIO': 'LABORATORIO',
    'CRED_LABORATORIO': 'CRED_LABORATORIO',
    'VIDA_IMAGEM': 'VIDA IMAGEM',
    'CRED_EXAMES': 'CRED_EXAMES',
    'CRED_INTERNACAO': 'CRED_INTERNACAO',
    'INTERNACAO': 'INTERNACAO',
    'INTERNACAO_PGC': 'INTERNACAO PGC',
}


def carregar_grupos():
    with open(arquivo_grupos, 'r', encoding='utf-8') as arquivo:
        return json.load(arquivo)


def normalizar_valor_ou_lista(valor):
    if valor is None:
        return None

    if isinstance(valor, list):
        return valor

    return [valor]


def montar_contagem_coluna(dataframe, nome_coluna):
    contagem_coluna = (
        dataframe[nome_coluna]
        .fillna('VAZIO')
        .value_counts()
        .reset_index()
    )
    contagem_coluna.columns = [nome_coluna, 'QUANTIDADE']

    if not contagem_coluna.empty:
        contagem_coluna = contagem_coluna.sort_values(
            by=['QUANTIDADE', nome_coluna],
            ascending=[False, True]
        ).reset_index(drop=True)

    return contagem_coluna


def montar_lista_valores(dataframe, nome_coluna):
    contagem_coluna = montar_contagem_coluna(dataframe, nome_coluna)
    return contagem_coluna[nome_coluna].dropna().astype(str).tolist()


def formatar_valor_descricao(valor):
    valores = normalizar_valor_ou_lista(valor)

    if valores is None:
        return ''

    valores_texto = [str(item) for item in valores]

    if len(valores_texto) == 1:
        return valores_texto[0]

    return ', '.join(valores_texto[:-1]) + ' ou ' + valores_texto[-1]


def descrever_tipo(grupo):
    if grupo['tipo_igual'] is not None:
        return f"TIPO igual a {formatar_valor_descricao(grupo['tipo_igual'])}"

    if grupo['tipo_diferente'] is not None:
        return f"TIPO diferente de {formatar_valor_descricao(grupo['tipo_diferente'])}"

    return 'sem filtro em TIPO'


def descrever_contratacao(grupo):
    if grupo['contratacao_igual']:
        return f"CONTRATACAO igual a '{formatar_valor_descricao(grupo['contratacao_igual'])}'"

    if grupo['contratacao_diferente']:
        return f"CONTRATACAO diferente de '{formatar_valor_descricao(grupo['contratacao_diferente'])}'"

    return 'sem filtro em CONTRATACAO'


def descrever_texto(nome_coluna, filtro_igual, filtro_diferente):
    if filtro_igual:
        return f"{nome_coluna} contem '{filtro_igual}'"

    if filtro_diferente:
        return f"{nome_coluna} diferente de '{filtro_diferente}'"

    return f'sem filtro em {nome_coluna}'


def obter_nome_grupo(chave_grupo):
    return nomes_grupo_personalizados.get(chave_grupo, chave_grupo.replace('_', ' '))


grupos_local = carregar_grupos()

df = pd.read_csv(arquivo_base, low_memory=False)
df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
df['CONTRATACAO'] = df['CONTRATACAO'].astype('string').str.strip().str.lower()
df['LOCAL'] = df['LOCAL'].astype('string').str.strip()
df['ESPECIALIDADE'] = df['ESPECIALIDADE'].astype('string').str.strip()

todas_as_contagens = []
linhas_txt = [
    'INVESTIGACAO DE ESPECIALIDADE PARA CLASSIFICACAO',
    '',
    f'Arquivo analisado: {arquivo_base}',
    f'Arquivo de grupos: {arquivo_grupos}',
    f'Total de linhas da base: {len(df)}',
    '',
]

for grupo in grupos_local:
    nome_grupo = obter_nome_grupo(grupo['grupo_classificacao'])
    descricao_tipo = descrever_tipo(grupo)
    descricao_contratacao = descrever_contratacao(grupo)
    descricao_local = descrever_texto('LOCAL', grupo['filtro_local'], grupo['filtro_local_diferente'])
    descricao_especialidade = descrever_texto(
        'ESPECIALIDADE',
        grupo['filtro_especialidade'],
        grupo['filtro_especialidade_diferente']
    )

    df_tipo_filtrado = df.copy()
    tipos_iguais = normalizar_valor_ou_lista(grupo['tipo_igual'])
    tipos_diferentes = normalizar_valor_ou_lista(grupo['tipo_diferente'])

    if tipos_iguais is not None:
        df_tipo_filtrado = df_tipo_filtrado[df_tipo_filtrado['TIPO'].isin(tipos_iguais)].copy()

    if tipos_diferentes is not None:
        df_tipo_filtrado = df_tipo_filtrado[~df_tipo_filtrado['TIPO'].isin(tipos_diferentes)].copy()

    df_contratacao_filtrado = df_tipo_filtrado.copy()

    if grupo['contratacao_igual']:
        df_contratacao_filtrado = df_contratacao_filtrado[
            df_contratacao_filtrado['CONTRATACAO'] == grupo['contratacao_igual']
        ].copy()

    if grupo['contratacao_diferente']:
        df_contratacao_filtrado = df_contratacao_filtrado[
            df_contratacao_filtrado['CONTRATACAO'] != grupo['contratacao_diferente']
        ].copy()

    df_local_filtrado = df_contratacao_filtrado.copy()

    if grupo['filtro_local']:
        filtro_local = df_local_filtrado['LOCAL'].str.contains(
            grupo['filtro_local'],
            case=False,
            na=False,
            regex=True
        )
        df_local_filtrado = df_local_filtrado[filtro_local].copy()

    if grupo['filtro_local_diferente']:
        filtro_local_diferente = df_local_filtrado['LOCAL'].str.contains(
            grupo['filtro_local_diferente'],
            case=False,
            na=False,
            regex=True
        )
        df_local_filtrado = df_local_filtrado[~filtro_local_diferente].copy()

    if grupo['filtro_especialidade']:
        filtro_especialidade = df_local_filtrado['ESPECIALIDADE'].str.contains(
            grupo['filtro_especialidade'],
            case=False,
            na=False,
            regex=True
        )
        df_final_filtrado = df_local_filtrado[filtro_especialidade].copy()
    else:
        df_final_filtrado = df_local_filtrado.copy()

    if grupo['filtro_especialidade_diferente']:
        filtro_especialidade_diferente = df_final_filtrado['ESPECIALIDADE'].str.contains(
            grupo['filtro_especialidade_diferente'],
            case=False,
            na=False,
            regex=True
        )
        df_final_filtrado = df_final_filtrado[~filtro_especialidade_diferente].copy()

    contagem_especialidade = montar_contagem_coluna(df_final_filtrado, 'ESPECIALIDADE')
    contagem_especialidade['GRUPO'] = nome_grupo
    contagem_especialidade['CHAVE_GRUPO'] = grupo['grupo_classificacao']
    contagem_especialidade['NOME_LISTA'] = grupo['nome_lista']
    contagem_especialidade['PALAVRA_FILTRO'] = grupo['palavra_filtro']
    contagem_especialidade['TIPO_FILTRO'] = descricao_tipo
    contagem_especialidade['CONTRATACAO_FILTRO'] = descricao_contratacao
    contagem_especialidade['LOCAL_FILTRO'] = descricao_local
    contagem_especialidade['ESPECIALIDADE_FILTRO'] = descricao_especialidade
    todas_as_contagens.append(contagem_especialidade)

    lista_local = montar_lista_valores(df_local_filtrado, 'LOCAL')
    lista_especialidade = montar_lista_valores(df_final_filtrado, 'ESPECIALIDADE')

    linhas_txt.append(f'GRUPO: {nome_grupo}')
    linhas_txt.append(f"CHAVE_GRUPO: {grupo['grupo_classificacao']}")
    linhas_txt.append(f"NOME_LISTA: {grupo['nome_lista']}")
    linhas_txt.append(f"PALAVRA_FILTRO: {grupo['palavra_filtro']}")
    linhas_txt.append(f'- filtro TIPO: {descricao_tipo}')
    linhas_txt.append(f'- filtro CONTRATACAO: {descricao_contratacao}')
    linhas_txt.append(f'- filtro LOCAL: {descricao_local}')
    linhas_txt.append(f'- filtro ESPECIALIDADE: {descricao_especialidade}')
    linhas_txt.append(f'- total apos filtro TIPO: {len(df_tipo_filtrado)}')
    linhas_txt.append(f'- total apos filtro CONTRATACAO: {len(df_contratacao_filtrado)}')
    linhas_txt.append(f'- total apos filtro LOCAL: {len(df_local_filtrado)}')
    linhas_txt.append(f'- total final do grupo: {len(df_final_filtrado)}')
    linhas_txt.append('')

    if grupo['filtro_local'] or grupo['filtro_local_diferente']:
        linhas_txt.append(f"{grupo['nome_lista']}_filtro_local = [")

        for valor in lista_local:
            linhas_txt.append(f"    '{valor}',")

        linhas_txt.append(']')
        linhas_txt.append('')

    if grupo['filtro_especialidade'] or grupo['filtro_especialidade_diferente']:
        linhas_txt.append(f"{grupo['nome_lista']}_filtro_especialidade = [")

        for valor in lista_especialidade:
            linhas_txt.append(f"    '{valor}',")

        linhas_txt.append(']')
        linhas_txt.append('')

    if (
        not grupo['filtro_local']
        and not grupo['filtro_local_diferente']
        and not grupo['filtro_especialidade']
        and not grupo['filtro_especialidade_diferente']
    ):
        linhas_txt.append(f"{grupo['nome_lista']} = [")

        for valor in lista_especialidade:
            linhas_txt.append(f"    '{valor}',")

        linhas_txt.append(']')
        linhas_txt.append('')

arquivo_txt = pasta_saida / 'investigacao_especialidade_classificacao.txt'
arquivo_csv = pasta_saida / 'investigacao_especialidade_classificacao.csv'

with open(arquivo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

if todas_as_contagens:
    df_saida = pd.concat(todas_as_contagens, ignore_index=True)
else:
    df_saida = pd.DataFrame(
        columns=[
            'ESPECIALIDADE',
            'QUANTIDADE',
            'GRUPO',
            'CHAVE_GRUPO',
            'NOME_LISTA',
            'PALAVRA_FILTRO',
            'TIPO_FILTRO',
            'CONTRATACAO_FILTRO',
            'LOCAL_FILTRO',
            'ESPECIALIDADE_FILTRO'
        ]
    )

df_saida.to_csv(arquivo_csv, index=False, encoding='utf-8-sig')
