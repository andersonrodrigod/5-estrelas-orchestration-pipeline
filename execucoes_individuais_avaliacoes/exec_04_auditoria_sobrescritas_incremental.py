# -*- coding: utf-8 -*-
import json
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/03_base_com_nota.csv')
arquivo_grupos = Path('data/grupos_classificacao.json')
arquivo_nomes = Path('data/nomes_classificacao.json')
pasta_saida = Path('saida_resumo_avaliacoes') / 'exec_04_classificacao'

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


def formatar_valor_regra(valor):
    if valor is None:
        return 'sem filtro'

    if isinstance(valor, list):
        return ' | '.join(str(item) for item in valor) if valor else 'sem filtro'

    if isinstance(valor, str) and not valor.strip():
        return 'sem filtro'

    return str(valor)


def slugificar(texto):
    texto_normalizado = unicodedata.normalize('NFKD', str(texto))
    texto_ascii = texto_normalizado.encode('ascii', 'ignore').decode('ascii')
    texto_limpo = re.sub(r'[^a-zA-Z0-9]+', '_', texto_ascii).strip('_').lower()
    return texto_limpo or 'sem_nome'


def preparar_base():
    df = ler_csv_padronizado(arquivo_entrada)
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
    return df


def aplicar_regras_com_prefixo_false(df_base, grupos, nomes_classificacao, total_regras_false):
    df = df_base.copy()
    sobrescritas = []

    for ordem, grupo_original in enumerate(grupos, start=1):
        grupo = dict(grupo_original)
        grupo['aplicar_somente_vazios'] = ordem > total_regras_false

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

        filtro_aplicacao = filtro
        if grupo.get('aplicar_somente_vazios'):
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

        if not sobrescritas_regra.empty:
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
                    CLASSIFICACAO_NOVA=nome_classificacao,
                    LOCAL_LINHA_SOBRESCRITA=sobrescritas_regra['LOCAL'].fillna('VAZIO'),
                    ESPECIALIDADE_LINHA_SOBRESCRITA=sobrescritas_regra['ESPECIALIDADE'].fillna('VAZIO'),
                    CONTRATACAO_LINHA_SOBRESCRITA=sobrescritas_regra['CONTRATACAO'].fillna('VAZIO'),
                    APLICAR_SOMENTE_VAZIOS_REGRA='false' if not grupo.get('aplicar_somente_vazios') else 'true',
                    PALAVRA_FILTRO_REGRA=formatar_valor_regra(grupo.get('palavra_filtro')),
                    TIPO_IGUAL_REGRA=formatar_valor_regra(grupo.get('tipo_igual')),
                    TIPO_DIFERENTE_REGRA=formatar_valor_regra(grupo.get('tipo_diferente')),
                    CONTRATACAO_IGUAL_REGRA=formatar_valor_regra(grupo.get('contratacao_igual')),
                    CONTRATACAO_DIFERENTE_REGRA=formatar_valor_regra(grupo.get('contratacao_diferente')),
                    FILTRO_LOCAL_REGRA=formatar_valor_regra(grupo.get('filtro_local')),
                    FILTRO_LOCAL_DIFERENTE_REGRA=formatar_valor_regra(grupo.get('filtro_local_diferente')),
                    FILTRO_ESPECIALIDADE_REGRA=formatar_valor_regra(grupo.get('filtro_especialidade')),
                    FILTRO_ESPECIALIDADE_DIFERENTE_REGRA=formatar_valor_regra(grupo.get('filtro_especialidade_diferente'))
                )
                .groupby([
                    'ORDEM_REGRA_ANTERIOR',
                    'ORDEM_REGRA_NOVA',
                    'NOME_LISTA_ANTERIOR',
                    'NOME_LISTA_NOVA',
                    'CHAVE_GRUPO_ANTERIOR',
                    'CHAVE_GRUPO_NOVA',
                    'CLASSIFICACAO_ANTERIOR',
                    'CLASSIFICACAO_NOVA',
                    'LOCAL_LINHA_SOBRESCRITA',
                    'ESPECIALIDADE_LINHA_SOBRESCRITA',
                    'CONTRATACAO_LINHA_SOBRESCRITA',
                    'APLICAR_SOMENTE_VAZIOS_REGRA',
                    'PALAVRA_FILTRO_REGRA',
                    'TIPO_IGUAL_REGRA',
                    'TIPO_DIFERENTE_REGRA',
                    'CONTRATACAO_IGUAL_REGRA',
                    'CONTRATACAO_DIFERENTE_REGRA',
                    'FILTRO_LOCAL_REGRA',
                    'FILTRO_LOCAL_DIFERENTE_REGRA',
                    'FILTRO_ESPECIALIDADE_REGRA',
                    'FILTRO_ESPECIALIDADE_DIFERENTE_REGRA'
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
                    'LOCAL_LINHA_SOBRESCRITA': linha['LOCAL_LINHA_SOBRESCRITA'],
                    'ESPECIALIDADE_LINHA_SOBRESCRITA': linha['ESPECIALIDADE_LINHA_SOBRESCRITA'],
                    'CONTRATACAO_LINHA_SOBRESCRITA': linha['CONTRATACAO_LINHA_SOBRESCRITA'],
                    'APLICAR_SOMENTE_VAZIOS_REGRA': linha['APLICAR_SOMENTE_VAZIOS_REGRA'],
                    'PALAVRA_FILTRO_REGRA': linha['PALAVRA_FILTRO_REGRA'],
                    'TIPO_IGUAL_REGRA': linha['TIPO_IGUAL_REGRA'],
                    'TIPO_DIFERENTE_REGRA': linha['TIPO_DIFERENTE_REGRA'],
                    'CONTRATACAO_IGUAL_REGRA': linha['CONTRATACAO_IGUAL_REGRA'],
                    'CONTRATACAO_DIFERENTE_REGRA': linha['CONTRATACAO_DIFERENTE_REGRA'],
                    'FILTRO_LOCAL_REGRA': linha['FILTRO_LOCAL_REGRA'],
                    'FILTRO_LOCAL_DIFERENTE_REGRA': linha['FILTRO_LOCAL_DIFERENTE_REGRA'],
                    'FILTRO_ESPECIALIDADE_REGRA': linha['FILTRO_ESPECIALIDADE_REGRA'],
                    'FILTRO_ESPECIALIDADE_DIFERENTE_REGRA': linha['FILTRO_ESPECIALIDADE_DIFERENTE_REGRA'],
                    'QUANTIDADE': int(linha['QUANTIDADE'])
                })

        df.loc[filtro_aplicacao, 'CLASSIFICACAO'] = nome_classificacao
        df.loc[filtro_aplicacao, coluna_ordem_regra] = ordem
        df.loc[filtro_aplicacao, coluna_nome_lista] = grupo['nome_lista']
        df.loc[filtro_aplicacao, coluna_chave_grupo] = grupo['grupo_classificacao']

    sobrescritas_ultima_regra = [
        item for item in sobrescritas
        if item['ORDEM_REGRA_NOVA'] == total_regras_false
    ]

    return sobrescritas_ultima_regra


print('Iniciando auditoria incremental de sobrescritas da execucao 04...')
print(f'Lendo base: {arquivo_entrada}')
print(f'Lendo grupos: {arquivo_grupos}')
print(f'Lendo nomes: {arquivo_nomes}')

grupos = carregar_json(arquivo_grupos)
nomes_classificacao = carregar_json(arquivo_nomes)
df_base = preparar_base()

pasta_saida.mkdir(parents=True, exist_ok=True)

colunas_sobrescritas = [
    'ORDEM_REGRA_ANTERIOR',
    'ORDEM_REGRA_NOVA',
    'NOME_LISTA_ANTERIOR',
    'NOME_LISTA_NOVA',
    'CHAVE_GRUPO_ANTERIOR',
    'CHAVE_GRUPO_NOVA',
    'CLASSIFICACAO_ANTERIOR',
    'CLASSIFICACAO_NOVA',
    'LOCAL_LINHA_SOBRESCRITA',
    'ESPECIALIDADE_LINHA_SOBRESCRITA',
    'CONTRATACAO_LINHA_SOBRESCRITA',
    'APLICAR_SOMENTE_VAZIOS_REGRA',
    'PALAVRA_FILTRO_REGRA',
    'TIPO_IGUAL_REGRA',
    'TIPO_DIFERENTE_REGRA',
    'CONTRATACAO_IGUAL_REGRA',
    'CONTRATACAO_DIFERENTE_REGRA',
    'FILTRO_LOCAL_REGRA',
    'FILTRO_LOCAL_DIFERENTE_REGRA',
    'FILTRO_ESPECIALIDADE_REGRA',
    'FILTRO_ESPECIALIDADE_DIFERENTE_REGRA',
    'QUANTIDADE'
]

for total_regras_false in range(1, len(grupos) + 1):
    ultima_regra = grupos[total_regras_false - 1]
    nome_lista_slug = slugificar(ultima_regra['nome_lista'])
    grupo_slug = slugificar(ultima_regra['grupo_classificacao'])
    arquivo_sobrescritas = (
        pasta_saida
        / (
            f'exec_04_classificacao_sobrescritas_ate_regra_{total_regras_false:03d}'
            f'__{nome_lista_slug}__{grupo_slug}.csv'
        )
    )

    print(
        f'Processando ate a regra {total_regras_false:03d}: '
        f"{ultima_regra['nome_lista']} / {ultima_regra['grupo_classificacao']}"
    )

    sobrescritas = aplicar_regras_com_prefixo_false(
        df_base=df_base,
        grupos=grupos,
        nomes_classificacao=nomes_classificacao,
        total_regras_false=total_regras_false
    )

    df_sobrescritas = pd.DataFrame(sobrescritas, columns=colunas_sobrescritas)
    if not df_sobrescritas.empty:
        df_sobrescritas = df_sobrescritas.sort_values(
            ['QUANTIDADE', 'ORDEM_REGRA_NOVA'],
            ascending=[False, True]
        )

    salvar_csv_padronizado(df_sobrescritas, arquivo_sobrescritas)
    print(f'Arquivo gerado: {arquivo_sobrescritas}')

print('Auditoria incremental finalizada.')
