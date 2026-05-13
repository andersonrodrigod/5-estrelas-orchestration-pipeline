# -*- coding: utf-8 -*-
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/03_base_com_nota.csv')
arquivo_regras_classificacao = Path('utils/insumos/regra_classificacao.xlsx')
pasta_saida = Path('saida_resumo_avaliacoes') / 'exec_04_classificacao'

coluna_ordem_regra = '_ORDEM_REGRA_CLASSIFICACAO'
coluna_nome_lista = '_NOME_LISTA_CLASSIFICACAO'
coluna_chave_grupo = '_CHAVE_GRUPO_CLASSIFICACAO'

colunas_regras = [
    'CHAVE_GRUPO',
    'CLASSIFICACAO',
    'NOME_LISTA',
    'PALAVRA_FILTRO',
    'APLICAR_SOMENTE_VAZIO',
    'COLUNA_FILTRO_1',
    'COMPARADOR_1',
    'VALOR_1',
    'COLUNA_FILTRO_2',
    'COMPARADOR_2',
    'VALOR_2',
    'COLUNA_FILTRO_3',
    'COMPARADOR_3',
    'VALOR_3',
    'ORDEM',
    'STATUS_ATIVO',
    'DESCRICAO',
]

mapa_colunas_regras = {
    'CHAVE_GRUPO': 'chave_grupo',
    'CLASSIFICACAO': 'classificacao',
    'NOME_LISTA': 'nome_lista',
    'PALAVRA_FILTRO': 'palavra_filtro',
    'APLICAR_SOMENTE_VAZIO': 'aplicar_somente_vazios',
    'COLUNA_FILTRO_1': 'coluna_1',
    'COMPARADOR_1': 'comparador_1',
    'VALOR_1': 'valor_1',
    'COLUNA_FILTRO_2': 'coluna_2',
    'COMPARADOR_2': 'comparador_2',
    'VALOR_2': 'valor_2',
    'COLUNA_FILTRO_3': 'coluna_3',
    'COMPARADOR_3': 'comparador_3',
    'VALOR_3': 'valor_3',
    'ORDEM': 'ordem',
    'STATUS_ATIVO': 'ativo',
    'DESCRICAO': 'descricao',
}


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip().fillna('')


def normalizar_flag(valor):
    if pd.isna(valor):
        return False

    return str(valor).strip().lower() in {'sim', 's', 'true', '1', 'yes'}


def carregar_regras_classificacao(caminho):
    df_regras = pd.read_excel(caminho, sheet_name='regras_classificacao')
    df_regras['_LINHA_EXCEL'] = df_regras.index + 2

    for coluna in colunas_regras:
        if coluna not in df_regras.columns:
            df_regras[coluna] = pd.NA

    df_regras = df_regras[colunas_regras + ['_LINHA_EXCEL']].copy()
    df_regras = df_regras.rename(columns=mapa_colunas_regras)
    df_regras['ativo'] = df_regras['ativo'].apply(normalizar_flag)
    df_regras['aplicar_somente_vazios'] = (
        df_regras['aplicar_somente_vazios'].apply(normalizar_flag)
    )
    df_regras['ordem'] = pd.to_numeric(df_regras['ordem'], errors='coerce')
    df_regras = df_regras[df_regras['ativo']].copy()
    df_regras = df_regras.sort_values('ordem', kind='stable')

    for coluna in [
        'chave_grupo',
        'classificacao',
        'nome_lista',
        'palavra_filtro',
        'descricao',
        'coluna_1',
        'comparador_1',
        'valor_1',
        'coluna_2',
        'comparador_2',
        'valor_2',
        'coluna_3',
        'comparador_3',
        'valor_3',
    ]:
        df_regras[coluna] = normalizar_texto(df_regras[coluna])

    return df_regras.reset_index(drop=True)


def formatar_valor_regra(valor):
    if pd.isna(valor) or str(valor).strip() == '':
        return 'sem filtro'

    return str(valor)


def separar_lista(valor):
    if ';' in valor:
        return [item.strip() for item in valor.split(';') if item.strip() != '']

    return [valor]


def montar_mascara_tipo(df_base, comparador, valor):
    serie = pd.to_numeric(df_base['TIPO'], errors='coerce')

    if comparador in {'igual', 'diferente'}:
        valor_numerico = pd.to_numeric(pd.Series([valor]), errors='coerce').iloc[0]
        if comparador == 'igual':
            return serie == valor_numerico

        return serie != valor_numerico

    if comparador in {'em_lista', 'fora_lista'}:
        valores = pd.to_numeric(pd.Series(separar_lista(valor)), errors='coerce')
        if comparador == 'em_lista':
            return serie.isin(valores)

        return ~serie.isin(valores)

    return montar_mascara_texto(df_base, 'TIPO', comparador, valor)


def montar_mascara_texto(df_base, coluna, comparador, valor):
    serie = normalizar_texto(df_base[coluna])

    if comparador == 'igual':
        return serie == valor

    if comparador == 'diferente':
        return serie != valor

    if comparador == 'em_lista':
        return serie.isin(separar_lista(valor))

    if comparador == 'fora_lista':
        return ~serie.isin(separar_lista(valor))

    if comparador == 'contem':
        return serie.str.contains(valor, case=False, na=False, regex=False)

    if comparador == 'nao_contem':
        return ~serie.str.contains(valor, case=False, na=False, regex=False)

    if comparador == 'contem_regex':
        return serie.str.contains(valor, case=False, na=False, regex=True)

    if comparador == 'nao_contem_regex':
        return ~serie.str.contains(valor, case=False, na=False, regex=True)

    if comparador == 'vazio':
        return serie == ''

    if comparador == 'nao_vazio':
        return serie != ''

    raise ValueError(f"Comparador nao suportado: {comparador}")


def montar_mascara_filtro(df_base, coluna, comparador, valor):
    if coluna == 'TIPO':
        return montar_mascara_tipo(df_base, comparador, valor)

    return montar_mascara_texto(df_base, coluna, comparador, valor)


def montar_mascara_regra(df_base, regra):
    mascara = pd.Series(True, index=df_base.index)

    for numero in range(1, 4):
        coluna = regra[f'coluna_{numero}']
        comparador = regra[f'comparador_{numero}']
        valor = regra[f'valor_{numero}']

        if coluna == '' and comparador == '' and valor == '':
            continue

        mascara = mascara & montar_mascara_filtro(df_base, coluna, comparador, valor)

    return mascara


def slugificar(texto):
    texto_normalizado = unicodedata.normalize('NFKD', str(texto))
    texto_ascii = texto_normalizado.encode('ascii', 'ignore').decode('ascii')
    texto_limpo = re.sub(r'[^a-zA-Z0-9]+', '_', texto_ascii).strip('_').lower()
    return texto_limpo or 'sem_nome'


def preparar_base():
    df = ler_csv_padronizado(arquivo_entrada)
    df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
    df['CONTRATACAO'] = normalizar_texto(df['CONTRATACAO']).str.lower()
    df['LOCAL'] = normalizar_texto(df['LOCAL'])
    df['ESPECIALIDADE'] = normalizar_texto(df['ESPECIALIDADE'])

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


def aplicar_regras_incrementais(df_base, regras, total_regras):
    df = df_base.copy()
    sobrescritas = []

    for _, regra_original in regras.iterrows():
        regra = regra_original.copy()

        filtro = montar_mascara_regra(df, regra)

        filtro_aplicacao = filtro
        if regra['aplicar_somente_vazios']:
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
                    ORDEM_REGRA_NOVA=int(regra['ordem']),
                    NOME_LISTA_ANTERIOR=sobrescritas_regra[coluna_nome_lista].astype('string').fillna('DESCONHECIDA'),
                    NOME_LISTA_NOVA=regra['nome_lista'],
                    CHAVE_GRUPO_ANTERIOR=sobrescritas_regra[coluna_chave_grupo].astype('string').fillna('DESCONHECIDA'),
                    CHAVE_GRUPO_NOVA=regra['chave_grupo'],
                    CLASSIFICACAO_ANTERIOR=sobrescritas_regra['CLASSIFICACAO'].fillna('VAZIO'),
                    CLASSIFICACAO_NOVA=regra['classificacao'],
                    LOCAL_LINHA_SOBRESCRITA=sobrescritas_regra['LOCAL'].fillna('VAZIO'),
                    ESPECIALIDADE_LINHA_SOBRESCRITA=sobrescritas_regra['ESPECIALIDADE'].fillna('VAZIO'),
                    CONTRATACAO_LINHA_SOBRESCRITA=sobrescritas_regra['CONTRATACAO'].fillna('VAZIO'),
                    APLICAR_SOMENTE_VAZIOS_REGRA='false' if not regra['aplicar_somente_vazios'] else 'true',
                    PALAVRA_FILTRO_REGRA=formatar_valor_regra(regra['palavra_filtro']),
                    COLUNA_FILTRO_1_REGRA=formatar_valor_regra(regra['coluna_1']),
                    COMPARADOR_1_REGRA=formatar_valor_regra(regra['comparador_1']),
                    VALOR_1_REGRA=formatar_valor_regra(regra['valor_1']),
                    COLUNA_FILTRO_2_REGRA=formatar_valor_regra(regra['coluna_2']),
                    COMPARADOR_2_REGRA=formatar_valor_regra(regra['comparador_2']),
                    VALOR_2_REGRA=formatar_valor_regra(regra['valor_2']),
                    COLUNA_FILTRO_3_REGRA=formatar_valor_regra(regra['coluna_3']),
                    COMPARADOR_3_REGRA=formatar_valor_regra(regra['comparador_3']),
                    VALOR_3_REGRA=formatar_valor_regra(regra['valor_3'])
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
                    'COLUNA_FILTRO_1_REGRA',
                    'COMPARADOR_1_REGRA',
                    'VALOR_1_REGRA',
                    'COLUNA_FILTRO_2_REGRA',
                    'COMPARADOR_2_REGRA',
                    'VALOR_2_REGRA',
                    'COLUNA_FILTRO_3_REGRA',
                    'COMPARADOR_3_REGRA',
                    'VALOR_3_REGRA'
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
                    'COLUNA_FILTRO_1_REGRA': linha['COLUNA_FILTRO_1_REGRA'],
                    'COMPARADOR_1_REGRA': linha['COMPARADOR_1_REGRA'],
                    'VALOR_1_REGRA': linha['VALOR_1_REGRA'],
                    'COLUNA_FILTRO_2_REGRA': linha['COLUNA_FILTRO_2_REGRA'],
                    'COMPARADOR_2_REGRA': linha['COMPARADOR_2_REGRA'],
                    'VALOR_2_REGRA': linha['VALOR_2_REGRA'],
                    'COLUNA_FILTRO_3_REGRA': linha['COLUNA_FILTRO_3_REGRA'],
                    'COMPARADOR_3_REGRA': linha['COMPARADOR_3_REGRA'],
                    'VALOR_3_REGRA': linha['VALOR_3_REGRA'],
                    'QUANTIDADE': int(linha['QUANTIDADE'])
                })

        df.loc[filtro_aplicacao, 'CLASSIFICACAO'] = regra['classificacao']
        df.loc[filtro_aplicacao, coluna_ordem_regra] = int(regra['ordem'])
        df.loc[filtro_aplicacao, coluna_nome_lista] = regra['nome_lista']
        df.loc[filtro_aplicacao, coluna_chave_grupo] = regra['chave_grupo']

    sobrescritas_ultima_regra = [
        item for item in sobrescritas
        if item['ORDEM_REGRA_NOVA'] == int(regras.iloc[total_regras - 1]['ordem'])
    ]

    return sobrescritas_ultima_regra


print('Iniciando auditoria incremental de sobrescritas da execucao 04...')
print(f'Lendo base: {arquivo_entrada}')
print(f'Lendo regras de classificacao: {arquivo_regras_classificacao}')

regras = carregar_regras_classificacao(arquivo_regras_classificacao)
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
    'COLUNA_FILTRO_1_REGRA',
    'COMPARADOR_1_REGRA',
    'VALOR_1_REGRA',
    'COLUNA_FILTRO_2_REGRA',
    'COMPARADOR_2_REGRA',
    'VALOR_2_REGRA',
    'COLUNA_FILTRO_3_REGRA',
    'COMPARADOR_3_REGRA',
    'VALOR_3_REGRA',
    'QUANTIDADE'
]

for total_regras in range(1, len(regras) + 1):
    ultima_regra = regras.iloc[total_regras - 1]
    ordem_regra = int(ultima_regra['ordem'])
    nome_lista_slug = slugificar(ultima_regra['nome_lista'])
    grupo_slug = slugificar(ultima_regra['chave_grupo'])
    arquivo_sobrescritas = (
        pasta_saida
        / (
            f'exec_04_classificacao_sobrescritas_ate_regra_{ordem_regra:03d}'
            f'__{nome_lista_slug}__{grupo_slug}.csv'
        )
    )

    print(
        f'Processando ate a regra {ordem_regra:03d}: '
        f"{ultima_regra['nome_lista']} / {ultima_regra['chave_grupo']}"
    )

    sobrescritas = aplicar_regras_incrementais(
        df_base=df_base,
        regras=regras,
        total_regras=total_regras
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
