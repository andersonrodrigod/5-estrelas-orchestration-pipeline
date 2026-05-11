# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/04_base_com_classificacao.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/05_base_com_local_editado.csv')
ARQUIVO_INSUMOS = Path('utils/insumos/insumos 5 estrelas.xlsx')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_05_local_editado'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_05_local_editado_resumo.json'
ARQUIVO_RESUMO_CSV = PASTA_RESUMO / 'exec_05_local_editado_resumo.csv'
ARQUIVO_ATUALIZADOS = PASTA_RESUMO / 'exec_05_local_editado_atualizados.csv'
ARQUIVO_NAO_ENCONTRADOS = PASTA_RESUMO / 'exec_05_local_editado_nao_encontrados.csv'

COLUNAS_OBRIGATORIAS = ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO']
COLUNAS_OBRIGATORIAS_INSUMOS = ['Local', 'local editado']
COLUNA_QUANTIDADE = 'QUANTIDADE'
LIMITE_EXEMPLOS = 20


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def normalizar_texto(serie):
    return serie.astype('string').fillna('').str.strip()


def normalizar_chave(serie):
    return normalizar_texto(serie).str.lower()


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_SAIDA,
        ARQUIVO_INSUMOS,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_CSV,
        ARQUIVO_ATUALIZADOS,
        ARQUIVO_NAO_ENCONTRADOS,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def validar_colunas(df, colunas, contexto):
    return [
        f"Coluna obrigatoria ausente em {contexto}: {coluna}"
        for coluna in colunas
        if coluna not in df.columns
    ]


def carregar_insumos_local_editado():
    return pd.read_excel(ARQUIVO_INSUMOS, sheet_name='insumos')


def validar_insumos(df_insumos):
    erros = []
    erros.extend(
        validar_colunas(df_insumos, COLUNAS_OBRIGATORIAS_INSUMOS, 'insumos')
    )

    if erros:
        return erros, None

    df_mapa = df_insumos.copy()
    df_mapa['LOCAL_COMPARACAO'] = normalizar_chave(df_mapa['Local'])
    df_mapa['LOCAL_EDITADO_COMPARACAO'] = normalizar_texto(df_mapa['local editado'])

    locais_vazios = df_mapa['LOCAL_COMPARACAO'] == ''
    for linha in df_mapa.index[locais_vazios][:LIMITE_EXEMPLOS] + 2:
        registrar_erro(erros, f'Linha {int(linha)} do insumo com Local vazio.')

    if int(locais_vazios.sum()) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {int(locais_vazios.sum()) - LIMITE_EXEMPLOS} "
            "linha(s) do insumo com Local vazio."
        )

    locais_editados_vazios = (
        (df_mapa['LOCAL_COMPARACAO'] != '')
        & (df_mapa['LOCAL_EDITADO_COMPARACAO'] == '')
    )
    for linha, local in zip(
        df_mapa.index[locais_editados_vazios][:LIMITE_EXEMPLOS] + 2,
        df_mapa.loc[locais_editados_vazios, 'Local'].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} do insumo com local editado vazio "
            f"para Local '{local}'."
        )

    if int(locais_editados_vazios.sum()) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {int(locais_editados_vazios.sum()) - LIMITE_EXEMPLOS} "
            "linha(s) do insumo com local editado vazio."
        )

    preenchidos = (
        (df_mapa['LOCAL_COMPARACAO'] != '')
        & (df_mapa['LOCAL_EDITADO_COMPARACAO'] != '')
    )
    df_validos = df_mapa[preenchidos].copy()
    conflitos = (
        df_validos
        .groupby('LOCAL_COMPARACAO')['LOCAL_EDITADO_COMPARACAO']
        .nunique()
    )
    locais_conflitantes = conflitos[conflitos > 1].index

    for local_comparacao in locais_conflitantes[:LIMITE_EXEMPLOS]:
        locais_editados = sorted(
            df_validos.loc[
                df_validos['LOCAL_COMPARACAO'] == local_comparacao,
                'LOCAL_EDITADO_COMPARACAO',
            ].unique()
        )
        registrar_erro(
            erros,
            f"Insumo com local editado conflitante para Local "
            f"'{local_comparacao}': {', '.join(locais_editados)}."
        )

    if len(locais_conflitantes) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {len(locais_conflitantes) - LIMITE_EXEMPLOS} "
            "Local(is) com local editado conflitante."
        )

    if erros:
        return erros, None

    df_validos = df_validos.drop_duplicates(
        subset=['LOCAL_COMPARACAO', 'LOCAL_EDITADO_COMPARACAO']
    )
    df_validos = df_validos.drop_duplicates(
        subset=['LOCAL_COMPARACAO'],
        keep='first',
    )
    mapa = df_validos.set_index('LOCAL_COMPARACAO')['LOCAL_EDITADO_COMPARACAO']
    return erros, mapa


def calcular_mascaras(df_entrada, df_saida, mapa_local_editado):
    locais = normalizar_chave(df_saida['LOCAL'])
    esperado = locais.map(mapa_local_editado)
    encontrado = normalizar_texto(df_saida['LOCAL EDITADO'])
    antes = normalizar_texto(df_entrada['LOCAL EDITADO'])

    encontrados = esperado.notna() & (esperado != '')
    nao_encontrados = ~encontrados
    nao_alterados = encontrados & (antes == esperado)
    atualizados = encontrados & ~nao_alterados
    divergencias = encontrados & (encontrado != esperado)

    return {
        'esperado': esperado,
        'encontrado': encontrado,
        'atualizados': atualizados,
        'nao_alterados': nao_alterados,
        'nao_encontrados': nao_encontrados,
        'divergencias': divergencias,
    }


def validar_local_editado_por_insumo(df_saida, mascaras):
    erros = []
    sem_insumo = mascaras['nao_encontrados']
    divergencias = mascaras['divergencias']

    for linha, local in zip(
        df_saida.index[sem_insumo][:LIMITE_EXEMPLOS] + 2,
        df_saida.loc[sem_insumo, 'LOCAL'].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com LOCAL sem correspondencia no insumo: "
            f"'{local}'."
        )

    if int(sem_insumo.sum()) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {int(sem_insumo.sum()) - LIMITE_EXEMPLOS} linha(s) "
            "com LOCAL sem correspondencia no insumo."
        )

    for linha, local, valor_encontrado, valor_esperado in zip(
        df_saida.index[divergencias][:LIMITE_EXEMPLOS] + 2,
        df_saida.loc[divergencias, 'LOCAL'].head(LIMITE_EXEMPLOS),
        mascaras['encontrado'][divergencias].head(LIMITE_EXEMPLOS),
        mascaras['esperado'][divergencias].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com LOCAL EDITADO divergente para LOCAL "
            f"'{local}': esperado '{valor_esperado}', "
            f"encontrado '{valor_encontrado}'."
        )

    if int(divergencias.sum()) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {int(divergencias.sum()) - LIMITE_EXEMPLOS} linha(s) "
            "com LOCAL EDITADO divergente do insumo."
        )

    return erros


def calcular_totais(mascaras):
    return {
        'total_atualizados': int(mascaras['atualizados'].sum()),
        'total_nao_alterados': int(mascaras['nao_alterados'].sum()),
        'total_nao_encontrados': int(mascaras['nao_encontrados'].sum()),
    }


def validar_resumo_json(df_saida, mascaras, resumo):
    erros = []
    totais = {
        'total_linhas_entrada': int(len(df_saida)),
        **calcular_totais(mascaras),
    }

    for campo, valor_esperado in totais.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != valor_esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': "
                f"esperado {valor_esperado}, encontrado {valor_resumo}."
            )

    return erros


def validar_resumo_csv(df_resumo_csv, resumo):
    erros = []
    if df_resumo_csv.empty:
        return ['Resumo CSV esta vazio.']

    linha = df_resumo_csv.iloc[0]
    campos = {
        'TOTAL_LINHAS_ENTRADA': 'total_linhas_entrada',
        'TOTAL_ATUALIZADOS': 'total_atualizados',
        'TOTAL_NAO_ALTERADOS': 'total_nao_alterados',
        'TOTAL_NAO_ENCONTRADOS': 'total_nao_encontrados',
    }

    for coluna_csv, campo_json in campos.items():
        if coluna_csv not in df_resumo_csv.columns:
            registrar_erro(erros, f"Coluna ausente no resumo CSV: {coluna_csv}")
            continue

        valor_csv = int(pd.to_numeric(linha[coluna_csv], errors='coerce'))
        valor_json = int(resumo.get(campo_json, -1))
        if valor_csv != valor_json:
            registrar_erro(
                erros,
                f"Resumo CSV divergente em '{coluna_csv}': "
                f"esperado {valor_json}, encontrado {valor_csv}."
            )

    return erros


def validar_arquivo_atualizados(df_atualizados, resumo):
    erros = []
    total_esperado = int(resumo.get('total_atualizados', 0))
    total_arquivo = 0

    if COLUNA_QUANTIDADE in df_atualizados.columns and not df_atualizados.empty:
        total_arquivo = int(
            pd.to_numeric(df_atualizados[COLUNA_QUANTIDADE], errors='coerce')
            .fillna(0)
            .sum()
        )

    if total_arquivo != total_esperado:
        registrar_erro(
            erros,
            'Arquivo de atualizados divergente: '
            f'esperado {total_esperado}, encontrado {total_arquivo}.'
        )

    return erros


def validar_arquivo_nao_encontrados(df_nao_encontrados, resumo):
    erros = []
    total_esperado = int(resumo.get('total_nao_encontrados', 0))
    total_arquivo = 0

    if COLUNA_QUANTIDADE in df_nao_encontrados.columns and not df_nao_encontrados.empty:
        total_arquivo = int(
            pd.to_numeric(df_nao_encontrados[COLUNA_QUANTIDADE], errors='coerce')
            .fillna(0)
            .sum()
        )

    if total_arquivo != total_esperado:
        registrar_erro(
            erros,
            'Arquivo de nao encontrados divergente: '
            f'esperado {total_esperado}, encontrado {total_arquivo}.'
        )

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_05_local_editado')
    print('')
    print(f'Total de problemas encontrados: {len(erros)}')
    print('')

    for erro in erros[:LIMITE_EXEMPLOS]:
        print(f'- {erro}')

    if len(erros) > LIMITE_EXEMPLOS:
        print(f'- ... mais {len(erros) - LIMITE_EXEMPLOS} problema(s)')


def executar():
    erros = []

    arquivos_faltando = validar_arquivos_obrigatorios()
    if arquivos_faltando:
        print('TESTE FALHOU - exec_05_local_editado')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_05_local_editado.py')
        return 1

    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)
    df_insumos = carregar_insumos_local_editado()
    df_resumo_csv = pd.read_csv(ARQUIVO_RESUMO_CSV, low_memory=False)
    df_atualizados = pd.read_csv(ARQUIVO_ATUALIZADOS, low_memory=False)
    df_nao_encontrados = pd.read_csv(ARQUIVO_NAO_ENCONTRADOS, low_memory=False)

    with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
        resumo = json.load(arquivo)

    erros.extend(validar_colunas(df_saida, COLUNAS_OBRIGATORIAS, 'saida'))

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} '
            f'e saida tem {len(df_saida)}.'
        )

    erros_insumos, mapa_local_editado = validar_insumos(df_insumos)
    erros.extend(erros_insumos)

    if mapa_local_editado is not None and not validar_colunas(
        df_saida,
        COLUNAS_OBRIGATORIAS,
        'saida',
    ):
        mascaras = calcular_mascaras(df_entrada, df_saida, mapa_local_editado)
        erros.extend(validar_local_editado_por_insumo(df_saida, mascaras))
        erros.extend(validar_resumo_json(df_saida, mascaras, resumo))
        erros.extend(validar_resumo_csv(df_resumo_csv, resumo))
        erros.extend(validar_arquivo_atualizados(df_atualizados, resumo))
        erros.extend(validar_arquivo_nao_encontrados(df_nao_encontrados, resumo))
        erros.extend(validar_tipos(df_saida))

    if erros:
        imprimir_erros(erros)
        return 1

    totais = calcular_totais(mascaras)
    print('TESTE OK - exec_05_local_editado')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f'Total de linhas analisadas: {len(df_saida)}')
    print(f"Atualizados: {totais['total_atualizados']}")
    print(f"Nao alterados: {totais['total_nao_alterados']}")
    print(f"Nao encontrados: {totais['total_nao_encontrados']}")
    print('Quantidade de linhas preservada.')
    print('LOCAL EDITADO bate com o insumo por LOCAL.')
    print('Resumo JSON e resumo CSV batem com o CSV.')
    print('Arquivos de atualizados e nao encontrados batem com o resumo.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
