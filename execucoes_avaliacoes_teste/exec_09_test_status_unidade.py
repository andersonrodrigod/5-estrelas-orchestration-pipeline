# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/08_base_com_resultado_unidade.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/09_base_com_status_unidade.csv')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_09_status_unidade'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_09_status_unidade_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_09_status_unidade_resumo.txt'
ARQUIVO_STATUS_CSV = PASTA_RESUMO / 'exec_09_status_unidade_status.csv'

COLUNA_RESULTADO = 'RESULTADO DA UNIDADE'
COLUNA_META = 'META'
COLUNA_STATUS = 'STATUS UNIDADE'
COLUNA_QUANTIDADE = 'QUANTIDADE'
LIMITE_EXEMPLOS = 20


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_SAIDA,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
        ARQUIVO_STATUS_CSV,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def calcular_status_esperado(df_saida):
    resultado = pd.to_numeric(df_saida[COLUNA_RESULTADO], errors='coerce')
    meta = pd.to_numeric(df_saida[COLUNA_META], errors='coerce')

    sem_status = resultado.isna() | meta.isna()
    fora_meta = (~sem_status) & (resultado < meta)
    dentro_meta = (~sem_status) & (resultado >= meta)

    status_esperado = pd.Series([None] * len(df_saida), index=df_saida.index, dtype='object')
    status_esperado.loc[fora_meta] = 'fora da meta'
    status_esperado.loc[dentro_meta] = 'dentro da meta'

    return status_esperado, dentro_meta, fora_meta, sem_status


def validar_status_linha_a_linha(df_saida):
    erros = []
    status_esperado, dentro_meta, fora_meta, sem_status = calcular_status_esperado(df_saida)
    status_saida = df_saida[COLUNA_STATUS].astype('string').fillna('').str.strip().str.lower()
    status_esp = pd.Series(status_esperado, index=df_saida.index).astype('string').fillna('').str.strip().str.lower()
    divergencias = status_saida != status_esp

    for linha, resultado, meta, esperado, encontrado in zip(
        df_saida.index[divergencias][:LIMITE_EXEMPLOS] + 2,
        df_saida.loc[divergencias, COLUNA_RESULTADO].head(LIMITE_EXEMPLOS),
        df_saida.loc[divergencias, COLUNA_META].head(LIMITE_EXEMPLOS),
        status_esp[divergencias].head(LIMITE_EXEMPLOS),
        status_saida[divergencias].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com STATUS UNIDADE divergente (resultado={resultado}, meta={meta}): "
            f"esperado '{esperado}', encontrado '{encontrado}'.",
        )

    return erros, dentro_meta, fora_meta, sem_status


def validar_resumo_json(df_saida, resumo, dentro_meta, fora_meta, sem_status):
    erros = []
    totais_esperados = {
        'total_linhas_entrada': int(len(df_saida)),
        'total_dentro_meta': int(dentro_meta.sum()),
        'total_fora_meta': int(fora_meta.sum()),
        'total_sem_status': int(sem_status.sum()),
    }

    for campo, esperado in totais_esperados.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': esperado {esperado}, encontrado {valor_resumo}.",
            )

    return erros


def validar_arquivo_status_csv(df_saida, df_status_csv):
    erros = []
    if df_status_csv.empty:
        return ['Arquivo de status por valor esta vazio.']

    contagem_esperada = (
        df_saida[COLUNA_STATUS]
        .fillna('sem status')
        .astype('string')
        .str.strip()
        .replace('', 'sem status')
        .value_counts()
        .to_dict()
    )

    if 'STATUS UNIDADE' not in df_status_csv.columns or COLUNA_QUANTIDADE not in df_status_csv.columns:
        return ["Arquivo de status sem colunas obrigatorias 'STATUS UNIDADE' e 'QUANTIDADE'."]

    contagem_arquivo = (
        df_status_csv
        .assign(
            STATUS_NORMALIZADO=df_status_csv['STATUS UNIDADE'].astype('string').str.strip(),
            QUANTIDADE_NUM=pd.to_numeric(df_status_csv[COLUNA_QUANTIDADE], errors='coerce').fillna(0).astype(int),
        )
        .groupby('STATUS_NORMALIZADO')['QUANTIDADE_NUM']
        .sum()
        .to_dict()
    )

    if contagem_arquivo != contagem_esperada:
        registrar_erro(
            erros,
            f'Arquivo status.csv divergente do CSV final. Esperado={contagem_esperada} | Encontrado={contagem_arquivo}',
        )

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_09_status_unidade')
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
        print('TESTE FALHOU - exec_09_status_unidade')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_09_status_unidade.py')
        return 1

    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)
    df_status_csv = pd.read_csv(ARQUIVO_STATUS_CSV, low_memory=False)

    with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
        resumo = json.load(arquivo)

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} e saida tem {len(df_saida)}.',
        )

    colunas_obrigatorias = [COLUNA_RESULTADO, COLUNA_META, COLUNA_STATUS]
    for coluna in colunas_obrigatorias:
        if coluna not in df_saida.columns:
            registrar_erro(erros, f'Coluna obrigatoria ausente: {coluna}')

    if not erros:
        erros_status, dentro_meta, fora_meta, sem_status = validar_status_linha_a_linha(df_saida)
        erros.extend(erros_status)
        erros.extend(validar_resumo_json(df_saida, resumo, dentro_meta, fora_meta, sem_status))
        erros.extend(validar_arquivo_status_csv(df_saida, df_status_csv))
        erros.extend(validar_tipos(df_saida))

    if erros:
        imprimir_erros(erros)
        return 1

    print('TESTE OK - exec_09_status_unidade')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f'Total de linhas analisadas: {len(df_saida)}')
    print('Quantidade de linhas preservada.')
    print("STATUS UNIDADE bate com regra: resultado<meta='fora', resultado>=meta='dentro', nulo='sem status'.")
    print('Resumo JSON e status.csv coerentes com o CSV final.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
