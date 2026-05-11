# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/05_base_com_local_editado.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/06_base_com_operadora_v2.csv')
ARQUIVO_REGRAS = Path('utils/insumos/regras_operadora.xlsx')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_06_operadora_v2'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_06_operadora_v2_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_06_operadora_v2_resumo.txt'
ARQUIVO_RESUMO_CSV = PASTA_RESUMO / 'exec_06_operadora_v2_resumo.csv'
ARQUIVO_AUDITORIA = PASTA_RESUMO / 'exec_06_operadora_v2_auditoria.csv'
ARQUIVO_DISTINTOS = PASTA_RESUMO / 'exec_06_operadora_v2_local_editado_operadora.csv'
ARQUIVO_NAO_CLASSIFICADOS = PASTA_RESUMO / 'exec_06_operadora_v2_nao_classificados.csv'
ARQUIVO_SOBRESCRITOS = PASTA_RESUMO / 'exec_06_operadora_v2_sobrescritos.csv'
ARQUIVO_HAPVIDA_DISTINTOS = PASTA_RESUMO / 'exec_06_operadora_v2_hapvida_distintos.csv'

COLUNA_OPERADORA = 'OPERADORA'
COLUNA_QUANTIDADE = 'QUANTIDADE'
LIMITE_EXEMPLOS = 20


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def normalizar_texto(serie):
    return serie.astype('string').fillna('').str.strip()


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_SAIDA,
        ARQUIVO_REGRAS,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
        ARQUIVO_RESUMO_CSV,
        ARQUIVO_AUDITORIA,
        ARQUIVO_DISTINTOS,
        ARQUIVO_NAO_CLASSIFICADOS,
        ARQUIVO_SOBRESCRITOS,
        ARQUIVO_HAPVIDA_DISTINTOS,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def carregar_regras_ativas():
    df_regras = pd.read_excel(ARQUIVO_REGRAS, sheet_name='regras_operadora')
    ativo = df_regras['STATUS_ATIVO'].astype('string').str.strip().str.lower()
    return df_regras[ativo.isin(['sim', 's', 'true', '1', 'yes'])].copy()


def calcular_totais(df_saida, df_sobrescritos, df_regras):
    operadora = normalizar_texto(df_saida[COLUNA_OPERADORA]).str.upper()
    total_hapvida = int((operadora == 'HAPVIDA').sum())
    total_sobrescritos = 0

    if COLUNA_QUANTIDADE in df_sobrescritos.columns and not df_sobrescritos.empty:
        total_sobrescritos = int(
            pd.to_numeric(df_sobrescritos[COLUNA_QUANTIDADE], errors='coerce')
            .fillna(0)
            .sum()
        )

    return {
        'total_linhas_entrada': int(len(df_saida)),
        'total_classificadas_final': int(len(df_saida)),
        'total_preenchidas_com_hapvida': total_hapvida,
        'total_sobrescritos': total_sobrescritos,
        'total_regras_ativas': int(len(df_regras)),
    }


def validar_resumo_json(df_saida, df_sobrescritos, df_regras, resumo):
    erros = []
    totais = calcular_totais(df_saida, df_sobrescritos, df_regras)

    for campo, valor_esperado in totais.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != valor_esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': "
                f"esperado {valor_esperado}, encontrado {valor_resumo}.",
            )

    return erros


def validar_auditoria(df_auditoria, df_regras):
    erros = []
    colunas_esperadas = [
        'ORDEM',
        'REGRA',
        'OPERADORA_APLICADA',
        'APLICAR_SOMENTE_VAZIOS',
        'TOTAL_ATINGIDAS',
        'TOTAL_VAZIAS',
        'TOTAL_SOBRESCRITAS',
    ]

    for coluna in colunas_esperadas:
        if coluna not in df_auditoria.columns:
            registrar_erro(erros, f'Coluna ausente na auditoria: {coluna}')

    if erros:
        return erros

    if len(df_auditoria) == 0:
        registrar_erro(erros, 'Auditoria vazia.')

    if len(df_auditoria) > 0:
        total_sobrescritas_auditoria = int(
            pd.to_numeric(df_auditoria['TOTAL_SOBRESCRITAS'], errors='coerce')
            .fillna(0)
            .sum()
        )
        if total_sobrescritas_auditoria < 0:
            registrar_erro(erros, 'Auditoria com TOTAL_SOBRESCRITAS negativo.')

    if int(len(df_regras)) > 0 and len(df_auditoria) > int(len(df_regras)):
        registrar_erro(
            erros,
            f'Auditoria com mais linhas que regras ativas ({len(df_regras)}).',
        )

    return erros


def validar_operadora_preenchida(df_saida):
    erros = []
    if COLUNA_OPERADORA not in df_saida.columns:
        return [f'Coluna obrigatoria ausente: {COLUNA_OPERADORA}']

    vazias = normalizar_texto(df_saida[COLUNA_OPERADORA]) == ''
    for linha in df_saida.index[vazias][:LIMITE_EXEMPLOS] + 2:
        registrar_erro(erros, f'Linha {int(linha)} com OPERADORA vazia.')

    if int(vazias.sum()) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {int(vazias.sum()) - LIMITE_EXEMPLOS} linha(s) com OPERADORA vazia.",
        )

    return erros


def validar_hapvida_distintos(df_hapvida_distintos, resumo):
    erros = []
    total_esperado = int(resumo.get('total_preenchidas_com_hapvida', 0))
    total_arquivo = 0

    if COLUNA_QUANTIDADE in df_hapvida_distintos.columns and not df_hapvida_distintos.empty:
        total_arquivo = int(
            pd.to_numeric(df_hapvida_distintos[COLUNA_QUANTIDADE], errors='coerce')
            .fillna(0)
            .sum()
        )

    if total_arquivo != total_esperado:
        registrar_erro(
            erros,
            'Arquivo de hapvida distintos divergente: '
            f'esperado {total_esperado}, encontrado {total_arquivo}.',
        )

    return erros


def validar_sobrescritos(df_sobrescritos, resumo):
    erros = []
    total_esperado = int(resumo.get('total_sobrescritos', 0))
    total_arquivo = 0

    if COLUNA_QUANTIDADE in df_sobrescritos.columns and not df_sobrescritos.empty:
        total_arquivo = int(
            pd.to_numeric(df_sobrescritos[COLUNA_QUANTIDADE], errors='coerce')
            .fillna(0)
            .sum()
        )

    if total_arquivo != total_esperado:
        registrar_erro(
            erros,
            'Arquivo de sobrescritos divergente: '
            f'esperado {total_esperado}, encontrado {total_arquivo}.',
        )

    return erros


def validar_resumo_csv(df_resumo_csv, resumo):
    erros = []
    if df_resumo_csv.empty:
        return ['Resumo CSV esta vazio.']

    campos = {
        'TOTAL_LINHAS_ENTRADA': 'total_linhas_entrada',
        'TOTAL_CLASSIFICADAS_FINAL': 'total_classificadas_final',
        'TOTAL_PREENCHIDAS_COM_HAPVIDA': 'total_preenchidas_com_hapvida',
        'TOTAL_SOBRESCRITOS': 'total_sobrescritos',
        'TOTAL_REGRAS_ATIVAS': 'total_regras_ativas',
    }
    linha = df_resumo_csv.iloc[0]

    for coluna_csv, campo_json in campos.items():
        if coluna_csv not in df_resumo_csv.columns:
            registrar_erro(erros, f'Coluna ausente no resumo CSV: {coluna_csv}')
            continue

        valor_csv = int(pd.to_numeric(linha[coluna_csv], errors='coerce'))
        valor_json = int(resumo.get(campo_json, -1))
        if valor_csv != valor_json:
            registrar_erro(
                erros,
                f"Resumo CSV divergente em '{coluna_csv}': "
                f'esperado {valor_json}, encontrado {valor_csv}.',
            )

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_06_operadora_v2')
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
        print('TESTE FALHOU - exec_06_operadora_v2')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_06_operadora_v2.py')
        return 1

    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)
    df_regras = carregar_regras_ativas()
    df_auditoria = pd.read_csv(ARQUIVO_AUDITORIA, low_memory=False)
    df_resumo_csv = pd.read_csv(ARQUIVO_RESUMO_CSV, low_memory=False)
    df_sobrescritos = pd.read_csv(ARQUIVO_SOBRESCRITOS, low_memory=False)
    df_hapvida_distintos = pd.read_csv(ARQUIVO_HAPVIDA_DISTINTOS, low_memory=False)

    with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
        resumo = json.load(arquivo)

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} e saida tem {len(df_saida)}.',
        )

    erros.extend(validar_operadora_preenchida(df_saida))
    erros.extend(validar_resumo_json(df_saida, df_sobrescritos, df_regras, resumo))
    erros.extend(validar_resumo_csv(df_resumo_csv, resumo))
    erros.extend(validar_auditoria(df_auditoria, df_regras))
    erros.extend(validar_sobrescritos(df_sobrescritos, resumo))
    erros.extend(validar_hapvida_distintos(df_hapvida_distintos, resumo))
    erros.extend(validar_tipos(df_saida))

    if erros:
        imprimir_erros(erros)
        return 1

    totais = calcular_totais(df_saida, df_sobrescritos, df_regras)
    print('TESTE OK - exec_06_operadora_v2')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f"Total de linhas analisadas: {totais['total_linhas_entrada']}")
    print(f"Total classificadas final: {totais['total_classificadas_final']}")
    print(f"Total preenchidas com HAPVIDA: {totais['total_preenchidas_com_hapvida']}")
    print(f"Total sobrescritos: {totais['total_sobrescritos']}")
    print(f"Total regras ativas: {totais['total_regras_ativas']}")
    print('Quantidade de linhas preservada.')
    print('OPERADORA preenchida em toda a base.')
    print('Resumo JSON e CSV coerentes com a saida.')
    print('Sobrescritos e hapvida distintos coerentes com o resumo.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
