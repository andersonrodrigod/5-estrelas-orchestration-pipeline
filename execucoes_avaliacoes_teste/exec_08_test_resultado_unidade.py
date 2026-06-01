# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/07_base_com_meta.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/08_base_com_resultado_unidade.csv')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_08_resultado_unidade'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_08_resultado_unidade_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_08_resultado_unidade_resumo.txt'
ARQUIVO_RESUMO_CSV = PASTA_RESUMO / 'exec_08_resultado_unidade_resumo.csv'
ARQUIVO_INSPECAO_GRUPOS = PASTA_RESUMO / 'exec_08_resultado_unidade_inspecao_grupos.csv'
ARQUIVO_LINHAS_CHAVE_VAZIA = PASTA_RESUMO / 'exec_08_resultado_unidade_linhas_chave_vazia.csv'

COLUNAS_GRUPO = ['CLASSIFICACAO', 'LOCAL EDITADO', 'UF']
COLUNA_RESULTADO = 'RESULTADO DA UNIDADE'
COLUNA_NOTA_GERAL = 'NOTA GERAL'
COLUNA_QUANTIDADE = 'QUANTIDADE'
LIMITE_EXEMPLOS = 20


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip()


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_SAIDA,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
        ARQUIVO_RESUMO_CSV,
        ARQUIVO_INSPECAO_GRUPOS,
        ARQUIVO_LINHAS_CHAVE_VAZIA,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def padronizar_chaves(df):
    df_base = df.copy()
    for coluna in COLUNAS_GRUPO:
        df_base[coluna] = normalizar_texto(df_base[coluna])

    mascara_uf_vazia = (
        df_base['UF'].isna()
        | (df_base['UF'] == '')
        | (df_base['UF'].str.lower() == 'vazio')
    )
    df_base.loc[mascara_uf_vazia, 'UF'] = 'CE'
    return df_base


def validar_resultado_unidade(df_saida):
    erros = []
    if COLUNA_RESULTADO not in df_saida.columns:
        return [f'Coluna obrigatoria ausente: {COLUNA_RESULTADO}']

    df_calc = padronizar_chaves(df_saida)
    df_calc[COLUNA_NOTA_GERAL] = pd.to_numeric(df_calc[COLUNA_NOTA_GERAL], errors='coerce')
    esperado = (
        df_calc.groupby(COLUNAS_GRUPO, dropna=False)[COLUNA_NOTA_GERAL]
        .transform('mean')
        .round(2)
    )
    encontrado = pd.to_numeric(df_calc[COLUNA_RESULTADO], errors='coerce').round(2)

    divergencias = esperado.notna() & (encontrado != esperado)
    for linha, exp, enc in zip(
        df_saida.index[divergencias][:LIMITE_EXEMPLOS] + 2,
        esperado[divergencias].head(LIMITE_EXEMPLOS),
        encontrado[divergencias].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com RESULTADO DA UNIDADE divergente: esperado {exp}, encontrado {enc}.",
        )

    return erros, esperado


def validar_uf_preenchida(df_saida):
    erros = []
    uf = normalizar_texto(df_saida['UF']).str.upper()
    vazio = (uf == '') | (uf == 'VAZIO')

    for linha in df_saida.index[vazio][:LIMITE_EXEMPLOS] + 2:
        registrar_erro(erros, f"Linha {int(linha)} com UF vazia apos execucao 08.")

    return erros


def validar_resumo_json(df_saida, resumo, esperado_resultado):
    erros = []
    df_calc = padronizar_chaves(df_saida)
    total_grupos = int(len(df_calc.groupby(COLUNAS_GRUPO, dropna=False)))
    total_linhas = int(len(df_calc))
    total_com_resultado = int(esperado_resultado.notna().sum())
    total_sem_resultado = int(esperado_resultado.isna().sum())

    mascara_grupo_chave_vazia = (
        df_calc['CLASSIFICACAO'].isna()
        | (df_calc['CLASSIFICACAO'] == '')
        | df_calc['LOCAL EDITADO'].isna()
        | (df_calc['LOCAL EDITADO'] == '')
        | df_calc['UF'].isna()
        | (df_calc['UF'] == '')
    )
    total_linhas_chave_vazia = int(mascara_grupo_chave_vazia.sum())

    totais_esperados = {
        'total_linhas_entrada': total_linhas,
        'total_grupos': total_grupos,
        'total_linhas_com_resultado': total_com_resultado,
        'total_linhas_sem_resultado': total_sem_resultado,
        'total_linhas_chave_vazia': total_linhas_chave_vazia,
    }

    for campo, esperado in totais_esperados.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': esperado {esperado}, encontrado {valor_resumo}.",
            )

    return erros


def validar_arquivos_auxiliares(df_inspecao, df_linhas_chave_vazia, resumo):
    erros = []
    if COLUNA_QUANTIDADE in df_inspecao.columns:
        total_grupos = int(len(df_inspecao))
        if total_grupos != int(resumo.get('total_grupos', -1)):
            registrar_erro(
                erros,
                f"Inspecao de grupos divergente: esperado {resumo.get('total_grupos')}, encontrado {total_grupos}.",
            )

    if len(df_linhas_chave_vazia) != int(resumo.get('total_linhas_chave_vazia', -1)):
        registrar_erro(
            erros,
            'Arquivo de linhas com chave vazia divergente: '
            f"esperado {resumo.get('total_linhas_chave_vazia')}, encontrado {len(df_linhas_chave_vazia)}.",
        )

    return erros


def validar_resumo_csv(df_resumo_csv, resumo):
    erros = []
    if df_resumo_csv.empty:
        return ['Resumo CSV esta vazio.']

    linha = df_resumo_csv.iloc[0]
    campos = {
        'TOTAL_LINHAS_ENTRADA': 'total_linhas_entrada',
        'TOTAL_GRUPOS': 'total_grupos',
        'TOTAL_LINHAS_COM_RESULTADO': 'total_linhas_com_resultado',
        'TOTAL_LINHAS_SEM_RESULTADO': 'total_linhas_sem_resultado',
        'TOTAL_GRUPOS_CHAVE_VAZIA': 'total_grupos_chave_vazia',
        'TOTAL_LINHAS_CHAVE_VAZIA': 'total_linhas_chave_vazia',
    }

    for coluna_csv, campo_json in campos.items():
        if coluna_csv not in df_resumo_csv.columns:
            registrar_erro(erros, f'Coluna ausente no resumo CSV: {coluna_csv}')
            continue

        valor_csv = int(pd.to_numeric(linha[coluna_csv], errors='coerce'))
        valor_json = int(resumo.get(campo_json, -1))
        if valor_csv != valor_json:
            registrar_erro(
                erros,
                f"Resumo CSV divergente em '{coluna_csv}': esperado {valor_json}, encontrado {valor_csv}.",
            )

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_08_resultado_unidade')
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
        print('TESTE FALHOU - exec_08_resultado_unidade')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_08_resultado_unidade.py')
        return 1

    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)
    df_inspecao = pd.read_csv(ARQUIVO_INSPECAO_GRUPOS, low_memory=False)
    df_linhas_chave_vazia = pd.read_csv(ARQUIVO_LINHAS_CHAVE_VAZIA, low_memory=False)
    df_resumo_csv = pd.read_csv(ARQUIVO_RESUMO_CSV, low_memory=False)

    with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
        resumo = json.load(arquivo)

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} e saida tem {len(df_saida)}.',
        )

    erros_resultado, esperado_resultado = validar_resultado_unidade(df_saida)
    erros.extend(erros_resultado)
    erros.extend(validar_uf_preenchida(df_saida))
    erros.extend(validar_resumo_json(df_saida, resumo, esperado_resultado))
    erros.extend(validar_arquivos_auxiliares(df_inspecao, df_linhas_chave_vazia, resumo))
    erros.extend(validar_resumo_csv(df_resumo_csv, resumo))
    erros.extend(validar_tipos(df_saida))

    if erros:
        imprimir_erros(erros)
        return 1

    print('TESTE OK - exec_08_resultado_unidade')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f'Total de linhas analisadas: {len(df_saida)}')
    print('Quantidade de linhas preservada.')
    print('RESULTADO DA UNIDADE bate com media por CLASSIFICACAO + LOCAL EDITADO + UF.')
    print("UF vazia/'vazio' tratada para CE.")
    print('Resumo JSON/CSV e arquivos auxiliares coerentes com o CSV.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
