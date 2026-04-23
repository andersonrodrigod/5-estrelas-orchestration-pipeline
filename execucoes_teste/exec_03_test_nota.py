# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/02_base_com_contratacao.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/03_base_com_nota.csv')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_03_nota'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_03_nota_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_03_nota_resumo.txt'

COLUNAS_NOTAS = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']
COLUNA_QUANTIDADE = 'Quantidade notas validas'
COLUNA_NOTA_GERAL = 'NOTA GERAL'
COLUNAS_OBRIGATORIAS = [*COLUNAS_NOTAS, COLUNA_QUANTIDADE, COLUNA_NOTA_GERAL]
VALOR_MINIMO_NOTA = 1
VALOR_MAXIMO_NOTA = 5
LIMITE_EXEMPLOS = 20


def carregar_csv(caminho):
    return pd.read_csv(caminho, dtype='string', keep_default_na=False, low_memory=False)


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_SAIDA,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def validar_colunas(df):
    return [coluna for coluna in COLUNAS_OBRIGATORIAS if coluna not in df.columns]


def converter_colunas_notas(df):
    return df[COLUNAS_NOTAS].apply(pd.to_numeric, errors='coerce')


def validar_quantidade_notas(df, notas_numericas):
    erros = []
    quantidade_esperada = notas_numericas.notna().sum(axis=1)
    quantidade_saida = pd.to_numeric(df[COLUNA_QUANTIDADE], errors='coerce')
    divergencias = quantidade_saida.ne(quantidade_esperada)

    for linha, valor_saida, valor_esperado in zip(
        df.index[divergencias] + 2,
        quantidade_saida[divergencias],
        quantidade_esperada[divergencias],
    ):
        registrar_erro(
            erros,
            f'Linha {int(linha)} com quantidade de notas validas incorreta: '
            f'esperado {int(valor_esperado)}, encontrado {valor_saida}.'
        )

    return erros


def validar_nota_geral(df, notas_numericas):
    erros = []
    nota_esperada = notas_numericas.mean(axis=1).round(2)
    nota_saida = pd.to_numeric(df[COLUNA_NOTA_GERAL], errors='coerce')

    divergencias = nota_saida.notna() != nota_esperada.notna()
    divergencias = divergencias | (nota_saida.notna() & (nota_saida.round(2) != nota_esperada))

    for linha, valor_saida, valor_esperado in zip(
        df.index[divergencias] + 2,
        nota_saida[divergencias],
        nota_esperada[divergencias],
    ):
        registrar_erro(
            erros,
            f'Linha {int(linha)} com NOTA GERAL incorreta: '
            f'esperado {valor_esperado}, encontrado {valor_saida}.'
        )

    fora_intervalo = nota_saida.notna() & (
        (nota_saida < VALOR_MINIMO_NOTA) | (nota_saida > VALOR_MAXIMO_NOTA)
    )
    for linha, valor in zip(df.index[fora_intervalo] + 2, nota_saida[fora_intervalo]):
        registrar_erro(
            erros,
            f'Linha {int(linha)} com NOTA GERAL fora do intervalo 1 a 5: {valor}.'
        )

    return erros


def calcular_totais_resumo(df, notas_numericas):
    nota_geral = pd.to_numeric(df[COLUNA_NOTA_GERAL], errors='coerce')
    quantidade = notas_numericas.notna().sum(axis=1)
    distribuicao = quantidade.value_counts().sort_index().to_dict()

    return {
        'total_linhas_entrada': int(len(df)),
        'total_com_nota_geral': int(nota_geral.notna().sum()),
        'total_sem_nota_geral': int(nota_geral.isna().sum()),
        'distribuicao_quantidade_notas_validas': {
            str(chave): int(valor)
            for chave, valor in distribuicao.items()
        },
    }


def validar_resumo_json(df, notas_numericas, resumo):
    erros = []
    totais_esperados = calcular_totais_resumo(df, notas_numericas)

    for campo, valor_esperado in totais_esperados.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != valor_esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': "
                f"esperado {valor_esperado}, encontrado {valor_resumo}."
            )

    return erros


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_03_nota')
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
        print('TESTE FALHOU - exec_03_nota')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_03_nota.py')
        return 1

    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)

    colunas_faltando = validar_colunas(df_saida)
    for coluna in colunas_faltando:
        registrar_erro(erros, f"Coluna obrigatoria ausente: {coluna}")

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} '
            f'e saida tem {len(df_saida)}.'
        )

    if not colunas_faltando:
        notas_numericas = converter_colunas_notas(df_saida)
        erros.extend(validar_quantidade_notas(df_saida, notas_numericas))
        erros.extend(validar_nota_geral(df_saida, notas_numericas))

        with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
            resumo = json.load(arquivo)
        erros.extend(validar_resumo_json(df_saida, notas_numericas, resumo))

    if erros:
        imprimir_erros(erros)
        return 1

    notas_numericas = converter_colunas_notas(df_saida)
    totais = calcular_totais_resumo(df_saida, notas_numericas)
    print('TESTE OK - exec_03_nota')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f'Total de linhas analisadas: {len(df_saida)}')
    print(f"Com NOTA GERAL: {totais['total_com_nota_geral']}")
    print(f"Sem NOTA GERAL: {totais['total_sem_nota_geral']}")
    print(
        'Distribuicao de notas validas: '
        f"{totais['distribuicao_quantidade_notas_validas']}"
    )
    print('Quantidade de linhas preservada.')
    print('Quantidade notas validas bate com NOTA1 a NOTA5.')
    print('NOTA GERAL bate com a media recalculada.')
    print('Resumo JSON bate com o CSV.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
