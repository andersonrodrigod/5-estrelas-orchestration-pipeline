# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/01_base_limpa.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/02_base_com_contratacao.csv')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_02_contratacao'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_02_contratacao_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_02_contratacao_resumo.txt'
ARQUIVO_LOCAIS_SEM_CONTRATACAO = PASTA_RESUMO / 'exec_02_locais_sem_contratacao.csv'
ARQUIVO_LINHAS_SEM_CONTRATACAO = PASTA_RESUMO / 'exec_02_linhas_sem_contratacao.csv'

COLUNAS_OBRIGATORIAS = ['LOCAL', 'CONTRATACAO']
VALORES_VALIDOS_CONTRATACAO = {'rede propria', 'rede credenciada'}
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
        ARQUIVO_LOCAIS_SEM_CONTRATACAO,
        ARQUIVO_LINHAS_SEM_CONTRATACAO,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def validar_colunas(df):
    return [coluna for coluna in COLUNAS_OBRIGATORIAS if coluna not in df.columns]


def validar_valores_contratacao(df):
    valores = df['CONTRATACAO'].astype('string').fillna('').str.strip().str.lower()
    preenchidos = valores != ''
    invalidos = preenchidos & ~valores.isin(VALORES_VALIDOS_CONTRATACAO)

    exemplos = []
    for linha, valor in zip(df.index[invalidos] + 2, valores[invalidos]):
        exemplos.append({
            'linha_csv': int(linha),
            'valor': str(valor),
        })

    return exemplos


def contar_contratacoes(df):
    valores = df['CONTRATACAO'].astype('string').fillna('').str.strip().str.lower()
    return {
        'total_rede_propria': int((valores == 'rede propria').sum()),
        'total_rede_credenciada': int((valores == 'rede credenciada').sum()),
        'total_sem_contratacao': int((valores == '').sum()),
    }


def validar_resumo_json(df_saida, resumo):
    erros = []
    contagens = contar_contratacoes(df_saida)

    campos_esperados = {
        'total_linhas_entrada': int(len(df_saida)),
        **contagens,
    }

    for campo, valor_esperado in campos_esperados.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != valor_esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': "
                f"esperado {valor_esperado}, encontrado {valor_resumo}."
            )

    return erros


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_02_contratacao')
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
        print('TESTE FALHOU - exec_02_contratacao')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_02_contratacao.py')
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
        valores_invalidos = validar_valores_contratacao(df_saida)
        for item in valores_invalidos:
            registrar_erro(
                erros,
                f"Linha {item['linha_csv']} com CONTRATACAO invalida: "
                f"'{item['valor']}'"
            )

        with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
            resumo = json.load(arquivo)
        erros.extend(validar_resumo_json(df_saida, resumo))

    if erros:
        imprimir_erros(erros)
        return 1

    contagens = contar_contratacoes(df_saida)
    print('TESTE OK - exec_02_contratacao')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f'Total de linhas analisadas: {len(df_saida)}')
    print(f"Rede propria: {contagens['total_rede_propria']}")
    print(f"Rede credenciada: {contagens['total_rede_credenciada']}")
    print(f"Sem contratacao: {contagens['total_sem_contratacao']}")
    print('Quantidade de linhas preservada.')
    print('Valores de CONTRATACAO validos.')
    print('Resumo JSON bate com o CSV.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
