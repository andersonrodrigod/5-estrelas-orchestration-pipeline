# -*- coding: utf-8 -*-
from pathlib import Path
import sys

import pandas as pd


ARQUIVO_BASE_LIMPA = Path('data_exec_indiv/avaliacoes/01_base_limpa.csv')
COLUNAS_NOTAS = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']
VALOR_MINIMO_NOTA = 1
VALOR_MAXIMO_NOTA = 5
LIMITE_EXEMPLOS = 20


def registrar_problemas(problemas, tipo, coluna, linhas, valores):
    for linha, valor in zip(linhas, valores):
        problemas.append({
            'tipo': tipo,
            'coluna': coluna,
            'linha_csv': int(linha),
            'valor': str(valor),
        })


def validar_colunas_notas(df):
    return [coluna for coluna in COLUNAS_NOTAS if coluna not in df.columns]


def validar_valores_notas(df):
    problemas = []

    for coluna in COLUNAS_NOTAS:
        valores_texto = df[coluna].astype('string').fillna('').str.strip()
        preenchidos = valores_texto != ''
        valores_numericos = pd.to_numeric(valores_texto, errors='coerce')

        nao_numericos = preenchidos & valores_numericos.isna()
        registrar_problemas(
            problemas,
            'valor nao numerico',
            coluna,
            df.index[nao_numericos] + 2,
            valores_texto[nao_numericos],
        )

        numericos_validos = preenchidos & valores_numericos.notna()
        nao_inteiros = numericos_validos & (valores_numericos % 1 != 0)
        registrar_problemas(
            problemas,
            'valor numerico nao inteiro',
            coluna,
            df.index[nao_inteiros] + 2,
            valores_texto[nao_inteiros],
        )

        fora_intervalo = numericos_validos & (
            (valores_numericos < VALOR_MINIMO_NOTA)
            | (valores_numericos > VALOR_MAXIMO_NOTA)
        )
        registrar_problemas(
            problemas,
            'valor fora do intervalo 1 a 5',
            coluna,
            df.index[fora_intervalo] + 2,
            valores_texto[fora_intervalo],
        )

    return problemas


def imprimir_problemas(problemas):
    print('TESTE FALHOU - exec_01_limpeza')
    print('')
    print(f'Total de problemas encontrados: {len(problemas)}')
    print(f'Mostrando os primeiros {min(len(problemas), LIMITE_EXEMPLOS)}:')
    print('')

    for problema in problemas[:LIMITE_EXEMPLOS]:
        print(
            f"- Linha {problema['linha_csv']} | "
            f"Coluna {problema['coluna']} | "
            f"Valor '{problema['valor']}' | "
            f"Problema: {problema['tipo']}"
        )


def executar():
    if not ARQUIVO_BASE_LIMPA.exists():
        print('TESTE FALHOU - exec_01_limpeza')
        print('')
        print(f'Arquivo nao encontrado: {ARQUIVO_BASE_LIMPA}')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_01_limpeza.py')
        return 1

    df = pd.read_csv(ARQUIVO_BASE_LIMPA, dtype='string', keep_default_na=False)

    colunas_faltando = validar_colunas_notas(df)
    if colunas_faltando:
        print('TESTE FALHOU - exec_01_limpeza')
        print('')
        print('Colunas de nota ausentes:')
        for coluna in colunas_faltando:
            print(f'- {coluna}')
        return 1

    problemas = validar_valores_notas(df)
    if problemas:
        imprimir_problemas(problemas)
        return 1

    print('TESTE OK - exec_01_limpeza')
    print(f'Arquivo testado: {ARQUIVO_BASE_LIMPA}')
    print(f'Total de linhas analisadas: {len(df)}')
    print(f'Colunas testadas: {", ".join(COLUNAS_NOTAS)}')
    print('Nenhum valor nao numerico encontrado.')
    print('Nenhum valor decimal encontrado.')
    print('Nenhum valor fora do intervalo 1 a 5 encontrado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
