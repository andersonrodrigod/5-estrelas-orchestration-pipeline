# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec/avaliacoes/02_base_com_contratacao.csv')
arquivo_saida = Path('data_exec/avaliacoes/03_base_com_nota.csv')
pasta_resumo = Path('auditoria') / 'saida_resumo_avaliacoes' / 'exec_03_nota'

colunas_notas = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']


def processar_nota(df):
    df = df.copy()

    for coluna in colunas_notas:
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce')

    df['Quantidade notas validas'] = df[colunas_notas].notna().sum(axis=1)
    df['NOTA GERAL'] = df[colunas_notas].mean(axis=1).round(2)

    distribuicao_notas_validas = (
        df['Quantidade notas validas']
        .value_counts()
        .sort_index()
        .to_dict()
    )

    resumo = {
        'execucao': 'exec_03_nota',
        'total_linhas_entrada': int(len(df)),
        'total_com_nota_geral': int(df['NOTA GERAL'].notna().sum()),
        'total_sem_nota_geral': int(df['NOTA GERAL'].isna().sum()),
        'distribuicao_quantidade_notas_validas': {
            str(chave): int(valor)
            for chave, valor in distribuicao_notas_validas.items()
        },
    }

    return df, resumo


def salvar_resumos_nota(
    resumo,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)

    resumo = {
        **resumo,
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else None,
        'arquivo_saida': str(arquivo_saida_resumo) if arquivo_saida_resumo else None,
    }

    with open(pasta_destino / 'exec_03_nota_resumo.json', 'w', encoding='utf-8') as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 03 - NOTA',
        '',
    ]

    if resumo['arquivo_entrada']:
        linhas_txt.append(f"Arquivo de entrada: {resumo['arquivo_entrada']}")

    if resumo['arquivo_saida']:
        linhas_txt.append(f"Arquivo de saida: {resumo['arquivo_saida']}")

    linhas_txt.extend([
        '',
        f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
        f"Total com nota geral calculada: {resumo['total_com_nota_geral']}",
        f"Total sem nota geral: {resumo['total_sem_nota_geral']}",
        '',
        'Quantidade de notas validas por linha:',
    ])

    for quantidade, total in resumo['distribuicao_quantidade_notas_validas'].items():
        linhas_txt.append(f'- {quantidade} nota(s) valida(s): {total}')

    with open(pasta_destino / 'exec_03_nota_resumo.txt', 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def executar(salvar_base=True):
    print('Iniciando execucao 03 - nota...')
    print(f'Lendo arquivo da execucao 02: {arquivo_entrada}')

    df = ler_csv_padronizado(arquivo_entrada)
    df, resumo = processar_nota(df)

    print(f"Total de linhas recebidas: {resumo['total_linhas_entrada']}")
    print(f"Total com nota geral calculada: {resumo['total_com_nota_geral']}")
    print(f"Total sem nota geral: {resumo['total_sem_nota_geral']}")

    if salvar_base:
        print(f'Gravando arquivo da execucao 03: {arquivo_saida}')
        arquivo_saida.parent.mkdir(exist_ok=True)
        salvar_csv_padronizado(df, arquivo_saida)

    salvar_resumos_nota(resumo, pasta_resumo, arquivo_entrada, arquivo_saida)

    print('Execucao 03 finalizada.')
    return df, resumo


if __name__ == '__main__':
    executar()
