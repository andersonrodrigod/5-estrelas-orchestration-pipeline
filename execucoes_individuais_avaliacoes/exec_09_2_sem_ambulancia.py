# -*- coding: utf-8 -*-
import json
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado


arquivo_entrada = Path('data_exec_indiv/avaliacoes/09_base_com_status_unidade.csv')
arquivo_saida = Path('data_exec_indiv/avaliacoes/09_2_base_sem_ambulancia.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_09_2_sem_ambulancia'
arquivo_resumo_json = pasta_resumo / 'exec_09_2_sem_ambulancia_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_09_2_sem_ambulancia_resumo.txt'
arquivo_excluidas_csv = pasta_resumo / 'exec_09_2_sem_ambulancia_excluidas.csv'

coluna_classificacao = 'CLASSIFICACAO'
classificacao_excluir = 'AMBULANCIA'


def normalizar_texto(valor):
    if pd.isna(valor):
        return ''

    texto = str(valor).strip().upper()
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(caractere for caractere in texto if not unicodedata.combining(caractere))
    return ' '.join(texto.split())


def salvar_resumos(df_entrada, df_saida, df_excluidas):
    pasta_resumo.mkdir(parents=True, exist_ok=True)

    resumo_excluidas = (
        df_excluidas[coluna_classificacao]
        .astype('string')
        .fillna('VAZIO')
        .value_counts(dropna=False)
        .reset_index()
    )
    resumo_excluidas.columns = ['CLASSIFICACAO', 'QUANTIDADE']
    salvar_csv_padronizado(resumo_excluidas, arquivo_excluidas_csv)

    resumo = {
        'execucao': 'exec_09_2_sem_ambulancia',
        'arquivo_entrada': str(arquivo_entrada),
        'arquivo_saida': str(arquivo_saida),
        'arquivo_excluidas_csv': str(arquivo_excluidas_csv),
        'classificacao_excluir': classificacao_excluir,
        'total_linhas_entrada': int(len(df_entrada)),
        'total_linhas_excluidas': int(len(df_excluidas)),
        'total_linhas_saida': int(len(df_saida)),
        'classificacoes_excluidas': resumo_excluidas.to_dict(orient='records'),
    }

    with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 09.2 - REMOVER AMBULANCIA',
        '',
        f"Arquivo de entrada: {resumo['arquivo_entrada']}",
        f"Arquivo de saida: {resumo['arquivo_saida']}",
        f"Resumo das linhas excluidas: {resumo['arquivo_excluidas_csv']}",
        '',
        f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
        f"Total de linhas excluidas: {resumo['total_linhas_excluidas']}",
        f"Total de linhas na saida: {resumo['total_linhas_saida']}",
        '',
        'Classificacoes excluidas:',
    ]

    if resumo['classificacoes_excluidas']:
        for item in resumo['classificacoes_excluidas']:
            linhas_txt.append(f"- {item['CLASSIFICACAO']}: {item['QUANTIDADE']}")
    else:
        linhas_txt.append('- Nenhuma')

    with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def executar():
    print('Iniciando execucao 09.2 - remover ambulancia...')
    print(f'Lendo arquivo da execucao 09: {arquivo_entrada}')

    if not arquivo_entrada.exists():
        print(f'ERRO - arquivo nao encontrado: {arquivo_entrada}')
        return 1

    df = ler_csv_padronizado(arquivo_entrada)

    if coluna_classificacao not in df.columns:
        print(f"ERRO - coluna obrigatoria ausente: {coluna_classificacao}")
        return 1

    classificacao_normalizada = df[coluna_classificacao].apply(normalizar_texto)
    mascara_ambulancia = classificacao_normalizada.eq(classificacao_excluir)

    df_saida = df.loc[~mascara_ambulancia].copy()
    df_excluidas = df.loc[mascara_ambulancia].copy()

    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
    print(f'Total de linhas recebidas: {len(df)}')
    print(f'Total ambulancia excluidas: {len(df_excluidas)}')
    print(f'Gravando arquivo sem ambulancia: {arquivo_saida}')
    salvar_csv_padronizado(df_saida, arquivo_saida)
    salvar_resumos(df, df_saida, df_excluidas)

    print('Execucao 09.2 finalizada.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
