# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data/manual/junho_incompleto_5_estrelas.csv')
arquivo_saida = Path('data_exec/avaliacoes/01_base_limpa.csv')
arquivo_auditoria_classificacao = Path('data/base_auditoria_classificacao.csv')
pasta_resumo = Path('auditoria') / 'saida_resumo_avaliacoes' / 'exec_01_limpeza'

renomear_colunas = {
    'Nota geral': 'NOTA GERAL',
    'Contratacao': 'CONTRATACAO',
    'Operadora': 'OPERADORA',
    'Local editado': 'LOCAL EDITADO',
    'Meta': 'META',
    'Resultado da unidade': 'RESULTADO DA UNIDADE',
    'Status unidade': 'STATUS UNIDADE',
}

colunas_obrigatorias = [
    'NOTA GERAL',
    'CONTRATACAO',
    'CLASSIFICACAO',
    'OPERADORA',
    'LOCAL EDITADO',
    'META',
    'RESULTADO DA UNIDADE',
    'STATUS UNIDADE',
]

colunas_notas = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']
colunas_auditoria_classificacao = [
    'CDUSUARIO',
    'MES',
    'DIA',
    'ANO',
    'TIPO',
    'CONTRATACAO',
    'LOCAL',
    'ESPECIALIDADE',
]


def remover_ponto_final(serie):
    return serie.astype('string').str.strip().str.replace(r'\.+$', '', regex=True).str.strip()


def processar_limpeza(df):
    df = df.rename(columns=renomear_colunas).copy()

    for coluna in colunas_obrigatorias:
        if coluna not in df.columns:
            df[coluna] = None

    if 'LOCAL' in df.columns:
        df['LOCAL'] = remover_ponto_final(df['LOCAL'])

    total_entrada = int(len(df))

    for coluna in colunas_notas:
        df[coluna] = pd.to_numeric(df[coluna], errors='coerce')

    df['Quantidade notas validas'] = df[colunas_notas].notna().sum(axis=1)
    df_limpo = df[df['Quantidade notas validas'] > 0].copy()
    df_limpo['NOTA GERAL'] = None
    df_auditoria_classificacao = df_limpo[colunas_auditoria_classificacao].copy()

    resumo = {
        'execucao': 'exec_01_limpeza',
        'total_linhas_entrada': total_entrada,
        'total_linhas_saida': int(len(df_limpo)),
        'total_linhas_removidas': int(total_entrada - len(df_limpo)),
        'total_linhas_com_alguma_nota_valida': int((df['Quantidade notas validas'] > 0).sum()),
        'total_linhas_sem_nota_valida': int((df['Quantidade notas validas'] == 0).sum()),
    }

    artefatos = {
        'auditoria_classificacao': df_auditoria_classificacao,
    }

    return df_limpo, resumo, artefatos


def salvar_resumos_limpeza(
    resumo,
    artefatos,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
    arquivo_auditoria_resumo=None,
):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)
    arquivo_auditoria = arquivo_auditoria_resumo or (
        pasta_destino / 'exec_01_base_auditoria_classificacao.csv'
    )
    Path(arquivo_auditoria).parent.mkdir(parents=True, exist_ok=True)
    salvar_csv_padronizado(artefatos['auditoria_classificacao'], arquivo_auditoria)

    resumo = {
        **resumo,
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else None,
        'arquivo_saida': str(arquivo_saida_resumo) if arquivo_saida_resumo else None,
        'arquivo_auditoria_classificacao': str(arquivo_auditoria),
    }

    with open(pasta_destino / 'exec_01_limpeza_resumo.json', 'w', encoding='utf-8') as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 01 - LIMPEZA',
        '',
    ]

    if resumo['arquivo_entrada']:
        linhas_txt.append(f"Arquivo de entrada: {resumo['arquivo_entrada']}")

    if resumo['arquivo_saida']:
        linhas_txt.append(f"Arquivo de saida: {resumo['arquivo_saida']}")

    linhas_txt.extend([
        f"Arquivo de auditoria da classificacao: {resumo['arquivo_auditoria_classificacao']}",
        '',
        f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
        f"Total de linhas na saida: {resumo['total_linhas_saida']}",
        f"Total de linhas removidas: {resumo['total_linhas_removidas']}",
        f"Linhas com alguma nota valida: {resumo['total_linhas_com_alguma_nota_valida']}",
        f"Linhas sem nota valida: {resumo['total_linhas_sem_nota_valida']}",
    ])

    with open(pasta_destino / 'exec_01_limpeza_resumo.txt', 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def executar(salvar_base=True):
    print('Iniciando execucao 01 - limpeza...')
    print(f'Lendo arquivo original: {arquivo_entrada}')

    df = ler_csv_padronizado(arquivo_entrada)
    df_limpo, resumo, artefatos = processar_limpeza(df)

    print(f"Total de linhas antes da limpeza: {resumo['total_linhas_entrada']}")
    print(f"Total de linhas apos tirar IGN, NQA e sem nota valida: {resumo['total_linhas_saida']}")
    print(f"Total de linhas removidas: {resumo['total_linhas_removidas']}")

    if salvar_base:
        print(f'Gravando arquivo da execucao 01: {arquivo_saida}')
        arquivo_saida.parent.mkdir(exist_ok=True)
        salvar_csv_padronizado(df_limpo, arquivo_saida)

    salvar_resumos_limpeza(
        resumo,
        artefatos,
        pasta_resumo,
        arquivo_entrada,
        arquivo_saida,
        arquivo_auditoria_classificacao,
    )

    print('Execucao 01 finalizada.')
    return df_limpo, resumo


if __name__ == '__main__':
    executar()
