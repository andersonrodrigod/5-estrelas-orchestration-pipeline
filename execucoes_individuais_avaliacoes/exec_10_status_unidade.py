# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/09_base_com_resultado_unidade.csv')
arquivo_saida = Path('data_exec_indiv/avaliacoes/10_base_com_status_unidade.csv')

pasta_resumo = Path('auditoria') / 'saida_resumo_avaliacoes' / 'exec_10_status_unidade'
arquivo_resumo_json = pasta_resumo / 'exec_10_status_unidade_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_10_status_unidade_resumo.txt'
arquivo_status_csv = pasta_resumo / 'exec_10_status_unidade_status.csv'


def processar_status_unidade(df_base):
    df = df_base.copy()

    df['RESULTADO DA UNIDADE'] = pd.to_numeric(df['RESULTADO DA UNIDADE'], errors='coerce')
    df['META'] = pd.to_numeric(df['META'], errors='coerce')
    df['STATUS UNIDADE'] = df['STATUS UNIDADE'].astype('object')

    mascara_sem_status = df['RESULTADO DA UNIDADE'].isna() | df['META'].isna()
    mascara_fora_meta = (~mascara_sem_status) & (df['RESULTADO DA UNIDADE'] < df['META'])
    mascara_dentro_meta = (~mascara_sem_status) & (df['RESULTADO DA UNIDADE'] >= df['META'])

    df.loc[mascara_fora_meta, 'STATUS UNIDADE'] = 'fora da meta'
    df.loc[mascara_dentro_meta, 'STATUS UNIDADE'] = 'dentro da meta'
    df.loc[mascara_sem_status, 'STATUS UNIDADE'] = None

    resumo_status = (
        df['STATUS UNIDADE']
        .fillna('sem status')
        .value_counts()
        .reset_index()
    )
    resumo_status.columns = ['STATUS UNIDADE', 'QUANTIDADE']

    resumo = {
        'execucao': 'exec_10_status_unidade',
        'total_linhas_entrada': int(len(df)),
        'total_dentro_meta': int(mascara_dentro_meta.sum()),
        'total_fora_meta': int(mascara_fora_meta.sum()),
        'total_sem_status': int(mascara_sem_status.sum())
    }
    artefatos = {
        'resumo_status': resumo_status,
    }

    return df, resumo, artefatos


def salvar_resumos_status_unidade(
    resumo,
    artefatos,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)

    destino_resumo_json = pasta_destino / 'exec_10_status_unidade_resumo.json'
    destino_resumo_txt = pasta_destino / 'exec_10_status_unidade_resumo.txt'
    destino_status_csv = pasta_destino / 'exec_10_status_unidade_status.csv'

    salvar_csv_padronizado(artefatos['resumo_status'], destino_status_csv)

    resumo_saida = dict(resumo)
    resumo_saida.update({
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else '',
        'arquivo_saida': str(arquivo_saida_resumo) if arquivo_saida_resumo else '',
        'arquivo_status_csv': str(destino_status_csv),
    })

    with open(destino_resumo_json, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo_saida, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 10 - STATUS UNIDADE',
        '',
        f"Arquivo de entrada: {resumo_saida['arquivo_entrada']}",
        f"Arquivo de saida: {resumo_saida['arquivo_saida']}",
        f"Arquivo de status: {resumo_saida['arquivo_status_csv']}",
        '',
        f"Total de linhas na entrada: {resumo_saida['total_linhas_entrada']}",
        f"Total dentro da meta: {resumo_saida['total_dentro_meta']}",
        f"Total fora da meta: {resumo_saida['total_fora_meta']}",
        f"Total sem status: {resumo_saida['total_sem_status']}"
    ]

    with open(destino_resumo_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def executar(salvar_base=True):
    print('Iniciando execucao 10 - status unidade...')
    print(f'Lendo arquivo da execucao 09: {arquivo_entrada}')

    df_entrada = ler_csv_padronizado(arquivo_entrada)
    df_saida, resumo, artefatos = processar_status_unidade(df_entrada)

    print(f"Total de linhas recebidas: {resumo['total_linhas_entrada']}")
    print(f"Total dentro da meta: {resumo['total_dentro_meta']}")
    print(f"Total fora da meta: {resumo['total_fora_meta']}")
    print(f"Total sem status: {resumo['total_sem_status']}")

    if salvar_base:
        print(f'Gravando arquivo da execucao 10: {arquivo_saida}')
        arquivo_saida.parent.mkdir(exist_ok=True)
        salvar_csv_padronizado(df_saida, arquivo_saida)

    salvar_resumos_status_unidade(
        resumo,
        artefatos,
        pasta_resumo,
        arquivo_entrada_resumo=arquivo_entrada,
        arquivo_saida_resumo=arquivo_saida,
    )

    print('Execucao 10 finalizada.')
    return df_saida, resumo, artefatos


if __name__ == '__main__':
    executar()

