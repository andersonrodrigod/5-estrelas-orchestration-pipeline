# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/08_base_com_meta.csv')
arquivo_saida = Path('data_exec_indiv/avaliacoes/09_base_com_resultado_unidade.csv')

pasta_resumo = Path('auditoria') / 'saida_resumo_avaliacoes' / 'exec_09_resultado_unidade'
arquivo_resumo_json = pasta_resumo / 'exec_09_resultado_unidade_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_09_resultado_unidade_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_09_resultado_unidade_resumo.csv'
arquivo_inspecao_grupos_csv = pasta_resumo / 'exec_09_resultado_unidade_inspecao_grupos.csv'
arquivo_linhas_chave_vazia_csv = pasta_resumo / 'exec_09_resultado_unidade_linhas_chave_vazia.csv'

colunas_grupo = ['CLASSIFICACAO', 'LOCAL EDITADO', 'UF']


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip()


def identificar_colunas_vazias(linha):
    colunas_vazias = []

    for coluna in colunas_grupo:
        valor = linha[coluna]

        if pd.isna(valor) or valor == '':
            colunas_vazias.append(coluna)

    return ', '.join(colunas_vazias)


def processar_resultado_unidade(df_base):
    df = df_base.copy()

    for coluna in colunas_grupo:
        df[coluna] = normalizar_texto(df[coluna])

    mascara_uf_vazia = (
        df['UF'].isna() | (df['UF'] == '') | (df['UF'].str.lower() == 'vazio')
    )
    df.loc[mascara_uf_vazia, 'UF'] = 'CE'

    df['NOTA GERAL'] = pd.to_numeric(df['NOTA GERAL'], errors='coerce')
    df['RESULTADO DA UNIDADE'] = (
        df.groupby(colunas_grupo, dropna=False)['NOTA GERAL']
        .transform('mean')
        .round(2)
    )

    df_inspecao_grupos = (
        df.groupby(colunas_grupo, dropna=False)
        .agg(
            QUANTIDADE_LINHAS=('NOTA GERAL', 'size'),
            RESULTADO_DA_UNIDADE=('RESULTADO DA UNIDADE', 'first'),
            NOTA_GERAL_MIN=('NOTA GERAL', 'min'),
            NOTA_GERAL_MAX=('NOTA GERAL', 'max'),
            META=('META', 'first'),
            OPERADORA=('OPERADORA', 'first')
        )
        .reset_index()
        .sort_values('QUANTIDADE_LINHAS', ascending=False)
    )

    mascara_grupos_chave_vazia = (
        df_inspecao_grupos['CLASSIFICACAO'].isna() |
        (df_inspecao_grupos['CLASSIFICACAO'] == '') |
        df_inspecao_grupos['LOCAL EDITADO'].isna() |
        (df_inspecao_grupos['LOCAL EDITADO'] == '') |
        df_inspecao_grupos['UF'].isna() |
        (df_inspecao_grupos['UF'] == '')
    )

    mascara_linhas_chave_vazia = (
        df['CLASSIFICACAO'].isna() |
        (df['CLASSIFICACAO'] == '') |
        df['LOCAL EDITADO'].isna() |
        (df['LOCAL EDITADO'] == '') |
        df['UF'].isna() |
        (df['UF'] == '')
    )
    df_linhas_chave_vazia = df.loc[mascara_linhas_chave_vazia].copy()
    df_linhas_chave_vazia['COLUNA_VAZIA'] = df_linhas_chave_vazia.apply(
        identificar_colunas_vazias,
        axis=1,
    )

    resumo = {
        'execucao': 'exec_09_resultado_unidade',
        'total_linhas_entrada': int(len(df)),
        'total_grupos': int(len(df_inspecao_grupos)),
        'total_linhas_com_resultado': int(df['RESULTADO DA UNIDADE'].notna().sum()),
        'total_linhas_sem_resultado': int(df['RESULTADO DA UNIDADE'].isna().sum()),
        'total_grupos_chave_vazia': int(mascara_grupos_chave_vazia.sum()),
        'total_linhas_chave_vazia': int(len(df_linhas_chave_vazia)),
    }
    artefatos = {
        'inspecao_grupos': df_inspecao_grupos,
        'linhas_chave_vazia': df_linhas_chave_vazia,
    }

    return df, resumo, artefatos


def salvar_resumos_resultado_unidade(
    resumo,
    artefatos,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)

    destino_resumo_json = pasta_destino / 'exec_09_resultado_unidade_resumo.json'
    destino_resumo_txt = pasta_destino / 'exec_09_resultado_unidade_resumo.txt'
    destino_resumo_csv = pasta_destino / 'exec_09_resultado_unidade_resumo.csv'
    destino_inspecao_csv = pasta_destino / 'exec_09_resultado_unidade_inspecao_grupos.csv'
    destino_chave_vazia_csv = (
        pasta_destino / 'exec_09_resultado_unidade_linhas_chave_vazia.csv'
    )

    salvar_csv_padronizado(artefatos['inspecao_grupos'], destino_inspecao_csv)
    salvar_csv_padronizado(artefatos['linhas_chave_vazia'], destino_chave_vazia_csv)

    resumo_saida = dict(resumo)
    resumo_saida.update({
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else '',
        'arquivo_saida': str(arquivo_saida_resumo) if arquivo_saida_resumo else '',
        'arquivo_inspecao_grupos': str(destino_inspecao_csv),
        'arquivo_linhas_chave_vazia': str(destino_chave_vazia_csv),
    })

    with open(destino_resumo_json, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo_saida, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 09 - RESULTADO DA UNIDADE',
        '',
        f"Arquivo de entrada: {resumo_saida['arquivo_entrada']}",
        f"Arquivo de saida: {resumo_saida['arquivo_saida']}",
        f"Arquivo de inspecao: {resumo_saida['arquivo_inspecao_grupos']}",
        f"Arquivo de linhas com chave vazia: {resumo_saida['arquivo_linhas_chave_vazia']}",
        '',
        f"Total de linhas na entrada: {resumo_saida['total_linhas_entrada']}",
        f"Total de grupos: {resumo_saida['total_grupos']}",
        f"Total de linhas com resultado da unidade: {resumo_saida['total_linhas_com_resultado']}",
        f"Total de linhas sem resultado da unidade: {resumo_saida['total_linhas_sem_resultado']}",
        f"Total de grupos com alguma chave vazia: {resumo_saida['total_grupos_chave_vazia']}",
        f"Total de linhas com alguma chave vazia: {resumo_saida['total_linhas_chave_vazia']}"
    ]

    with open(destino_resumo_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))

    salvar_csv_padronizado(pd.DataFrame([{
        'EXECUCAO': resumo_saida['execucao'],
        'ARQUIVO_ENTRADA': resumo_saida['arquivo_entrada'],
        'ARQUIVO_SAIDA': resumo_saida['arquivo_saida'],
        'ARQUIVO_INSPECAO': resumo_saida['arquivo_inspecao_grupos'],
        'ARQUIVO_LINHAS_CHAVE_VAZIA': resumo_saida['arquivo_linhas_chave_vazia'],
        'TOTAL_LINHAS_ENTRADA': resumo_saida['total_linhas_entrada'],
        'TOTAL_GRUPOS': resumo_saida['total_grupos'],
        'TOTAL_LINHAS_COM_RESULTADO': resumo_saida['total_linhas_com_resultado'],
        'TOTAL_LINHAS_SEM_RESULTADO': resumo_saida['total_linhas_sem_resultado'],
        'TOTAL_GRUPOS_CHAVE_VAZIA': resumo_saida['total_grupos_chave_vazia'],
        'TOTAL_LINHAS_CHAVE_VAZIA': resumo_saida['total_linhas_chave_vazia']
    }]), destino_resumo_csv)


def executar(salvar_base=True):
    print('Iniciando execucao 09 - resultado da unidade...')
    print(f'Lendo arquivo da execucao 08: {arquivo_entrada}')

    df_entrada = ler_csv_padronizado(arquivo_entrada)
    df_saida, resumo, artefatos = processar_resultado_unidade(df_entrada)

    print(f"Total de linhas recebidas: {resumo['total_linhas_entrada']}")
    print(f"Total de grupos encontrados: {resumo['total_grupos']}")
    print(f"Total de linhas com resultado da unidade: {resumo['total_linhas_com_resultado']}")
    print(f"Total de linhas sem resultado da unidade: {resumo['total_linhas_sem_resultado']}")

    if salvar_base:
        print(f'Gravando arquivo da execucao 09: {arquivo_saida}')
        arquivo_saida.parent.mkdir(exist_ok=True)
        salvar_csv_padronizado(df_saida, arquivo_saida)

    salvar_resumos_resultado_unidade(
        resumo,
        artefatos,
        pasta_resumo,
        arquivo_entrada_resumo=arquivo_entrada,
        arquivo_saida_resumo=arquivo_saida,
    )

    print('Execucao 09 finalizada.')
    return df_saida, resumo, artefatos


if __name__ == '__main__':
    executar()

