# -*- coding: utf-8 -*-
import json
import re
from pathlib import Path

import pandas as pd

from execucoes_individuais_avaliacoes.exec_02_contratacao import (
    processar_contratacao,
    salvar_resumos_contratacao,
)
from execucoes_individuais_avaliacoes.exec_04_classificacao import (
    processar_classificacao,
    salvar_resumos_classificacao,
)
from execucoes_individuais_avaliacoes.exec_05_local_editado import (
    processar_local_editado,
    salvar_resumos_local_editado,
)
from funcoes_auxiliares.padronizacao_csv import salvar_csv_padronizado


colunas_verificacao = ['LOCAL', 'ESPECIALIDADE', 'UF']
regex_letra = re.compile(r'[A-Za-zÀ-ÖØ-öø-ÿ]')
regex_apenas_especial = re.compile(r'^[^A-Za-zÀ-ÖØ-öø-ÿ0-9]+$')

renomear_colunas = {
    'Contratacao': 'CONTRATACAO',
    'Local editado': 'LOCAL EDITADO',
}

colunas_obrigatorias_limpeza = [
    'CONTRATACAO',
    'CLASSIFICACAO',
    'LOCAL EDITADO',
]


def preparar_texto(serie):
    texto = serie.astype('string')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip()


def transformar_em_lista_registros(df_base, colunas):
    if df_base.empty:
        return []

    registros = []
    for _, linha in df_base.iterrows():
        item = {}
        for coluna in colunas:
            valor = linha[coluna]
            item[coluna] = None if pd.isna(valor) else str(valor)
        registros.append(item)

    return registros


def processar_verificacao_campos_negativas(df_base):
    df = df_base.copy()
    resumos = []
    valores_invalidos = []
    mascaras_invalidas = {}

    for coluna in colunas_verificacao:
        if coluna not in df.columns:
            resumos.append({
                'COLUNA': coluna,
                'TOTAL_LINHAS': int(len(df)),
                'TOTAL_VAZIOS': None,
                'TOTAL_APENAS_HIFEN': None,
                'TOTAL_SEM_LETRAS': None,
                'TOTAL_APENAS_CARACTERES_ESPECIAIS': None,
                'OBSERVACAO': 'Coluna nao encontrada',
            })
            continue

        texto = preparar_texto(df[coluna])
        mascara_vazio = texto.isna() | (texto == '')
        mascara_apenas_hifen = texto.fillna('').str.fullmatch(r'-+')
        mascara_sem_letras = (~mascara_vazio) & ~texto.fillna('').str.contains(regex_letra)
        mascara_apenas_especial = (
            (~mascara_vazio) & texto.fillna('').str.fullmatch(regex_apenas_especial)
        )
        mascaras_invalidas[coluna] = mascara_vazio | mascara_sem_letras

        resumos.append({
            'COLUNA': coluna,
            'TOTAL_LINHAS': int(len(df)),
            'TOTAL_VAZIOS': int(mascara_vazio.sum()),
            'TOTAL_APENAS_HIFEN': int(mascara_apenas_hifen.sum()),
            'TOTAL_SEM_LETRAS': int(mascara_sem_letras.sum()),
            'TOTAL_APENAS_CARACTERES_ESPECIAIS': int(mascara_apenas_especial.sum()),
            'OBSERVACAO': '',
        })

        mascara_invalidos = mascara_vazio | mascara_sem_letras
        df_valores = (
            pd.DataFrame({
                'VALOR': texto[mascara_invalidos].fillna('VAZIO'),
                'TIPO_PROBLEMA': [
                    'vazio' if vazio else (
                        'apenas_hifen' if hifen else (
                            'apenas_caracteres_especiais' if especial else 'sem_letras'
                        )
                    )
                    for vazio, hifen, especial in zip(
                        mascara_vazio[mascara_invalidos],
                        mascara_apenas_hifen[mascara_invalidos],
                        mascara_apenas_especial[mascara_invalidos],
                    )
                ],
            })
            .value_counts()
            .reset_index(name='QUANTIDADE')
            .sort_values(['QUANTIDADE', 'VALOR'], ascending=[False, True])
        )
        df_valores.insert(0, 'COLUNA', coluna)
        valores_invalidos.append(df_valores)

    df_resumo = pd.DataFrame(resumos)
    df_valores_invalidos = (
        pd.concat(valores_invalidos, ignore_index=True)
        if valores_invalidos
        else pd.DataFrame(columns=['COLUNA', 'VALOR', 'TIPO_PROBLEMA', 'QUANTIDADE'])
    )
    mascara_local_especialidade_invalidos = (
        mascaras_invalidas.get('LOCAL', pd.Series(False, index=df.index))
        & mascaras_invalidas.get('ESPECIALIDADE', pd.Series(False, index=df.index))
    )
    df_local_especialidade_invalidos = df.loc[
        mascara_local_especialidade_invalidos
    ].copy()

    resumo = {
        'execucao': 'exec_00_verificacao_campos_negativas',
        'total_linhas_entrada': int(len(df)),
        'total_local_especialidade_invalidos': int(
            mascara_local_especialidade_invalidos.sum()
        ),
        'colunas_verificadas': colunas_verificacao,
        'resumo_colunas': transformar_em_lista_registros(
            df_resumo,
            [
                'COLUNA',
                'TOTAL_LINHAS',
                'TOTAL_VAZIOS',
                'TOTAL_APENAS_HIFEN',
                'TOTAL_SEM_LETRAS',
                'TOTAL_APENAS_CARACTERES_ESPECIAIS',
                'OBSERVACAO',
            ],
        ),
    }
    artefatos = {
        'resumo': df_resumo,
        'valores_invalidos': df_valores_invalidos,
        'local_especialidade_invalidos': df_local_especialidade_invalidos,
    }

    return df, resumo, artefatos


def salvar_resumos_verificacao_campos_negativas(
    resumo,
    artefatos,
    pasta_destino,
    arquivo_entrada_resumo=None,
):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)

    arquivo_resumo_csv = pasta_destino / 'exec_00_verificacao_campos_resumo.csv'
    arquivo_valores_invalidos_csv = (
        pasta_destino / 'exec_00_verificacao_campos_valores_invalidos.csv'
    )
    arquivo_local_especialidade_invalidos_csv = (
        pasta_destino / 'exec_00_verificacao_campos_local_especialidade_invalidos.csv'
    )

    salvar_csv_padronizado(artefatos['resumo'], arquivo_resumo_csv)
    salvar_csv_padronizado(artefatos['valores_invalidos'], arquivo_valores_invalidos_csv)
    salvar_csv_padronizado(
        artefatos['local_especialidade_invalidos'],
        arquivo_local_especialidade_invalidos_csv,
    )

    resumo_saida = {
        **resumo,
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else '',
        'arquivo_resumo_csv': str(arquivo_resumo_csv),
        'arquivo_valores_invalidos_csv': str(arquivo_valores_invalidos_csv),
        'arquivo_local_especialidade_invalidos_csv': str(
            arquivo_local_especialidade_invalidos_csv
        ),
    }

    with open(
        pasta_destino / 'exec_00_verificacao_campos_resumo.json',
        'w',
        encoding='utf-8',
    ) as arquivo:
        json.dump(resumo_saida, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 00 - VERIFICACAO DE CAMPOS NEGATIVAS',
        '',
        f"Arquivo de entrada: {resumo_saida['arquivo_entrada']}",
        f"Arquivo de resumo CSV: {resumo_saida['arquivo_resumo_csv']}",
        f"Valores invalidos agrupados: {resumo_saida['arquivo_valores_invalidos_csv']}",
        (
            'Linhas com LOCAL e ESPECIALIDADE invalidos: '
            f"{resumo_saida['arquivo_local_especialidade_invalidos_csv']}"
        ),
        '',
        f"Total de linhas na entrada: {resumo_saida['total_linhas_entrada']}",
        (
            'Total com LOCAL e ESPECIALIDADE invalidos juntos: '
            f"{resumo_saida['total_local_especialidade_invalidos']}"
        ),
        '',
        'COLUNAS VERIFICADAS:',
    ]

    for item in resumo_saida['resumo_colunas']:
        linhas_txt.extend([
            '',
            f"- Coluna: {item['COLUNA']}",
            f"  Total vazios: {item['TOTAL_VAZIOS']}",
            f"  Total apenas hifen: {item['TOTAL_APENAS_HIFEN']}",
            f"  Total sem letras: {item['TOTAL_SEM_LETRAS']}",
            (
                '  Total apenas caracteres especiais: '
                f"{item['TOTAL_APENAS_CARACTERES_ESPECIAIS']}"
            ),
        ])

    with open(
        pasta_destino / 'exec_00_verificacao_campos_resumo.txt',
        'w',
        encoding='utf-8',
    ) as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def processar_limpeza_negativas(df_base):
    df = df_base.copy()
    df = df.rename(columns=renomear_colunas)

    colunas_criadas = []
    for coluna in colunas_obrigatorias_limpeza:
        if coluna not in df.columns:
            df[coluna] = None
            colunas_criadas.append(coluna)

    resumo = {
        'execucao': 'exec_01_limpeza_negativas',
        'total_linhas_entrada': int(len(df)),
        'total_linhas_saida': int(len(df)),
        'total_colunas_saida': int(len(df.columns)),
        'colunas_criadas': colunas_criadas,
        'colunas_obrigatorias': colunas_obrigatorias_limpeza,
    }

    return df, resumo


def salvar_resumos_limpeza_negativas(
    resumo,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)

    resumo_saida = {
        **resumo,
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else '',
        'arquivo_saida': str(arquivo_saida_resumo) if arquivo_saida_resumo else '',
    }

    with open(pasta_destino / 'exec_01_limpeza_resumo.json', 'w', encoding='utf-8') as arquivo:
        json.dump(resumo_saida, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 01 - LIMPEZA NEGATIVAS',
        '',
        f"Arquivo de entrada: {resumo_saida['arquivo_entrada']}",
        f"Arquivo de saida: {resumo_saida['arquivo_saida']}",
        '',
        f"Total de linhas na entrada: {resumo_saida['total_linhas_entrada']}",
        f"Total de linhas na saida: {resumo_saida['total_linhas_saida']}",
        f"Total de colunas na saida: {resumo_saida['total_colunas_saida']}",
        '',
        'COLUNAS OBRIGATORIAS:',
    ]

    for coluna in resumo_saida['colunas_obrigatorias']:
        linhas_txt.append(f'- {coluna}')

    linhas_txt.append('')
    linhas_txt.append('COLUNAS CRIADAS:')

    if resumo_saida['colunas_criadas']:
        for coluna in resumo_saida['colunas_criadas']:
            linhas_txt.append(f'- {coluna}')
    else:
        linhas_txt.append('- Nenhuma coluna criada')

    with open(pasta_destino / 'exec_01_limpeza_resumo.txt', 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def processar_contratacao_negativas(df, caminho_insumos):
    df_saida, resumo, artefatos = processar_contratacao(df, caminho_insumos)
    resumo = {**resumo, 'execucao': 'exec_02_contratacao_negativas'}
    return df_saida, resumo, artefatos


def salvar_resumos_contratacao_negativas(*args, **kwargs):
    return salvar_resumos_contratacao(*args, **kwargs)


def processar_classificacao_negativas(df, caminho_regras_classificacao):
    df_saida, resumo, artefatos = processar_classificacao(df, caminho_regras_classificacao)
    resumo = {**resumo, 'execucao': 'exec_03_classificacao_negativas'}
    return df_saida, resumo, artefatos


def salvar_resumos_classificacao_negativas(
    resumo,
    artefatos,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
):
    return salvar_resumos_classificacao(
        resumo,
        artefatos,
        pasta_destino,
        arquivo_entrada_resumo=arquivo_entrada_resumo,
        arquivo_saida_resumo=arquivo_saida_resumo,
        prefixo='exec_03_classificacao',
    )


def processar_local_editado_negativas(df, caminho_insumos):
    df_saida, resumo, artefatos = processar_local_editado(df, caminho_insumos)
    resumo = {**resumo, 'execucao': 'exec_04_local_editado_negativas'}
    return df_saida, resumo, artefatos


def salvar_resumos_local_editado_negativas(
    resumo,
    artefatos,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
):
    return salvar_resumos_local_editado(
        resumo,
        artefatos,
        pasta_destino,
        arquivo_entrada_resumo=arquivo_entrada_resumo,
        arquivo_saida_resumo=arquivo_saida_resumo,
        prefixo='exec_04_local_editado',
    )
