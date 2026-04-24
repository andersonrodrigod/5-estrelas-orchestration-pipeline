# -*- coding: utf-8 -*-
from pathlib import Path

import pandas as pd


COLUNAS_TEXTO = {
    'CDATENDIMENTO',
    'NUM_BENEFICIARIO',
    'CDEMPRESA',
    'PROFISSIONAL',
    'PROCEDIMENTO',
    'ESPECIALIDADE',
    'DATA_ATENDIMENTO',
    'LOCAL',
    'UF',
    'CLASSIFICACAO_TIPO_DESC',
    'MOTIVADOR1',
    'DESC_MOTIVADOR1',
    'MOTIVADOR2',
    'DESC_MOTIVADOR2',
    'MOTIVADOR3',
    'DESC_MOTIVADOR3',
    'MOTIVADOR4',
    'DESC_MOTIVADOR4',
    'MOTIVADOR5',
    'DESC_MOTIVADOR5',
    'CDUSUARIO',
    'STATUS',
    'NOME',
    'TELEFONE',
    'EMAIL',
    'DT_RESPOSTA',
    'CONTRATACAO',
    'CLASSIFICACAO',
    'OPERADORA',
    'LOCAL EDITADO',
    'STATUS UNIDADE',
}

COLUNAS_FLOAT = {
    'NOTA5',
    'NOTA GERAL',
    'META',
    'RESULTADO DA UNIDADE',
}

COLUNAS_INT = {
    'NOTA1',
    'NOTA2',
    'NOTA3',
    'NOTA4',
    'MES',
    'DIA',
    'ANO',
    'TIPO',
    'Quantidade notas validas',
}

COLUNAS_REMOVER_ZERO_DECIMAL = {
    'NUM_BENEFICIARIO',
    'TELEFONE',
}

DTYPES_LEITURA = {
    'NUM_BENEFICIARIO': 'string',
    'TELEFONE': 'string',
}

TIPOS_ESPERADOS = {
    **{coluna: 'object' for coluna in COLUNAS_TEXTO},
    **{coluna: 'float64' for coluna in COLUNAS_FLOAT},
    **{coluna: 'int64' for coluna in COLUNAS_INT},
}


def _normalizar_texto(serie, remover_zero_decimal=False):
    texto = serie.astype('string')

    if remover_zero_decimal:
        texto = texto.str.replace(r'(?<=\d)\.0$', '', regex=True)

    return pd.Series(
        [None if pd.isna(valor) else str(valor) for valor in texto],
        index=serie.index,
        dtype='object',
    )


def padronizar_dataframe(df):
    df = df.copy()

    for coluna in COLUNAS_TEXTO:
        if coluna in df.columns:
            df[coluna] = _normalizar_texto(
                df[coluna],
                remover_zero_decimal=coluna in COLUNAS_REMOVER_ZERO_DECIMAL,
            )

    for coluna in COLUNAS_FLOAT:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors='coerce').astype('float64')

    for coluna in COLUNAS_INT:
        if coluna in df.columns:
            serie_numerica = pd.to_numeric(df[coluna], errors='coerce')

            if serie_numerica.isna().any():
                raise ValueError(
                    f"A coluna '{coluna}' deveria ser int64, mas possui valores vazios ou invalidos."
                )

            df[coluna] = serie_numerica.astype('int64')

    return df


def ler_csv_padronizado(caminho):
    df = pd.read_csv(caminho, low_memory=False, dtype=DTYPES_LEITURA)
    return padronizar_dataframe(df)


def salvar_csv_padronizado(df, caminho, **kwargs):
    caminho = Path(caminho)
    df_padronizado = padronizar_dataframe(df)
    df_padronizado.to_csv(caminho, index=False, encoding='utf-8-sig', **kwargs)


def obter_tipos_presentes(df):
    tipos = {}

    for coluna, tipo_esperado in TIPOS_ESPERADOS.items():
        if coluna in df.columns:
            tipos[coluna] = tipo_esperado

    return tipos


def validar_tipos_dataframe(df):
    divergencias = []

    for coluna, tipo_esperado in obter_tipos_presentes(df).items():
        tipo_encontrado = str(df[coluna].dtype)

        if tipo_encontrado != tipo_esperado:
            divergencias.append({
                'coluna': coluna,
                'tipo_esperado': tipo_esperado,
                'tipo_encontrado': tipo_encontrado,
            })

    return divergencias
