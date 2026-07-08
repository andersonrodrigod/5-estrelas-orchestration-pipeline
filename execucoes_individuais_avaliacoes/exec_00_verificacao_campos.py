# -*- coding: utf-8 -*-
import json
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data/premissa_5_estrelas_maio.csv')
pasta_resumo = Path('auditoria') / 'saida_resumo_avaliacoes' / 'exec_00_verificacao_campos'

colunas_verificacao = ['LOCAL', 'ESPECIALIDADE', 'UF']
coluna_tipo = 'TIPO'
regex_letra = re.compile(r'[^\W\d_]')
regex_apenas_especial = re.compile(r'^[\W_]+$')


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


def criar_mascara_vazio(df_base, coluna):
    if coluna not in df_base.columns:
        return pd.Series(False, index=df_base.index)

    texto = preparar_texto(df_base[coluna])
    return texto.isna() | (texto == '')


def processar_verificacao_campos(df):
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

        mascara_problema = mascara_vazio | mascara_sem_letras
        df_valores = (
            pd.DataFrame({
                'VALOR': texto[mascara_problema].fillna('VAZIO'),
                'TIPO_PROBLEMA': [
                    'vazio' if vazio else (
                        'apenas_hifen' if hifen else (
                            'apenas_caracteres_especiais' if especial else 'sem_letras'
                        )
                    )
                    for vazio, hifen, especial in zip(
                        mascara_vazio[mascara_problema],
                        mascara_apenas_hifen[mascara_problema],
                        mascara_apenas_especial[mascara_problema],
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
    mascara_uf_vazio = criar_mascara_vazio(df, 'UF')
    mascara_tipo_vazio = criar_mascara_vazio(df, coluna_tipo)

    resumo = {
        'execucao': 'exec_00_verificacao_campos',
        'total_linhas_entrada': int(len(df)),
        'total_local_especialidade_invalidos': int(mascara_local_especialidade_invalidos.sum()),
        'total_uf_vazio': int(mascara_uf_vazio.sum()),
        'total_tipo_vazio': int(mascara_tipo_vazio.sum()),
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
        'resumo_csv': df_resumo,
        'valores_invalidos': df_valores_invalidos,
        'local_especialidade_invalidos': df.loc[mascara_local_especialidade_invalidos].copy(),
        'uf_vazio': df.loc[mascara_uf_vazio].copy(),
        'tipo_vazio': df.loc[mascara_tipo_vazio].copy(),
    }

    return df, resumo, artefatos


def salvar_resumos_verificacao_campos(resumo, artefatos, pasta_destino, arquivo_entrada_resumo=None):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)
    arquivo_resumo_csv = pasta_destino / 'exec_00_verificacao_campos_resumo.csv'
    arquivo_valores_invalidos_csv = pasta_destino / 'exec_00_verificacao_campos_valores_invalidos.csv'
    arquivo_local_especialidade_invalidos_csv = (
        pasta_destino / 'exec_00_verificacao_campos_local_especialidade_invalidos.csv'
    )
    arquivo_uf_vazio_csv = pasta_destino / 'exec_00_verificacao_campos_uf_vazio.csv'
    arquivo_tipo_vazio_csv = pasta_destino / 'exec_00_verificacao_campos_tipo_vazio.csv'

    salvar_csv_padronizado(artefatos['resumo_csv'], arquivo_resumo_csv)
    salvar_csv_padronizado(artefatos['valores_invalidos'], arquivo_valores_invalidos_csv)
    salvar_csv_padronizado(
        artefatos['local_especialidade_invalidos'],
        arquivo_local_especialidade_invalidos_csv,
    )
    salvar_csv_padronizado(artefatos['uf_vazio'], arquivo_uf_vazio_csv)
    salvar_csv_padronizado(artefatos['tipo_vazio'], arquivo_tipo_vazio_csv)

    resumo = {
        **resumo,
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else None,
        'arquivo_resumo_csv': str(arquivo_resumo_csv),
        'arquivo_valores_invalidos_csv': str(arquivo_valores_invalidos_csv),
        'arquivo_local_especialidade_invalidos_csv': str(
            arquivo_local_especialidade_invalidos_csv
        ),
        'arquivo_uf_vazio_csv': str(arquivo_uf_vazio_csv),
        'arquivo_tipo_vazio_csv': str(arquivo_tipo_vazio_csv),
    }

    with open(
        pasta_destino / 'exec_00_verificacao_campos_resumo.json',
        'w',
        encoding='utf-8',
    ) as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 00 - VERIFICACAO DE CAMPOS',
        '',
    ]

    if resumo['arquivo_entrada']:
        linhas_txt.append(f"Arquivo de entrada: {resumo['arquivo_entrada']}")

    linhas_txt.extend([
        f"Arquivo de resumo CSV: {resumo['arquivo_resumo_csv']}",
        f"Valores invalidos agrupados: {resumo['arquivo_valores_invalidos_csv']}",
        (
            'Linhas com LOCAL e ESPECIALIDADE invalidos: '
            f"{resumo['arquivo_local_especialidade_invalidos_csv']}"
        ),
        f"Linhas com UF vazio: {resumo['arquivo_uf_vazio_csv']}",
        f"Linhas com TIPO vazio: {resumo['arquivo_tipo_vazio_csv']}",
        '',
        f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
        (
            'Total LOCAL e ESPECIALIDADE invalidos: '
            f"{resumo['total_local_especialidade_invalidos']}"
        ),
        f"Total UF vazio: {resumo['total_uf_vazio']}",
        f"Total TIPO vazio: {resumo['total_tipo_vazio']}",
    ])

    with open(
        pasta_destino / 'exec_00_verificacao_campos_resumo.txt',
        'w',
        encoding='utf-8',
    ) as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def executar():
    print('Iniciando execucao 00 - verificacao de campos...')
    print(f'Lendo arquivo da execucao 01: {arquivo_entrada}')

    df = ler_csv_padronizado(arquivo_entrada)
    df, resumo, artefatos = processar_verificacao_campos(df)
    salvar_resumos_verificacao_campos(resumo, artefatos, pasta_resumo, arquivo_entrada)

    print(f"Total de linhas na entrada: {resumo['total_linhas_entrada']}")
    print(
        'Total LOCAL e ESPECIALIDADE invalidos: '
        f"{resumo['total_local_especialidade_invalidos']}"
    )
    print(f"Total UF vazio: {resumo['total_uf_vazio']}")
    print(f"Total TIPO vazio: {resumo['total_tipo_vazio']}")
    print('Execucao 00 finalizada.')
    return df, resumo


if __name__ == '__main__':
    executar()
