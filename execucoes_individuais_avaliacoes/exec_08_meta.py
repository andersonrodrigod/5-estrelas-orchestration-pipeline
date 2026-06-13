# -*- coding: utf-8 -*-
import json
import sys
import unicodedata
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.caminhos import resolver_caminho_onedrive_comercial
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/07_base_com_operadora.csv')
arquivo_insumos_local = Path('utils/insumos/insumos 5 estrelas.xlsx')
arquivo_insumos_onedrive = Path('5 Estrelas/INSUMOS/insumos 5 estrelas.xlsx')
arquivo_saida = Path('data_exec_indiv/avaliacoes/08_base_com_meta.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_08_meta'
arquivo_resumo_json = pasta_resumo / 'exec_08_meta_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_08_meta_resumo.txt'
arquivo_nao_encontrados_detalhado_csv = pasta_resumo / 'exec_08_meta_nao_encontrados_detalhado.csv'


def obter_arquivo_insumos():
    return resolver_caminho_onedrive_comercial(
        arquivo_insumos_local,
        arquivo_insumos_onedrive,
    )


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.fillna('')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    texto = texto.str.strip()
    return texto.apply(remover_acentos)


def remover_acentos(valor):
    if valor is None:
        return ''

    valor = str(valor)
    valor = unicodedata.normalize('NFKD', valor)
    valor = ''.join(caractere for caractere in valor if not unicodedata.combining(caractere))
    return valor


def transformar_em_lista_registros(df_base, colunas):
    if df_base.empty:
        return []

    registros = []

    for _, linha in df_base.iterrows():
        item = {}

        for coluna in colunas:
            valor = linha[coluna]

            if pd.isna(valor):
                item[coluna] = None
            else:
                item[coluna] = str(valor)

        registros.append(item)

    return registros


def processar_meta(df_base, caminho_insumos):
    df = df_base.copy()
    df_insumos = pd.read_excel(caminho_insumos, sheet_name='insumos')

    df['CLASSIFICACAO'] = df['CLASSIFICACAO'].astype('string').str.strip()
    df['CHAVE_META'] = normalizar_texto(df['CLASSIFICACAO']).str.upper()

    df_insumos['grupo'] = df_insumos['grupo'].astype('string').str.strip()
    df_insumos['CHAVE_META'] = normalizar_texto(df_insumos['grupo']).str.upper()
    df_insumos = df_insumos.dropna(subset=['CHAVE_META'])
    df_insumos = df_insumos[df_insumos['CHAVE_META'] != '']
    df_insumos = df_insumos.drop_duplicates(subset=['CHAVE_META'], keep='first')

    mapa_meta = df_insumos.set_index('CHAVE_META')['meta']
    mapa_grupo_original = df_insumos.set_index('CHAVE_META')['grupo']

    df['META'] = df['CHAVE_META'].map(mapa_meta)
    df['GRUPO_ENCONTRADO'] = df['CHAVE_META'].map(mapa_grupo_original)

    mascara_encontrados = df['GRUPO_ENCONTRADO'].notna() & (df['GRUPO_ENCONTRADO'] != '')
    mascara_nao_encontrados = ~mascara_encontrados

    resumo_grupos_encontrados = (
        df.loc[mascara_encontrados, ['GRUPO_ENCONTRADO']]
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'GRUPO_ENCONTRADO'], ascending=[False, True])
    )
    resumo_grupos_encontrados.columns = ['GRUPO', 'QUANTIDADE']

    resumo_classificacoes_nao_encontradas = (
        df.loc[mascara_nao_encontrados, ['CLASSIFICACAO']]
        .fillna('VAZIO')
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'CLASSIFICACAO'], ascending=[False, True])
    )

    df_saida = df.drop(columns=['CHAVE_META', 'GRUPO_ENCONTRADO'])
    df_nao_encontrados_detalhado = df_saida.loc[mascara_nao_encontrados].copy()

    resumo = {
        'execucao': 'exec_08_meta',
        'arquivo_insumos': str(caminho_insumos),
        'total_linhas_entrada': int(len(df)),
        'total_encontrados': int(mascara_encontrados.sum()),
        'total_nao_encontrados': int(mascara_nao_encontrados.sum()),
        'grupos_encontrados': transformar_em_lista_registros(
            resumo_grupos_encontrados,
            ['GRUPO', 'QUANTIDADE']
        ),
        'classificacoes_nao_encontradas': transformar_em_lista_registros(
            resumo_classificacoes_nao_encontradas,
            ['CLASSIFICACAO', 'QUANTIDADE']
        )
    }
    artefatos = {
        'nao_encontrados_detalhado': df_nao_encontrados_detalhado,
    }

    return df_saida, resumo, artefatos


def salvar_resumos_meta(
    resumo,
    artefatos,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)

    destino_resumo_json = pasta_destino / 'exec_08_meta_resumo.json'
    destino_resumo_txt = pasta_destino / 'exec_08_meta_resumo.txt'
    destino_nao_encontrados_csv = (
        pasta_destino / 'exec_08_meta_nao_encontrados_detalhado.csv'
    )

    resumo_saida = dict(resumo)
    resumo_saida.update({
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else '',
        'arquivo_saida': str(arquivo_saida_resumo) if arquivo_saida_resumo else '',
        'arquivo_nao_encontrados_detalhado': str(destino_nao_encontrados_csv),
    })

    salvar_csv_padronizado(
        artefatos['nao_encontrados_detalhado'],
        destino_nao_encontrados_csv,
    )

    with open(destino_resumo_json, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo_saida, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 08 - META',
        '',
        f"Arquivo de entrada: {resumo_saida['arquivo_entrada']}",
        f"Arquivo de insumos: {resumo_saida['arquivo_insumos']}",
        f"Arquivo de saida: {resumo_saida['arquivo_saida']}",
        f"Nao encontrados detalhado: {resumo_saida['arquivo_nao_encontrados_detalhado']}",
        '',
        f"Total de linhas na entrada: {resumo_saida['total_linhas_entrada']}",
        f"Total encontrados: {resumo_saida['total_encontrados']}",
        f"Total nao encontrados: {resumo_saida['total_nao_encontrados']}",
        '',
        'GRUPOS ENCONTRADOS:'
    ]

    if resumo_saida['grupos_encontrados']:
        for item in resumo_saida['grupos_encontrados']:
            linhas_txt.append(f"- {item['GRUPO']}: {item['QUANTIDADE']}")
    else:
        linhas_txt.append('- Nenhum grupo encontrado')

    linhas_txt.append('')
    linhas_txt.append('CLASSIFICACOES SEM CORRESPONDENCIA:')

    if resumo_saida['classificacoes_nao_encontradas']:
        for item in resumo_saida['classificacoes_nao_encontradas']:
            linhas_txt.append(f"- {item['CLASSIFICACAO']}: {item['QUANTIDADE']}")
    else:
        linhas_txt.append('- Nenhuma classificacao sem correspondencia')

    with open(destino_resumo_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def executar(salvar_base=True):
    print('Iniciando execucao 08 - meta...')
    print(f'Lendo arquivo da execucao 07: {arquivo_entrada}')
    caminho_insumos = obter_arquivo_insumos()
    print(f'Lendo arquivo de insumos: {caminho_insumos}')

    df_entrada = ler_csv_padronizado(arquivo_entrada)
    df_saida, resumo, artefatos = processar_meta(df_entrada, caminho_insumos)

    print(f"Total de linhas recebidas: {resumo['total_linhas_entrada']}")
    print(f"Total de metas encontradas: {resumo['total_encontrados']}")
    print(f"Total sem grupo correspondente: {resumo['total_nao_encontrados']}")

    if salvar_base:
        print(f'Gravando arquivo da execucao 08: {arquivo_saida}')
        arquivo_saida.parent.mkdir(exist_ok=True)
        salvar_csv_padronizado(df_saida, arquivo_saida)

    salvar_resumos_meta(
        resumo,
        artefatos,
        pasta_resumo,
        arquivo_entrada_resumo=arquivo_entrada,
        arquivo_saida_resumo=arquivo_saida,
    )

    print('Execucao 08 finalizada.')
    return df_saida, resumo, artefatos


if __name__ == '__main__':
    executar()

