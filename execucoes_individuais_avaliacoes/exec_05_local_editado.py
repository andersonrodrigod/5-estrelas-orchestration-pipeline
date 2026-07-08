# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.caminhos import resolver_caminho_onedrive_comercial
from funcoes_auxiliares.normalizacao_local import normalizar_local_comparacao
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/04_base_com_classificacao.csv')
arquivo_insumos_local = Path('utils/insumos/insumos 5 estrelas.xlsx')
arquivo_insumos_onedrive = Path('5 Estrelas/INSUMOS/insumos 5 estrelas.xlsx')
arquivo_saida = Path('data_exec_indiv/avaliacoes/05_base_com_local_editado.csv')

pasta_resumo = Path('auditoria') / 'saida_resumo_avaliacoes' / 'exec_05_local_editado'
arquivo_resumo_json = pasta_resumo / 'exec_05_local_editado_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_05_local_editado_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_05_local_editado_resumo.csv'
arquivo_atualizados_csv = pasta_resumo / 'exec_05_local_editado_atualizados.csv'
arquivo_nao_encontrados_csv = pasta_resumo / 'exec_05_local_editado_nao_encontrados.csv'


def obter_arquivo_insumos():
    return resolver_caminho_onedrive_comercial(
        arquivo_insumos_local,
        arquivo_insumos_onedrive,
    )


def normalizar_texto(serie):
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

            if pd.isna(valor):
                item[coluna] = None
            else:
                item[coluna] = str(valor)

        registros.append(item)

    return registros


def processar_local_editado(df_base, caminho_insumos):
    df = df_base.copy()
    df_insumos = pd.read_excel(caminho_insumos, sheet_name='contratacao')

    df['LOCAL'] = normalizar_texto(df['LOCAL'])
    df['LOCAL EDITADO'] = normalizar_texto(df['LOCAL EDITADO'])
    df['LOCAL_COMPARACAO'] = normalizar_local_comparacao(df['LOCAL'])

    df_insumos['Local'] = normalizar_texto(df_insumos['Local'])
    df_insumos['Local editado'] = normalizar_texto(df_insumos['Local editado'])
    df_insumos['LOCAL_COMPARACAO'] = normalizar_local_comparacao(df_insumos['Local'])

    df_insumos = df_insumos.dropna(subset=['LOCAL_COMPARACAO'])
    df_insumos = df_insumos.drop_duplicates(subset=['LOCAL_COMPARACAO'], keep='first')

    mapa_local_editado = df_insumos.set_index('LOCAL_COMPARACAO')['Local editado']

    local_editado_antes = df['LOCAL EDITADO'].copy()
    local_editado_novo = df['LOCAL_COMPARACAO'].map(mapa_local_editado)

    tipo_numerico = pd.to_numeric(df['TIPO'], errors='coerce')
    local_vazio_ou_hifen = df['LOCAL_COMPARACAO'].fillna('').isin(['', '-'])
    tipo_15_local_vazio = tipo_numerico.eq(15) & local_vazio_ou_hifen
    local_editado_novo.loc[tipo_15_local_vazio] = 'AMBULÂNCIA'

    encontrados = local_editado_novo.notna() & (local_editado_novo != '')
    nao_encontrados = ~encontrados
    nao_alterados = (
        encontrados & (local_editado_antes.fillna('') == local_editado_novo.fillna(''))
    )
    atualizados = encontrados & ~nao_alterados

    df.loc[atualizados, 'LOCAL EDITADO'] = local_editado_novo.loc[atualizados]
    df = df.drop(columns=['LOCAL_COMPARACAO'])

    resumo_atualizados = (
        df.loc[atualizados, ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO']]
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'LOCAL'], ascending=[False, True])
    )

    resumo_nao_alterados = (
        df.loc[nao_alterados, ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO']]
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'LOCAL'], ascending=[False, True])
    )

    resumo_nao_encontrados = (
        df.loc[nao_encontrados, ['LOCAL']]
        .fillna('VAZIO')
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'LOCAL'], ascending=[False, True])
    )

    resumo = {
        'execucao': 'exec_05_local_editado',
        'arquivo_insumos': str(caminho_insumos),
        'total_linhas_entrada': int(len(df)),
        'total_atualizados': int(atualizados.sum()),
        'total_nao_alterados': int(nao_alterados.sum()),
        'total_nao_encontrados': int(nao_encontrados.sum()),
        'valores_atualizados': transformar_em_lista_registros(
            resumo_atualizados,
            ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO', 'QUANTIDADE']
        ),
        'valores_nao_alterados': transformar_em_lista_registros(
            resumo_nao_alterados,
            ['LOCAL', 'LOCAL EDITADO', 'CONTRATACAO', 'QUANTIDADE']
        ),
        'valores_nao_encontrados': transformar_em_lista_registros(
            resumo_nao_encontrados,
            ['LOCAL', 'QUANTIDADE']
        )
    }
    artefatos = {
        'resumo_atualizados': resumo_atualizados,
        'resumo_nao_encontrados': resumo_nao_encontrados,
    }

    return df, resumo, artefatos


def salvar_resumos_local_editado(
    resumo,
    artefatos,
    pasta_destino,
    arquivo_entrada_resumo=None,
    arquivo_saida_resumo=None,
    prefixo='exec_05_local_editado',
):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)

    destino_resumo_json = pasta_destino / f'{prefixo}_resumo.json'
    destino_resumo_txt = pasta_destino / f'{prefixo}_resumo.txt'
    destino_resumo_csv = pasta_destino / f'{prefixo}_resumo.csv'
    destino_atualizados_csv = pasta_destino / f'{prefixo}_atualizados.csv'
    destino_nao_encontrados_csv = (
        pasta_destino / f'{prefixo}_nao_encontrados.csv'
    )

    resumo_saida = dict(resumo)
    resumo_saida.update({
        'arquivo_entrada': str(arquivo_entrada_resumo) if arquivo_entrada_resumo else '',
        'arquivo_saida': str(arquivo_saida_resumo) if arquivo_saida_resumo else '',
    })

    with open(destino_resumo_json, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo_saida, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 05 - LOCAL EDITADO',
        '',
        f"Arquivo de entrada: {resumo_saida['arquivo_entrada']}",
        f"Arquivo de insumos: {resumo_saida['arquivo_insumos']}",
        f"Arquivo de saida: {resumo_saida['arquivo_saida']}",
        '',
        f"Total de linhas na entrada: {resumo_saida['total_linhas_entrada']}",
        f"Registros atualizados: {resumo_saida['total_atualizados']}",
        f"Registros nao alterados: {resumo_saida['total_nao_alterados']}",
        f"Registros nao encontrados: {resumo_saida['total_nao_encontrados']}",
        '',
        'PRINCIPAIS LOCAIS NAO ENCONTRADOS:'
    ]

    if resumo_saida['valores_nao_encontrados']:
        for item in resumo_saida['valores_nao_encontrados'][:50]:
            linhas_txt.append(f"- {item['LOCAL']}: {item['QUANTIDADE']}")
    else:
        linhas_txt.append('- Nenhum local sem correspondencia')

    with open(destino_resumo_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))

    df_resumo_csv = pd.DataFrame([
        {
            'EXECUCAO': resumo_saida['execucao'],
            'ARQUIVO_ENTRADA': resumo_saida['arquivo_entrada'],
            'ARQUIVO_INSUMOS': resumo_saida['arquivo_insumos'],
            'ARQUIVO_SAIDA': resumo_saida['arquivo_saida'],
            'TOTAL_LINHAS_ENTRADA': resumo_saida['total_linhas_entrada'],
            'TOTAL_ATUALIZADOS': resumo_saida['total_atualizados'],
            'TOTAL_NAO_ALTERADOS': resumo_saida['total_nao_alterados'],
            'TOTAL_NAO_ENCONTRADOS': resumo_saida['total_nao_encontrados']
        }
    ])
    salvar_csv_padronizado(df_resumo_csv, destino_resumo_csv)
    salvar_csv_padronizado(artefatos['resumo_atualizados'], destino_atualizados_csv)
    salvar_csv_padronizado(
        artefatos['resumo_nao_encontrados'],
        destino_nao_encontrados_csv,
    )


def executar(salvar_base=True):
    print('Iniciando execucao 05 - local editado...')
    print(f'Lendo arquivo da execucao 04: {arquivo_entrada}')
    caminho_insumos = obter_arquivo_insumos()
    print(f'Lendo arquivo de insumos: {caminho_insumos}')

    df_entrada = ler_csv_padronizado(arquivo_entrada)
    df_saida, resumo, artefatos = processar_local_editado(df_entrada, caminho_insumos)

    print(f"Total de linhas recebidas: {resumo['total_linhas_entrada']}")
    print(f"Total atualizadas: {resumo['total_atualizados']}")
    print(f"Total nao alteradas: {resumo['total_nao_alterados']}")
    print(f"Total nao encontradas: {resumo['total_nao_encontrados']}")

    if salvar_base:
        print(f'Gravando arquivo da execucao 05: {arquivo_saida}')
        arquivo_saida.parent.mkdir(exist_ok=True)
        salvar_csv_padronizado(df_saida, arquivo_saida)

    salvar_resumos_local_editado(
        resumo,
        artefatos,
        pasta_resumo,
        arquivo_entrada_resumo=arquivo_entrada,
        arquivo_saida_resumo=arquivo_saida,
    )

    print('Execucao 05 finalizada.')
    return df_saida, resumo, artefatos


if __name__ == '__main__':
    executar()

