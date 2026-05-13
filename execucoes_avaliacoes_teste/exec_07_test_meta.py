# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys
import unicodedata

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/06_base_com_operadora.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/07_base_com_meta.csv')
ARQUIVO_INSUMOS = Path('utils/insumos/insumos 5 estrelas.xlsx')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_07_meta'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_07_meta_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_07_meta_resumo.txt'

COLUNAS_OBRIGATORIAS = ['CLASSIFICACAO', 'META']
LIMITE_EXEMPLOS = 20


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def remover_acentos(valor):
    if valor is None:
        return ''
    valor = str(valor)
    valor = unicodedata.normalize('NFKD', valor)
    return ''.join(c for c in valor if not unicodedata.combining(c))


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.fillna('')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    texto = texto.str.strip()
    return texto.apply(remover_acentos)


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_SAIDA,
        ARQUIVO_INSUMOS,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def validar_colunas(df_saida):
    return [coluna for coluna in COLUNAS_OBRIGATORIAS if coluna not in df_saida.columns]


def construir_mapa_meta():
    df_insumos = pd.read_excel(ARQUIVO_INSUMOS, sheet_name='insumos')
    if 'grupo' not in df_insumos.columns or 'meta' not in df_insumos.columns:
        return None

    df_insumos['grupo'] = df_insumos['grupo'].astype('string').str.strip()
    df_insumos['CHAVE_META'] = normalizar_texto(df_insumos['grupo']).str.upper()
    df_insumos = df_insumos.dropna(subset=['CHAVE_META'])
    df_insumos = df_insumos[df_insumos['CHAVE_META'] != '']
    df_insumos = df_insumos.drop_duplicates(subset=['CHAVE_META'], keep='first')
    return df_insumos.set_index('CHAVE_META')['meta']


def validar_mapeamento_meta(df_saida, mapa_meta):
    erros = []
    chave_meta = normalizar_texto(df_saida['CLASSIFICACAO']).str.upper()
    meta_esperada = chave_meta.map(mapa_meta)
    meta_encontrada = df_saida['META']

    esperada_str = meta_esperada.astype('string').fillna('').str.strip()
    encontrada_str = meta_encontrada.astype('string').fillna('').str.strip()
    divergencias = esperada_str != encontrada_str

    for linha, classificacao, esp, enc in zip(
        df_saida.index[divergencias][:LIMITE_EXEMPLOS] + 2,
        df_saida.loc[divergencias, 'CLASSIFICACAO'].head(LIMITE_EXEMPLOS),
        esperada_str[divergencias].head(LIMITE_EXEMPLOS),
        encontrada_str[divergencias].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com META divergente para CLASSIFICACAO '{classificacao}': "
            f"esperado '{esp}', encontrado '{enc}'.",
        )

    return erros, meta_esperada


def validar_resumo_json(df_saida, resumo, meta_esperada):
    erros = []
    encontrados = meta_esperada.notna() & (meta_esperada.astype('string').str.strip() != '')
    totais_esperados = {
        'total_linhas_entrada': int(len(df_saida)),
        'total_encontrados': int(encontrados.sum()),
        'total_nao_encontrados': int((~encontrados).sum()),
    }

    for campo, esperado in totais_esperados.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': esperado {esperado}, encontrado {valor_resumo}.",
            )

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_07_meta')
    print('')
    print(f'Total de problemas encontrados: {len(erros)}')
    print('')

    for erro in erros[:LIMITE_EXEMPLOS]:
        print(f'- {erro}')

    if len(erros) > LIMITE_EXEMPLOS:
        print(f'- ... mais {len(erros) - LIMITE_EXEMPLOS} problema(s)')


def executar():
    erros = []

    arquivos_faltando = validar_arquivos_obrigatorios()
    if arquivos_faltando:
        print('TESTE FALHOU - exec_07_meta')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_07_meta.py')
        return 1

    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)

    colunas_faltando = validar_colunas(df_saida)
    for coluna in colunas_faltando:
        registrar_erro(erros, f'Coluna obrigatoria ausente: {coluna}')

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} e saida tem {len(df_saida)}.',
        )

    mapa_meta = construir_mapa_meta()
    if mapa_meta is None:
        registrar_erro(erros, "Aba 'insumos' sem colunas obrigatorias 'grupo' e 'meta'.")
    elif not colunas_faltando:
        erros_mapa, meta_esperada = validar_mapeamento_meta(df_saida, mapa_meta)
        erros.extend(erros_mapa)

        with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
            resumo = json.load(arquivo)
        erros.extend(validar_resumo_json(df_saida, resumo, meta_esperada))
        erros.extend(validar_tipos(df_saida))

    if erros:
        imprimir_erros(erros)
        return 1

    print('TESTE OK - exec_07_meta')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f'Total de linhas analisadas: {len(df_saida)}')
    print('Quantidade de linhas preservada.')
    print('META bate com mapeamento da aba insumos (grupo -> meta).')
    print('Resumo JSON coerente com o CSV.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
