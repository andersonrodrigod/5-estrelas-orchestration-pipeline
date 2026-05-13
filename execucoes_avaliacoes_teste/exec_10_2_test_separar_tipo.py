# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys
import unicodedata

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/09_base_com_status_unidade.csv')
ARQUIVO_NOMES = Path('data/nomes_classificacao.json')
ARQUIVO_SAIDA_1_A_3 = Path('data_exec_indiv/avaliacoes/10_2_base_tipo_1_a_3.csv')
ARQUIVO_SAIDA_4_A_7 = Path('data_exec_indiv/avaliacoes/10_2_base_tipo_4_a_7.csv')
ARQUIVO_SAIDA_8_OU_MAIS = Path('data_exec_indiv/avaliacoes/10_2_base_tipo_8_ou_mais.csv')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_10_2_separar_tipo'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_10_2_separar_tipo_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_10_2_separar_tipo_resumo.txt'
ARQUIVO_RESUMO_CSV = PASTA_RESUMO / 'exec_10_2_separar_tipo_resumo.csv'

COLUNA_TIPO = 'TIPO'
COLUNA_CLASSIFICACAO = 'CLASSIFICACAO'
LIMITE_EXEMPLOS = 20


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def remover_decimal_zero_identificador(serie):
    texto = serie.astype('string').str.strip()
    return texto.str.replace(r'\.0$', '', regex=True)


def normalizar_texto(valor):
    if pd.isna(valor):
        return ''
    texto = str(valor).strip().upper()
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(c for c in texto if not unicodedata.combining(c))
    return ' '.join(texto.split())


def criar_mapa_nomes_envio(nomes_classificacao):
    mapa = {}
    for chave, nome in nomes_classificacao.items():
        mapa[normalizar_texto(chave)] = nome
        mapa[normalizar_texto(nome)] = nome
    return mapa


def aplicar_nomes_envio(df, mapa_nomes_envio):
    classificacao_normalizada = df[COLUNA_CLASSIFICACAO].apply(normalizar_texto)
    nomes_envio = classificacao_normalizada.map(mapa_nomes_envio)
    return nomes_envio.fillna(df[COLUNA_CLASSIFICACAO])


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_NOMES,
        ARQUIVO_SAIDA_1_A_3,
        ARQUIVO_SAIDA_4_A_7,
        ARQUIVO_SAIDA_8_OU_MAIS,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
        ARQUIVO_RESUMO_CSV,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def validar_faixas_tipo(df_1_a_3, df_4_a_7, df_8_ou_mais):
    erros = []

    tipo_1 = pd.to_numeric(df_1_a_3[COLUNA_TIPO], errors='coerce')
    fora_1 = ~tipo_1.between(1, 3, inclusive='both')
    for linha, valor in zip(df_1_a_3.index[fora_1][:LIMITE_EXEMPLOS] + 2, tipo_1[fora_1].head(LIMITE_EXEMPLOS)):
        registrar_erro(erros, f"Linha {int(linha)} no arquivo 1_a_3 com TIPO invalido: {valor}.")

    tipo_4 = pd.to_numeric(df_4_a_7[COLUNA_TIPO], errors='coerce')
    fora_4 = ~tipo_4.between(4, 7, inclusive='both')
    for linha, valor in zip(df_4_a_7.index[fora_4][:LIMITE_EXEMPLOS] + 2, tipo_4[fora_4].head(LIMITE_EXEMPLOS)):
        registrar_erro(erros, f"Linha {int(linha)} no arquivo 4_a_7 com TIPO invalido: {valor}.")

    tipo_8 = pd.to_numeric(df_8_ou_mais[COLUNA_TIPO], errors='coerce')
    fora_8 = ~(tipo_8 >= 8)
    for linha, valor in zip(df_8_ou_mais.index[fora_8][:LIMITE_EXEMPLOS] + 2, tipo_8[fora_8].head(LIMITE_EXEMPLOS)):
        registrar_erro(erros, f"Linha {int(linha)} no arquivo 8_ou_mais com TIPO invalido: {valor}.")

    return erros


def validar_integridade_linhas(df_entrada, df_1_a_3, df_4_a_7, df_8_ou_mais):
    erros = []
    total_saida = int(len(df_1_a_3) + len(df_4_a_7) + len(df_8_ou_mais))
    tipo_entrada = pd.to_numeric(df_entrada[COLUNA_TIPO], errors='coerce')
    total_fora_recorte = int(
        (~(tipo_entrada.between(1, 3, inclusive='both') | tipo_entrada.between(4, 7, inclusive='both') | (tipo_entrada >= 8))).sum()
    )
    total_esperado = int(len(df_entrada) - total_fora_recorte)

    if total_saida != total_esperado:
        registrar_erro(
            erros,
            f'Total de linhas separadas divergente: esperado {total_esperado}, encontrado {total_saida}.',
        )

    hash_1 = set(pd.util.hash_pandas_object(df_1_a_3.astype('string').fillna(''), index=False).tolist())
    hash_4 = set(pd.util.hash_pandas_object(df_4_a_7.astype('string').fillna(''), index=False).tolist())
    hash_8 = set(pd.util.hash_pandas_object(df_8_ou_mais.astype('string').fillna(''), index=False).tolist())

    inter_1_4 = len(hash_1.intersection(hash_4))
    inter_1_8 = len(hash_1.intersection(hash_8))
    inter_4_8 = len(hash_4.intersection(hash_8))
    if inter_1_4 or inter_1_8 or inter_4_8:
        registrar_erro(
            erros,
            f'Sobreposicao entre arquivos: 1_3x4_7={inter_1_4}, 1_3x8={inter_1_8}, 4_7x8={inter_4_8}.',
        )

    return erros, total_fora_recorte


def validar_transformacoes(df_entrada, df_1_a_3, df_4_a_7, df_8_ou_mais):
    erros = []
    with open(ARQUIVO_NOMES, 'r', encoding='utf-8-sig') as arquivo:
        nomes_classificacao = json.load(arquivo)
    mapa = criar_mapa_nomes_envio(nomes_classificacao)

    df_ref = df_entrada.copy()
    df_ref[COLUNA_TIPO] = pd.to_numeric(df_ref[COLUNA_TIPO], errors='coerce')
    for coluna in ['NUM_BENEFICIARIO', 'TELEFONE']:
        if coluna in df_ref.columns:
            df_ref[coluna] = remover_decimal_zero_identificador(df_ref[coluna])
    df_ref[COLUNA_CLASSIFICACAO] = aplicar_nomes_envio(df_ref, mapa)

    ref_1 = df_ref[df_ref[COLUNA_TIPO].between(1, 3, inclusive='both')].copy()
    ref_4 = df_ref[df_ref[COLUNA_TIPO].between(4, 7, inclusive='both')].copy()
    ref_8 = df_ref[df_ref[COLUNA_TIPO] >= 8].copy()

    if not pd.util.hash_pandas_object(ref_1.astype('string').fillna(''), index=False).equals(
        pd.util.hash_pandas_object(df_1_a_3.astype('string').fillna(''), index=False)
    ):
        registrar_erro(erros, 'Arquivo 10_2_base_tipo_1_a_3.csv difere da transformacao esperada.')

    if not pd.util.hash_pandas_object(ref_4.astype('string').fillna(''), index=False).equals(
        pd.util.hash_pandas_object(df_4_a_7.astype('string').fillna(''), index=False)
    ):
        registrar_erro(erros, 'Arquivo 10_2_base_tipo_4_a_7.csv difere da transformacao esperada.')

    if not pd.util.hash_pandas_object(ref_8.astype('string').fillna(''), index=False).equals(
        pd.util.hash_pandas_object(df_8_ou_mais.astype('string').fillna(''), index=False)
    ):
        registrar_erro(erros, 'Arquivo 10_2_base_tipo_8_ou_mais.csv difere da transformacao esperada.')

    return erros


def validar_resumos(df_entrada, df_1_a_3, df_4_a_7, df_8_ou_mais, total_fora_recorte):
    erros = []
    with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
        resumo = json.load(arquivo)

    totais_esperados = {
        'total_linhas_entrada': int(len(df_entrada)),
        'total_tipo_1_a_3': int(len(df_1_a_3)),
        'total_tipo_4_a_7': int(len(df_4_a_7)),
        'total_tipo_8_ou_mais': int(len(df_8_ou_mais)),
        'total_tipo_fora_recorte': int(total_fora_recorte),
    }
    for campo, esperado in totais_esperados.items():
        valor = resumo.get(campo)
        if valor != esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': esperado {esperado}, encontrado {valor}.",
            )

    df_resumo_csv = pd.read_csv(ARQUIVO_RESUMO_CSV, low_memory=False)
    if df_resumo_csv.empty:
        registrar_erro(erros, 'Resumo CSV vazio.')
        return erros

    linha = df_resumo_csv.iloc[0]
    mapa = {
        'TOTAL_LINHAS_ENTRADA': 'total_linhas_entrada',
        'TOTAL_TIPO_1_A_3': 'total_tipo_1_a_3',
        'TOTAL_TIPO_4_A_7': 'total_tipo_4_a_7',
        'TOTAL_TIPO_8_OU_MAIS': 'total_tipo_8_ou_mais',
        'TOTAL_TIPO_FORA_RECORTE': 'total_tipo_fora_recorte',
    }
    for coluna_csv, campo_json in mapa.items():
        if coluna_csv not in df_resumo_csv.columns:
            registrar_erro(erros, f'Coluna ausente no resumo CSV: {coluna_csv}')
            continue
        valor_csv = int(pd.to_numeric(linha[coluna_csv], errors='coerce'))
        valor_json = int(resumo.get(campo_json, -1))
        if valor_csv != valor_json:
            registrar_erro(
                erros,
                f"Resumo CSV divergente em '{coluna_csv}': esperado {valor_json}, encontrado {valor_csv}.",
            )

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_10_2_separar_tipo')
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
        print('TESTE FALHOU - exec_10_2_separar_tipo')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_10_2_separar_tipo.py')
        return 1

    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_1_a_3 = carregar_csv(ARQUIVO_SAIDA_1_A_3)
    df_4_a_7 = carregar_csv(ARQUIVO_SAIDA_4_A_7)
    df_8_ou_mais = carregar_csv(ARQUIVO_SAIDA_8_OU_MAIS)

    erros.extend(validar_faixas_tipo(df_1_a_3, df_4_a_7, df_8_ou_mais))
    erros_integridade, total_fora_recorte = validar_integridade_linhas(df_entrada, df_1_a_3, df_4_a_7, df_8_ou_mais)
    erros.extend(erros_integridade)
    erros.extend(validar_transformacoes(df_entrada, df_1_a_3, df_4_a_7, df_8_ou_mais))
    erros.extend(validar_resumos(df_entrada, df_1_a_3, df_4_a_7, df_8_ou_mais, total_fora_recorte))
    erros.extend(validar_tipos(df_1_a_3))
    erros.extend(validar_tipos(df_4_a_7))
    erros.extend(validar_tipos(df_8_ou_mais))

    if erros:
        imprimir_erros(erros)
        return 1

    print('TESTE OK - exec_10_2_separar_tipo')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo TIPO 1 a 3: {ARQUIVO_SAIDA_1_A_3}')
    print(f'Arquivo TIPO 4 a 7: {ARQUIVO_SAIDA_4_A_7}')
    print(f'Arquivo TIPO 8 ou mais: {ARQUIVO_SAIDA_8_OU_MAIS}')
    print(f'Total de linhas entrada: {len(df_entrada)}')
    print(f'Total linhas TIPO 1 a 3: {len(df_1_a_3)}')
    print(f'Total linhas TIPO 4 a 7: {len(df_4_a_7)}')
    print(f'Total linhas TIPO 8 ou mais: {len(df_8_ou_mais)}')
    print('Separacao por faixa de TIPO consistente e sem sobreposicao.')
    print('Transformacoes (classificacao e identificadores) conferem com esperado.')
    print('Resumo JSON/CSV coerente com os arquivos de saida.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
