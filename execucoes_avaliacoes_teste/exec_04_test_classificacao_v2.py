# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/03_base_com_nota.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/04_base_com_classificacao_v2.csv')
ARQUIVO_REGRAS = Path('utils/insumos/regra_classificacao.xlsx')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_04_classificacao_v2'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_04_classificacao_v2_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_04_classificacao_v2_resumo.txt'
ARQUIVO_RESUMO_CSV = PASTA_RESUMO / 'exec_04_classificacao_v2_resumo.csv'
ARQUIVO_AUDITORIA = PASTA_RESUMO / 'exec_04_classificacao_v2_auditoria.csv'
ARQUIVO_SOBRESCRITAS = PASTA_RESUMO / 'exec_04_classificacao_v2_sobrescritas.csv'
ARQUIVO_NAO_CLASSIFICADOS = (
    PASTA_RESUMO / 'exec_04_classificacao_v2_nao_classificados_detalhado.csv'
)

COLUNA_CLASSIFICACAO = 'CLASSIFICACAO'
COLUNA_QUANTIDADE = 'QUANTIDADE'
LIMITE_EXEMPLOS = 20


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def normalizar_texto(serie):
    return serie.astype('string').fillna('').str.strip()


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def carregar_regras_ativas():
    df_regras = pd.read_excel(ARQUIVO_REGRAS, sheet_name='regras_classificacao')
    ativo = df_regras['STATUS_ATIVO'].astype('string').str.strip().str.lower()
    return df_regras[ativo.isin(['sim', 's', 'true', '1', 'yes'])].copy()


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_SAIDA,
        ARQUIVO_REGRAS,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
        ARQUIVO_RESUMO_CSV,
        ARQUIVO_AUDITORIA,
        ARQUIVO_SOBRESCRITAS,
        ARQUIVO_NAO_CLASSIFICADOS,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def calcular_totais_resumo(df_saida, df_sobrescritas, df_regras):
    classificacoes = normalizar_texto(df_saida[COLUNA_CLASSIFICACAO])
    nao_classificadas = classificacoes == ''

    total_sobrescritas = 0
    if COLUNA_QUANTIDADE in df_sobrescritas.columns and not df_sobrescritas.empty:
        total_sobrescritas = int(
            pd.to_numeric(df_sobrescritas[COLUNA_QUANTIDADE], errors='coerce')
            .fillna(0)
            .sum()
        )

    return {
        'total_linhas_entrada': int(len(df_saida)),
        'total_classificadas': int((~nao_classificadas).sum()),
        'total_nao_classificadas': int(nao_classificadas.sum()),
        'total_sobrescritas': total_sobrescritas,
        'total_regras_ativas': int(len(df_regras)),
    }


def validar_resumo_json(df_saida, df_sobrescritas, df_regras, resumo):
    erros = []
    totais_esperados = calcular_totais_resumo(df_saida, df_sobrescritas, df_regras)

    for campo, valor_esperado in totais_esperados.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != valor_esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': "
                f"esperado {valor_esperado}, encontrado {valor_resumo}.",
            )

    return erros


def validar_classificacoes_esperadas(df_saida, df_regras):
    erros = []
    classificacoes = normalizar_texto(df_saida[COLUNA_CLASSIFICACAO])
    esperadas = set(normalizar_texto(df_regras['CLASSIFICACAO']))
    preenchidas = classificacoes != ''
    invalidas = preenchidas & ~classificacoes.isin(esperadas)

    for linha, classificacao in zip(
        df_saida.index[invalidas][:LIMITE_EXEMPLOS] + 2,
        classificacoes[invalidas].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com CLASSIFICACAO fora da planilha: '{classificacao}'.",
        )

    return erros


def validar_auditoria(df_auditoria, df_regras):
    erros = []
    colunas = ['ORDEM_REGRA', 'CHAVE_GRUPO', 'CLASSIFICACAO', 'NOME_LISTA']
    for coluna in colunas:
        if coluna not in df_auditoria.columns:
            registrar_erro(erros, f'Coluna ausente na auditoria: {coluna}')

    if erros:
        return erros

    if len(df_auditoria) != len(df_regras):
        registrar_erro(
            erros,
            f'Auditoria deveria ter {len(df_regras)} regra(s), mas tem {len(df_auditoria)}.',
        )

    ordens_auditoria = pd.to_numeric(df_auditoria['ORDEM_REGRA'], errors='coerce')
    ordens_regras = pd.to_numeric(df_regras['ORDEM'], errors='coerce')
    if ordens_auditoria.dropna().astype(int).tolist() != ordens_regras.dropna().astype(int).tolist():
        registrar_erro(erros, 'Auditoria nao segue a ORDEM da planilha.')

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_04_classificacao_v2')
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
        print('TESTE FALHOU - exec_04_classificacao_v2')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_04_classificacao_v2.py')
        return 1

    df_regras = carregar_regras_ativas()
    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)
    df_auditoria = pd.read_csv(ARQUIVO_AUDITORIA, low_memory=False)
    df_sobrescritas = pd.read_csv(ARQUIVO_SOBRESCRITAS, low_memory=False)

    with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
        resumo = json.load(arquivo)

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} '
            f'e saida tem {len(df_saida)}.',
        )

    if COLUNA_CLASSIFICACAO not in df_saida.columns:
        registrar_erro(erros, f'Coluna obrigatoria ausente: {COLUNA_CLASSIFICACAO}')
    else:
        erros.extend(validar_classificacoes_esperadas(df_saida, df_regras))
        erros.extend(validar_resumo_json(df_saida, df_sobrescritas, df_regras, resumo))
        erros.extend(validar_tipos(df_saida))

    erros.extend(validar_auditoria(df_auditoria, df_regras))

    if erros:
        imprimir_erros(erros)
        return 1

    totais = calcular_totais_resumo(df_saida, df_sobrescritas, df_regras)
    print('TESTE OK - exec_04_classificacao_v2')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f"Total de linhas analisadas: {totais['total_linhas_entrada']}")
    print(f"Classificadas: {totais['total_classificadas']}")
    print(f"Nao classificadas: {totais['total_nao_classificadas']}")
    print(f"Sobrescritas: {totais['total_sobrescritas']}")
    print(f"Total de regras auditadas: {totais['total_regras_ativas']}")
    print('Quantidade de linhas preservada.')
    print('CLASSIFICACAO preenchida e dentro da planilha.')
    print('Resumo JSON bate com o CSV.')
    print('Auditoria segue a ordem da planilha.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
