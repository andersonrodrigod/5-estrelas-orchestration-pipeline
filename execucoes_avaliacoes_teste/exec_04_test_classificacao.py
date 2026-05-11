# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/03_base_com_nota.csv')
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/04_base_com_classificacao.csv')
ARQUIVO_GRUPOS = Path('data/grupos_classificacao.json')
ARQUIVO_NOMES = Path('data/nomes_classificacao.json')
PASTA_RESUMO = Path('saida_resumo_avaliacoes') / 'exec_04_classificacao'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_04_classificacao_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_04_classificacao_resumo.txt'
ARQUIVO_AUDITORIA = PASTA_RESUMO / 'exec_04_classificacao_auditoria.csv'
ARQUIVO_SOBRESCRITAS = PASTA_RESUMO / 'exec_04_classificacao_sobrescritas.csv'
ARQUIVO_NAO_CLASSIFICADOS = (
    PASTA_RESUMO / 'exec_04_classificacao_nao_classificados_detalhado.csv'
)

COLUNA_CLASSIFICACAO = 'CLASSIFICACAO'
COLUNA_QUANTIDADE = 'QUANTIDADE'
COLUNAS_AUDITORIA = [
    'ORDEM_REGRA',
    'CHAVE_GRUPO',
    'CLASSIFICACAO',
    'NOME_LISTA',
    'TOTAL_ATINGIDAS',
    'TOTAL_CLASSIFICADAS_VAZIAS',
    'TOTAL_JA_CLASSIFICADAS',
    'TOTAL_SOBRESCRITAS',
]
LIMITE_EXEMPLOS = 20


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def carregar_json(caminho):
    with open(caminho, 'r', encoding='utf-8-sig') as arquivo:
        return json.load(arquivo)


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def normalizar_texto(serie):
    return serie.astype('string').fillna('').str.strip()


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_SAIDA,
        ARQUIVO_GRUPOS,
        ARQUIVO_NOMES,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
        ARQUIVO_AUDITORIA,
        ARQUIVO_SOBRESCRITAS,
        ARQUIVO_NAO_CLASSIFICADOS,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def obter_nome_classificacao(chave_grupo, mapa_nomes):
    return mapa_nomes.get(chave_grupo, chave_grupo.replace('_', ' '))


def obter_classificacoes_esperadas(grupos, nomes_classificacao):
    return {
        obter_nome_classificacao(grupo['grupo_classificacao'], nomes_classificacao)
        for grupo in grupos
    }


def validar_coluna_classificacao(df_saida):
    erros = []

    if COLUNA_CLASSIFICACAO not in df_saida.columns:
        registrar_erro(erros, f"Coluna obrigatoria ausente: {COLUNA_CLASSIFICACAO}")
        return erros

    classificacoes = normalizar_texto(df_saida[COLUNA_CLASSIFICACAO])
    vazias = classificacoes == ''

    for linha in df_saida.index[vazias][:LIMITE_EXEMPLOS] + 2:
        registrar_erro(erros, f'Linha {int(linha)} sem CLASSIFICACAO.')

    if int(vazias.sum()) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {int(vazias.sum()) - LIMITE_EXEMPLOS} linha(s) sem CLASSIFICACAO."
        )

    return erros


def validar_classificacoes_esperadas(df_saida, classificacoes_esperadas):
    erros = []
    classificacoes = normalizar_texto(df_saida[COLUNA_CLASSIFICACAO])
    preenchidas = classificacoes != ''
    invalidas = preenchidas & ~classificacoes.isin(classificacoes_esperadas)

    for linha, classificacao in zip(
        df_saida.index[invalidas][:LIMITE_EXEMPLOS] + 2,
        classificacoes[invalidas].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com CLASSIFICACAO fora do mapa de nomes: "
            f"'{classificacao}'."
        )

    if int(invalidas.sum()) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {int(invalidas.sum()) - LIMITE_EXEMPLOS} "
            "linha(s) com CLASSIFICACAO fora do mapa de nomes."
        )

    return erros


def calcular_totais_resumo(df_saida, df_sobrescritas, grupos):
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
        'total_regras': int(len(grupos)),
    }


def validar_resumo_json(df_saida, df_sobrescritas, grupos, resumo):
    erros = []
    totais_esperados = calcular_totais_resumo(df_saida, df_sobrescritas, grupos)

    for campo, valor_esperado in totais_esperados.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != valor_esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': "
                f"esperado {valor_esperado}, encontrado {valor_resumo}."
            )

    return erros


def validar_auditoria(df_auditoria, grupos, nomes_classificacao):
    erros = []
    colunas_faltando = [
        coluna for coluna in COLUNAS_AUDITORIA if coluna not in df_auditoria.columns
    ]
    for coluna in colunas_faltando:
        registrar_erro(erros, f"Coluna ausente na auditoria: {coluna}")

    if colunas_faltando:
        return erros

    if len(df_auditoria) != len(grupos):
        registrar_erro(
            erros,
            f'Auditoria deveria ter {len(grupos)} regra(s), '
            f'mas tem {len(df_auditoria)}.'
        )

    ordens = pd.to_numeric(df_auditoria['ORDEM_REGRA'], errors='coerce')
    ordens_esperadas = list(range(1, len(grupos) + 1))
    if ordens.dropna().astype(int).tolist() != ordens_esperadas:
        registrar_erro(
            erros,
            'Auditoria nao possui ORDEM_REGRA sequencial conforme grupos_classificacao.json.'
        )

    for indice, grupo in enumerate(grupos):
        if indice >= len(df_auditoria):
            break

        linha = df_auditoria.iloc[indice]
        classificacao_esperada = obter_nome_classificacao(
            grupo['grupo_classificacao'],
            nomes_classificacao,
        )

        if linha['CHAVE_GRUPO'] != grupo['grupo_classificacao']:
            registrar_erro(
                erros,
                f"Regra {indice + 1} com CHAVE_GRUPO divergente: "
                f"esperado '{grupo['grupo_classificacao']}', "
                f"encontrado '{linha['CHAVE_GRUPO']}'."
            )

        if linha['CLASSIFICACAO'] != classificacao_esperada:
            registrar_erro(
                erros,
                f"Regra {indice + 1} com CLASSIFICACAO divergente: "
                f"esperado '{classificacao_esperada}', "
                f"encontrado '{linha['CLASSIFICACAO']}'."
            )

    return erros[:LIMITE_EXEMPLOS]


def validar_arquivo_nao_classificados(df_nao_classificados, resumo):
    erros = []
    total_esperado = int(resumo.get('total_nao_classificadas', 0))

    if len(df_nao_classificados) != total_esperado:
        registrar_erro(
            erros,
            'Arquivo de nao classificados divergente: '
            f'esperado {total_esperado} linha(s), encontrado {len(df_nao_classificados)}.'
        )

    return erros


def validar_arquivo_sobrescritas(df_sobrescritas, resumo):
    erros = []
    total_esperado = int(resumo.get('total_sobrescritas', 0))

    total_arquivo = 0
    if COLUNA_QUANTIDADE in df_sobrescritas.columns and not df_sobrescritas.empty:
        total_arquivo = int(
            pd.to_numeric(df_sobrescritas[COLUNA_QUANTIDADE], errors='coerce')
            .fillna(0)
            .sum()
        )

    if total_arquivo != total_esperado:
        registrar_erro(
            erros,
            'Arquivo de sobrescritas divergente: '
            f'esperado {total_esperado}, encontrado {total_arquivo}.'
        )

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_04_classificacao')
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
        print('TESTE FALHOU - exec_04_classificacao')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_04_classificacao.py')
        return 1

    grupos = carregar_json(ARQUIVO_GRUPOS)
    nomes_classificacao = carregar_json(ARQUIVO_NOMES)
    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)
    df_auditoria = pd.read_csv(ARQUIVO_AUDITORIA, low_memory=False)
    df_sobrescritas = pd.read_csv(ARQUIVO_SOBRESCRITAS, low_memory=False)
    df_nao_classificados = pd.read_csv(ARQUIVO_NAO_CLASSIFICADOS, low_memory=False)

    with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
        resumo = json.load(arquivo)

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} '
            f'e saida tem {len(df_saida)}.'
        )

    erros.extend(validar_coluna_classificacao(df_saida))

    if COLUNA_CLASSIFICACAO in df_saida.columns:
        classificacoes_esperadas = obter_classificacoes_esperadas(
            grupos,
            nomes_classificacao,
        )
        erros.extend(
            validar_classificacoes_esperadas(df_saida, classificacoes_esperadas)
        )
        erros.extend(validar_resumo_json(df_saida, df_sobrescritas, grupos, resumo))
        erros.extend(validar_tipos(df_saida))

    erros.extend(validar_auditoria(df_auditoria, grupos, nomes_classificacao))
    erros.extend(validar_arquivo_nao_classificados(df_nao_classificados, resumo))
    erros.extend(validar_arquivo_sobrescritas(df_sobrescritas, resumo))

    if erros:
        imprimir_erros(erros)
        return 1

    totais = calcular_totais_resumo(df_saida, df_sobrescritas, grupos)
    print('TESTE OK - exec_04_classificacao')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f"Total de linhas analisadas: {totais['total_linhas_entrada']}")
    print(f"Classificadas: {totais['total_classificadas']}")
    print(f"Nao classificadas: {totais['total_nao_classificadas']}")
    print(f"Sobrescritas: {totais['total_sobrescritas']}")
    print(f"Total de regras auditadas: {totais['total_regras']}")
    print('Quantidade de linhas preservada.')
    print('CLASSIFICACAO preenchida e dentro do mapa de nomes.')
    print('Resumo JSON bate com o CSV.')
    print('Auditoria bate com grupos_classificacao.json.')
    print('Arquivos de sobrescritas e nao classificados batem com o resumo.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
