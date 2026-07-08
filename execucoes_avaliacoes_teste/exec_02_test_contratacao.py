# -*- coding: utf-8 -*-
from pathlib import Path
import json
import sys

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.caminhos import resolver_caminho_onedrive_comercial
from funcoes_auxiliares.normalizacao_local import normalizar_local_comparacao
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, validar_tipos_dataframe


ARQUIVO_ENTRADA = Path('data_exec_indiv/avaliacoes/01_base_limpa.csv')
ARQUIVO_INSUMOS = resolver_caminho_onedrive_comercial(
    Path('utils/insumos/insumos 5 estrelas.xlsx'),
    Path('5 Estrelas/INSUMOS/insumos 5 estrelas.xlsx'),
)
ARQUIVO_SAIDA = Path('data_exec_indiv/avaliacoes/02_base_com_contratacao.csv')
PASTA_RESUMO = Path('auditoria') / 'saida_resumo_avaliacoes' / 'exec_02_contratacao'
ARQUIVO_RESUMO_JSON = PASTA_RESUMO / 'exec_02_contratacao_resumo.json'
ARQUIVO_RESUMO_TXT = PASTA_RESUMO / 'exec_02_contratacao_resumo.txt'
ARQUIVO_LOCAIS_SEM_CONTRATACAO = PASTA_RESUMO / 'exec_02_locais_sem_contratacao.csv'
ARQUIVO_LINHAS_SEM_CONTRATACAO = PASTA_RESUMO / 'exec_02_linhas_sem_contratacao.csv'
ARQUIVO_LOCAIS_SEM_UF = PASTA_RESUMO / 'exec_02_locais_sem_uf.csv'

COLUNAS_OBRIGATORIAS = ['LOCAL', 'CONTRATACAO']
COLUNAS_OBRIGATORIAS_INSUMOS = ['Local', 'contratacao']
VALORES_VALIDOS_CONTRATACAO = {'rede propria', 'rede credenciada'}
LIMITE_EXEMPLOS = 20


def carregar_csv(caminho):
    return ler_csv_padronizado(caminho)


def registrar_erro(erros, mensagem):
    erros.append(mensagem)


def validar_arquivos_obrigatorios():
    arquivos = [
        ARQUIVO_ENTRADA,
        ARQUIVO_INSUMOS,
        ARQUIVO_SAIDA,
        ARQUIVO_RESUMO_JSON,
        ARQUIVO_RESUMO_TXT,
        ARQUIVO_LOCAIS_SEM_CONTRATACAO,
        ARQUIVO_LINHAS_SEM_CONTRATACAO,
        ARQUIVO_LOCAIS_SEM_UF,
    ]
    return [arquivo for arquivo in arquivos if not arquivo.exists()]


def validar_colunas(df):
    return [coluna for coluna in COLUNAS_OBRIGATORIAS if coluna not in df.columns]


def normalizar_texto(serie):
    return serie.astype('string').fillna('').str.strip().str.lower()


def validar_valores_contratacao(df):
    valores = normalizar_texto(df['CONTRATACAO'])
    preenchidos = valores != ''
    invalidos = preenchidos & ~valores.isin(VALORES_VALIDOS_CONTRATACAO)

    exemplos = []
    for linha, valor in zip(df.index[invalidos] + 2, valores[invalidos]):
        exemplos.append({
            'linha_csv': int(linha),
            'valor': str(valor),
        })

    return exemplos


def carregar_insumos_contratacao():
    return pd.read_excel(ARQUIVO_INSUMOS, sheet_name='contratacao')


def validar_insumos_contratacao(df_insumos):
    erros = []

    colunas_faltando = [
        coluna for coluna in COLUNAS_OBRIGATORIAS_INSUMOS
        if coluna not in df_insumos.columns
    ]
    for coluna in colunas_faltando:
        registrar_erro(erros, f"Coluna obrigatoria ausente no insumo: {coluna}")

    if colunas_faltando:
        return erros, None

    df_mapa = df_insumos.copy()
    df_mapa['LOCAL_COMPARACAO'] = normalizar_local_comparacao(df_mapa['Local'])
    df_mapa['CONTRATACAO_COMPARACAO'] = normalizar_texto(df_mapa['contratacao'])

    locais_vazios = df_mapa['LOCAL_COMPARACAO'] == ''
    for indice, contratacao in zip(
        df_mapa.index[locais_vazios] + 2,
        df_mapa.loc[locais_vazios, 'CONTRATACAO_COMPARACAO'],
    ):
        registrar_erro(
            erros,
            f"Linha {int(indice)} do insumo com Local vazio "
            f"e contratacao '{contratacao}'."
        )

    contratacoes_invalidas = (
        (df_mapa['CONTRATACAO_COMPARACAO'] != '')
        & ~df_mapa['CONTRATACAO_COMPARACAO'].isin(VALORES_VALIDOS_CONTRATACAO)
    )
    for indice, local, contratacao in zip(
        df_mapa.index[contratacoes_invalidas] + 2,
        df_mapa.loc[contratacoes_invalidas, 'Local'],
        df_mapa.loc[contratacoes_invalidas, 'CONTRATACAO_COMPARACAO'],
    ):
        registrar_erro(
            erros,
            f"Linha {int(indice)} do insumo com contratacao invalida "
            f"para Local '{local}': '{contratacao}'."
        )

    preenchidos = (
        (df_mapa['LOCAL_COMPARACAO'] != '')
        & (df_mapa['CONTRATACAO_COMPARACAO'] != '')
    )
    df_validos = df_mapa[preenchidos].copy()
    conflitos = (
        df_validos
        .groupby('LOCAL_COMPARACAO')['CONTRATACAO_COMPARACAO']
        .nunique()
    )
    locais_conflitantes = conflitos[conflitos > 1].index

    for local_comparacao in locais_conflitantes[:LIMITE_EXEMPLOS]:
        contratacoes = sorted(
            df_validos.loc[
                df_validos['LOCAL_COMPARACAO'] == local_comparacao,
                'CONTRATACAO_COMPARACAO',
            ].unique()
        )
        registrar_erro(
            erros,
            f"Insumo com contratacoes conflitantes para Local "
            f"'{local_comparacao}': {', '.join(contratacoes)}."
        )

    if len(locais_conflitantes) > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {len(locais_conflitantes) - LIMITE_EXEMPLOS} "
            "Local(is) com contratacoes conflitantes no insumo."
        )

    if erros:
        return erros, None

    df_validos = df_validos.drop_duplicates(
        subset=['LOCAL_COMPARACAO', 'CONTRATACAO_COMPARACAO']
    )
    df_validos = df_validos.drop_duplicates(
        subset=['LOCAL_COMPARACAO'],
        keep='first',
    )
    mapa = df_validos.set_index('LOCAL_COMPARACAO')['CONTRATACAO_COMPARACAO']
    return erros, mapa


def criar_mapa_uf_insumos(df_insumos):
    if 'UF' not in df_insumos.columns:
        return pd.Series(dtype='string')

    df_mapa = df_insumos.copy()
    df_mapa['LOCAL_COMPARACAO'] = normalizar_local_comparacao(df_mapa['Local'])
    df_mapa['UF_COMPARACAO'] = df_mapa['UF'].astype('string').str.strip().str.upper()
    df_mapa = df_mapa[
        df_mapa['LOCAL_COMPARACAO'].notna()
        & df_mapa['UF_COMPARACAO'].notna()
        & ~df_mapa['UF_COMPARACAO'].fillna('').isin(['', '-'])
    ].drop_duplicates(subset=['LOCAL_COMPARACAO'], keep='first')
    return df_mapa.set_index('LOCAL_COMPARACAO')['UF_COMPARACAO']


def validar_contratacao_por_insumo(df_saida, mapa_contratacao):
    erros = []
    locais = normalizar_local_comparacao(df_saida['LOCAL'])
    esperado = locais.map(mapa_contratacao)
    encontrado = normalizar_texto(df_saida['CONTRATACAO'])

    sem_insumo = esperado.isna()
    total_sem_insumo = int(sem_insumo.sum())
    for linha, local in zip(
        df_saida.index[sem_insumo][:LIMITE_EXEMPLOS] + 2,
        df_saida.loc[sem_insumo, 'LOCAL'].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com LOCAL sem correspondencia no insumo: "
            f"'{local}'."
        )

    if total_sem_insumo > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {total_sem_insumo - LIMITE_EXEMPLOS} linha(s) "
            "com LOCAL sem correspondencia no insumo."
        )

    divergencias = esperado.notna() & (encontrado != esperado)
    total_divergencias = int(divergencias.sum())
    for linha, local, valor_encontrado, valor_esperado in zip(
        df_saida.index[divergencias][:LIMITE_EXEMPLOS] + 2,
        df_saida.loc[divergencias, 'LOCAL'].head(LIMITE_EXEMPLOS),
        encontrado[divergencias].head(LIMITE_EXEMPLOS),
        esperado[divergencias].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} com CONTRATACAO divergente para LOCAL "
            f"'{local}': esperado '{valor_esperado}', "
            f"encontrado '{valor_encontrado}'."
        )

    if total_divergencias > LIMITE_EXEMPLOS:
        registrar_erro(
            erros,
            f"... mais {total_divergencias - LIMITE_EXEMPLOS} linha(s) "
            "com CONTRATACAO divergente do insumo."
        )

    return erros


def validar_uf_por_insumo(df_entrada, df_saida, mapa_uf):
    erros = []
    locais = normalizar_local_comparacao(df_entrada['LOCAL'])
    uf_entrada = df_entrada['UF'].astype('string').str.strip()
    uf_saida = df_saida['UF'].astype('string').str.strip().str.upper()
    uf_insumo = locais.map(mapa_uf)

    entrada_sem_uf = uf_entrada.isna() | uf_entrada.fillna('').isin(['', '-'])
    deveria_preencher = entrada_sem_uf & uf_insumo.notna()
    divergencias_preenchimento = deveria_preencher & (uf_saida != uf_insumo)

    for linha, local, valor_saida, valor_esperado in zip(
        df_saida.index[divergencias_preenchimento][:LIMITE_EXEMPLOS] + 2,
        df_saida.loc[divergencias_preenchimento, 'LOCAL'].head(LIMITE_EXEMPLOS),
        uf_saida[divergencias_preenchimento].head(LIMITE_EXEMPLOS),
        uf_insumo[divergencias_preenchimento].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} deveria receber UF do insumo para LOCAL "
            f"'{local}': esperado '{valor_esperado}', encontrado '{valor_saida}'."
        )

    uf_entrada_comparacao = uf_entrada.str.upper()
    saida_alterou_uf_valida = ~entrada_sem_uf & (uf_saida != uf_entrada_comparacao)
    for linha, local, valor_original, valor_saida in zip(
        df_saida.index[saida_alterou_uf_valida][:LIMITE_EXEMPLOS] + 2,
        df_saida.loc[saida_alterou_uf_valida, 'LOCAL'].head(LIMITE_EXEMPLOS),
        uf_entrada_comparacao[saida_alterou_uf_valida].head(LIMITE_EXEMPLOS),
        uf_saida[saida_alterou_uf_valida].head(LIMITE_EXEMPLOS),
    ):
        registrar_erro(
            erros,
            f"Linha {int(linha)} alterou UF que ja existia para LOCAL "
            f"'{local}': original '{valor_original}', encontrado '{valor_saida}'."
        )

    return erros, int(deveria_preencher.sum())


def contar_contratacoes(df):
    valores = normalizar_texto(df['CONTRATACAO'])
    return {
        'total_rede_propria': int((valores == 'rede propria').sum()),
        'total_rede_credenciada': int((valores == 'rede credenciada').sum()),
        'total_sem_contratacao': int((valores == '').sum()),
    }


def contar_sem_uf(df):
    valores = df['UF'].astype('string').str.strip()
    return int((valores.isna() | valores.fillna('').isin(['', '-'])).sum())


def validar_arquivo_locais_sem_uf(df_saida):
    erros = []
    df_locais_sem_uf = carregar_csv(ARQUIVO_LOCAIS_SEM_UF)
    colunas_esperadas = ['LOCAL', 'CONTRATACAO', 'QUANTIDADE']
    colunas_faltando = [
        coluna for coluna in colunas_esperadas
        if coluna not in df_locais_sem_uf.columns
    ]

    for coluna in colunas_faltando:
        registrar_erro(
            erros,
            f"Coluna obrigatoria ausente em {ARQUIVO_LOCAIS_SEM_UF}: {coluna}"
        )

    if colunas_faltando:
        return erros

    total_arquivo = int(df_locais_sem_uf['QUANTIDADE'].sum())
    total_esperado = contar_sem_uf(df_saida)
    if total_arquivo != total_esperado:
        registrar_erro(
            erros,
            f"Arquivo de locais sem UF soma {total_arquivo}, "
            f"mas o CSV final tem {total_esperado} linha(s) sem UF."
        )

    return erros


def validar_resumo_json(df_saida, resumo, total_uf_preenchida_esperado):
    erros = []
    contagens = contar_contratacoes(df_saida)

    campos_esperados = {
        'total_linhas_entrada': int(len(df_saida)),
        'total_uf_preenchida_por_insumo': total_uf_preenchida_esperado,
        'total_sem_uf': contar_sem_uf(df_saida),
        **contagens,
    }

    for campo, valor_esperado in campos_esperados.items():
        valor_resumo = resumo.get(campo)
        if valor_resumo != valor_esperado:
            registrar_erro(
                erros,
                f"Resumo JSON divergente em '{campo}': "
                f"esperado {valor_esperado}, encontrado {valor_resumo}."
            )

    return erros


def validar_tipos(df_saida):
    return [
        f"Coluna {item['coluna']} com tipo {item['tipo_encontrado']} "
        f"(esperado {item['tipo_esperado']})."
        for item in validar_tipos_dataframe(df_saida)
    ]


def imprimir_erros(erros):
    print('TESTE FALHOU - exec_02_contratacao')
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
        print('TESTE FALHOU - exec_02_contratacao')
        print('')
        print('Arquivos obrigatorios nao encontrados:')
        for arquivo in arquivos_faltando:
            print(f'- {arquivo}')
        print('')
        print('Rode primeiro: python execucoes_individuais_avaliacoes\\exec_02_contratacao.py')
        return 1

    df_entrada = carregar_csv(ARQUIVO_ENTRADA)
    df_saida = carregar_csv(ARQUIVO_SAIDA)
    df_insumos = carregar_insumos_contratacao()

    colunas_faltando = validar_colunas(df_saida)
    for coluna in colunas_faltando:
        registrar_erro(erros, f"Coluna obrigatoria ausente: {coluna}")

    if len(df_entrada) != len(df_saida):
        registrar_erro(
            erros,
            f'Quantidade de linhas mudou: entrada tem {len(df_entrada)} '
            f'e saida tem {len(df_saida)}.'
        )

    if not colunas_faltando:
        erros_insumos, mapa_contratacao = validar_insumos_contratacao(df_insumos)
        erros.extend(erros_insumos)

        valores_invalidos = validar_valores_contratacao(df_saida)
        for item in valores_invalidos:
            registrar_erro(
                erros,
                f"Linha {item['linha_csv']} com CONTRATACAO invalida: "
                f"'{item['valor']}'"
            )

        if mapa_contratacao is not None:
            erros.extend(validar_contratacao_por_insumo(df_saida, mapa_contratacao))

        total_uf_preenchida_esperado = 0
        if 'UF' in df_entrada.columns and 'UF' in df_saida.columns:
            mapa_uf = criar_mapa_uf_insumos(df_insumos)
            erros_uf, total_uf_preenchida_esperado = validar_uf_por_insumo(
                df_entrada,
                df_saida,
                mapa_uf,
            )
            erros.extend(erros_uf)

        with open(ARQUIVO_RESUMO_JSON, 'r', encoding='utf-8') as arquivo:
            resumo = json.load(arquivo)
        erros.extend(validar_resumo_json(df_saida, resumo, total_uf_preenchida_esperado))
        erros.extend(validar_arquivo_locais_sem_uf(df_saida))
        erros.extend(validar_tipos(df_saida))

    if erros:
        imprimir_erros(erros)
        return 1

    contagens = contar_contratacoes(df_saida)
    print('TESTE OK - exec_02_contratacao')
    print(f'Arquivo de entrada: {ARQUIVO_ENTRADA}')
    print(f'Arquivo testado: {ARQUIVO_SAIDA}')
    print(f'Total de linhas analisadas: {len(df_saida)}')
    print(f"Rede propria: {contagens['total_rede_propria']}")
    print(f"Rede credenciada: {contagens['total_rede_credenciada']}")
    print(f"Sem contratacao: {contagens['total_sem_contratacao']}")
    print('Quantidade de linhas preservada.')
    print('Valores de CONTRATACAO validos.')
    print('CONTRATACAO bate com o insumo por LOCAL.')
    print('UF vazia ou "-" foi preenchida pelo insumo sem sobrescrever UF existente.')
    print('Resumo de locais sem UF foi gerado.')
    print('Resumo JSON bate com o CSV.')
    print('Schema das colunas esta padronizado.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
