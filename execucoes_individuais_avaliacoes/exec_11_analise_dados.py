# -*- coding: utf-8 -*-
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/10_base_com_status_unidade.csv')

pasta_saida = Path('saida_resumo_avaliacoes') / 'exec_11_analise_dados'
arquivo_saida_excel = pasta_saida / 'exec_11_analise_dados.xlsx'
arquivo_documentacao_txt = pasta_saida / 'exec_11_analise_dados_explicacao.txt'

coluna_nota = 'NOTA GERAL'
coluna_classificacao = 'CLASSIFICACAO'
coluna_operadora = 'OPERADORA'
coluna_unidade = 'LOCAL EDITADO'
coluna_tipo = 'TIPO'
coluna_especialidade = 'ESPECIALIDADE'
coluna_meta = 'META'
coluna_resultado_unidade = 'RESULTADO DA UNIDADE'
coluna_status_unidade = 'STATUS UNIDADE'

ordem_classificacao = [
    'HAPCLINICA',
    'TELECONSULTA ELETIVA',
    'HOSPITALAR',
    'LABORATÓRIO',
    'TELECONSULTA URGÊNCIA',
    'CRED_TRATAMENTO',
    'VIDA IMAGEM',
    'ODONTOLOGIA',
    'MED PREV',
    'QUALIVIDA',
    'NASCER BEM',
    'CRED_LABORATÓRIO',
    'CRED_ATEND ELETIVO',
    'INTERNAÇÃO',
    'CASE',
    'CRED_ATEND EMERGENCIA',
    'PRODUTO COORDENADO',
    'GESTAR BEM',
    'TEA',
    'TELECONSULTA PGC',
    'CRED_EXAMES',
    'CRED_INTERNACAO',
    'INTERNAÇÃO PGC',
    'TELEMEDICINA',
    'TRANSFUSÃO DE SANGUE',
    'TRANSPLANTE RENAL',
]

operadoras_atendimento_presencial = [
    'HAPVIDA',
    'PROMED',
]

operadoras_hapvida_corporativo = [
    'HAPVIDA',
    'NDI SP E RJ',
    'TELECONSULTA',
    'CLINIPAN',
    'NDI MG',
    'CCG',
    'PROMED',
    'ODONTOLOGIA',
]


def normalizar_texto(serie):
    texto = serie.astype('string').fillna('')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip()


def validar_colunas_obrigatorias(df):
    colunas = [
        coluna_nota,
        coluna_classificacao,
        coluna_operadora,
        coluna_unidade,
        coluna_tipo,
        coluna_especialidade,
        coluna_meta,
        coluna_resultado_unidade,
        coluna_status_unidade,
    ]
    return [coluna for coluna in colunas if coluna not in df.columns]


def criar_analise_classificacao(df):
    resumo = (
        df.groupby(coluna_classificacao, dropna=False)
        .agg(
            QUANTIDADE=(coluna_nota, 'size'),
            MEDIA_NOTA_GERAL=(coluna_nota, 'mean'),
        )
        .reset_index()
    )

    resumo['MEDIA_NOTA_GERAL'] = resumo['MEDIA_NOTA_GERAL'].round(2)
    resumo['ORDEM'] = resumo[coluna_classificacao].map(
        {valor: indice for indice, valor in enumerate(ordem_classificacao, start=1)}
    )
    resumo['FORA_DA_ORDEM_INFORMADA'] = resumo['ORDEM'].isna()
    resumo['ORDEM'] = resumo['ORDEM'].fillna(len(ordem_classificacao) + 1)
    resumo = resumo.sort_values(
        ['ORDEM', coluna_classificacao],
        ascending=[True, True],
    )

    resumo = resumo[[
        coluna_classificacao,
        'QUANTIDADE',
        'MEDIA_NOTA_GERAL',
        'FORA_DA_ORDEM_INFORMADA',
    ]]

    total = pd.DataFrame([{
        coluna_classificacao: 'TOTAL GERAL',
        'QUANTIDADE': int(len(df)),
        'MEDIA_NOTA_GERAL': round(float(df[coluna_nota].mean()), 2),
        'FORA_DA_ORDEM_INFORMADA': False,
    }])

    return pd.concat([resumo, total], ignore_index=True)


def criar_analise_por_coluna(df, coluna):
    resumo = (
        df.groupby(coluna, dropna=False)
        .agg(
            QUANTIDADE=(coluna_nota, 'size'),
            MEDIA_NOTA_GERAL=(coluna_nota, 'mean'),
        )
        .reset_index()
    )
    resumo['MEDIA_NOTA_GERAL'] = resumo['MEDIA_NOTA_GERAL'].round(2)
    return resumo.sort_values(
        ['QUANTIDADE', coluna],
        ascending=[False, True],
    )


def listar_valores_distintos(serie):
    valores = (
        serie.astype('string')
        .fillna('')
        .str.strip()
    )
    valores = sorted(valor for valor in valores.unique() if valor != '')
    return ' | '.join(valores)


def criar_analise_unidade_por_classificacao(df):
    resumo = (
        df.groupby([coluna_classificacao, coluna_unidade], dropna=False)
        .agg(
            QUANTIDADE=(coluna_nota, 'size'),
            MEDIA_NOTA_GERAL=(coluna_nota, 'mean'),
            META=(coluna_meta, 'first'),
            **{
                coluna_resultado_unidade: (coluna_resultado_unidade, 'first'),
                coluna_status_unidade: (coluna_status_unidade, 'first'),
            },
            TIPOS=(coluna_tipo, listar_valores_distintos),
            ESPECIALIDADES=(coluna_especialidade, listar_valores_distintos),
        )
        .reset_index()
    )

    resumo['MEDIA_NOTA_GERAL'] = resumo['MEDIA_NOTA_GERAL'].round(2)
    resumo['META'] = resumo['META'].round(2)
    resumo[coluna_resultado_unidade] = resumo[coluna_resultado_unidade].round(2)
    resumo['ORDEM_CLASSIFICACAO'] = resumo[coluna_classificacao].map(
        {valor: indice for indice, valor in enumerate(ordem_classificacao, start=1)}
    )
    resumo['FORA_DA_ORDEM_INFORMADA'] = resumo['ORDEM_CLASSIFICACAO'].isna()
    resumo['ORDEM_CLASSIFICACAO'] = resumo['ORDEM_CLASSIFICACAO'].fillna(
        len(ordem_classificacao) + 1
    )

    resumo = resumo.sort_values(
        ['ORDEM_CLASSIFICACAO', coluna_classificacao, 'QUANTIDADE', coluna_unidade],
        ascending=[True, True, False, True],
    )

    return resumo[[
        coluna_classificacao,
        coluna_unidade,
        'QUANTIDADE',
        'MEDIA_NOTA_GERAL',
        'META',
        coluna_resultado_unidade,
        coluna_status_unidade,
        'TIPOS',
        'ESPECIALIDADES',
    ]]


def criar_linha_operadora_consolidada(df, nome, operadoras):
    mascara = df[coluna_operadora].isin(operadoras)
    df_recorte = df.loc[mascara]

    return {
        coluna_operadora: nome,
        'QUANTIDADE': int(len(df_recorte)),
        'MEDIA_NOTA_GERAL': round(float(df_recorte[coluna_nota].mean()), 2),
        'TIPO_LINHA': 'CONSOLIDADO',
    }


def criar_analise_operadora(df):
    resumo = criar_analise_por_coluna(df, coluna_operadora)
    resumo['TIPO_LINHA'] = 'OPERADORA'

    consolidados = pd.DataFrame([
        criar_linha_operadora_consolidada(
            df,
            'ATENDIMENTO PRESENCIAL',
            operadoras_atendimento_presencial,
        ),
        criar_linha_operadora_consolidada(
            df,
            'HAPVIDA CORPORATIVO',
            operadoras_hapvida_corporativo,
        ),
    ])

    return pd.concat([resumo, consolidados], ignore_index=True)


def criar_resumo_geral(df):
    return pd.DataFrame([
        {'METRICA': 'TOTAL_LINHAS', 'VALOR': int(len(df))},
        {'METRICA': 'MEDIA_GERAL_NOTA_GERAL', 'VALOR': round(float(df[coluna_nota].mean()), 2)},
        {'METRICA': 'TOTAL_CLASSIFICACOES_DISTINTAS', 'VALOR': int(df[coluna_classificacao].nunique(dropna=False))},
        {'METRICA': 'TOTAL_OPERADORAS_DISTINTAS', 'VALOR': int(df[coluna_operadora].nunique(dropna=False))},
        {'METRICA': 'TOTAL_UNIDADES_DISTINTAS', 'VALOR': int(df[coluna_unidade].nunique(dropna=False))},
    ])


def salvar_documentacao(caminho_documentacao=arquivo_documentacao_txt, caminho_excel=arquivo_saida_excel):
    linhas = [
        'EXECUCAO 11 - ANALISE DE DADOS',
        '',
        'Objetivo:',
        'Gerar uma visao consolidada de quantidade e media da NOTA GERAL a partir da base completa da execucao 10.',
        '',
        'Arquivo de entrada:',
        str(arquivo_entrada),
        '',
        'Arquivo Excel gerado:',
        str(caminho_excel),
        '',
        'Observacao sobre ambulancia:',
        'A remocao de ambulancia ficou como fluxo legado/alternativo.',
        'Esta analise usa a base completa da execucao 10 para nao depender de uma etapa temporaria.',
        '',
        'Abas do Excel:',
        '1. RESUMO: totais gerais da base e media geral da NOTA GERAL.',
        '2. CLASSIFICACAO: quantidade e media da NOTA GERAL por CLASSIFICACAO, seguindo a ordem solicitada.',
        '3. OPERADORA: quantidade e media da NOTA GERAL por OPERADORA, ordenado da maior quantidade para a menor.',
        '   Ao final da aba OPERADORA sao adicionadas linhas consolidadas:',
        '   - ATENDIMENTO PRESENCIAL = soma de HAPVIDA + PROMED.',
        '   - HAPVIDA CORPORATIVO = soma de HAPVIDA, NDI SP E RJ, TELECONSULTA, CLINIPAN, NDI MG, CCG, PROMED e ODONTOLOGIA.',
        '4. UNIDADE: quantidade e media da NOTA GERAL por LOCAL EDITADO, ordenado da maior quantidade para a menor.',
        '5. LOCAL_POR_CLASSIFICACAO: quantidade, media da NOTA GERAL, META, RESULTADO DA UNIDADE, STATUS UNIDADE, tipos e especialidades por CLASSIFICACAO e LOCAL EDITADO.',
        '',
        'Observacoes:',
        '- A media e calculada usando a coluna NOTA GERAL.',
        '- CLASSIFICACOES que nao estiverem na ordem informada aparecem no final da aba CLASSIFICACAO.',
        '- A linha TOTAL GERAL da aba CLASSIFICACAO usa todas as linhas da base.',
    ]

    with open(caminho_documentacao, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas))


def processar_analise_dados(df_base):
    df = df_base.copy()
    colunas_faltando = validar_colunas_obrigatorias(df)
    if colunas_faltando:
        raise ValueError('\n'.join(colunas_faltando))

    df[coluna_nota] = pd.to_numeric(df[coluna_nota], errors='coerce')
    df[coluna_meta] = pd.to_numeric(df[coluna_meta], errors='coerce')
    df[coluna_resultado_unidade] = pd.to_numeric(df[coluna_resultado_unidade], errors='coerce')
    for coluna in [
        coluna_classificacao,
        coluna_operadora,
        coluna_unidade,
        coluna_especialidade,
        coluna_status_unidade,
    ]:
        df[coluna] = normalizar_texto(df[coluna])

    return {
        'resumo': criar_resumo_geral(df),
        'classificacao': criar_analise_classificacao(df),
        'operadora': criar_analise_operadora(df),
        'unidade': criar_analise_por_coluna(df, coluna_unidade),
        'local_por_classificacao': criar_analise_unidade_por_classificacao(df),
    }


def salvar_analise_dados(analises, pasta_destino):
    pasta_destino = Path(pasta_destino)
    pasta_destino.mkdir(parents=True, exist_ok=True)
    destino_excel = pasta_destino / 'exec_11_analise_dados.xlsx'
    destino_documentacao = pasta_destino / 'exec_11_analise_dados_explicacao.txt'

    with pd.ExcelWriter(destino_excel, engine='openpyxl') as writer:
        analises['resumo'].to_excel(writer, sheet_name='RESUMO', index=False)
        analises['classificacao'].to_excel(writer, sheet_name='CLASSIFICACAO', index=False)
        analises['operadora'].to_excel(writer, sheet_name='OPERADORA', index=False)
        analises['unidade'].to_excel(writer, sheet_name='UNIDADE', index=False)
        analises['local_por_classificacao'].to_excel(
            writer,
            sheet_name='LOCAL_POR_CLASSIFICACAO',
            index=False,
        )

    salvar_documentacao(destino_documentacao, destino_excel)
    return destino_excel, destino_documentacao


def executar():
    print('Iniciando execucao 11 - analise de dados...')
    print(f'Lendo arquivo da execucao 10: {arquivo_entrada}')

    if not arquivo_entrada.exists():
        print(f'ERRO - arquivo nao encontrado: {arquivo_entrada}')
        return 1

    df = ler_csv_padronizado(arquivo_entrada)

    try:
        analises = processar_analise_dados(df)
    except ValueError as erro:
        print('ERRO - colunas obrigatorias ausentes:')
        for coluna in str(erro).splitlines():
            print(f'- {coluna}')
        return 1

    print(f'Gravando Excel de analise: {arquivo_saida_excel}')
    _, destino_documentacao = salvar_analise_dados(analises, pasta_saida)

    print(f'Documentacao gerada: {destino_documentacao}')
    print('Execucao 11 finalizada.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
