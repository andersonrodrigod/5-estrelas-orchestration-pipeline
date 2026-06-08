# -*- coding: utf-8 -*-
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/11_base_sem_ambulancia.csv')

pasta_saida = Path('saida_resumo_avaliacoes') / 'exec_13_analise_dados'
arquivo_saida_excel = pasta_saida / 'exec_13_analise_dados.xlsx'
arquivo_documentacao_txt = pasta_saida / 'exec_13_analise_dados_explicacao.txt'

coluna_nota = 'NOTA GERAL'
coluna_classificacao = 'CLASSIFICACAO'
coluna_operadora = 'OPERADORA'
coluna_unidade = 'LOCAL EDITADO'

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


def salvar_documentacao():
    linhas = [
        'EXECUCAO 13 - ANALISE DE DADOS',
        '',
        'Objetivo:',
        'Gerar uma visao consolidada de quantidade e media da NOTA GERAL a partir do arquivo sem ambulancia da execucao 11.',
        '',
        'Arquivo de entrada:',
        str(arquivo_entrada),
        '',
        'Arquivo Excel gerado:',
        str(arquivo_saida_excel),
        '',
        'Por que usa a execucao 11:',
        'A execucao 11 preserva a base completa da execucao 10 e remove somente classificacoes de ambulancia.',
        'A separacao por TIPO acontece depois, entao a analise geral fica melhor antes dessa separacao.',
        '',
        'Abas do Excel:',
        '1. RESUMO: totais gerais da base e media geral da NOTA GERAL.',
        '2. CLASSIFICACAO: quantidade e media da NOTA GERAL por CLASSIFICACAO, seguindo a ordem solicitada.',
        '3. OPERADORA: quantidade e media da NOTA GERAL por OPERADORA, ordenado da maior quantidade para a menor.',
        '   Ao final da aba OPERADORA sao adicionadas linhas consolidadas:',
        '   - ATENDIMENTO PRESENCIAL = soma de HAPVIDA + PROMED.',
        '   - HAPVIDA CORPORATIVO = soma de HAPVIDA, NDI SP E RJ, TELECONSULTA, CLINIPAN, NDI MG, CCG, PROMED e ODONTOLOGIA.',
        '4. UNIDADE: quantidade e media da NOTA GERAL por LOCAL EDITADO, ordenado da maior quantidade para a menor.',
        '',
        'Observacoes:',
        '- A media e calculada usando a coluna NOTA GERAL.',
        '- CLASSIFICACOES que nao estiverem na ordem informada aparecem no final da aba CLASSIFICACAO.',
        '- A linha TOTAL GERAL da aba CLASSIFICACAO usa todas as linhas da base.',
    ]

    with open(arquivo_documentacao_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas))


def executar():
    print('Iniciando execucao 13 - analise de dados...')
    print(f'Lendo arquivo da execucao 11: {arquivo_entrada}')

    if not arquivo_entrada.exists():
        print(f'ERRO - arquivo nao encontrado: {arquivo_entrada}')
        return 1

    df = ler_csv_padronizado(arquivo_entrada)

    colunas_faltando = validar_colunas_obrigatorias(df)
    if colunas_faltando:
        print('ERRO - colunas obrigatorias ausentes:')
        for coluna in colunas_faltando:
            print(f'- {coluna}')
        return 1

    df[coluna_nota] = pd.to_numeric(df[coluna_nota], errors='coerce')
    for coluna in [coluna_classificacao, coluna_operadora, coluna_unidade]:
        df[coluna] = normalizar_texto(df[coluna])

    pasta_saida.mkdir(parents=True, exist_ok=True)

    df_resumo = criar_resumo_geral(df)
    df_classificacao = criar_analise_classificacao(df)
    df_operadora = criar_analise_operadora(df)
    df_unidade = criar_analise_por_coluna(df, coluna_unidade)

    print(f'Gravando Excel de analise: {arquivo_saida_excel}')
    with pd.ExcelWriter(arquivo_saida_excel, engine='openpyxl') as writer:
        df_resumo.to_excel(writer, sheet_name='RESUMO', index=False)
        df_classificacao.to_excel(writer, sheet_name='CLASSIFICACAO', index=False)
        df_operadora.to_excel(writer, sheet_name='OPERADORA', index=False)
        df_unidade.to_excel(writer, sheet_name='UNIDADE', index=False)

    salvar_documentacao()

    print(f'Documentacao gerada: {arquivo_documentacao_txt}')
    print('Execucao 13 finalizada.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
