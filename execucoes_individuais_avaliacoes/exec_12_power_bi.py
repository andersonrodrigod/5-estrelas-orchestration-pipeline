# -*- coding: utf-8 -*-
import csv
import json
import sys
from pathlib import Path

import pandas as pd


arquivo_entrada = Path('data_exec_indiv/avaliacoes/10_base_com_status_unidade.csv')
arquivo_saida = Path('data_exec_indiv/avaliacoes/12_base_power_bi.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_12_power_bi'
arquivo_resumo_json = pasta_resumo / 'exec_12_power_bi_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_12_power_bi_resumo.txt'
arquivo_colunas_csv = pasta_resumo / 'exec_12_power_bi_colunas.csv'
arquivo_classificacao_csv = pasta_resumo / 'exec_12_power_bi_classificacao_ajustes.csv'


# Layout usado pelos arquivos historicos que ja alimentam o Power BI.
colunas_power_bi = [
    'cdatendimento',
    'num_beneficiario',
    'cdempresa',
    'profissional',
    'procedimento',
    'especialidade',
    'data_atendimento',
    'local',
    'uf',
    'classificacao_tipo_desc',
    'nota1',
    'motivador1',
    'desc_motivador1',
    'nota2',
    'motivador2',
    'desc_motivador2',
    'nota3',
    'motivador3',
    'desc_motivador3',
    'nota4',
    'motivador4',
    'desc_motivador4',
    'nota5',
    'motivador5',
    'desc_motivador5',
    'cdusuario',
    'mes',
    'dia',
    'ano',
    'status',
    'nome',
    'telefone',
    'email',
    'tipo',
    'dt_resposta',
    'nota geral',
    'contratação',
    'CLASSIFICAÇÃO',
    'operadora',
    'local editado',
    'Meta',
    'resultado da unidade',
    'staus unidade',
]

mapa_colunas_origem = {
    'cdatendimento': 'CDATENDIMENTO',
    'num_beneficiario': 'NUM_BENEFICIARIO',
    'cdempresa': 'CDEMPRESA',
    'profissional': 'PROFISSIONAL',
    'procedimento': 'PROCEDIMENTO',
    'especialidade': 'ESPECIALIDADE',
    'data_atendimento': 'DATA_ATENDIMENTO',
    'local': 'LOCAL',
    'uf': 'UF',
    'classificacao_tipo_desc': 'CLASSIFICACAO_TIPO_DESC',
    'nota1': 'NOTA1',
    'motivador1': 'MOTIVADOR1',
    'desc_motivador1': 'DESC_MOTIVADOR1',
    'nota2': 'NOTA2',
    'motivador2': 'MOTIVADOR2',
    'desc_motivador2': 'DESC_MOTIVADOR2',
    'nota3': 'NOTA3',
    'motivador3': 'MOTIVADOR3',
    'desc_motivador3': 'DESC_MOTIVADOR3',
    'nota4': 'NOTA4',
    'motivador4': 'MOTIVADOR4',
    'desc_motivador4': 'DESC_MOTIVADOR4',
    'nota5': 'NOTA5',
    'motivador5': 'MOTIVADOR5',
    'desc_motivador5': 'DESC_MOTIVADOR5',
    'cdusuario': 'CDUSUARIO',
    'mes': 'MES',
    'dia': 'DIA',
    'ano': 'ANO',
    'status': 'STATUS',
    'nome': 'NOME',
    'telefone': 'TELEFONE',
    'email': 'EMAIL',
    'tipo': 'TIPO',
    'dt_resposta': 'DT_RESPOSTA',
    'nota geral': 'NOTA GERAL',
    'contratação': 'CONTRATACAO',
    'CLASSIFICAÇÃO': 'CLASSIFICACAO',
    'operadora': 'OPERADORA',
    'local editado': 'LOCAL EDITADO',
    'Meta': 'META',
    'resultado da unidade': 'RESULTADO DA UNIDADE',
    'staus unidade': 'STATUS UNIDADE',
}

ajustes_classificacao = {
    'CRED_ATEND EMERGÊNCIA': 'CRED_ATEND EMERGENCIA',
    'CRED_INTERNAÇÃO': 'CRED_INTERNACAO',
}


def ler_base_texto(caminho):
    return pd.read_csv(caminho, dtype='string', keep_default_na=False, low_memory=False)


def normalizar_texto_colunas(df):
    for coluna in df.columns:
        df[coluna] = (
            df[coluna]
            .astype('string')
            .str.replace('\xa0', ' ', regex=False)
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip()
        )

    return df


def validar_colunas_obrigatorias(df):
    return [
        coluna_origem
        for coluna_origem in mapa_colunas_origem.values()
        if coluna_origem not in df.columns
    ]


def criar_base_power_bi(df):
    df_saida = pd.DataFrame(index=df.index)

    for coluna_destino in colunas_power_bi:
        coluna_origem = mapa_colunas_origem[coluna_destino]
        df_saida[coluna_destino] = df[coluna_origem]

    datas_atendimento = pd.to_datetime(
        df_saida['data_atendimento'],
        errors='coerce',
        dayfirst=False,
    )
    df_saida['data_atendimento'] = datas_atendimento.dt.strftime('%d/%m/%Y')
    df_saida.loc[datas_atendimento.isna(), 'data_atendimento'] = ''

    classificacao_antes = df_saida['CLASSIFICAÇÃO'].copy()
    df_saida['CLASSIFICAÇÃO'] = df_saida['CLASSIFICAÇÃO'].replace(ajustes_classificacao)

    return df_saida, classificacao_antes


def salvar_csv_texto(df, caminho):
    caminho.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(
        caminho,
        index=False,
        encoding='utf-8-sig',
        quoting=csv.QUOTE_MINIMAL,
        lineterminator='\n',
    )


def contar_ajustes_classificacao(classificacao_antes, classificacao_depois):
    df_ajustes = pd.DataFrame({
        'CLASSIFICACAO_ORIGINAL': classificacao_antes,
        'CLASSIFICACAO_POWER_BI': classificacao_depois,
    })
    df_ajustes = df_ajustes[
        df_ajustes['CLASSIFICACAO_ORIGINAL'] != df_ajustes['CLASSIFICACAO_POWER_BI']
    ]

    if df_ajustes.empty:
        return pd.DataFrame(columns=[
            'CLASSIFICACAO_ORIGINAL',
            'CLASSIFICACAO_POWER_BI',
            'QUANTIDADE',
        ])

    return (
        df_ajustes
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'CLASSIFICACAO_ORIGINAL'], ascending=[False, True])
    )


def salvar_resumos(df_entrada, df_saida, classificacao_antes):
    pasta_resumo.mkdir(parents=True, exist_ok=True)

    df_colunas = pd.DataFrame([
        {
            'COLUNA_POWER_BI': coluna_destino,
            'COLUNA_ORIGEM': mapa_colunas_origem[coluna_destino],
        }
        for coluna_destino in colunas_power_bi
    ])
    df_classificacao = contar_ajustes_classificacao(
        classificacao_antes,
        df_saida['CLASSIFICAÇÃO'],
    )

    salvar_csv_texto(df_colunas, arquivo_colunas_csv)
    salvar_csv_texto(df_classificacao, arquivo_classificacao_csv)

    resumo = {
        'execucao': 'exec_12_power_bi',
        'objetivo': 'Gerar CSV no layout historico esperado pelo Power BI.',
        'arquivo_entrada': str(arquivo_entrada),
        'arquivo_saida': str(arquivo_saida),
        'arquivo_colunas_csv': str(arquivo_colunas_csv),
        'arquivo_classificacao_csv': str(arquivo_classificacao_csv),
        'total_linhas_entrada': int(len(df_entrada)),
        'total_linhas_saida': int(len(df_saida)),
        'total_colunas_entrada': int(len(df_entrada.columns)),
        'total_colunas_saida': int(len(df_saida.columns)),
        'colunas_removidas': [
            coluna for coluna in df_entrada.columns
            if coluna not in set(mapa_colunas_origem.values())
        ],
        'ajustes_classificacao': ajustes_classificacao,
        'ajustes_data': {
            'data_atendimento': 'Formato dd/mm/aaaa na saida Power BI.',
        },
    }

    with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 12 - POWER BI',
        '',
        f"Arquivo de entrada: {resumo['arquivo_entrada']}",
        f"Arquivo de saida: {resumo['arquivo_saida']}",
        f"Mapeamento de colunas: {resumo['arquivo_colunas_csv']}",
        f"Ajustes de classificacao: {resumo['arquivo_classificacao_csv']}",
        '',
        f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
        f"Total de linhas na saida: {resumo['total_linhas_saida']}",
        f"Total de colunas na entrada: {resumo['total_colunas_entrada']}",
        f"Total de colunas na saida: {resumo['total_colunas_saida']}",
        '',
        'Colunas removidas:',
    ]

    if resumo['colunas_removidas']:
        linhas_txt.extend(f"- {coluna}" for coluna in resumo['colunas_removidas'])
    else:
        linhas_txt.append('- Nenhuma')

    linhas_txt.extend([
        '',
        'Data:',
        '- data_atendimento formatada como dd/mm/aaaa',
        '',
        'Classificacoes ajustadas para o padrao historico:',
    ])

    for origem, destino in ajustes_classificacao.items():
        linhas_txt.append(f'- {origem} -> {destino}')

    with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))


def executar():
    print('Iniciando execucao 12 - Power BI...')
    print(f'Lendo arquivo da execucao 10: {arquivo_entrada}')

    if not arquivo_entrada.exists():
        print(f'ERRO - arquivo nao encontrado: {arquivo_entrada}')
        return 1

    df = ler_base_texto(arquivo_entrada)
    colunas_faltando = validar_colunas_obrigatorias(df)

    if colunas_faltando:
        print('ERRO - colunas obrigatorias ausentes:')
        for coluna in colunas_faltando:
            print(f'- {coluna}')
        return 1

    df = normalizar_texto_colunas(df)
    df_saida, classificacao_antes = criar_base_power_bi(df)

    print(f'Total de linhas recebidas: {len(df)}')
    print(f'Gravando arquivo Power BI: {arquivo_saida}')
    salvar_csv_texto(df_saida, arquivo_saida)
    salvar_resumos(df, df_saida, classificacao_antes)

    print('Execucao 12 finalizada.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
