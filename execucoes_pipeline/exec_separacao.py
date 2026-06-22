# -*- coding: utf-8 -*-
import json
import os
import sys
import unicodedata
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell

sys.path.append(str(Path(__file__).resolve().parents[1]))
from execucoes_pipeline.config import criar_config_pre_validacao
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

config = criar_config_pre_validacao()
arquivo_avaliacoes = config.arquivo_csv_final_pre_validacao
arquivo_negativas = config.arquivo_csv_final_pre_validacao_negativas
arquivo_nomes_classificacao = Path('data/nomes_classificacao.json')
pasta_resumo = Path('saida_resumo_separacao')
pasta_saida_excel_local = Path('data_exec_indiv/separacao')
pasta_saida_excel_sharepoint = Path(
    r'C:\Users\anderson.dossantos\HAPVIDA ASSISTÊNCIA MÉDICA LTDA'
    r'\5 Estrelas - Documentos\Base de Dados 5 Estrelas'
    r'\base de dados maio 26\atualizações das classificações'
)


def resolver_pasta_saida_excel():
    if pasta_saida_excel_sharepoint.exists():
        print(f'Usando pasta do SharePoint/OneDrive: {pasta_saida_excel_sharepoint}')
        return pasta_saida_excel_sharepoint

    print(f'Usando pasta local do projeto: {pasta_saida_excel_local}')
    return pasta_saida_excel_local


pasta_saida_excel = resolver_pasta_saida_excel()

arquivo_resumo_json = pasta_resumo / 'exec_separacao_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_separacao_resumo.txt'
arquivo_quantidades_csv = pasta_resumo / 'exec_separacao_quantidades_por_arquivo.csv'
arquivo_classificacoes_csv = pasta_resumo / 'exec_separacao_classificacoes_por_grupo.csv'
arquivo_nao_enviadas_csv = pasta_resumo / 'exec_separacao_classificacoes_nao_enviadas.csv'

prefixo_saida = 'base de dados 5 estrelas maio_26 - '
coluna_classificacao = 'CLASSIFICACAO'

regras_grupos = {
    'ambulancia': [
        'AMBULANCIA',
    ],
    'diagnostico': [
        'VIDA IMAGEM',
        'LABORATORIO',
    ],
    'hapclinica': [
        'HAPCLINICA',
    ],
    'hospitalar': [
        'HOSPITALAR',
        'INTERNACAO',
    ],
    'med prev e programas especiais': [
        'CASE',
        'GESTAR BEM',
        'INTERNACAO PGC',
        'MED PREV',
        'NASCER BEM',
        'PRODUTO COORDENADO',
        'TEA',
        'TRANSPLANTE RENAL',
        'QUALIVIDA',
        'TRANSPLANTE',
    ],
    'odontologia': [
        'ODONTOLOGIA',
    ],
    'rede credenciada': [
        'CRED_ATEND ELETIVO',
        'CRED_ATEND EMERGENCIA',
        'CRED_EXAMES',
        'CRED_INTERNACAO',
        'CRED_LABORATORIO',
        'CRED_TRATAMENTO',
    ],
    'teleconsulta': [
        'TELECONSULTA ELETIVA',
        'TELEMEDICINA',
        'TELECONSULTA CASE',
        'TELECONSULTA URG\u00caNCIA',
    ],
}

grupos_filtrados = [
    grupo.strip()
    for grupo in os.environ.get('GRUPOS_SEPARACAO', '').split(';')
    if grupo.strip()
]

if grupos_filtrados:
    grupos_inexistentes = [
        grupo
        for grupo in grupos_filtrados
        if grupo not in regras_grupos
    ]

    if grupos_inexistentes:
        raise ValueError(
            'Grupo(s) informado(s) em GRUPOS_SEPARACAO nao existem: '
            + ', '.join(grupos_inexistentes)
        )

    regras_grupos = {
        grupo: regras_grupos[grupo]
        for grupo in grupos_filtrados
    }

colunas_numericas_avaliacoes = {
    'nota1',
    'nota2',
    'nota3',
    'nota4',
    'nota5',
    'mes',
    'dia',
    'ano',
    'tipo',
    'notageral',
    'meta',
    'resultadodaunidade',
}

colunas_texto_forcado_avaliacoes = {
    'cdatendimento',
    'numbeneficiario',
    'cdempresa',
    'cdusuario',
    'telefone',
    'dataatendimento',
}

colunas_numericas_negativas = {
    'nota',
    'mes',
    'dia',
    'ano',
    'tipo',
}

colunas_texto_forcado_negativas = {
    'cdatendimento',
    'numbeneficiario',
    'cdempresa',
    'cdusuario',
    'telefone',
    'dataatendimento',
}

cabecalhos_avaliacoes = [
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

cabecalhos_negativas = [
    'CDATENDIMENTO',
    'NUM_BENEFICIARIO',
    'CDEMPRESA',
    'PROFISSIONAL',
    'PROCEDIMENTO',
    'ESPECIALIDADE',
    'DATA_ATENDIMENTO',
    'LOCAL',
    'UF',
    'CLASSIFICACAO_TIPO_DESC',
    'NOTA',
    'TITULO_MOTIVADOR',
    'DESC_MOTIVADOR',
    'DESCRICAO',
    'RESPOSTA',
    'CDUSUARIO',
    'MES',
    'DIA',
    'ANO',
    'STATUS',
    'NOME',
    'TELEFONE',
    'EMAIL',
    'TIPO',
    'DT_RESPOSTA',
    'CONTRATACAO',
    'CLASSIFICACAO',
    'LOCAL EDITADO',
]

aliases_cabecalhos = {
    'contratacao': 'contratação',
    'classificacao': 'CLASSIFICAÇÃO',
    'statusunidade': 'staus unidade',
}


def normalizar_texto(valor):
    if pd.isna(valor):
        return ''

    texto = str(valor).strip().upper()
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(caractere for caractere in texto if not unicodedata.combining(caractere))
    return ' '.join(texto.split())


def normalizar_cabecalho(valor):
    texto = normalizar_texto(valor)
    return texto.replace(' ', '').replace('_', '').lower()


def chave_cabecalho_saida(coluna):
    chave = normalizar_cabecalho(coluna)
    return normalizar_cabecalho(aliases_cabecalhos.get(chave, coluna))


def padronizar_layout(df, cabecalhos):
    mapa_colunas = {
        chave_cabecalho_saida(coluna): coluna
        for coluna in df.columns
    }
    df_saida = pd.DataFrame(index=df.index)

    for cabecalho in cabecalhos:
        chave = normalizar_cabecalho(cabecalho)
        coluna_origem = mapa_colunas.get(chave)
        if coluna_origem is None:
            df_saida[cabecalho] = pd.NA
        else:
            df_saida[cabecalho] = df[coluna_origem]

    return df_saida


def carregar_json(caminho):
    with open(caminho, 'r', encoding='utf-8-sig') as arquivo:
        return json.load(arquivo)


def criar_mapa_nomes_envio(nomes_classificacao):
    mapa = {}

    for chave, nome in nomes_classificacao.items():
        mapa[normalizar_texto(chave)] = nome
        mapa[normalizar_texto(nome)] = nome

    return mapa


def aplicar_nomes_envio(df, coluna_original, mapa_nomes_envio):
    df_saida = df.copy()
    classificacao_normalizada = df_saida[coluna_original].apply(normalizar_texto)
    nomes_envio = classificacao_normalizada.map(mapa_nomes_envio)
    df_saida[coluna_original] = nomes_envio.fillna(df_saida[coluna_original])
    return df_saida


def resolver_coluna_classificacao(df, nome_arquivo):
    coluna_esperada = normalizar_cabecalho(coluna_classificacao)

    for coluna in df.columns:
        if normalizar_cabecalho(coluna) == coluna_esperada:
            return coluna

    raise ValueError(
        f"Coluna de classificacao nao encontrada em '{nome_arquivo}'. "
        f"Esperado: {coluna_classificacao}."
    )


def criar_mapas_regras():
    classe_para_grupo = {}
    regras_normalizadas = {}
    rotulos_classes = {}

    for grupo, classes in regras_grupos.items():
        regras_normalizadas[grupo] = set()
        rotulos_classes[grupo] = {}

        for classe in classes:
            classe_normalizada = normalizar_texto(classe)
            regras_normalizadas[grupo].add(classe_normalizada)
            rotulos_classes[grupo][classe_normalizada] = classe

            if classe_normalizada in classe_para_grupo:
                grupo_anterior = classe_para_grupo[classe_normalizada]
                raise ValueError(
                    f"Classificacao duplicada em grupos diferentes: "
                    f"'{classe}' -> '{grupo_anterior}' e '{grupo}'."
                )

            classe_para_grupo[classe_normalizada] = grupo

    return classe_para_grupo, regras_normalizadas, rotulos_classes


def resumo_nao_enviadas(df, origem, coluna_original):
    df_nao_enviado = df[df['__grupo'].isna()].copy()

    if df_nao_enviado.empty:
        return pd.DataFrame(columns=['ORIGEM', 'CLASSIFICACAO', 'QUANTIDADE'])

    resumo = (
        df_nao_enviado[coluna_original]
        .astype('string')
        .fillna('VAZIO')
        .value_counts(dropna=False)
        .reset_index()
    )
    resumo.columns = ['CLASSIFICACAO', 'QUANTIDADE']
    resumo.insert(0, 'ORIGEM', origem)
    return resumo.sort_values(['ORIGEM', 'CLASSIFICACAO'])


def valor_vazio(valor):
    return valor is None or pd.isna(valor) or str(valor).strip() == ''


def converter_numero(valor):
    if valor_vazio(valor):
        return None

    texto = str(valor).strip().replace(',', '.')

    try:
        numero = Decimal(texto)
    except InvalidOperation:
        return str(valor)

    if numero == numero.to_integral_value():
        return int(numero)

    return float(numero)


def criar_celula_texto(ws, valor):
    celula = WriteOnlyCell(ws, value='' if valor_vazio(valor) else str(valor))
    celula.number_format = '@'
    return celula


def criar_celula_excel(ws, coluna, valor, colunas_numericas, colunas_texto_forcado):
    coluna_normalizada = normalizar_cabecalho(coluna)

    if coluna_normalizada in colunas_numericas:
        return converter_numero(valor)

    if coluna_normalizada in colunas_texto_forcado:
        return criar_celula_texto(ws, valor)

    if valor_vazio(valor):
        return None

    return str(valor)


def escrever_aba_dataframe(workbook, nome_aba, df, colunas_numericas, colunas_texto_forcado):
    ws = workbook.create_sheet(title=nome_aba)
    cabecalho = list(df.columns)
    ws.append(cabecalho)

    for linha in df.itertuples(index=False, name=None):
        ws.append([
            criar_celula_excel(
                ws,
                coluna,
                valor,
                colunas_numericas,
                colunas_texto_forcado,
            )
            for coluna, valor in zip(cabecalho, linha)
        ])


def salvar_excel_grupo(grupo, df_avaliacoes, df_negativas):
    caminho_saida = pasta_saida_excel / f'{prefixo_saida}{grupo}.xlsx'
    workbook = Workbook(write_only=True)
    df_avaliacoes = padronizar_layout(df_avaliacoes, cabecalhos_avaliacoes)
    df_negativas = padronizar_layout(df_negativas, cabecalhos_negativas)
    escrever_aba_dataframe(
        workbook,
        'avaliacoes',
        df_avaliacoes,
        colunas_numericas_avaliacoes,
        colunas_texto_forcado_avaliacoes,
    )
    escrever_aba_dataframe(
        workbook,
        'negativas',
        df_negativas,
        colunas_numericas_negativas,
        colunas_texto_forcado_negativas,
    )
    workbook.save(caminho_saida)

    return caminho_saida


print('Iniciando execucao - separacao por classificacao...')
print(f'Lendo avaliacoes: {arquivo_avaliacoes}')
print(f'Lendo negativas: {arquivo_negativas}')
print(f'Lendo nomes de classificacao para envio: {arquivo_nomes_classificacao}')

df_avaliacoes = ler_csv_padronizado(arquivo_avaliacoes)
df_negativas = ler_csv_padronizado(arquivo_negativas)
nomes_classificacao = carregar_json(arquivo_nomes_classificacao)
mapa_nomes_envio = criar_mapa_nomes_envio(nomes_classificacao)

coluna_avaliacoes = resolver_coluna_classificacao(df_avaliacoes, str(arquivo_avaliacoes))
coluna_negativas = resolver_coluna_classificacao(df_negativas, str(arquivo_negativas))

classe_para_grupo, regras_normalizadas, rotulos_classes = criar_mapas_regras()

df_avaliacoes = df_avaliacoes.copy()
df_negativas = df_negativas.copy()

df_avaliacoes['__classificacao_normalizada'] = df_avaliacoes[coluna_avaliacoes].apply(normalizar_texto)
df_negativas['__classificacao_normalizada'] = df_negativas[coluna_negativas].apply(normalizar_texto)
df_avaliacoes['__grupo'] = df_avaliacoes['__classificacao_normalizada'].map(classe_para_grupo)
df_negativas['__grupo'] = df_negativas['__classificacao_normalizada'].map(classe_para_grupo)

pasta_saida_excel.mkdir(parents=True, exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)

contagem_avaliacoes = df_avaliacoes.groupby(['__grupo', '__classificacao_normalizada']).size()
contagem_negativas = df_negativas.groupby(['__grupo', '__classificacao_normalizada']).size()

linhas_quantidades = []
linhas_classificacoes = []
arquivos_gerados = []

for grupo in regras_grupos:
    avaliacoes_filtradas = (
        df_avaliacoes[df_avaliacoes['__grupo'] == grupo]
        .drop(columns=['__classificacao_normalizada', '__grupo'])
    )
    negativas_filtradas = (
        df_negativas[df_negativas['__grupo'] == grupo]
        .drop(columns=['__classificacao_normalizada', '__grupo'])
    )

    # A execucao 14 ja ajusta a CLASSIFICACAO das avaliacoes para o padrao
    # historico do Power BI. Reaplicar nomes aqui recolocaria acentos.
    negativas_filtradas = aplicar_nomes_envio(
        negativas_filtradas,
        coluna_negativas,
        mapa_nomes_envio
    )

    arquivo_excel = salvar_excel_grupo(grupo, avaliacoes_filtradas, negativas_filtradas)
    arquivos_gerados.append(str(arquivo_excel))

    linhas_quantidades.append({
        'GRUPO': grupo,
        'ARQUIVO': str(arquivo_excel),
        'AVALIACOES': int(len(avaliacoes_filtradas)),
        'NEGATIVAS': int(len(negativas_filtradas)),
        'TOTAL': int(len(avaliacoes_filtradas) + len(negativas_filtradas)),
    })

    for classe_normalizada in sorted(regras_normalizadas[grupo]):
        quantidade_avaliacoes = int(contagem_avaliacoes.get((grupo, classe_normalizada), 0))
        quantidade_negativas = int(contagem_negativas.get((grupo, classe_normalizada), 0))

        linhas_classificacoes.append({
            'GRUPO': grupo,
            'CLASSIFICACAO': rotulos_classes[grupo].get(classe_normalizada, classe_normalizada),
            'AVALIACOES': quantidade_avaliacoes,
            'NEGATIVAS': quantidade_negativas,
            'TOTAL': quantidade_avaliacoes + quantidade_negativas,
        })

df_quantidades = pd.DataFrame(linhas_quantidades)
df_classificacoes = pd.DataFrame(linhas_classificacoes)
resumos_nao_enviadas = [
    resumo_nao_enviadas(df_avaliacoes, 'avaliacoes', coluna_avaliacoes),
    resumo_nao_enviadas(df_negativas, 'negativas', coluna_negativas),
]
resumos_nao_enviadas = [resumo for resumo in resumos_nao_enviadas if not resumo.empty]

if resumos_nao_enviadas:
    df_nao_enviadas = pd.concat(resumos_nao_enviadas, ignore_index=True)
else:
    df_nao_enviadas = pd.DataFrame(columns=['ORIGEM', 'CLASSIFICACAO', 'QUANTIDADE'])

total_avaliacoes = int(len(df_avaliacoes))
total_negativas = int(len(df_negativas))
total_origem = total_avaliacoes + total_negativas
total_enviado_avaliacoes = int(df_avaliacoes['__grupo'].notna().sum())
total_enviado_negativas = int(df_negativas['__grupo'].notna().sum())
total_enviado = total_enviado_avaliacoes + total_enviado_negativas
total_nao_enviado = total_origem - total_enviado

salvar_csv_padronizado(df_quantidades, arquivo_quantidades_csv)
salvar_csv_padronizado(df_classificacoes, arquivo_classificacoes_csv)
salvar_csv_padronizado(df_nao_enviadas, arquivo_nao_enviadas_csv)

resumo = {
    'execucao': 'exec_separacao',
    'arquivo_avaliacoes': str(arquivo_avaliacoes),
    'arquivo_negativas': str(arquivo_negativas),
    'arquivo_nomes_classificacao': str(arquivo_nomes_classificacao),
    'pasta_saida_excel': str(pasta_saida_excel),
    'pasta_resumo': str(pasta_resumo),
    'arquivo_quantidades': str(arquivo_quantidades_csv),
    'arquivo_classificacoes': str(arquivo_classificacoes_csv),
    'arquivo_nao_enviadas': str(arquivo_nao_enviadas_csv),
    'total_avaliacoes_origem': total_avaliacoes,
    'total_negativas_origem': total_negativas,
    'total_origem': total_origem,
    'total_avaliacoes_enviadas': total_enviado_avaliacoes,
    'total_negativas_enviadas': total_enviado_negativas,
    'total_enviado': total_enviado,
    'total_nao_enviado': total_nao_enviado,
    'arquivos_gerados': arquivos_gerados,
    'quantidades_por_arquivo': linhas_quantidades,
    'classificacoes_por_grupo': linhas_classificacoes,
    'classificacoes_nao_enviadas': df_nao_enviadas.to_dict(orient='records'),
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO - SEPARACAO POR CLASSIFICACAO',
    '',
    'ARQUIVOS DE ENTRADA:',
    f'Avaliacoes: {resumo["arquivo_avaliacoes"]}',
    f'Negativas: {resumo["arquivo_negativas"]}',
    '',
    'PASTAS DE SAIDA:',
    f'Excels separados: {resumo["pasta_saida_excel"]}',
    f'Resumos e inspecoes: {resumo["pasta_resumo"]}',
    '',
    'TOTAIS DE ORIGEM:',
    f'Avaliacoes: {resumo["total_avaliacoes_origem"]}',
    f'Negativas: {resumo["total_negativas_origem"]}',
    f'Total: {resumo["total_origem"]}',
    '',
    'TOTAIS ENVIADOS:',
    f'Avaliacoes: {resumo["total_avaliacoes_enviadas"]}',
    f'Negativas: {resumo["total_negativas_enviadas"]}',
    f'Total: {resumo["total_enviado"]}',
    f'Nao enviados: {resumo["total_nao_enviado"]}',
    '',
    'ARQUIVOS GERADOS:',
]

for item in linhas_quantidades:
    linhas_txt.append(
        f"- {Path(item['ARQUIVO']).name}: "
        f"avaliacoes={item['AVALIACOES']} | negativas={item['NEGATIVAS']} | total={item['TOTAL']}"
    )

linhas_txt.extend([
    '',
    'CLASSIFICACOES NAO ENVIADAS:',
])

if df_nao_enviadas.empty:
    linhas_txt.append('- Nenhuma classificacao ficou fora dos arquivos')
else:
    for _, linha in df_nao_enviadas.iterrows():
        linhas_txt.append(
            f"- {linha['ORIGEM']} | {linha['CLASSIFICACAO']}: {int(linha['QUANTIDADE'])}"
        )

linhas_txt.extend([
    '',
    'INSPECOES:',
    f'Quantidades por arquivo: {arquivo_quantidades_csv}',
    f'Classificacoes por grupo: {arquivo_classificacoes_csv}',
    f'Classificacoes nao enviadas: {arquivo_nao_enviadas_csv}',
])

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print(f'Total origem: {total_origem}')
print(f'Total enviado: {total_enviado}')
print(f'Total nao enviado: {total_nao_enviado}')
print(f'Excels gerados em: {pasta_saida_excel}')
print(f'Resumo gerado em: {arquivo_resumo_txt}')
print('Execucao de separacao finalizada.')
