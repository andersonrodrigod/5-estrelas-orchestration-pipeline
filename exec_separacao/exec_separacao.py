# -*- coding: utf-8 -*-
import json
import unicodedata
from pathlib import Path

import pandas as pd

arquivo_avaliacoes = Path('data_exec_indiv/avaliacoes/09_base_com_status_unidade.csv')
arquivo_negativas = Path('data_exec_indiv/negativas/04_base_com_local_editado.csv')
pasta_saida_excel = Path('data_exec_indiv/separacao')
pasta_resumo = Path('saida_resumo_separacao')

arquivo_resumo_json = pasta_resumo / 'exec_separacao_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_separacao_resumo.txt'
arquivo_quantidades_csv = pasta_resumo / 'exec_separacao_quantidades_por_arquivo.csv'
arquivo_classificacoes_csv = pasta_resumo / 'exec_separacao_classificacoes_por_grupo.csv'
arquivo_nao_enviadas_csv = pasta_resumo / 'exec_separacao_classificacoes_nao_enviadas.csv'

prefixo_saida = 'base de dados 5 estrelas fevereiro 25 - '
coluna_classificacao = 'CLASSIFICACAO'

regras_grupos = {
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


def normalizar_texto(valor):
    if pd.isna(valor):
        return ''

    texto = str(valor).strip().upper()
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(caractere for caractere in texto if not unicodedata.combining(caractere))
    return ' '.join(texto.split())


def normalizar_cabecalho(valor):
    texto = normalizar_texto(valor)
    return texto.replace(' ', '').replace('_', '')


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


def salvar_excel_grupo(grupo, df_avaliacoes, df_negativas):
    caminho_saida = pasta_saida_excel / f'{prefixo_saida}{grupo}.xlsx'

    with pd.ExcelWriter(caminho_saida, engine='openpyxl') as writer:
        df_avaliacoes.to_excel(writer, sheet_name='avaliacoes', index=False)
        df_negativas.to_excel(writer, sheet_name='negativas', index=False)

    return caminho_saida


print('Iniciando execucao - separacao por classificacao...')
print(f'Lendo avaliacoes: {arquivo_avaliacoes}')
print(f'Lendo negativas: {arquivo_negativas}')

df_avaliacoes = pd.read_csv(arquivo_avaliacoes, low_memory=False)
df_negativas = pd.read_csv(arquivo_negativas, low_memory=False)

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

df_quantidades.to_csv(arquivo_quantidades_csv, index=False, encoding='utf-8-sig')
df_classificacoes.to_csv(arquivo_classificacoes_csv, index=False, encoding='utf-8-sig')
df_nao_enviadas.to_csv(arquivo_nao_enviadas_csv, index=False, encoding='utf-8-sig')

resumo = {
    'execucao': 'exec_separacao',
    'arquivo_avaliacoes': str(arquivo_avaliacoes),
    'arquivo_negativas': str(arquivo_negativas),
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
