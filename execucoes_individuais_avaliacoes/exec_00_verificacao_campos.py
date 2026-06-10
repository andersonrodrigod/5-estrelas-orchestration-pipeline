# -*- coding: utf-8 -*-
import json
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data/premissa_5_estrelas_maio.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_00_verificacao_campos'
arquivo_resumo_json = pasta_resumo / 'exec_00_verificacao_campos_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_00_verificacao_campos_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_00_verificacao_campos_resumo.csv'
arquivo_valores_invalidos_csv = pasta_resumo / 'exec_00_verificacao_campos_valores_invalidos.csv'
arquivo_local_especialidade_invalidos_csv = (
    pasta_resumo / 'exec_00_verificacao_campos_local_especialidade_invalidos.csv'
)
arquivo_uf_vazio_csv = pasta_resumo / 'exec_00_verificacao_campos_uf_vazio.csv'
arquivo_tipo_vazio_csv = pasta_resumo / 'exec_00_verificacao_campos_tipo_vazio.csv'

colunas_verificacao = ['LOCAL', 'ESPECIALIDADE', 'UF']
coluna_tipo = 'TIPO'
regex_letra = re.compile(r'[A-Za-zÀ-ÖØ-öø-ÿ]')
regex_apenas_especial = re.compile(r'^[^A-Za-zÀ-ÖØ-öø-ÿ0-9]+$')


def preparar_texto(serie):
    texto = serie.astype('string')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip()


def transformar_em_lista_registros(df_base, colunas):
    if df_base.empty:
        return []

    registros = []

    for _, linha in df_base.iterrows():
        item = {}

        for coluna in colunas:
            valor = linha[coluna]
            item[coluna] = None if pd.isna(valor) else str(valor)

        registros.append(item)

    return registros


def criar_mascara_vazio(df_base, coluna):
    if coluna not in df_base.columns:
        return pd.Series(False, index=df_base.index)

    texto = preparar_texto(df_base[coluna])
    return texto.isna() | (texto == '')


print('Iniciando execucao 00 - verificacao de campos...')
print(f'Lendo arquivo da execucao 01: {arquivo_entrada}')

df = ler_csv_padronizado(arquivo_entrada)
pasta_resumo.mkdir(parents=True, exist_ok=True)

resumos = []
valores_invalidos = []
mascaras_invalidas = {}

for coluna in colunas_verificacao:
    if coluna not in df.columns:
        resumos.append({
            'COLUNA': coluna,
            'TOTAL_LINHAS': int(len(df)),
            'TOTAL_VAZIOS': None,
            'TOTAL_APENAS_HIFEN': None,
            'TOTAL_SEM_LETRAS': None,
            'TOTAL_APENAS_CARACTERES_ESPECIAIS': None,
            'OBSERVACAO': 'Coluna nao encontrada',
        })
        continue

    texto = preparar_texto(df[coluna])
    mascara_vazio = texto.isna() | (texto == '')
    mascara_apenas_hifen = texto.fillna('').str.fullmatch(r'-+')
    mascara_sem_letras = (~mascara_vazio) & ~texto.fillna('').str.contains(regex_letra)
    mascara_apenas_especial = (~mascara_vazio) & texto.fillna('').str.fullmatch(regex_apenas_especial)
    mascaras_invalidas[coluna] = mascara_vazio | mascara_sem_letras

    resumos.append({
        'COLUNA': coluna,
        'TOTAL_LINHAS': int(len(df)),
        'TOTAL_VAZIOS': int(mascara_vazio.sum()),
        'TOTAL_APENAS_HIFEN': int(mascara_apenas_hifen.sum()),
        'TOTAL_SEM_LETRAS': int(mascara_sem_letras.sum()),
        'TOTAL_APENAS_CARACTERES_ESPECIAIS': int(mascara_apenas_especial.sum()),
        'OBSERVACAO': '',
    })

    df_valores = (
        pd.DataFrame({
            'VALOR': texto[mascara_vazio | mascara_sem_letras].fillna('VAZIO'),
            'TIPO_PROBLEMA': [
                'vazio' if vazio else (
                    'apenas_hifen' if hifen else (
                        'apenas_caracteres_especiais' if especial else 'sem_letras'
                    )
                )
                for vazio, hifen, especial in zip(
                    mascara_vazio[mascara_vazio | mascara_sem_letras],
                    mascara_apenas_hifen[mascara_vazio | mascara_sem_letras],
                    mascara_apenas_especial[mascara_vazio | mascara_sem_letras],
                )
            ],
        })
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'VALOR'], ascending=[False, True])
    )
    df_valores.insert(0, 'COLUNA', coluna)
    valores_invalidos.append(df_valores)

df_resumo = pd.DataFrame(resumos)
df_valores_invalidos = (
    pd.concat(valores_invalidos, ignore_index=True)
    if valores_invalidos
    else pd.DataFrame(columns=['COLUNA', 'VALOR', 'TIPO_PROBLEMA', 'QUANTIDADE'])
)
mascara_local_especialidade_invalidos = (
    mascaras_invalidas.get('LOCAL', pd.Series(False, index=df.index))
    & mascaras_invalidas.get('ESPECIALIDADE', pd.Series(False, index=df.index))
)
df_local_especialidade_invalidos = df.loc[mascara_local_especialidade_invalidos].copy()
mascara_uf_vazio = criar_mascara_vazio(df, 'UF')
mascara_tipo_vazio = criar_mascara_vazio(df, coluna_tipo)
df_uf_vazio = df.loc[mascara_uf_vazio].copy()
df_tipo_vazio = df.loc[mascara_tipo_vazio].copy()

salvar_csv_padronizado(df_resumo, arquivo_resumo_csv)
salvar_csv_padronizado(df_valores_invalidos, arquivo_valores_invalidos_csv)
salvar_csv_padronizado(df_local_especialidade_invalidos, arquivo_local_especialidade_invalidos_csv)
salvar_csv_padronizado(df_uf_vazio, arquivo_uf_vazio_csv)
salvar_csv_padronizado(df_tipo_vazio, arquivo_tipo_vazio_csv)

resumo = {
    'execucao': 'exec_00_verificacao_campos',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_resumo_csv': str(arquivo_resumo_csv),
    'arquivo_valores_invalidos_csv': str(arquivo_valores_invalidos_csv),
    'arquivo_local_especialidade_invalidos_csv': str(arquivo_local_especialidade_invalidos_csv),
    'arquivo_uf_vazio_csv': str(arquivo_uf_vazio_csv),
    'arquivo_tipo_vazio_csv': str(arquivo_tipo_vazio_csv),
    'total_linhas_entrada': int(len(df)),
    'total_local_especialidade_invalidos': int(mascara_local_especialidade_invalidos.sum()),
    'total_uf_vazio': int(mascara_uf_vazio.sum()),
    'total_tipo_vazio': int(mascara_tipo_vazio.sum()),
    'colunas_verificadas': colunas_verificacao,
    'resumo_colunas': transformar_em_lista_registros(
        df_resumo,
        [
            'COLUNA',
            'TOTAL_LINHAS',
            'TOTAL_VAZIOS',
            'TOTAL_APENAS_HIFEN',
            'TOTAL_SEM_LETRAS',
            'TOTAL_APENAS_CARACTERES_ESPECIAIS',
            'OBSERVACAO',
        ],
    ),
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 00 - VERIFICACAO DE CAMPOS',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de resumo CSV: {resumo['arquivo_resumo_csv']}",
    f"Valores invalidos agrupados: {resumo['arquivo_valores_invalidos_csv']}",
    f"Linhas com LOCAL e ESPECIALIDADE invalidos: {resumo['arquivo_local_especialidade_invalidos_csv']}",
    f"Linhas com UF vazio: {resumo['arquivo_uf_vazio_csv']}",
    f"Linhas com TIPO vazio: {resumo['arquivo_tipo_vazio_csv']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total com LOCAL e ESPECIALIDADE invalidos juntos: {resumo['total_local_especialidade_invalidos']}",
    f"Total com UF vazio: {resumo['total_uf_vazio']}",
    f"Total com TIPO vazio: {resumo['total_tipo_vazio']}",
    '',
    'COLUNAS VERIFICADAS:',
]

for item in resumo['resumo_colunas']:
    linhas_txt.extend([
        '',
        f"- Coluna: {item['COLUNA']}",
        f"  Total vazios: {item['TOTAL_VAZIOS']}",
        f"  Total apenas hifen: {item['TOTAL_APENAS_HIFEN']}",
        f"  Total sem letras: {item['TOTAL_SEM_LETRAS']}",
        f"  Total apenas caracteres especiais: {item['TOTAL_APENAS_CARACTERES_ESPECIAIS']}",
    ])

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print(f'Total de linhas recebidas: {len(df)}')
print(f'Gravando resumo: {arquivo_resumo_csv}')
print(f'Gravando valores invalidos agrupados: {arquivo_valores_invalidos_csv}')
print(f'Gravando linhas com LOCAL e ESPECIALIDADE invalidos: {arquivo_local_especialidade_invalidos_csv}')
print(f'Gravando linhas com UF vazio: {arquivo_uf_vazio_csv}')
print(f'Gravando linhas com TIPO vazio: {arquivo_tipo_vazio_csv}')
print('Execucao 00 finalizada.')
