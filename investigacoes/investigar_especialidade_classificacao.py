# -*- coding: utf-8 -*-
import difflib
import unicodedata
from pathlib import Path

import pandas as pd

termo_classificacao = 'QUALIVIDA'

arquivo_json_especialidade = Path('saida_inspecao/inspecao_especialidade.json')
arquivo_base = Path('data/5_estrelas_fevereiro_tratado.csv')
pasta_saida = Path('saida_investigacao')

pasta_saida.mkdir(exist_ok=True)


def normalizar_texto(texto):
    if pd.isna(texto):
        return ''

    texto = str(texto).strip().upper()
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(caractere for caractere in texto if not unicodedata.combining(caractere))
    return ' '.join(texto.split())


termo_normalizado = normalizar_texto(termo_classificacao)
valores_especialidade = {}
fonte_dados = ''

if arquivo_json_especialidade.exists():
    df_json = pd.read_json(arquivo_json_especialidade)

    if 'especialidades' in df_json.columns:
        valores_especialidade = df_json['especialidades'].to_dict()
        fonte_dados = str(arquivo_json_especialidade)

if not valores_especialidade:
    df = pd.read_csv(arquivo_base, low_memory=False)
    df['ESPECIALIDADE'] = df['ESPECIALIDADE'].astype('string').str.strip()
    valores_especialidade = (
        df['ESPECIALIDADE']
        .fillna('VAZIO')
        .value_counts()
        .to_dict()
    )
    fonte_dados = str(arquivo_base)

lista_encontrados = []

for valor_original, quantidade in valores_especialidade.items():
    valor_normalizado = normalizar_texto(valor_original)

    if not valor_normalizado:
        continue

    similaridade = difflib.SequenceMatcher(None, termo_normalizado, valor_normalizado).ratio()

    encontrou = False

    if termo_normalizado in valor_normalizado:
        encontrou = True
    elif valor_normalizado in termo_normalizado:
        encontrou = True
    elif similaridade >= 0.75:
        encontrou = True

    if encontrou:
        lista_encontrados.append({
            'valor_original': valor_original,
            'quantidade': int(quantidade),
            'similaridade': round(similaridade, 4)
        })

lista_encontrados = sorted(
    lista_encontrados,
    key=lambda item: (-item['quantidade'], -item['similaridade'], item['valor_original'])
)

arquivo_txt = pasta_saida / 'investigacao_especialidade_qualivida.txt'

linhas_txt = [
    'INVESTIGACAO DE ESPECIALIDADE PARA CLASSIFICACAO',
    '',
    f'Termo alvo: {termo_classificacao}',
    f'Fonte usada: {fonte_dados}',
    '',
    'LISTA DE VALORES CANDIDATOS PARA A CLASSIFICACAO:',
    ''
]

if lista_encontrados:
    linhas_txt.append('lista_qualivida = [')

    for item in lista_encontrados:
        linhas_txt.append(
            f"    '{item['valor_original']}',"
        )

    linhas_txt.append(']')
    linhas_txt.append('')
    linhas_txt.append('DETALHES DA LISTA:')
    linhas_txt.append('')

    for item in lista_encontrados:
        linhas_txt.append(
            f"- valor='{item['valor_original']}' | quantidade={item['quantidade']} | similaridade={item['similaridade']}"
        )
else:
    linhas_txt.append('- Nenhum valor encontrado')

with open(arquivo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print(f'Investigacao concluida. Fonte usada: {fonte_dados}')
print(f'Relatorio TXT: {arquivo_txt}')
