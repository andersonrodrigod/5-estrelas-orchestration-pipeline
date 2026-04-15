import json
from pathlib import Path

import pandas as pd

print('Lendo o arquivo CSV para inspecionar as notas...')
df = pd.read_csv('data/5_estrelas_fevereiro.csv')

colunas_notas = ['NOTA1', 'NOTA2', 'NOTA3', 'NOTA4', 'NOTA5']

pasta_saida = Path('saida_inspecao')
pasta_saida.mkdir(exist_ok=True)

resultado = {}
resultado['total_linhas'] = len(df)
resultado['colunas_analisadas'] = colunas_notas
resultado['resumo_por_coluna'] = {}
resultado['total_valores_numericos_validos'] = 0

print('Analisando cada coluna de nota...')

for coluna in colunas_notas:
    print(f'Analisando a coluna {coluna}...')

    serie_original = df[coluna]
    serie_texto = serie_original.astype('string').str.strip()
    serie_numerica = pd.to_numeric(serie_original, errors='coerce')

    mascara_nan = serie_original.isna()
    mascara_vazio = serie_texto.eq('')
    mascara_numerico = serie_numerica.notna()
    mascara_nao_numerico = ~(mascara_nan | mascara_vazio | mascara_numerico)

    contagem_valores_originais = serie_texto.fillna('NaN').value_counts(dropna=False).to_dict()

    serie_padronizada = serie_texto.copy()
    serie_padronizada.loc[mascara_nan] = 'NaN'
    serie_padronizada.loc[mascara_vazio] = 'vazio'

    valores_numericos_padronizados = serie_numerica.loc[mascara_numerico].astype(int).astype(str)
    serie_padronizada.loc[mascara_numerico] = valores_numericos_padronizados

    contagem_valores_padronizados = serie_padronizada.value_counts(dropna=False).to_dict()

    formatos_diferentes = {}

    valores_originais_numericos = serie_texto.loc[mascara_numerico]

    for valor_original in valores_originais_numericos.unique():
        valor_convertido = pd.to_numeric(pd.Series([valor_original]), errors='coerce').iloc[0]
        valor_padronizado = str(int(valor_convertido))

        if valor_original != valor_padronizado:
            if valor_padronizado not in formatos_diferentes:
                formatos_diferentes[valor_padronizado] = []

            formatos_diferentes[valor_padronizado].append(valor_original)

    resultado['resumo_por_coluna'][coluna] = {
        'total_linhas': len(df),
        'total_nan': int(mascara_nan.sum()),
        'total_vazio': int(mascara_vazio.sum()),
        'total_numerico': int(mascara_numerico.sum()),
        'total_nao_numerico': int(mascara_nao_numerico.sum()),
        'contagem_valores_originais': contagem_valores_originais,
        'contagem_valores_padronizados': contagem_valores_padronizados,
        'formatos_diferentes_encontrados': formatos_diferentes
    }

    resultado['total_valores_numericos_validos'] = (
        resultado['total_valores_numericos_validos'] + int(mascara_numerico.sum())
    )

print('Verificando linha por linha de forma resumida...')

mascaras_nao_numerico = []
mascaras_numerico = []

for coluna in colunas_notas:
    serie_original = df[coluna]
    serie_texto = serie_original.astype('string').str.strip()
    serie_numerica = pd.to_numeric(serie_original, errors='coerce')

    mascara_nan = serie_original.isna()
    mascara_vazio = serie_texto.eq('')
    mascara_numerico = serie_numerica.notna()
    mascara_nao_numerico = ~(mascara_nan | mascara_vazio | mascara_numerico)

    mascaras_nao_numerico.append(mascara_nao_numerico)
    mascaras_numerico.append(mascara_numerico)

df_nao_numerico = pd.concat(mascaras_nao_numerico, axis=1)
df_numerico = pd.concat(mascaras_numerico, axis=1)

linhas_com_algum_nao_numerico = df_nao_numerico.any(axis=1)
linhas_com_algum_numerico = df_numerico.any(axis=1)
linhas_com_mistura = linhas_com_algum_nao_numerico & linhas_com_algum_numerico

exemplos_nao_numerico = df.loc[linhas_com_algum_nao_numerico, colunas_notas].head(20)
exemplos_mistura = df.loc[linhas_com_mistura, colunas_notas].head(20)

resultado['analise_linhas'] = {
    'total_linhas_com_algum_valor_nao_numerico': int(linhas_com_algum_nao_numerico.sum()),
    'total_linhas_com_algum_valor_numerico': int(linhas_com_algum_numerico.sum()),
    'total_linhas_com_mistura_de_numerico_e_nao_numerico': int(linhas_com_mistura.sum()),
    'exemplos_linhas_com_nao_numerico': exemplos_nao_numerico.fillna('NaN').to_dict(orient='records'),
    'exemplos_linhas_com_mistura': exemplos_mistura.fillna('NaN').to_dict(orient='records')
}

arquivo_json = pasta_saida / 'inspecao_notas.json'
arquivo_txt = pasta_saida / 'inspecao_notas.txt'

with open(arquivo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resultado, arquivo, ensure_ascii=False, indent=4)

linhas_txt = []
linhas_txt.append('INSPECAO DAS COLUNAS DE NOTA')
linhas_txt.append('')
linhas_txt.append(f"Total de linhas analisadas: {resultado['total_linhas']}")
linhas_txt.append(f"Total geral de valores numericos validos: {resultado['total_valores_numericos_validos']}")
linhas_txt.append('')

for coluna in colunas_notas:
    resumo_coluna = resultado['resumo_por_coluna'][coluna]
    linhas_txt.append(f'[{coluna}]')
    linhas_txt.append(f"Total de linhas: {resumo_coluna['total_linhas']}")
    linhas_txt.append(f"Total NaN: {resumo_coluna['total_nan']}")
    linhas_txt.append(f"Total vazio: {resumo_coluna['total_vazio']}")
    linhas_txt.append(f"Total numerico: {resumo_coluna['total_numerico']}")
    linhas_txt.append(f"Total nao numerico: {resumo_coluna['total_nao_numerico']}")
    linhas_txt.append('Contagem de valores originais:')

    for valor, quantidade in resumo_coluna['contagem_valores_originais'].items():
        linhas_txt.append(f'- {valor}: {quantidade}')

    linhas_txt.append('')
    linhas_txt.append('Contagem de valores padronizados:')

    for valor, quantidade in resumo_coluna['contagem_valores_padronizados'].items():
        linhas_txt.append(f'- {valor}: {quantidade}')

    linhas_txt.append('')
    linhas_txt.append('Formatos diferentes encontrados:')

    if resumo_coluna['formatos_diferentes_encontrados']:
        for valor_padronizado, formatos in resumo_coluna['formatos_diferentes_encontrados'].items():
            linhas_txt.append(f'- Valor padronizado {valor_padronizado}: {formatos}')
    else:
        linhas_txt.append('- Nenhum formato diferente encontrado')

    linhas_txt.append('')

linhas_txt.append('[ANALISE DE LINHAS]')
linhas_txt.append(
    'Linhas com algum valor numerico: '
    + str(resultado['analise_linhas']['total_linhas_com_algum_valor_numerico'])
)
linhas_txt.append(
    'Linhas com algum valor nao numerico: '
    + str(resultado['analise_linhas']['total_linhas_com_algum_valor_nao_numerico'])
)
linhas_txt.append(
    'Linhas com mistura de numerico e nao numerico: '
    + str(resultado['analise_linhas']['total_linhas_com_mistura_de_numerico_e_nao_numerico'])
)
linhas_txt.append('')
linhas_txt.append('Exemplos de linhas com algum valor nao numerico:')

for item in resultado['analise_linhas']['exemplos_linhas_com_nao_numerico']:
    linhas_txt.append(f'- {item}')

linhas_txt.append('')
linhas_txt.append('Exemplos de linhas com mistura de numerico e nao numerico:')

for item in resultado['analise_linhas']['exemplos_linhas_com_mistura']:
    linhas_txt.append(f'- {item}')

with open(arquivo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

print('Inspecao concluida com sucesso.')
print(f'Relatorio JSON: {arquivo_json}')
print(f'Relatorio TXT: {arquivo_txt}')
