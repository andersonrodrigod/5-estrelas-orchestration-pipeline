# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

arquivo_entrada = Path('data_exec_indiv/05_base_com_local_editado.csv')
arquivo_grupos_operadora = Path('data/grupos_operadora.json')
arquivo_insumos_ndi = Path('utils/insumos/Unidades ndi sp e rj.xlsx')
arquivo_saida = Path('data_exec_indiv/06_base_com_operadora.csv')

pasta_resumo = Path('saida_resumo') / 'exec_06_operadora'
arquivo_resumo_json = pasta_resumo / 'exec_06_operadora_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_06_operadora_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_06_operadora_resumo.csv'
arquivo_operadora_distintos_csv = pasta_resumo / 'exec_06_operadora_local_editado_operadora.csv'
arquivo_nao_classificados_csv = pasta_resumo / 'exec_06_operadora_nao_classificados.csv'
arquivo_sobrescritos_csv = pasta_resumo / 'exec_06_operadora_sobrescritos.csv'
arquivo_hapvida_distintos_csv = pasta_resumo / 'exec_06_operadora_hapvida_distintos.csv'


def normalizar_texto(serie):
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

            if pd.isna(valor):
                item[coluna] = None
            else:
                item[coluna] = str(valor)

        registros.append(item)

    return registros


def carregar_json(caminho):
    with open(caminho, 'r', encoding='utf-8-sig') as arquivo:
        return json.load(arquivo)


def aplicar_regra(df_base, mascara, valor_operadora, nome_regra, sobrescritos):
    total_atingidas = int(mascara.sum())

    if total_atingidas == 0:
        return {
            'REGRA': nome_regra,
            'OPERADORA_APLICADA': valor_operadora,
            'TOTAL_ATINGIDAS': 0,
            'TOTAL_VAZIAS': 0,
            'TOTAL_SOBRESCRITAS': 0
        }

    operadora_anterior = df_base.loc[mascara, 'OPERADORA'].copy()
    vazias_antes = operadora_anterior.isna() | (operadora_anterior == '')
    total_vazias = int(vazias_antes.sum())
    total_sobrescritas = int((~vazias_antes).sum())

    if total_sobrescritas > 0:
        df_sobrescritos = df_base.loc[mascara & ~(
            df_base['OPERADORA'].isna() | (df_base['OPERADORA'] == '')
        ), ['LOCAL EDITADO', 'OPERADORA']].copy()

        df_sobrescritos['OPERADORA_ANTERIOR'] = df_sobrescritos['OPERADORA']
        df_sobrescritos['OPERADORA'] = valor_operadora

        sobrescritos.append(df_sobrescritos[['LOCAL EDITADO', 'OPERADORA', 'OPERADORA_ANTERIOR']])

    df_base.loc[mascara, 'OPERADORA'] = valor_operadora

    return {
        'REGRA': nome_regra,
        'OPERADORA_APLICADA': valor_operadora,
        'TOTAL_ATINGIDAS': total_atingidas,
        'TOTAL_VAZIAS': total_vazias,
        'TOTAL_SOBRESCRITAS': total_sobrescritas
    }


def montar_mascara(df_base, regra):
    if 'filtros' in regra:
        mascara = pd.Series(True, index=df_base.index)

        for filtro in regra['filtros']:
            coluna = filtro['coluna']
            tipo_filtro = filtro['tipo_filtro']
            valor_filtro = filtro['valor_filtro']

            if tipo_filtro == 'igual':
                mascara = mascara & (df_base[coluna] == valor_filtro)
            elif tipo_filtro == 'contem':
                mascara = mascara & df_base[coluna].str.contains(valor_filtro, case=False, na=False)
            else:
                raise ValueError(f"Tipo de filtro nao suportado: {tipo_filtro}")

        return mascara

    coluna = regra['coluna']
    tipo_filtro = regra['tipo_filtro']
    valor_filtro = regra['valor_filtro']

    if tipo_filtro == 'igual':
        return df_base[coluna] == valor_filtro

    if tipo_filtro == 'contem':
        return df_base[coluna].str.contains(valor_filtro, case=False, na=False)

    raise ValueError(f"Tipo de filtro nao suportado: {tipo_filtro}")


print('Iniciando execucao 06 - operadora...')
print(f'Lendo arquivo da execucao 05: {arquivo_entrada}')
print(f'Lendo arquivo de grupos: {arquivo_grupos_operadora}')
print(f'Lendo arquivo ndi: {arquivo_insumos_ndi}')

df = pd.read_csv(arquivo_entrada, low_memory=False)
grupos_operadora = carregar_json(arquivo_grupos_operadora)
df_ndi = pd.read_excel(arquivo_insumos_ndi, sheet_name='Planilha2')

df['CLASSIFICACAO'] = normalizar_texto(df['CLASSIFICACAO']).str.upper()
df['UF'] = normalizar_texto(df['UF']).str.upper()
df['LOCAL EDITADO'] = normalizar_texto(df['LOCAL EDITADO']).str.upper()
df['OPERADORA'] = normalizar_texto(df['OPERADORA']).str.upper()
df['CONTRATACAO'] = normalizar_texto(df['CONTRATACAO']).str.lower()
df['ESPECIALIDADE'] = normalizar_texto(df['ESPECIALIDADE'])

df_ndi['UNIDADE'] = normalizar_texto(df_ndi['UNIDADE']).str.upper()
df_ndi['OPERA'] = normalizar_texto(df_ndi['OPERA']).str.upper()
df_ndi = df_ndi.dropna(subset=['UNIDADE'])
df_ndi = df_ndi.drop_duplicates(subset=['UNIDADE'], keep='first')

if 'OPERADORA' not in df.columns:
    df['OPERADORA'] = None

regras_auditoria = []
sobrescritos = []

# Regras simples vindas do JSON.
for regra in grupos_operadora:
    mascara_regra = montar_mascara(df, regra)
    regras_auditoria.append(
        aplicar_regra(
            df,
            mascara_regra,
            regra['valor_operadora'],
            regra['descricao'],
            sobrescritos
        )
    )

# Regra especial do PA CONTORNO, apenas quando ainda estiver vazio.
mascara_pa_contorno_vazio = (
    (df['LOCAL EDITADO'] == 'PA CONTORNO') &
    (df['OPERADORA'].isna() | (df['OPERADORA'] == ''))
)
regras_auditoria.append(
    aplicar_regra(
        df,
        mascara_pa_contorno_vazio,
        'NDI MG',
        'LOCAL EDITADO igual a PA CONTORNO e OPERADORA vazia',
        sobrescritos
    )
)

# Regra por planilha externa NDI SP E RJ.
mapa_ndi = df_ndi.set_index('UNIDADE')['OPERA']
operadora_ndi = df['LOCAL EDITADO'].map(mapa_ndi)
mascara_ndi = operadora_ndi.notna() & (operadora_ndi != '')

if int(mascara_ndi.sum()) > 0:
    operadora_anterior_ndi = df.loc[mascara_ndi, 'OPERADORA'].copy()
    vazias_ndi = operadora_anterior_ndi.isna() | (operadora_anterior_ndi == '')
    total_sobrescritas_ndi = int((~vazias_ndi).sum())

    if total_sobrescritas_ndi > 0:
        df_sobrescritos_ndi = df.loc[mascara_ndi & ~(
            df['OPERADORA'].isna() | (df['OPERADORA'] == '')
        ), ['LOCAL EDITADO', 'OPERADORA']].copy()

        df_sobrescritos_ndi['OPERADORA_ANTERIOR'] = df_sobrescritos_ndi['OPERADORA']
        df_sobrescritos_ndi['OPERADORA'] = operadora_ndi.loc[df_sobrescritos_ndi.index]

        sobrescritos.append(df_sobrescritos_ndi[['LOCAL EDITADO', 'OPERADORA', 'OPERADORA_ANTERIOR']])

    df.loc[mascara_ndi, 'OPERADORA'] = operadora_ndi.loc[mascara_ndi]

    regras_auditoria.append({
        'REGRA': 'PLANILHA NDI SP E RJ',
        'OPERADORA_APLICADA': 'DINAMICA',
        'TOTAL_ATINGIDAS': int(mascara_ndi.sum()),
        'TOTAL_VAZIAS': int(vazias_ndi.sum()),
        'TOTAL_SOBRESCRITAS': total_sobrescritas_ndi
    })
else:
    regras_auditoria.append({
        'REGRA': 'PLANILHA NDI SP E RJ',
        'OPERADORA_APLICADA': 'DINAMICA',
        'TOTAL_ATINGIDAS': 0,
        'TOTAL_VAZIAS': 0,
        'TOTAL_SOBRESCRITAS': 0
    })

# Guarda quem ficou vazio antes do fechamento em HAPVIDA.
mascara_vazios_antes_hapvida = df['OPERADORA'].isna() | (df['OPERADORA'] == '')
df_nao_classificados = df.loc[
    mascara_vazios_antes_hapvida,
    ['UF', 'LOCAL EDITADO', 'CLASSIFICACAO', 'CONTRATACAO', 'ESPECIALIDADE']
].copy()

df_hapvida_distintos = (
    df.loc[mascara_vazios_antes_hapvida, ['LOCAL EDITADO', 'UF']]
    .value_counts()
    .reset_index(name='QUANTIDADE')
    .sort_values(['QUANTIDADE', 'LOCAL EDITADO'], ascending=[False, True])
)

# Fechamento final com HAPVIDA.
regras_auditoria.append(
    aplicar_regra(
        df,
        mascara_vazios_antes_hapvida,
        'HAPVIDA',
        'OPERADORA vazia no final recebe HAPVIDA',
        sobrescritos
    )
)

df['OPERADORA'] = normalizar_texto(df['OPERADORA']).str.upper()

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
df.to_csv(arquivo_saida, index=False, encoding='utf-8-sig')

df_operadora_distintos = (
    df[['LOCAL EDITADO', 'OPERADORA']]
    .value_counts()
    .reset_index(name='QUANTIDADE')
    .sort_values(['LOCAL EDITADO', 'OPERADORA'], ascending=[True, True])
)

df_locais_multiplas_operadoras = (
    df_operadora_distintos.groupby('LOCAL EDITADO')['OPERADORA']
    .nunique()
    .reset_index(name='TOTAL_OPERADORAS')
)
df_locais_multiplas_operadoras = df_locais_multiplas_operadoras[
    df_locais_multiplas_operadoras['TOTAL_OPERADORAS'] > 1
].copy()

if not df_locais_multiplas_operadoras.empty:
    df_locais_multiplas_operadoras = df_locais_multiplas_operadoras.merge(
        df_operadora_distintos,
        on='LOCAL EDITADO',
        how='left'
    ).sort_values(['LOCAL EDITADO', 'OPERADORA'])

if sobrescritos:
    df_sobrescritos = pd.concat(sobrescritos, ignore_index=True)
    df_sobrescritos = (
        df_sobrescritos[['LOCAL EDITADO', 'OPERADORA', 'OPERADORA_ANTERIOR']]
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'LOCAL EDITADO'], ascending=[False, True])
    )
else:
    df_sobrescritos = pd.DataFrame(columns=['LOCAL EDITADO', 'OPERADORA', 'OPERADORA_ANTERIOR', 'QUANTIDADE'])

total_classificadas_antes_hapvida = int((~mascara_vazios_antes_hapvida).sum())
total_hapvida = int(mascara_vazios_antes_hapvida.sum())
total_sobrescritos = int(df_sobrescritos['QUANTIDADE'].sum()) if not df_sobrescritos.empty else 0

resumo = {
    'execucao': 'exec_06_operadora',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_grupos_operadora': str(arquivo_grupos_operadora),
    'arquivo_insumos_ndi': str(arquivo_insumos_ndi),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_classificadas_antes_hapvida': total_classificadas_antes_hapvida,
    'total_preenchidas_com_hapvida': total_hapvida,
    'total_classificadas_final': int(len(df)),
    'total_sobrescritos': total_sobrescritos,
    'total_locais_multiplas_operadoras': int(df_locais_multiplas_operadoras['LOCAL EDITADO'].nunique()) if not df_locais_multiplas_operadoras.empty else 0,
    'locais_multiplas_operadoras': transformar_em_lista_registros(
        df_locais_multiplas_operadoras,
        ['LOCAL EDITADO', 'TOTAL_OPERADORAS', 'OPERADORA', 'QUANTIDADE']
    ),
    'regras_aplicadas': regras_auditoria
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 06 - OPERADORA',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de grupos: {resumo['arquivo_grupos_operadora']}",
    f"Arquivo ndi: {resumo['arquivo_insumos_ndi']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total classificadas antes do HAPVIDA: {resumo['total_classificadas_antes_hapvida']}",
    f"Total preenchidas com HAPVIDA: {resumo['total_preenchidas_com_hapvida']}",
    f"Total classificadas no final: {resumo['total_classificadas_final']}",
    f"Total sobrescritos: {resumo['total_sobrescritos']}",
    f"Total de locais com mais de uma operadora: {resumo['total_locais_multiplas_operadoras']}",
    '',
    'NAO CLASSIFICADAS ANTES DO HAPVIDA - LOCAL EDITADO E UF:'
]

if not df_hapvida_distintos.empty:
    for _, linha in df_hapvida_distintos.iterrows():
        linhas_txt.append(f"- {linha['LOCAL EDITADO']} - {linha['UF']}: {int(linha['QUANTIDADE'])}")
else:
    linhas_txt.append('- Nenhum registro ficou vazio antes do HAPVIDA')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

pd.DataFrame([{
    'EXECUCAO': resumo['execucao'],
    'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
    'ARQUIVO_GRUPOS': resumo['arquivo_grupos_operadora'],
    'ARQUIVO_NDI': resumo['arquivo_insumos_ndi'],
    'ARQUIVO_SAIDA': resumo['arquivo_saida'],
    'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
    'TOTAL_CLASSIFICADAS_ANTES_HAPVIDA': resumo['total_classificadas_antes_hapvida'],
    'TOTAL_PREENCHIDAS_COM_HAPVIDA': resumo['total_preenchidas_com_hapvida'],
    'TOTAL_CLASSIFICADAS_FINAL': resumo['total_classificadas_final'],
    'TOTAL_SOBRESCRITOS': resumo['total_sobrescritos'],
    'TOTAL_LOCAIS_MULTIPLAS_OPERADORAS': resumo['total_locais_multiplas_operadoras']
}]).to_csv(arquivo_resumo_csv, index=False, encoding='utf-8-sig')

df_operadora_distintos.to_csv(arquivo_operadora_distintos_csv, index=False, encoding='utf-8-sig')
df_nao_classificados.to_csv(arquivo_nao_classificados_csv, index=False, encoding='utf-8-sig')
df_sobrescritos.to_csv(arquivo_sobrescritos_csv, index=False, encoding='utf-8-sig')
df_hapvida_distintos.to_csv(arquivo_hapvida_distintos_csv, index=False, encoding='utf-8-sig')

print(f'Total de linhas recebidas: {len(df)}')
print(f'Total classificadas antes do HAPVIDA: {total_classificadas_antes_hapvida}')
print(f'Total preenchidas com HAPVIDA: {total_hapvida}')
print(f'Total sobrescritos: {total_sobrescritos}')
print('Execucao 06 finalizada.')
