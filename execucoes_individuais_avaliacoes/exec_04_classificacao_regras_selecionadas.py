# -*- coding: utf-8 -*-
import json
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

# Edite aqui as regras que voce quer executar, usando a coluna ORDEM da planilha.
# Exemplo: [16, 17] executa primeiro a regra 16 e depois a regra 17.
REGRAS_PARA_EXECUTAR = [16, 17]

arquivo_entrada = Path('data_exec_indiv/avaliacoes/03_base_com_nota.csv')
arquivo_regras_classificacao = Path('utils/insumos/regra_classificacao.xlsx')
arquivo_saida = Path('data_exec_indiv/avaliacoes/04_base_com_classificacao_regras_selecionadas.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_04_classificacao_regras_selecionadas'
arquivo_resumo_json = pasta_resumo / 'exec_04_classificacao_regras_selecionadas_resumo.json'
arquivo_resumo_csv = pasta_resumo / 'exec_04_classificacao_regras_selecionadas_resumo.csv'
arquivo_auditoria_csv = pasta_resumo / 'exec_04_classificacao_regras_selecionadas_auditoria.csv'
arquivo_sobrescritas_csv = pasta_resumo / 'exec_04_classificacao_regras_selecionadas_sobrescritas.csv'

coluna_ordem_regra = '_ORDEM_REGRA_CLASSIFICACAO'
coluna_nome_lista = '_NOME_LISTA_CLASSIFICACAO'
coluna_chave_grupo = '_CHAVE_GRUPO_CLASSIFICACAO'

colunas_regras = [
    'CHAVE_GRUPO',
    'CLASSIFICACAO',
    'NOME_LISTA',
    'PALAVRA_FILTRO',
    'APLICAR_SOMENTE_VAZIO',
    'COLUNA_FILTRO_1',
    'COMPARADOR_1',
    'VALOR_1',
    'COLUNA_FILTRO_2',
    'COMPARADOR_2',
    'VALOR_2',
    'COLUNA_FILTRO_3',
    'COMPARADOR_3',
    'VALOR_3',
    'ORDEM',
    'STATUS_ATIVO',
    'DESCRICAO',
]

mapa_colunas_regras = {
    'CHAVE_GRUPO': 'chave_grupo',
    'CLASSIFICACAO': 'classificacao',
    'NOME_LISTA': 'nome_lista',
    'PALAVRA_FILTRO': 'palavra_filtro',
    'APLICAR_SOMENTE_VAZIO': 'aplicar_somente_vazios',
    'COLUNA_FILTRO_1': 'coluna_1',
    'COMPARADOR_1': 'comparador_1',
    'VALOR_1': 'valor_1',
    'COLUNA_FILTRO_2': 'coluna_2',
    'COMPARADOR_2': 'comparador_2',
    'VALOR_2': 'valor_2',
    'COLUNA_FILTRO_3': 'coluna_3',
    'COMPARADOR_3': 'comparador_3',
    'VALOR_3': 'valor_3',
    'ORDEM': 'ordem',
    'STATUS_ATIVO': 'ativo',
    'DESCRICAO': 'descricao',
}


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip().fillna('')


def normalizar_flag(valor):
    if pd.isna(valor):
        return False

    return str(valor).strip().lower() in {'sim', 's', 'true', '1', 'yes'}


def carregar_regras_classificacao(caminho):
    df_regras = pd.read_excel(caminho, sheet_name='regras_classificacao')
    df_regras['_LINHA_EXCEL'] = df_regras.index + 2

    for coluna in colunas_regras:
        if coluna not in df_regras.columns:
            df_regras[coluna] = pd.NA

    df_regras = df_regras[colunas_regras + ['_LINHA_EXCEL']].copy()
    df_regras = df_regras.rename(columns=mapa_colunas_regras)
    df_regras['ativo'] = df_regras['ativo'].apply(normalizar_flag)
    df_regras['aplicar_somente_vazios'] = (
        df_regras['aplicar_somente_vazios'].apply(normalizar_flag)
    )
    df_regras['ordem'] = pd.to_numeric(df_regras['ordem'], errors='coerce')
    df_regras = df_regras[df_regras['ativo']].copy()
    df_regras = df_regras.sort_values('ordem', kind='stable')

    for coluna in [
        'chave_grupo',
        'classificacao',
        'nome_lista',
        'palavra_filtro',
        'descricao',
        'coluna_1',
        'comparador_1',
        'valor_1',
        'coluna_2',
        'comparador_2',
        'valor_2',
        'coluna_3',
        'comparador_3',
        'valor_3',
    ]:
        df_regras[coluna] = normalizar_texto(df_regras[coluna])

    return df_regras.reset_index(drop=True)


def selecionar_regras(df_regras, ordens):
    ordens_unicas = list(dict.fromkeys(ordens))
    regras = df_regras[df_regras['ordem'].isin(ordens_unicas)].copy()
    encontradas = set(regras['ordem'].astype(int))
    nao_encontradas = [ordem for ordem in ordens_unicas if ordem not in encontradas]

    if nao_encontradas:
        print('ERRO - ORDEM nao encontrada ou inativa: ' + str(nao_encontradas))
        sys.exit(1)

    ordem_execucao = pd.Categorical(regras['ordem'].astype(int), categories=ordens_unicas, ordered=True)
    regras = regras.assign(_ORDEM_EXECUCAO=ordem_execucao)
    return regras.sort_values('_ORDEM_EXECUCAO').drop(columns=['_ORDEM_EXECUCAO'])


def separar_lista(valor):
    if ';' in valor:
        return [item.strip() for item in valor.split(';') if item.strip() != '']

    return [valor]


def montar_mascara_tipo(df_base, comparador, valor):
    serie = pd.to_numeric(df_base['TIPO'], errors='coerce')

    if comparador in {'igual', 'diferente'}:
        valor_numerico = pd.to_numeric(pd.Series([valor]), errors='coerce').iloc[0]
        if comparador == 'igual':
            return serie == valor_numerico

        return serie != valor_numerico

    if comparador in {'em_lista', 'fora_lista'}:
        valores = pd.to_numeric(pd.Series(separar_lista(valor)), errors='coerce')
        if comparador == 'em_lista':
            return serie.isin(valores)

        return ~serie.isin(valores)

    return montar_mascara_texto(df_base, 'TIPO', comparador, valor)


def montar_mascara_texto(df_base, coluna, comparador, valor):
    serie = normalizar_texto(df_base[coluna])

    if comparador == 'igual':
        return serie == valor

    if comparador == 'diferente':
        return serie != valor

    if comparador == 'em_lista':
        return serie.isin(separar_lista(valor))

    if comparador == 'fora_lista':
        return ~serie.isin(separar_lista(valor))

    if comparador == 'contem':
        return serie.str.contains(valor, case=False, na=False, regex=False)

    if comparador == 'nao_contem':
        return ~serie.str.contains(valor, case=False, na=False, regex=False)

    if comparador == 'contem_regex':
        return serie.str.contains(valor, case=False, na=False, regex=True)

    if comparador == 'nao_contem_regex':
        return ~serie.str.contains(valor, case=False, na=False, regex=True)

    if comparador == 'vazio':
        return serie == ''

    if comparador == 'nao_vazio':
        return serie != ''

    raise ValueError(f"Comparador nao suportado: {comparador}")


def montar_mascara_filtro(df_base, coluna, comparador, valor):
    if coluna == 'TIPO':
        return montar_mascara_tipo(df_base, comparador, valor)

    return montar_mascara_texto(df_base, coluna, comparador, valor)


def montar_mascara_regra(df_base, regra):
    mascara = pd.Series(True, index=df_base.index)

    for numero in range(1, 4):
        coluna = regra[f'coluna_{numero}']
        comparador = regra[f'comparador_{numero}']
        valor = regra[f'valor_{numero}']

        if coluna == '' and comparador == '' and valor == '':
            continue

        mascara = mascara & montar_mascara_filtro(df_base, coluna, comparador, valor)

    return mascara


def descrever_regra(regra):
    if regra['descricao'] != '':
        return regra['descricao']

    partes = []

    for numero in range(1, 4):
        coluna = regra[f'coluna_{numero}']
        comparador = regra[f'comparador_{numero}']
        valor = regra[f'valor_{numero}']

        if coluna == '' and comparador == '' and valor == '':
            continue

        partes.append(f'{coluna} {comparador} {valor}'.strip())

    if regra['aplicar_somente_vazios']:
        partes.append('CLASSIFICACAO vazia')

    return ' | '.join(partes) if partes else 'sem filtros'


def registrar_sobrescritas(df_base, filtro_aplicacao, regra, sobrescritas):
    classificacao_antes = df_base.loc[filtro_aplicacao, 'CLASSIFICACAO'].copy()
    vazias_antes = classificacao_antes.isna() | (classificacao_antes == '')
    sobrescritas_regra = df_base.loc[classificacao_antes.index[~vazias_antes]].copy()

    if sobrescritas_regra.empty:
        return 0

    resumo_sobrescritas = (
        sobrescritas_regra
        .assign(
            ORDEM_REGRA_ANTERIOR=sobrescritas_regra[coluna_ordem_regra].astype('string').fillna('DESCONHECIDA'),
            ORDEM_REGRA_NOVA=int(regra['ordem']),
            NOME_LISTA_ANTERIOR=sobrescritas_regra[coluna_nome_lista].astype('string').fillna('DESCONHECIDA'),
            NOME_LISTA_NOVA=regra['nome_lista'],
            CHAVE_GRUPO_ANTERIOR=sobrescritas_regra[coluna_chave_grupo].astype('string').fillna('DESCONHECIDA'),
            CHAVE_GRUPO_NOVA=regra['chave_grupo'],
            CLASSIFICACAO_ANTERIOR=sobrescritas_regra['CLASSIFICACAO'].fillna('VAZIO'),
            CLASSIFICACAO_NOVA=regra['classificacao'],
        )
        .groupby([
            'ORDEM_REGRA_ANTERIOR',
            'ORDEM_REGRA_NOVA',
            'NOME_LISTA_ANTERIOR',
            'NOME_LISTA_NOVA',
            'CHAVE_GRUPO_ANTERIOR',
            'CHAVE_GRUPO_NOVA',
            'CLASSIFICACAO_ANTERIOR',
            'CLASSIFICACAO_NOVA',
        ], dropna=False)
        .size()
        .reset_index(name='QUANTIDADE')
    )

    sobrescritas.extend(resumo_sobrescritas.to_dict('records'))
    return int(len(sobrescritas_regra))


print('Iniciando execucao 04 - regras selecionadas...')
print(f'Regras selecionadas: {REGRAS_PARA_EXECUTAR}')
print(f'Lendo base: {arquivo_entrada}')
print(f'Lendo regras: {arquivo_regras_classificacao}')

df = ler_csv_padronizado(arquivo_entrada)
df_regras = carregar_regras_classificacao(arquivo_regras_classificacao)
df_regras = selecionar_regras(df_regras, REGRAS_PARA_EXECUTAR)

df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
df['CONTRATACAO'] = normalizar_texto(df['CONTRATACAO']).str.lower()
df['LOCAL'] = normalizar_texto(df['LOCAL'])
df['ESPECIALIDADE'] = normalizar_texto(df['ESPECIALIDADE'])

if 'CLASSIFICACAO' not in df.columns:
    df['CLASSIFICACAO'] = None

df['CLASSIFICACAO'] = normalizar_texto(df['CLASSIFICACAO'])
df[coluna_ordem_regra] = pd.NA
df[coluna_nome_lista] = pd.NA
df[coluna_chave_grupo] = pd.NA

classificadas_antes_execucao = df['CLASSIFICACAO'].notna() & (df['CLASSIFICACAO'] != '')
df.loc[classificadas_antes_execucao, coluna_ordem_regra] = 'ORIGINAL'
df.loc[classificadas_antes_execucao, coluna_nome_lista] = 'BASE DE ENTRADA'
df.loc[classificadas_antes_execucao, coluna_chave_grupo] = 'CLASSIFICACAO_PRE_EXISTENTE'

auditoria_regras = []
sobrescritas = []

for _, regra in df_regras.iterrows():
    filtro = montar_mascara_regra(df, regra)
    classificacao_antes = df.loc[filtro, 'CLASSIFICACAO'].copy()
    vazias_antes = classificacao_antes.isna() | (classificacao_antes == '')
    filtro_aplicacao = filtro

    if regra['aplicar_somente_vazios']:
        filtro_aplicacao = filtro & (
            df['CLASSIFICACAO'].isna() | (df['CLASSIFICACAO'] == '')
        )

    total_sobrescritas = registrar_sobrescritas(
        df,
        filtro_aplicacao,
        regra,
        sobrescritas,
    )

    df.loc[filtro_aplicacao, 'CLASSIFICACAO'] = regra['classificacao']
    df.loc[filtro_aplicacao, coluna_ordem_regra] = int(regra['ordem'])
    df.loc[filtro_aplicacao, coluna_nome_lista] = regra['nome_lista']
    df.loc[filtro_aplicacao, coluna_chave_grupo] = regra['chave_grupo']

    auditoria_regras.append({
        'ORDEM_REGRA': int(regra['ordem']),
        'NOME_LISTA': regra['nome_lista'],
        'CHAVE_GRUPO': regra['chave_grupo'],
        'CLASSIFICACAO': regra['classificacao'],
        'REGRA': descrever_regra(regra),
        'APLICAR_SOMENTE_VAZIO': 'sim' if regra['aplicar_somente_vazios'] else 'nao',
        'TOTAL_ATINGIDAS': int(filtro.sum()),
        'TOTAL_CLASSIFICADAS_VAZIAS': int(vazias_antes.sum()),
        'TOTAL_JA_CLASSIFICADAS': int((~vazias_antes).sum()),
        'TOTAL_APLICADAS': int(filtro_aplicacao.sum()),
        'TOTAL_SOBRESCRITAS': total_sobrescritas,
    })

df_saida = df.drop(columns=[
    coluna_ordem_regra,
    coluna_nome_lista,
    coluna_chave_grupo,
]).copy()

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)

salvar_csv_padronizado(df_saida, arquivo_saida)
salvar_csv_padronizado(pd.DataFrame(auditoria_regras), arquivo_auditoria_csv)
salvar_csv_padronizado(pd.DataFrame(sobrescritas), arquivo_sobrescritas_csv)

resumo = {
    'execucao': 'exec_04_classificacao_regras_selecionadas',
    'regras_para_executar': REGRAS_PARA_EXECUTAR,
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_regras_classificacao': str(arquivo_regras_classificacao),
    'arquivo_saida': str(arquivo_saida),
    'arquivo_auditoria': str(arquivo_auditoria_csv),
    'arquivo_sobrescritas': str(arquivo_sobrescritas_csv),
    'total_linhas_entrada': int(len(df)),
    'total_regras_executadas': int(len(df_regras)),
    'total_sobrescritas': int(sum(item['QUANTIDADE'] for item in sobrescritas)),
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

salvar_csv_padronizado(pd.DataFrame([{
    'EXECUCAO': resumo['execucao'],
    'REGRAS_PARA_EXECUTAR': ','.join(str(item) for item in REGRAS_PARA_EXECUTAR),
    'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
    'ARQUIVO_REGRAS': resumo['arquivo_regras_classificacao'],
    'ARQUIVO_SAIDA': resumo['arquivo_saida'],
    'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
    'TOTAL_REGRAS_EXECUTADAS': resumo['total_regras_executadas'],
    'TOTAL_SOBRESCRITAS': resumo['total_sobrescritas'],
}]), arquivo_resumo_csv)

print(f'Arquivo gerado: {arquivo_saida}')
print(f'Auditoria gerada: {arquivo_auditoria_csv}')
print(f'Sobrescritas geradas: {arquivo_sobrescritas_csv}')
print('Execucao 04 de regras selecionadas finalizada.')
