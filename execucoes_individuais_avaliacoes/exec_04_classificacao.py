# -*- coding: utf-8 -*-
import json
import re
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/03_base_com_nota.csv')
arquivo_regras_classificacao = Path('utils/insumos/regra_classificacao.xlsx')
arquivo_saida = Path('data_exec_indiv/avaliacoes/04_base_com_classificacao.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_04_classificacao'
pasta_resumo_espelho = Path('saida_resumo') / 'exec_04_classificacao'
arquivo_resumo_json = pasta_resumo / 'exec_04_classificacao_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_04_classificacao_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_04_classificacao_resumo.csv'
arquivo_auditoria_csv = pasta_resumo / 'exec_04_classificacao_auditoria.csv'
arquivo_sobrescritas_csv = pasta_resumo / 'exec_04_classificacao_sobrescritas.csv'
arquivo_nao_classificados_detalhado_csv = (
    pasta_resumo / 'exec_04_classificacao_nao_classificados_detalhado.csv'
)

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

comparadores_validos = {
    'igual',
    'diferente',
    'em_lista',
    'fora_lista',
    'contem',
    'nao_contem',
    'contem_regex',
    'nao_contem_regex',
    'vazio',
    'nao_vazio',
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


def validar_regex(valor):
    try:
        re.compile(valor)
    except re.error:
        return False

    return True


def validar_regras(df_regras, colunas_base):
    erros = []

    if df_regras.empty:
        return ['Nenhuma regra ativa encontrada em utils/insumos/regra_classificacao.xlsx.']

    ordens_invalidas = df_regras['ordem'].isna()
    for _, regra in df_regras[ordens_invalidas].head(20).iterrows():
        erros.append(
            f"Linha {int(regra['_LINHA_EXCEL'])} da planilha com ordem vazia ou invalida."
        )

    for _, regra in df_regras.iterrows():
        linha_excel = int(regra['_LINHA_EXCEL'])

        if regra['chave_grupo'] == '':
            erros.append(f'Linha {linha_excel} da planilha com CHAVE_GRUPO vazia.')

        if regra['classificacao'] == '':
            erros.append(f'Linha {linha_excel} da planilha com CLASSIFICACAO vazia.')

        if regra['nome_lista'] == '':
            erros.append(f'Linha {linha_excel} da planilha com NOME_LISTA vazia.')

        for numero in range(1, 4):
            coluna = regra[f'coluna_{numero}']
            comparador = regra[f'comparador_{numero}']
            valor = regra[f'valor_{numero}']
            tem_algum_campo = any(
                str(campo).strip() != ''
                for campo in [coluna, comparador, valor]
                if not pd.isna(campo)
            )

            if not tem_algum_campo:
                continue

            if coluna == '':
                erros.append(
                    f"Linha {linha_excel} com COLUNA_FILTRO_{numero} vazia "
                    f"para a regra '{regra['descricao']}'."
                )
                continue

            if coluna not in colunas_base:
                erros.append(
                    f"Linha {linha_excel} usa coluna inexistente '{coluna}' "
                    f"na regra '{regra['descricao']}'."
                )

            if comparador not in comparadores_validos:
                erros.append(
                    f"Linha {linha_excel} usa comparador invalido '{comparador}' "
                    f"na regra '{regra['descricao']}'."
                )

            if comparador not in {'vazio', 'nao_vazio'} and valor == '':
                erros.append(
                    f"Linha {linha_excel} usa comparador '{comparador}' "
                    f"sem valor na regra '{regra['descricao']}'."
                )

            if comparador in {'contem_regex', 'nao_contem_regex'} and valor:
                if not validar_regex(valor):
                    erros.append(
                        f"Linha {linha_excel} possui regex invalido '{valor}' "
                        f"na regra '{regra['descricao']}'."
                    )

    return erros


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
    total_filtros = 0

    for numero in range(1, 4):
        coluna = regra[f'coluna_{numero}']
        comparador = regra[f'comparador_{numero}']
        valor = regra[f'valor_{numero}']

        if coluna == '' and comparador == '' and valor == '':
            continue

        total_filtros += 1
        mascara = mascara & montar_mascara_filtro(df_base, coluna, comparador, valor)

    if total_filtros == 0:
        return mascara

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

    if not partes:
        return 'sem filtros'

    return ' | '.join(partes)


def registrar_sobrescritas(df_base, filtro_aplicacao, regra, sobrescritas):
    mascara_sobrescritas = pd.Series(False, index=df_base.index)
    classificacao_antes_aplicacao = df_base.loc[filtro_aplicacao, 'CLASSIFICACAO'].copy()
    vazias_antes_aplicacao = (
        classificacao_antes_aplicacao.isna() | (classificacao_antes_aplicacao == '')
    )
    mascara_sobrescritas.loc[classificacao_antes_aplicacao.index] = ~vazias_antes_aplicacao
    sobrescritas_regra = df_base.loc[mascara_sobrescritas].copy()

    if sobrescritas_regra.empty:
        return 0

    resumo_sobrescritas = (
        sobrescritas_regra
        .assign(
            ORDEM_REGRA_ANTERIOR=sobrescritas_regra[coluna_ordem_regra]
            .astype('string')
            .fillna('DESCONHECIDA'),
            ORDEM_REGRA_NOVA=int(regra['ordem']),
            NOME_LISTA_ANTERIOR=sobrescritas_regra[coluna_nome_lista]
            .astype('string')
            .fillna('DESCONHECIDA'),
            NOME_LISTA_NOVA=regra['nome_lista'],
            CHAVE_GRUPO_ANTERIOR=sobrescritas_regra[coluna_chave_grupo]
            .astype('string')
            .fillna('DESCONHECIDA'),
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

    for _, linha in resumo_sobrescritas.iterrows():
        sobrescritas.append({
            'ORDEM_REGRA_ANTERIOR': linha['ORDEM_REGRA_ANTERIOR'],
            'ORDEM_REGRA_NOVA': int(linha['ORDEM_REGRA_NOVA']),
            'NOME_LISTA_ANTERIOR': linha['NOME_LISTA_ANTERIOR'],
            'NOME_LISTA_NOVA': linha['NOME_LISTA_NOVA'],
            'CHAVE_GRUPO_ANTERIOR': linha['CHAVE_GRUPO_ANTERIOR'],
            'CHAVE_GRUPO_NOVA': linha['CHAVE_GRUPO_NOVA'],
            'CLASSIFICACAO_ANTERIOR': linha['CLASSIFICACAO_ANTERIOR'],
            'CLASSIFICACAO_NOVA': linha['CLASSIFICACAO_NOVA'],
            'QUANTIDADE': int(linha['QUANTIDADE']),
        })

    return int(len(sobrescritas_regra))


print('Iniciando execucao 04 - classificacao por planilha de regras...')
print(f'Lendo arquivo da execucao 03: {arquivo_entrada}')
print(f'Lendo regras de classificacao: {arquivo_regras_classificacao}')

df = ler_csv_padronizado(arquivo_entrada)
df_regras = carregar_regras_classificacao(arquivo_regras_classificacao)

df['TIPO'] = pd.to_numeric(df['TIPO'], errors='coerce')
df['CONTRATACAO'] = normalizar_texto(df['CONTRATACAO']).str.lower()
df['LOCAL'] = normalizar_texto(df['LOCAL'])
df['ESPECIALIDADE'] = normalizar_texto(df['ESPECIALIDADE'])

if 'CLASSIFICACAO' not in df.columns:
    df['CLASSIFICACAO'] = None

df['CLASSIFICACAO'] = normalizar_texto(df['CLASSIFICACAO'])

erros_regras = validar_regras(df_regras, set(df.columns))
if erros_regras:
    print('ERRO - regra_classificacao.xlsx possui problemas:')
    for erro in erros_regras[:50]:
        print(f'- {erro}')
    if len(erros_regras) > 50:
        print(f'- ... mais {len(erros_regras) - 50} problema(s)')
    sys.exit(1)

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
    total_atingidas = int(filtro.sum())
    classificacao_antes = df.loc[filtro, 'CLASSIFICACAO'].copy()
    vazias_antes = classificacao_antes.isna() | (classificacao_antes == '')
    total_vazias_antes = int(vazias_antes.sum())
    total_ja_classificadas = int((~vazias_antes).sum())
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
        'CHAVE_GRUPO': regra['chave_grupo'],
        'CLASSIFICACAO': regra['classificacao'],
        'NOME_LISTA': regra['nome_lista'],
        'PALAVRA_FILTRO': regra['palavra_filtro'],
        'REGRA': descrever_regra(regra),
        'APLICAR_SOMENTE_VAZIO': 'sim' if regra['aplicar_somente_vazios'] else 'nao',
        'TOTAL_ATINGIDAS': total_atingidas,
        'TOTAL_CLASSIFICADAS_VAZIAS': total_vazias_antes,
        'TOTAL_JA_CLASSIFICADAS': total_ja_classificadas,
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

filtro_nao_classificados = df_saida['CLASSIFICACAO'].isna() | (df_saida['CLASSIFICACAO'] == '')
df_nao_classificados = df_saida[filtro_nao_classificados].copy()

resumo_nao_classificados = (
    df_nao_classificados['LOCAL']
    .fillna('VAZIO')
    .value_counts()
    .reset_index()
)
resumo_nao_classificados.columns = ['LOCAL', 'QUANTIDADE']
colunas_nao_classificados_detalhado = [
    'CDUSUARIO',
    'UF',
    'MES',
    'DIA',
    'ANO',
    'TIPO',
    'CONTRATACAO',
    'LOCAL',
    'ESPECIALIDADE',
]

df_nao_classificados_detalhado = df_nao_classificados[
    colunas_nao_classificados_detalhado
].copy()

total_classificadas = int((~filtro_nao_classificados).sum())
total_nao_classificadas = int(filtro_nao_classificados.sum())
total_sobrescritas_geral = int(sum(item['QUANTIDADE'] for item in sobrescritas))

resumo = {
    'execucao': 'exec_04_classificacao',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_regras_classificacao': str(arquivo_regras_classificacao),
    'arquivo_saida': str(arquivo_saida),
    'arquivo_auditoria': str(arquivo_auditoria_csv),
    'arquivo_sobrescritas': str(arquivo_sobrescritas_csv),
    'arquivo_nao_classificados_detalhado': str(arquivo_nao_classificados_detalhado_csv),
    'total_linhas_entrada': int(len(df)),
    'total_classificadas': total_classificadas,
    'total_nao_classificadas': total_nao_classificadas,
    'total_sobrescritas': total_sobrescritas_geral,
    'total_regras_ativas': int(len(df_regras)),
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 04 - CLASSIFICACAO',
    '',
    'ARQUIVOS:',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de regras: {resumo['arquivo_regras_classificacao']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    f"Auditoria por regra: {resumo['arquivo_auditoria']}",
    f"Sobrescritas detalhadas: {resumo['arquivo_sobrescritas']}",
    f"Nao classificados detalhado: {resumo['arquivo_nao_classificados_detalhado']}",
    '',
    'TOTAIS:',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total classificadas: {resumo['total_classificadas']}",
    f"Total nao classificadas: {resumo['total_nao_classificadas']}",
    f"Total sobrescritas: {resumo['total_sobrescritas']}",
    f"Total de regras ativas: {resumo['total_regras_ativas']}",
    '',
    'REGRAS COM MAIS LINHAS ATINGIDAS:',
]

for item in sorted(auditoria_regras, key=lambda regra: regra['TOTAL_ATINGIDAS'], reverse=True)[:10]:
    linhas_txt.append(
        f"- Regra {item['ORDEM_REGRA']} - {item['CLASSIFICACAO']}: "
        f"{item['TOTAL_ATINGIDAS']} linhas atingidas "
        f"({item['TOTAL_SOBRESCRITAS']} sobrescritas)"
    )

linhas_txt.append('')
linhas_txt.append('SOBRESCRITAS:')

if sobrescritas:
    linhas_txt.append(f"Total de linhas sobrescritas: {resumo['total_sobrescritas']}")
    linhas_txt.append(f"Detalhamento completo em: {resumo['arquivo_sobrescritas']}")
else:
    linhas_txt.append('- Nenhuma sobrescrita encontrada')

linhas_txt.append('')
linhas_txt.append('NAO CLASSIFICADAS - PRINCIPAIS LOCAIS:')

if not resumo_nao_classificados.empty:
    for _, linha in resumo_nao_classificados.head(50).iterrows():
        linhas_txt.append(f"- {linha['LOCAL']}: {int(linha['QUANTIDADE'])}")
else:
    linhas_txt.append('- Nenhuma linha sem classificacao')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

salvar_csv_padronizado(pd.DataFrame([{
    'EXECUCAO': resumo['execucao'],
    'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
    'ARQUIVO_REGRAS': resumo['arquivo_regras_classificacao'],
    'ARQUIVO_SAIDA': resumo['arquivo_saida'],
    'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
    'TOTAL_CLASSIFICADAS': resumo['total_classificadas'],
    'TOTAL_NAO_CLASSIFICADAS': resumo['total_nao_classificadas'],
    'TOTAL_SOBRESCRITAS': resumo['total_sobrescritas'],
    'TOTAL_REGRAS_ATIVAS': resumo['total_regras_ativas'],
}]), arquivo_resumo_csv)

df_auditoria = pd.DataFrame(auditoria_regras)
salvar_csv_padronizado(df_auditoria, arquivo_auditoria_csv)

colunas_sobrescritas = [
    'ORDEM_REGRA_ANTERIOR',
    'ORDEM_REGRA_NOVA',
    'NOME_LISTA_ANTERIOR',
    'NOME_LISTA_NOVA',
    'CHAVE_GRUPO_ANTERIOR',
    'CHAVE_GRUPO_NOVA',
    'CLASSIFICACAO_ANTERIOR',
    'CLASSIFICACAO_NOVA',
    'QUANTIDADE',
]
df_sobrescritas = pd.DataFrame(sobrescritas, columns=colunas_sobrescritas)
if not df_sobrescritas.empty:
    df_sobrescritas = df_sobrescritas.sort_values(
        ['QUANTIDADE', 'ORDEM_REGRA_NOVA'],
        ascending=[False, True],
    )
salvar_csv_padronizado(df_sobrescritas, arquivo_sobrescritas_csv)

salvar_csv_padronizado(df_nao_classificados_detalhado, arquivo_nao_classificados_detalhado_csv)

pasta_resumo_espelho.mkdir(parents=True, exist_ok=True)
for arquivo_resumo in [
    arquivo_resumo_json,
    arquivo_resumo_txt,
    arquivo_resumo_csv,
    arquivo_auditoria_csv,
    arquivo_sobrescritas_csv,
    arquivo_nao_classificados_detalhado_csv,
]:
    shutil.copy2(arquivo_resumo, pasta_resumo_espelho / arquivo_resumo.name)

print('Execucao 04 finalizada.')

