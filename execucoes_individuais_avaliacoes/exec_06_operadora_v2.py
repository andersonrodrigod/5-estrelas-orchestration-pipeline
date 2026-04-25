# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado

arquivo_entrada = Path('data_exec_indiv/avaliacoes/05_base_com_local_editado.csv')
arquivo_regras_operadora = Path('utils/insumos/regras_operadora.xlsx')
arquivo_saida = Path('data_exec_indiv/avaliacoes/06_base_com_operadora_v2.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_06_operadora_v2'
arquivo_resumo_json = pasta_resumo / 'exec_06_operadora_v2_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_06_operadora_v2_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_06_operadora_v2_resumo.csv'
arquivo_operadora_distintos_csv = pasta_resumo / 'exec_06_operadora_v2_local_editado_operadora.csv'
arquivo_nao_classificados_csv = pasta_resumo / 'exec_06_operadora_v2_nao_classificados.csv'
arquivo_sobrescritos_csv = pasta_resumo / 'exec_06_operadora_v2_sobrescritos.csv'
arquivo_hapvida_distintos_csv = pasta_resumo / 'exec_06_operadora_v2_hapvida_distintos.csv'

colunas_regras = [
    'OPERADORA',
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
    'OPERADORA': 'operadora',
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
    'contem',
    'diferente',
    'nao_contem',
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


def carregar_regras_operadora(caminho):
    df_regras = pd.read_excel(caminho, sheet_name='regras_operadora')

    for coluna in colunas_regras:
        if coluna not in df_regras.columns:
            df_regras[coluna] = pd.NA

    df_regras = df_regras[colunas_regras].copy()
    df_regras = df_regras.rename(columns=mapa_colunas_regras)
    df_regras['ativo'] = df_regras['ativo'].apply(normalizar_flag)
    df_regras['aplicar_somente_vazios'] = (
        df_regras['aplicar_somente_vazios'].apply(normalizar_flag)
    )
    df_regras['ordem'] = pd.to_numeric(df_regras['ordem'], errors='coerce')
    df_regras = df_regras[df_regras['ativo']].copy()
    df_regras = df_regras.sort_values('ordem', kind='stable')

    for coluna in [
        'descricao',
        'operadora',
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

    df_regras['operadora'] = df_regras['operadora'].str.upper()
    return df_regras.reset_index(drop=True)


def validar_regras(df_regras, colunas_base):
    erros = []

    if df_regras.empty:
        return ['Nenhuma regra ativa encontrada em utils/insumos/regras_operadora.xlsx.']

    ordens_invalidas = df_regras['ordem'].isna()
    for indice in df_regras.index[ordens_invalidas][:20]:
        erros.append(f'Linha {indice + 2} da planilha com ordem vazia ou invalida.')

    operadoras_vazias = df_regras['operadora'].fillna('') == ''
    for indice in df_regras.index[operadoras_vazias][:20]:
        erros.append(f'Linha {indice + 2} da planilha com operadora vazia.')

    for indice, regra in df_regras.iterrows():
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
                    f"Linha {indice + 2} com coluna_{numero} vazia "
                    f"para a regra '{regra['descricao']}'."
                )
                continue

            if coluna not in colunas_base:
                erros.append(
                    f"Linha {indice + 2} usa coluna inexistente '{coluna}' "
                    f"na regra '{regra['descricao']}'."
                )

            if comparador not in comparadores_validos:
                erros.append(
                    f"Linha {indice + 2} usa comparador invalido '{comparador}' "
                    f"na regra '{regra['descricao']}'."
                )

            if comparador not in {'vazio', 'nao_vazio'} and valor == '':
                erros.append(
                    f"Linha {indice + 2} usa comparador '{comparador}' "
                    f"sem valor_filtro na regra '{regra['descricao']}'."
                )

    return erros


def montar_mascara_filtro(df_base, coluna, comparador, valor):
    serie = normalizar_texto(df_base[coluna])

    if comparador == 'igual':
        return serie == valor

    if comparador == 'contem':
        return serie.str.contains(valor, case=False, na=False, regex=False)

    if comparador == 'diferente':
        return serie != valor

    if comparador == 'nao_contem':
        return ~serie.str.contains(valor, case=False, na=False, regex=False)

    if comparador == 'vazio':
        return serie == ''

    if comparador == 'nao_vazio':
        return serie != ''

    raise ValueError(f"Comparador nao suportado: {comparador}")


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


def aplicar_regra(df_base, mascara, regra, sobrescritos):
    valor_operadora = regra['operadora']
    nome_regra = regra['descricao']

    if regra['aplicar_somente_vazios']:
        mascara = mascara & (df_base['OPERADORA'].isna() | (df_base['OPERADORA'] == ''))

    total_atingidas = int(mascara.sum())

    if total_atingidas == 0:
        return {
            'ORDEM': int(regra['ordem']),
            'REGRA': nome_regra,
            'OPERADORA_APLICADA': valor_operadora,
            'APLICAR_SOMENTE_VAZIOS': 'sim' if regra['aplicar_somente_vazios'] else 'nao',
            'TOTAL_ATINGIDAS': 0,
            'TOTAL_VAZIAS': 0,
            'TOTAL_SOBRESCRITAS': 0,
        }

    operadora_anterior = df_base.loc[mascara, 'OPERADORA'].copy()
    vazias_antes = operadora_anterior.isna() | (operadora_anterior == '')
    total_vazias = int(vazias_antes.sum())
    total_sobrescritas = int((~vazias_antes).sum())

    if total_sobrescritas > 0:
        df_sobrescritos = df_base.loc[
            mascara & ~(df_base['OPERADORA'].isna() | (df_base['OPERADORA'] == '')),
            ['LOCAL EDITADO', 'OPERADORA'],
        ].copy()

        df_sobrescritos['OPERADORA_ANTERIOR'] = df_sobrescritos['OPERADORA']
        df_sobrescritos['OPERADORA'] = valor_operadora
        df_sobrescritos['REGRA'] = nome_regra

        sobrescritos.append(
            df_sobrescritos[
                ['LOCAL EDITADO', 'OPERADORA', 'OPERADORA_ANTERIOR', 'REGRA']
            ]
        )

    df_base.loc[mascara, 'OPERADORA'] = valor_operadora

    return {
        'ORDEM': int(regra['ordem']),
        'REGRA': nome_regra,
        'OPERADORA_APLICADA': valor_operadora,
        'APLICAR_SOMENTE_VAZIOS': 'sim' if regra['aplicar_somente_vazios'] else 'nao',
        'TOTAL_ATINGIDAS': total_atingidas,
        'TOTAL_VAZIAS': total_vazias,
        'TOTAL_SOBRESCRITAS': total_sobrescritas,
    }


def regra_mapa_local_editado(regra):
    return (
        regra['operadora'] == 'NDI SP E RJ'
        and not regra['aplicar_somente_vazios']
        and regra['coluna_1'] == 'LOCAL EDITADO'
        and regra['comparador_1'] == 'igual'
        and regra['valor_1'] != ''
        and all(regra[f'coluna_{numero}'] == '' for numero in range(2, 4))
    )


def aplicar_bloco_mapa_local_editado(df_base, df_bloco, sobrescritos):
    mapa_operadora = df_bloco.set_index('valor_1')['operadora']
    operadora_nova = df_base['LOCAL EDITADO'].map(mapa_operadora)
    mascara = operadora_nova.notna() & (operadora_nova != '')
    operadora_anterior = df_base.loc[mascara, 'OPERADORA'].copy()
    vazias_antes = operadora_anterior.isna() | (operadora_anterior == '')
    total_sobrescritas = int((~vazias_antes).sum())

    if total_sobrescritas > 0:
        df_sobrescritos = df_base.loc[
            mascara & ~(df_base['OPERADORA'].isna() | (df_base['OPERADORA'] == '')),
            ['LOCAL EDITADO', 'OPERADORA'],
        ].copy()
        df_sobrescritos['OPERADORA_ANTERIOR'] = df_sobrescritos['OPERADORA']
        df_sobrescritos['OPERADORA'] = operadora_nova.loc[df_sobrescritos.index]
        df_sobrescritos['REGRA'] = 'BLOCO PLANILHA NDI SP E RJ'
        sobrescritos.append(
            df_sobrescritos[
                ['LOCAL EDITADO', 'OPERADORA', 'OPERADORA_ANTERIOR', 'REGRA']
            ]
        )

    contagem_local = df_base['LOCAL EDITADO'].value_counts().to_dict()
    vazias_por_local = df_base.loc[
        mascara & (df_base['OPERADORA'].isna() | (df_base['OPERADORA'] == '')),
        'LOCAL EDITADO',
    ].value_counts().to_dict()
    sobrescritas_por_local = df_base.loc[
        mascara & ~(df_base['OPERADORA'].isna() | (df_base['OPERADORA'] == '')),
        'LOCAL EDITADO',
    ].value_counts().to_dict()

    df_base.loc[mascara, 'OPERADORA'] = operadora_nova.loc[mascara]

    auditoria = []
    for _, regra in df_bloco.iterrows():
        local = regra['valor_1']
        total_atingidas = int(contagem_local.get(local, 0))
        auditoria.append({
            'ORDEM': int(regra['ordem']),
            'REGRA': regra['descricao'],
            'OPERADORA_APLICADA': regra['operadora'],
            'APLICAR_SOMENTE_VAZIOS': 'nao',
            'TOTAL_ATINGIDAS': total_atingidas,
            'TOTAL_VAZIAS': int(vazias_por_local.get(local, 0)),
            'TOTAL_SOBRESCRITAS': int(sobrescritas_por_local.get(local, 0)),
        })

    return auditoria


def regra_fechamento_hapvida(regra):
    return (
        regra['operadora'] == 'HAPVIDA'
        and regra['aplicar_somente_vazios']
        and all(regra[f'coluna_{numero}'] == '' for numero in range(1, 4))
    )


print('Iniciando execucao 06 v2 - operadora por planilha de regras...')
print(f'Lendo arquivo da execucao 05: {arquivo_entrada}')
print(f'Lendo regras de operadora: {arquivo_regras_operadora}')

df = ler_csv_padronizado(arquivo_entrada)
df_regras = carregar_regras_operadora(arquivo_regras_operadora)

df['CLASSIFICACAO'] = normalizar_texto(df['CLASSIFICACAO']).str.upper()
df['UF'] = normalizar_texto(df['UF']).str.upper()
df['LOCAL EDITADO'] = normalizar_texto(df['LOCAL EDITADO']).str.upper()
df['OPERADORA'] = normalizar_texto(df['OPERADORA']).str.upper()
df['CONTRATACAO'] = normalizar_texto(df['CONTRATACAO']).str.lower()
df['ESPECIALIDADE'] = normalizar_texto(df['ESPECIALIDADE'])

if 'OPERADORA' not in df.columns:
    df['OPERADORA'] = None

erros_regras = validar_regras(df_regras, set(df.columns))
if erros_regras:
    print('ERRO - regras_operadora.xlsx possui problemas:')
    for erro in erros_regras[:50]:
        print(f'- {erro}')
    if len(erros_regras) > 50:
        print(f'- ... mais {len(erros_regras) - 50} problema(s)')
    sys.exit(1)

regras_auditoria = []
sobrescritos = []
df_nao_classificados = pd.DataFrame(
    columns=['UF', 'LOCAL EDITADO', 'CLASSIFICACAO', 'CONTRATACAO', 'ESPECIALIDADE']
)
df_hapvida_distintos = pd.DataFrame(columns=['LOCAL EDITADO', 'UF', 'QUANTIDADE'])

indice_regra = 0
while indice_regra < len(df_regras):
    regra = df_regras.iloc[indice_regra]

    if regra_mapa_local_editado(regra):
        inicio_bloco = indice_regra
        while (
            indice_regra < len(df_regras)
            and regra_mapa_local_editado(df_regras.iloc[indice_regra])
        ):
            indice_regra += 1

        df_bloco = df_regras.iloc[inicio_bloco:indice_regra].copy()
        regras_auditoria.extend(
            aplicar_bloco_mapa_local_editado(df, df_bloco, sobrescritos)
        )
        continue

    if regra_fechamento_hapvida(regra):
        mascara_vazios_antes_hapvida = df['OPERADORA'].isna() | (df['OPERADORA'] == '')
        df_nao_classificados = df.loc[
            mascara_vazios_antes_hapvida,
            ['UF', 'LOCAL EDITADO', 'CLASSIFICACAO', 'CONTRATACAO', 'ESPECIALIDADE'],
        ].copy()

        df_hapvida_distintos = (
            df.loc[mascara_vazios_antes_hapvida, ['LOCAL EDITADO', 'UF']]
            .value_counts()
            .reset_index(name='QUANTIDADE')
            .sort_values(['QUANTIDADE', 'LOCAL EDITADO'], ascending=[False, True])
        )

    mascara_regra = montar_mascara_regra(df, regra)
    regras_auditoria.append(aplicar_regra(df, mascara_regra, regra, sobrescritos))
    indice_regra += 1

df['OPERADORA'] = normalizar_texto(df['OPERADORA']).str.upper()

arquivo_saida.parent.mkdir(exist_ok=True)
pasta_resumo.mkdir(parents=True, exist_ok=True)
salvar_csv_padronizado(df, arquivo_saida)

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
        how='left',
    ).sort_values(['LOCAL EDITADO', 'OPERADORA'])

if sobrescritos:
    df_sobrescritos = pd.concat(sobrescritos, ignore_index=True)
    df_sobrescritos = (
        df_sobrescritos[
            ['LOCAL EDITADO', 'OPERADORA', 'OPERADORA_ANTERIOR', 'REGRA']
        ]
        .value_counts()
        .reset_index(name='QUANTIDADE')
        .sort_values(['QUANTIDADE', 'LOCAL EDITADO'], ascending=[False, True])
    )
else:
    df_sobrescritos = pd.DataFrame(
        columns=['LOCAL EDITADO', 'OPERADORA', 'OPERADORA_ANTERIOR', 'REGRA', 'QUANTIDADE']
    )

total_classificadas_antes_hapvida = int(len(df) - len(df_nao_classificados))
total_hapvida = int(len(df_nao_classificados))
total_sobrescritos = (
    int(df_sobrescritos['QUANTIDADE'].sum()) if not df_sobrescritos.empty else 0
)

resumo = {
    'execucao': 'exec_06_operadora_v2',
    'arquivo_entrada': str(arquivo_entrada),
    'arquivo_regras_operadora': str(arquivo_regras_operadora),
    'arquivo_saida': str(arquivo_saida),
    'total_linhas_entrada': int(len(df)),
    'total_classificadas_antes_hapvida': total_classificadas_antes_hapvida,
    'total_preenchidas_com_hapvida': total_hapvida,
    'total_classificadas_final': int(len(df)),
    'total_sobrescritos': total_sobrescritos,
    'total_regras_ativas': int(len(df_regras)),
    'total_locais_multiplas_operadoras': int(
        df_locais_multiplas_operadoras['LOCAL EDITADO'].nunique()
    ) if not df_locais_multiplas_operadoras.empty else 0,
    'locais_multiplas_operadoras': transformar_em_lista_registros(
        df_locais_multiplas_operadoras,
        ['LOCAL EDITADO', 'TOTAL_OPERADORAS', 'OPERADORA', 'QUANTIDADE'],
    ),
    'regras_aplicadas': regras_auditoria,
}

with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
    json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

linhas_txt = [
    'RESUMO DA EXECUCAO 06 V2 - OPERADORA',
    '',
    f"Arquivo de entrada: {resumo['arquivo_entrada']}",
    f"Arquivo de regras: {resumo['arquivo_regras_operadora']}",
    f"Arquivo de saida: {resumo['arquivo_saida']}",
    '',
    f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
    f"Total classificadas antes do HAPVIDA: {resumo['total_classificadas_antes_hapvida']}",
    f"Total preenchidas com HAPVIDA: {resumo['total_preenchidas_com_hapvida']}",
    f"Total classificadas no final: {resumo['total_classificadas_final']}",
    f"Total sobrescritos: {resumo['total_sobrescritos']}",
    f"Total de regras ativas: {resumo['total_regras_ativas']}",
    f"Total de locais com mais de uma operadora: {resumo['total_locais_multiplas_operadoras']}",
    '',
    'NAO CLASSIFICADAS ANTES DO HAPVIDA - LOCAL EDITADO E UF:',
]

if not df_hapvida_distintos.empty:
    for _, linha in df_hapvida_distintos.iterrows():
        linhas_txt.append(
            f"- {linha['LOCAL EDITADO']} - {linha['UF']}: {int(linha['QUANTIDADE'])}"
        )
else:
    linhas_txt.append('- Nenhum registro ficou vazio antes do HAPVIDA')

with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
    arquivo.write('\n'.join(linhas_txt))

salvar_csv_padronizado(pd.DataFrame([{
    'EXECUCAO': resumo['execucao'],
    'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
    'ARQUIVO_REGRAS': resumo['arquivo_regras_operadora'],
    'ARQUIVO_SAIDA': resumo['arquivo_saida'],
    'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
    'TOTAL_CLASSIFICADAS_ANTES_HAPVIDA': resumo['total_classificadas_antes_hapvida'],
    'TOTAL_PREENCHIDAS_COM_HAPVIDA': resumo['total_preenchidas_com_hapvida'],
    'TOTAL_CLASSIFICADAS_FINAL': resumo['total_classificadas_final'],
    'TOTAL_SOBRESCRITOS': resumo['total_sobrescritos'],
    'TOTAL_REGRAS_ATIVAS': resumo['total_regras_ativas'],
    'TOTAL_LOCAIS_MULTIPLAS_OPERADORAS': resumo['total_locais_multiplas_operadoras'],
}]), arquivo_resumo_csv)

salvar_csv_padronizado(df_operadora_distintos, arquivo_operadora_distintos_csv)
salvar_csv_padronizado(df_nao_classificados, arquivo_nao_classificados_csv)
salvar_csv_padronizado(df_sobrescritos, arquivo_sobrescritos_csv)
salvar_csv_padronizado(df_hapvida_distintos, arquivo_hapvida_distintos_csv)

print(f'Total de linhas recebidas: {len(df)}')
print(f'Total classificadas antes do HAPVIDA: {total_classificadas_antes_hapvida}')
print(f'Total preenchidas com HAPVIDA: {total_hapvida}')
print(f'Total sobrescritos: {total_sobrescritos}')
print(f'Total de regras ativas: {len(df_regras)}')
print('Execucao 06 v2 finalizada.')
