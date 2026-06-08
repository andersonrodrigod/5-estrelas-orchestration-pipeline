# -*- coding: utf-8 -*-
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado


arquivo_entrada = Path('data_exec_indiv/avaliacoes/05_base_com_local_editado.csv')
arquivo_regras_ajuste = Path('utils/insumos/regra_ajuste_final.xlsx')
arquivo_saida = Path('data_exec_indiv/avaliacoes/06_base_com_ajustes_finais.csv')

pasta_resumo = Path('saida_resumo_avaliacoes') / 'exec_06_ajustes_finais'
arquivo_resumo_json = pasta_resumo / 'exec_06_ajustes_finais_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_06_ajustes_finais_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_06_ajustes_finais_resumo.csv'
arquivo_auditoria_csv = pasta_resumo / 'exec_06_ajustes_finais_auditoria.csv'
arquivo_regras_csv = pasta_resumo / 'exec_06_ajustes_finais_regras.csv'

colunas_regras = [
    'STATUS_ATIVO',
    'ORDEM',
    'COLUNA_AJUSTAR',
    'VALOR_NOVO',
    'VALOR_ATUAL_ESPERADO',
    'PERMITIR_SOBRESCRITA',
    'COLUNA_FILTRO_1',
    'COMPARADOR_1',
    'VALOR_1',
    'COLUNA_FILTRO_2',
    'COMPARADOR_2',
    'VALOR_2',
    'COLUNA_FILTRO_3',
    'COMPARADOR_3',
    'VALOR_3',
    'DESCRICAO',
]

mapa_colunas_regras = {
    'STATUS_ATIVO': 'ativo',
    'ORDEM': 'ordem',
    'COLUNA_AJUSTAR': 'coluna_ajustar',
    'VALOR_NOVO': 'valor_novo',
    'VALOR_ATUAL_ESPERADO': 'valor_atual_esperado',
    'PERMITIR_SOBRESCRITA': 'permitir_sobrescrita',
    'COLUNA_FILTRO_1': 'coluna_1',
    'COMPARADOR_1': 'comparador_1',
    'VALOR_1': 'valor_1',
    'COLUNA_FILTRO_2': 'coluna_2',
    'COMPARADOR_2': 'comparador_2',
    'VALOR_2': 'valor_2',
    'COLUNA_FILTRO_3': 'coluna_3',
    'COMPARADOR_3': 'comparador_3',
    'VALOR_3': 'valor_3',
    'DESCRICAO': 'descricao',
}

comparadores_validos = {
    'igual',
    'diferente',
    'em_lista',
    'fora_lista',
    'contem',
    'nao_contem',
    'vazio',
    'nao_vazio',
}


def normalizar_texto(serie):
    texto = serie.astype('string')
    texto = texto.str.replace('\xa0', ' ', regex=False)
    texto = texto.str.replace(r'\s+', ' ', regex=True)
    return texto.str.strip().fillna('')


def normalizar_flag(valor, padrao=False):
    if pd.isna(valor):
        return padrao

    texto = str(valor).strip().lower()
    if texto == '':
        return padrao

    return texto in {'sim', 's', 'true', '1', 'yes'}


def separar_lista(valor):
    return [item.strip() for item in str(valor).split(';') if item.strip() != '']


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


def carregar_regras(caminho):
    df_regras = pd.read_excel(caminho, sheet_name='regras_ajuste_final')
    df_regras['_LINHA_EXCEL'] = df_regras.index + 2

    for coluna in colunas_regras:
        if coluna not in df_regras.columns:
            df_regras[coluna] = pd.NA

    df_regras = df_regras[colunas_regras + ['_LINHA_EXCEL']].copy()
    df_regras = df_regras.rename(columns=mapa_colunas_regras)
    df_regras['ativo'] = df_regras['ativo'].apply(normalizar_flag)
    df_regras['permitir_sobrescrita'] = df_regras['permitir_sobrescrita'].apply(
        lambda valor: normalizar_flag(valor, padrao=True)
    )
    df_regras['ordem'] = pd.to_numeric(df_regras['ordem'], errors='coerce')
    df_regras = df_regras[df_regras['ativo']].copy()
    df_regras = df_regras.sort_values('ordem', kind='stable')

    for coluna in [
        'coluna_ajustar',
        'valor_novo',
        'valor_atual_esperado',
        'coluna_1',
        'comparador_1',
        'valor_1',
        'coluna_2',
        'comparador_2',
        'valor_2',
        'coluna_3',
        'comparador_3',
        'valor_3',
        'descricao',
    ]:
        df_regras[coluna] = normalizar_texto(df_regras[coluna])

    df_regras['coluna_ajustar'] = df_regras['coluna_ajustar'].str.upper()
    for numero in range(1, 4):
        df_regras[f'coluna_{numero}'] = df_regras[f'coluna_{numero}'].str.upper()
        df_regras[f'comparador_{numero}'] = df_regras[f'comparador_{numero}'].str.lower()

    return df_regras.reset_index(drop=True)


def validar_regras(df_regras, colunas_base):
    erros = []

    for _, regra in df_regras.iterrows():
        linha_excel = int(regra['_LINHA_EXCEL'])

        if pd.isna(regra['ordem']):
            erros.append(f'Linha {linha_excel} com ORDEM vazia ou invalida.')

        if regra['coluna_ajustar'] == '':
            erros.append(f'Linha {linha_excel} com COLUNA_AJUSTAR vazia.')
        elif regra['coluna_ajustar'] not in colunas_base:
            erros.append(
                f"Linha {linha_excel} usa COLUNA_AJUSTAR inexistente "
                f"'{regra['coluna_ajustar']}'."
            )

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
                erros.append(f"Linha {linha_excel} com COLUNA_FILTRO_{numero} vazia.")
                continue

            if coluna not in colunas_base:
                erros.append(
                    f"Linha {linha_excel} usa coluna inexistente '{coluna}' "
                    f"em COLUNA_FILTRO_{numero}."
                )

            if comparador not in comparadores_validos:
                erros.append(
                    f"Linha {linha_excel} usa comparador invalido '{comparador}' "
                    f"em COMPARADOR_{numero}."
                )

            if comparador not in {'vazio', 'nao_vazio'} and valor == '':
                erros.append(
                    f"Linha {linha_excel} usa comparador '{comparador}' sem VALOR_{numero}."
                )

    return erros


def montar_mascara_filtro(df_base, coluna, comparador, valor):
    serie = normalizar_texto(df_base[coluna])

    if comparador == 'igual':
        return serie.str.upper() == str(valor).upper()

    if comparador == 'diferente':
        return serie.str.upper() != str(valor).upper()

    if comparador == 'em_lista':
        valores = {item.upper() for item in separar_lista(valor)}
        return serie.str.upper().isin(valores)

    if comparador == 'fora_lista':
        valores = {item.upper() for item in separar_lista(valor)}
        return ~serie.str.upper().isin(valores)

    if comparador == 'contem':
        return serie.str.contains(str(valor), case=False, na=False, regex=False)

    if comparador == 'nao_contem':
        return ~serie.str.contains(str(valor), case=False, na=False, regex=False)

    if comparador == 'vazio':
        return serie == ''

    if comparador == 'nao_vazio':
        return serie != ''

    raise ValueError(f'Comparador nao suportado: {comparador}')


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
        return pd.Series(False, index=df_base.index)

    return mascara


def aplicar_ajustes(df_base, df_regras):
    auditorias = []
    resumos = []
    colunas_auditaveis = [
        'CDATENDIMENTO',
        'NUM_BENEFICIARIO',
        'LOCAL',
        'UF',
        'CLASSIFICACAO',
        'LOCAL EDITADO',
    ]
    colunas_auditaveis = [coluna for coluna in colunas_auditaveis if coluna in df_base.columns]

    for _, regra in df_regras.iterrows():
        coluna_ajustar = regra['coluna_ajustar']
        valor_novo = regra['valor_novo']
        mascara = montar_mascara_regra(df_base, regra)

        if regra['valor_atual_esperado'] != '':
            atual = normalizar_texto(df_base[coluna_ajustar]).str.upper()
            mascara = mascara & (atual == regra['valor_atual_esperado'].upper())

        if not regra['permitir_sobrescrita']:
            atual = normalizar_texto(df_base[coluna_ajustar])
            mascara = mascara & (atual == '')

        alteradas = mascara & (normalizar_texto(df_base[coluna_ajustar]) != valor_novo)
        total_atingidas = int(mascara.sum())
        total_alteradas = int(alteradas.sum())

        if total_alteradas > 0:
            df_auditoria = df_base.loc[alteradas, colunas_auditaveis].copy()
            df_auditoria['ORDEM_REGRA'] = int(regra['ordem'])
            df_auditoria['DESCRICAO'] = regra['descricao']
            df_auditoria['COLUNA_AJUSTAR'] = coluna_ajustar
            df_auditoria['VALOR_ANTERIOR'] = df_base.loc[alteradas, coluna_ajustar]
            df_auditoria['VALOR_NOVO'] = valor_novo
            auditorias.append(df_auditoria)
            df_base.loc[alteradas, coluna_ajustar] = valor_novo

        resumos.append({
            'ORDEM_REGRA': int(regra['ordem']),
            'DESCRICAO': regra['descricao'],
            'COLUNA_AJUSTAR': coluna_ajustar,
            'VALOR_NOVO': valor_novo,
            'TOTAL_ATINGIDAS': total_atingidas,
            'TOTAL_ALTERADAS': total_alteradas,
        })

    if auditorias:
        df_auditoria = pd.concat(auditorias, ignore_index=True)
    else:
        df_auditoria = pd.DataFrame(
            columns=[
                *colunas_auditaveis,
                'ORDEM_REGRA',
                'DESCRICAO',
                'COLUNA_AJUSTAR',
                'VALOR_ANTERIOR',
                'VALOR_NOVO',
            ]
        )

    return df_base, pd.DataFrame(resumos), df_auditoria


def salvar_resumos(df_entrada, df_saida, df_regras_resumo, df_auditoria):
    pasta_resumo.mkdir(parents=True, exist_ok=True)
    salvar_csv_padronizado(df_regras_resumo, arquivo_regras_csv)
    salvar_csv_padronizado(df_auditoria, arquivo_auditoria_csv)

    total_alteradas = int(df_regras_resumo['TOTAL_ALTERADAS'].sum()) if not df_regras_resumo.empty else 0
    resumo = {
        'execucao': 'exec_06_ajustes_finais',
        'arquivo_entrada': str(arquivo_entrada),
        'arquivo_regras_ajuste': str(arquivo_regras_ajuste),
        'arquivo_saida': str(arquivo_saida),
        'arquivo_regras_csv': str(arquivo_regras_csv),
        'arquivo_auditoria_csv': str(arquivo_auditoria_csv),
        'total_linhas_entrada': int(len(df_entrada)),
        'total_linhas_saida': int(len(df_saida)),
        'total_regras_ativas': int(len(df_regras_resumo)),
        'total_linhas_alteradas': total_alteradas,
        'regras': transformar_em_lista_registros(
            df_regras_resumo,
            [
                'ORDEM_REGRA',
                'DESCRICAO',
                'COLUNA_AJUSTAR',
                'VALOR_NOVO',
                'TOTAL_ATINGIDAS',
                'TOTAL_ALTERADAS',
            ],
        ),
    }

    with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 06 - AJUSTES FINAIS',
        '',
        f"Arquivo de entrada: {resumo['arquivo_entrada']}",
        f"Arquivo de regras: {resumo['arquivo_regras_ajuste']}",
        f"Arquivo de saida: {resumo['arquivo_saida']}",
        f"Auditoria: {resumo['arquivo_auditoria_csv']}",
        '',
        f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
        f"Total de linhas na saida: {resumo['total_linhas_saida']}",
        f"Total de regras ativas: {resumo['total_regras_ativas']}",
        f"Total de linhas alteradas: {resumo['total_linhas_alteradas']}",
        '',
        'REGRAS APLICADAS:',
    ]

    if resumo['regras']:
        for regra in resumo['regras']:
            linhas_txt.append(
                f"- Regra {regra['ORDEM_REGRA']} - {regra['DESCRICAO']}: "
                f"{regra['TOTAL_ALTERADAS']} alteradas de {regra['TOTAL_ATINGIDAS']} atingidas"
            )
    else:
        linhas_txt.append('- Nenhuma regra ativa')

    with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))

    salvar_csv_padronizado(pd.DataFrame([{
        'EXECUCAO': resumo['execucao'],
        'ARQUIVO_ENTRADA': resumo['arquivo_entrada'],
        'ARQUIVO_REGRAS': resumo['arquivo_regras_ajuste'],
        'ARQUIVO_SAIDA': resumo['arquivo_saida'],
        'TOTAL_LINHAS_ENTRADA': resumo['total_linhas_entrada'],
        'TOTAL_LINHAS_SAIDA': resumo['total_linhas_saida'],
        'TOTAL_REGRAS_ATIVAS': resumo['total_regras_ativas'],
        'TOTAL_LINHAS_ALTERADAS': resumo['total_linhas_alteradas'],
    }]), arquivo_resumo_csv)


def executar():
    print('Iniciando execucao 06 - ajustes finais...')
    print(f'Lendo arquivo da execucao 05: {arquivo_entrada}')
    print(f'Lendo regras de ajuste final: {arquivo_regras_ajuste}')

    if not arquivo_entrada.exists():
        print(f'ERRO - arquivo nao encontrado: {arquivo_entrada}')
        return 1

    if not arquivo_regras_ajuste.exists():
        print(f'ERRO - arquivo de regras nao encontrado: {arquivo_regras_ajuste}')
        return 1

    df = ler_csv_padronizado(arquivo_entrada)
    df_original = df.copy()
    df_regras = carregar_regras(arquivo_regras_ajuste)

    erros_regras = validar_regras(df_regras, set(df.columns))
    if erros_regras:
        print('ERRO - regra_ajuste_final.xlsx possui problemas:')
        for erro in erros_regras[:50]:
            print(f'- {erro}')
        if len(erros_regras) > 50:
            print(f'- ... mais {len(erros_regras) - 50} problema(s)')
        return 1

    df, df_regras_resumo, df_auditoria = aplicar_ajustes(df, df_regras)
    total_alteradas = int(df_regras_resumo['TOTAL_ALTERADAS'].sum()) if not df_regras_resumo.empty else 0

    print(f'Total de linhas recebidas: {len(df)}')
    print(f'Total de linhas alteradas: {total_alteradas}')
    print(f'Gravando arquivo da execucao 06: {arquivo_saida}')

    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
    salvar_csv_padronizado(df, arquivo_saida)
    salvar_resumos(df_original, df, df_regras_resumo, df_auditoria)

    print('Execucao 06 finalizada.')
    return 0


if __name__ == '__main__':
    sys.exit(executar())
