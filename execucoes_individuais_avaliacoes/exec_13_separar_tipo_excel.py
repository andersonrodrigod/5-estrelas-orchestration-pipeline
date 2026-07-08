# -*- coding: utf-8 -*-
import csv
import json
import sys
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd
from openpyxl import Workbook
from openpyxl.cell import WriteOnlyCell

sys.path.append(str(Path(__file__).resolve().parents[1]))
from funcoes_auxiliares.padronizacao_csv import salvar_csv_padronizado


arquivo_entrada = Path('data_exec_indiv/avaliacoes/12_base_power_bi.csv')

arquivos_saida = {
    'TIPO 1 A 3': {
        'arquivo': Path('data/arquivos_bi/13_base_tipo_1_a_3_power_bi.xlsx'),
        'aba': 'tipos 1 a 3',
    },
    'TIPO 4 A 7': {
        'arquivo': Path('data/arquivos_bi/13_base_tipo_4_a_7_power_bi.xlsx'),
        'aba': 'tipos 4 a 7',
    },
    'TIPO 8 OU MAIS': {
        'arquivo': Path('data/arquivos_bi/13_base_tipo_8_ou_mais_power_bi.xlsx'),
        'aba': 'tipos 8 ou mais',
    },
}

pasta_resumo = Path('auditoria') / 'saida_resumo_avaliacoes' / 'exec_13_separar_tipo_excel'
arquivo_resumo_json = pasta_resumo / 'exec_13_separar_tipo_excel_resumo.json'
arquivo_resumo_txt = pasta_resumo / 'exec_13_separar_tipo_excel_resumo.txt'
arquivo_resumo_csv = pasta_resumo / 'exec_13_separar_tipo_excel_resumo.csv'

coluna_tipo = 'tipo'
intervalo_progresso = 100_000
limite_linhas_excel = 1_048_576

# Referencia: tipos observados nos Excel historicos usados pelo Power BI.
colunas_numericas = {
    'nota1',
    'nota2',
    'nota3',
    'nota4',
    'nota5',
    'mes',
    'dia',
    'ano',
    'tipo',
    'nota geral',
    'Meta',
    'resultado da unidade',
}

# Campos que precisam ficar texto para nao perder prefixos, zeros ou formato de data.
colunas_texto_forcado = {
    'cdatendimento',
    'num_beneficiario',
    'cdempresa',
    'cdusuario',
    'telefone',
    'data_atendimento',
}


def texto_tempo(segundos):
    segundos = max(int(segundos), 0)
    horas, resto = divmod(segundos, 3600)
    minutos, segundos = divmod(resto, 60)
    return f'{horas:02d}:{minutos:02d}:{segundos:02d}'


def imprimir_progresso(etapa, processadas, total, inicio):
    decorrido = time.monotonic() - inicio
    percentual = (processadas / total * 100) if total else 0
    restante = ((decorrido / processadas) * (total - processadas)) if processadas else 0
    print(
        f'{etapa}: {processadas:,}/{total:,} linhas | '
        f'{percentual:.1f}% | decorrido {texto_tempo(decorrido)} | '
        f'estimativa restante {texto_tempo(restante)}',
        flush=True,
    )


def contar_linhas_csv(caminho):
    print('Contando linhas do CSV para calcular o progresso...', flush=True)
    inicio = time.monotonic()
    total = 0

    with open(caminho, 'r', encoding='utf-8-sig', newline='') as arquivo:
        leitor = csv.reader(arquivo)
        next(leitor, None)

        for total, _ in enumerate(leitor, start=1):
            if total % intervalo_progresso == 0:
                print(
                    f'Contagem previa: {total:,} linhas | '
                    f'decorrido {texto_tempo(time.monotonic() - inicio)}',
                    flush=True,
                )

    print(
        f'Contagem concluida: {total:,} linhas em '
        f'{texto_tempo(time.monotonic() - inicio)}.',
        flush=True,
    )
    return total


def valor_vazio(valor):
    return valor is None or str(valor).strip() == ''


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


def converter_tipo(valor):
    numero = converter_numero(valor)

    if not isinstance(numero, (int, float)):
        return None

    return numero


def criar_celula_texto(ws, valor):
    celula = WriteOnlyCell(ws, value='' if valor is None else str(valor))
    celula.number_format = '@'
    return celula


def criar_celula_excel(ws, coluna, valor):
    if coluna in colunas_numericas:
        return converter_numero(valor)

    if coluna in colunas_texto_forcado:
        return criar_celula_texto(ws, valor)

    if valor_vazio(valor):
        return None

    return str(valor)


def obter_rotulo_tipo(valor):
    tipo = converter_tipo(valor)

    if tipo is None:
        return None

    if 1 <= tipo <= 3:
        return 'TIPO 1 A 3'

    if 4 <= tipo <= 7:
        return 'TIPO 4 A 7'

    if tipo >= 8:
        return 'TIPO 8 OU MAIS'

    return None


def criar_planilhas(cabecalho):
    planilhas = {}

    for rotulo, configuracao in arquivos_saida.items():
        workbook = Workbook(write_only=True)
        ws = workbook.create_sheet(title=configuracao['aba'])
        ws.append(cabecalho)
        planilhas[rotulo] = {
            **configuracao,
            'workbook': workbook,
            'ws': ws,
            'linhas': 0,
        }

    return planilhas


def validar_cabecalho(cabecalho):
    colunas_faltando = []

    if coluna_tipo not in cabecalho:
        colunas_faltando.append(coluna_tipo)

    return colunas_faltando


def separar_e_gravar_excel(total_linhas):
    print('Separando CSV e gravando os Excel com tipos controlados...', flush=True)
    inicio = time.monotonic()
    total_fora_recorte = 0
    tipos_fora_recorte = {}

    with open(arquivo_entrada, 'r', encoding='utf-8-sig', newline='') as arquivo:
        leitor = csv.reader(arquivo)
        cabecalho = next(leitor)
        colunas_faltando = validar_cabecalho(cabecalho)

        if colunas_faltando:
            raise ValueError(
                'Colunas obrigatorias ausentes no arquivo de entrada: '
                + ', '.join(colunas_faltando)
            )

        indice_tipo = cabecalho.index(coluna_tipo)
        planilhas = criar_planilhas(cabecalho)
        processadas = 0

        for processadas, linha in enumerate(leitor, start=1):
            valor_tipo = linha[indice_tipo] if indice_tipo < len(linha) else ''
            rotulo = obter_rotulo_tipo(valor_tipo)

            if rotulo is None:
                total_fora_recorte += 1
                tipos_fora_recorte[valor_tipo] = tipos_fora_recorte.get(valor_tipo, 0) + 1
            else:
                planilha = planilhas[rotulo]

                if planilha['linhas'] + 2 > limite_linhas_excel:
                    raise ValueError(
                        f"{planilha['arquivo']} excederia o limite de linhas do Excel "
                        f"({limite_linhas_excel}, incluindo cabecalho)."
                    )

                planilha['ws'].append([
                    criar_celula_excel(
                        planilha['ws'],
                        coluna,
                        linha[indice] if indice < len(linha) else '',
                    )
                    for indice, coluna in enumerate(cabecalho)
                ])
                planilha['linhas'] += 1

            if processadas % intervalo_progresso == 0:
                imprimir_progresso('Separacao/gravacao', processadas, total_linhas, inicio)

    if processadas and processadas % intervalo_progresso != 0:
        imprimir_progresso('Separacao/gravacao', processadas, total_linhas, inicio)

    registros = []
    print('Salvando arquivos Excel em disco...', flush=True)

    for rotulo, planilha in planilhas.items():
        destino = planilha['arquivo']
        destino.parent.mkdir(parents=True, exist_ok=True)
        print(
            f"Salvando {rotulo}: {destino} | {planilha['linhas']:,} linhas.",
            flush=True,
        )
        planilha['workbook'].save(destino)
        registros.append({
            'ROTULO': rotulo,
            'ARQUIVO_EXCEL': str(destino),
            'ABA': planilha['aba'],
            'LINHAS': int(planilha['linhas']),
            'COLUNAS': int(len(cabecalho)),
        })

    return registros, int(total_fora_recorte), tipos_fora_recorte


def salvar_resumos(total_linhas, registros, total_fora_recorte, tipos_fora_recorte):
    pasta_resumo.mkdir(parents=True, exist_ok=True)

    resumo = {
        'execucao': 'exec_13_separar_tipo_excel',
        'arquivo_entrada': str(arquivo_entrada),
        'arquivos_saida': registros,
        'total_linhas_entrada': int(total_linhas),
        'total_linhas_fora_recorte': int(total_fora_recorte),
        'tipos_fora_recorte': tipos_fora_recorte,
        'colunas_numericas_excel': sorted(colunas_numericas),
        'colunas_texto_forcado_excel': sorted(colunas_texto_forcado),
        'observacao': (
            'Excel gerado diretamente do CSV Power BI para preservar '
            'colunas numericas usadas em calculos e identificadores como texto.'
        ),
    }

    with open(arquivo_resumo_json, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    linhas_txt = [
        'RESUMO DA EXECUCAO 13 - SEPARAR TIPO EXCEL POWER BI',
        '',
        f"Arquivo de entrada: {resumo['arquivo_entrada']}",
        f"Total de linhas na entrada: {resumo['total_linhas_entrada']}",
        f"Total fora do recorte: {resumo['total_linhas_fora_recorte']}",
        '',
        'EXCEL GERADOS:',
    ]

    for registro in registros:
        linhas_txt.append(
            f"- {registro['ROTULO']}: {registro['ARQUIVO_EXCEL']} | "
            f"{registro['LINHAS']} linha(s) | {registro['COLUNAS']} coluna(s)"
        )
        if registro.get('ARQUIVO_COPIA_POWER_BI'):
            linhas_txt.append(f"  Copia Power BI: {registro['ARQUIVO_COPIA_POWER_BI']}")

    linhas_txt.extend([
        '',
        'Colunas numericas no Excel:',
        *[f'- {coluna}' for coluna in sorted(colunas_numericas)],
        '',
        'Colunas forcadas como texto no Excel:',
        *[f'- {coluna}' for coluna in sorted(colunas_texto_forcado)],
    ])

    with open(arquivo_resumo_txt, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas_txt))

    salvar_csv_padronizado(pd.DataFrame(registros), arquivo_resumo_csv)


def executar():
    print('Iniciando execucao 13 - separar por tipo para Power BI em Excel...', flush=True)
    print(f'Lendo arquivo da execucao 12: {arquivo_entrada}', flush=True)

    if not arquivo_entrada.exists():
        print(f'ERRO - arquivo nao encontrado: {arquivo_entrada}')
        return 1

    try:
        total_linhas = contar_linhas_csv(arquivo_entrada)
        registros, total_fora_recorte, tipos_fora_recorte = separar_e_gravar_excel(total_linhas)
    except (OSError, ValueError) as erro:
        print(f'ERRO - {erro}')
        return 1

    salvar_resumos(total_linhas, registros, total_fora_recorte, tipos_fora_recorte)

    print('Execucao 13 finalizada.', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(executar())
