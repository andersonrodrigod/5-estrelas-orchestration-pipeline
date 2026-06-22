# -*- coding: utf-8 -*-
import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from execucoes_individuais_avaliacoes import exec_12_power_bi as power_bi
from execucoes_individuais_avaliacoes import exec_13_separar_tipo_excel as separar_tipo
from execucoes_pipeline.config import criar_config_pre_validacao


config = criar_config_pre_validacao()
arquivo_entrada_padrao = config.arquivo_csv_final_pre_validacao

arquivo_power_bi = Path('data_exec_indiv/avaliacoes/12_base_power_bi.csv')
pasta_resumo = Path('saida_resumo_avaliacoes') / 'pipeline_arquivos_bi'


def configurar_resumo_power_bi():
    power_bi.arquivo_entrada = arquivo_entrada_padrao
    power_bi.arquivo_saida = arquivo_power_bi
    power_bi.pasta_resumo = pasta_resumo / 'exec_12_power_bi'
    power_bi.arquivo_resumo_json = (
        power_bi.pasta_resumo / 'exec_12_power_bi_resumo.json'
    )
    power_bi.arquivo_resumo_txt = (
        power_bi.pasta_resumo / 'exec_12_power_bi_resumo.txt'
    )
    power_bi.arquivo_colunas_csv = (
        power_bi.pasta_resumo / 'exec_12_power_bi_colunas.csv'
    )
    power_bi.arquivo_classificacao_csv = (
        power_bi.pasta_resumo / 'exec_12_power_bi_classificacao_ajustes.csv'
    )


def configurar_resumo_separacao():
    separar_tipo.arquivo_entrada = arquivo_power_bi
    separar_tipo.pasta_resumo = pasta_resumo / 'exec_13_separar_tipo_excel'
    separar_tipo.arquivo_resumo_json = (
        separar_tipo.pasta_resumo / 'exec_13_separar_tipo_excel_resumo.json'
    )
    separar_tipo.arquivo_resumo_txt = (
        separar_tipo.pasta_resumo / 'exec_13_separar_tipo_excel_resumo.txt'
    )
    separar_tipo.arquivo_resumo_csv = (
        separar_tipo.pasta_resumo / 'exec_13_separar_tipo_excel_resumo.csv'
    )


def executar_power_bi():
    print('Iniciando etapa final 12 - gerar CSV Power BI...')
    print(f'Entrada validada: {arquivo_entrada_padrao}')

    if not arquivo_entrada_padrao.exists():
        print(f'ERRO - arquivo de entrada validada nao encontrado: {arquivo_entrada_padrao}')
        return 1

    inicio = time.monotonic()
    df = power_bi.ler_base_texto(arquivo_entrada_padrao)
    colunas_faltando = power_bi.validar_colunas_obrigatorias(df)

    if colunas_faltando:
        print('ERRO - colunas obrigatorias ausentes:')
        for coluna in colunas_faltando:
            print(f'- {coluna}')
        return 1

    df = power_bi.normalizar_texto_colunas(df)
    df_saida, classificacao_antes = power_bi.criar_base_power_bi(df)

    print(f'Total de linhas recebidas: {len(df):,}')
    print(f'Gravando CSV Power BI: {arquivo_power_bi}')
    power_bi.salvar_csv_texto(df_saida, arquivo_power_bi)
    power_bi.salvar_resumos(df, df_saida, classificacao_antes)

    print(f'Etapa 12 finalizada em {time.monotonic() - inicio:.2f}s.')
    return 0


def executar_separacao_excel():
    print('Iniciando etapa final 13 - gerar Excels separados por tipo...')

    if not arquivo_power_bi.exists():
        print(f'ERRO - arquivo Power BI nao encontrado: {arquivo_power_bi}')
        return 1

    inicio = time.monotonic()

    try:
        total_linhas = separar_tipo.contar_linhas_csv(arquivo_power_bi)
        registros, total_fora_recorte, tipos_fora_recorte = (
            separar_tipo.separar_e_gravar_excel(total_linhas)
        )
    except (OSError, ValueError) as erro:
        print(f'ERRO - {erro}')
        return 1

    separar_tipo.salvar_resumos(
        total_linhas,
        registros,
        total_fora_recorte,
        tipos_fora_recorte,
    )

    print(f'Etapa 13 finalizada em {time.monotonic() - inicio:.2f}s.')
    return 0


def main():
    configurar_resumo_power_bi()
    configurar_resumo_separacao()

    codigo = executar_power_bi()
    if codigo != 0:
        return codigo

    codigo = executar_separacao_excel()
    if codigo != 0:
        return codigo

    print('Arquivos finais de BI gerados com sucesso.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
