# -*- coding: utf-8 -*-
import shutil
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
pasta_copia_power_bi = Path(
    r'G:\Superintendencia de Atendimento\Inteligência de Dados'
    r'\3 - Bases Gerais\3.5 - Base 5 Estrelas Power BI'
    r'\3.5.1 - Bases Consolidadas'
)
nomes_copia_power_bi = {
    'TIPO 1 A 3': '5_ESTRELAS_JULHO_26_1.xlsx',
    'TIPO 4 A 7': '5_ESTRELAS_JULHO_26_2.xlsx',
    'TIPO 8 OU MAIS': '5_ESTRELAS_JULHO_26_3.xlsx',
}


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


def copiar_excels_para_power_bi(registros):
    print(f'Copiando Excels finais para: {pasta_copia_power_bi}')
    pasta_copia_power_bi.mkdir(parents=True, exist_ok=True)

    for registro in registros:
        rotulo = registro['ROTULO']
        nome_copia = nomes_copia_power_bi.get(rotulo)

        if not nome_copia:
            raise ValueError(f'Nome de copia nao configurado para: {rotulo}')

        origem = Path(registro['ARQUIVO_EXCEL'])
        destino = pasta_copia_power_bi / nome_copia

        if not origem.exists():
            raise OSError(f'Arquivo gerado nao encontrado para copia: {origem}')

        print(f'Copiando {rotulo}: {origem} -> {destino}')
        shutil.copy2(origem, destino)
        registro['ARQUIVO_COPIA_POWER_BI'] = str(destino)

    return registros


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
        registros = copiar_excels_para_power_bi(registros)
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
