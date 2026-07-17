# -*- coding: utf-8 -*-
import json
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from execucoes_pipeline.config import criar_config_pre_validacao
from execucoes_individuais_avaliacoes.exec_00_verificacao_campos import (
    processar_verificacao_campos,
    salvar_resumos_verificacao_campos,
)
from execucoes_individuais_avaliacoes.exec_01_limpeza import (
    processar_limpeza,
    salvar_resumos_limpeza,
)
from execucoes_individuais_avaliacoes.exec_02_contratacao import (
    processar_contratacao,
    salvar_resumos_contratacao,
)
from execucoes_individuais_avaliacoes.exec_03_nota import processar_nota, salvar_resumos_nota
from execucoes_individuais_avaliacoes.exec_04_classificacao import (
    processar_classificacao,
    salvar_resumos_classificacao,
)
from execucoes_individuais_avaliacoes.exec_05_local_editado import (
    processar_local_editado,
    salvar_resumos_local_editado,
)
from execucoes_individuais_avaliacoes.exec_06_ajustes_finais import (
    processar_ajustes_finais,
    salvar_resumos_ajustes_finais,
)
from execucoes_individuais_avaliacoes.exec_07_operadora import (
    processar_operadora,
    salvar_resumos_operadora,
)
from execucoes_individuais_avaliacoes.exec_08_meta import processar_meta, salvar_resumos_meta
from execucoes_individuais_avaliacoes.exec_09_resultado_unidade import (
    processar_resultado_unidade,
    salvar_resumos_resultado_unidade,
)
from execucoes_individuais_avaliacoes.exec_10_status_unidade import (
    processar_status_unidade,
    salvar_resumos_status_unidade,
)
from execucoes_individuais_avaliacoes.exec_11_analise_dados import (
    processar_analise_dados,
    salvar_analise_dados,
)
from execucoes_pipeline.negativas import (
    processar_classificacao_negativas,
    processar_contratacao_negativas,
    processar_limpeza_negativas,
    processar_local_editado_negativas,
    processar_verificacao_campos_negativas,
    salvar_resumos_classificacao_negativas,
    salvar_resumos_contratacao_negativas,
    salvar_resumos_limpeza_negativas,
    salvar_resumos_local_editado_negativas,
    salvar_resumos_verificacao_campos_negativas,
)
from funcoes_auxiliares.padronizacao_csv import ler_csv_padronizado, salvar_csv_padronizado


def executar_etapa(nome, funcao, *args):
    inicio = time.monotonic()
    print(f'Iniciando {nome}...')
    resultado = funcao(*args)
    duracao = time.monotonic() - inicio
    print(f'{nome} finalizada em {duracao:.2f}s.')
    return resultado, duracao


def salvar_resumo_mestre(config, registros):
    config.pasta_resumo.mkdir(parents=True, exist_ok=True)
    caminho = config.pasta_resumo / 'pipeline_pre_validacao_resumo_mestre.json'

    resumo = {
        'execucao': 'pipeline_pre_validacao',
        'arquivo_entrada_bruta': str(config.arquivo_entrada_bruta),
        'arquivo_csv_final_pre_validacao': str(config.arquivo_csv_final_pre_validacao),
        'observacao': (
            'Versao inicial da pipeline em memoria. '
            'Neste momento executa as etapas 00 a 11.'
        ),
        'etapas': registros,
    }

    with open(caminho, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    return caminho


def salvar_resumo_mestre_negativas(config, registros):
    config.pasta_resumo_negativas.mkdir(parents=True, exist_ok=True)
    caminho = config.pasta_resumo_negativas / 'pipeline_pre_validacao_negativas_resumo_mestre.json'

    resumo = {
        'execucao': 'pipeline_pre_validacao_negativas',
        'arquivo_entrada_bruta': str(config.arquivo_entrada_negativas),
        'arquivo_csv_final_pre_validacao': str(
            config.arquivo_csv_final_pre_validacao_negativas
        ),
        'observacao': (
            'Fluxo de negativas executado dentro da pre-validacao. '
            'Executa as etapas negativas 00 a 04.'
        ),
        'etapas': registros,
    }

    with open(caminho, 'w', encoding='utf-8') as arquivo:
        json.dump(resumo, arquivo, ensure_ascii=False, indent=4)

    return caminho


def salvar_auditoria_insumos(arquivos_auditoria, destinos):
    caminhos_gerados = []
    for destino in destinos:
        destino.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(destino, engine='openpyxl') as writer:
            for nome_aba, caminho_csv in arquivos_auditoria:
                if caminho_csv.exists():
                    df = ler_csv_padronizado(caminho_csv)
                else:
                    df = pd.DataFrame([{
                        'ARQUIVO_ESPERADO': str(caminho_csv),
                        'STATUS': 'arquivo nao encontrado',
                    }])

                df.to_excel(writer, sheet_name=nome_aba, index=False)

        caminhos_gerados.append(destino)

    return caminhos_gerados


def salvar_auditoria_insumos_avaliacoes(config):
    arquivos_auditoria = [
        (
            '02_locais_sem_contratacao',
            config.pasta_resumo
            / 'exec_02_contratacao'
            / 'exec_02_locais_sem_contratacao.csv',
        ),
        (
            '02_linhas_sem_contratacao',
            config.pasta_resumo
            / 'exec_02_contratacao'
            / 'exec_02_linhas_sem_contratacao.csv',
        ),
        (
            '04_class_nao_classificados',
            config.pasta_resumo
            / 'exec_04_classificacao'
            / 'exec_04_classificacao_nao_classificados_detalhado.csv',
        ),
        (
            '05_local_nao_encontrados',
            config.pasta_resumo
            / 'exec_05_local_editado'
            / 'exec_05_local_editado_nao_encontrados.csv',
        ),
        (
            '07_operadora_nao_class',
            config.pasta_resumo
            / 'exec_07_operadora'
            / 'exec_07_operadora_nao_classificados.csv',
        ),
        (
            '02_locais_sem_uf',
            config.pasta_resumo
            / 'exec_02_contratacao'
            / 'exec_02_locais_sem_uf.csv',
        ),
    ]

    destinos = [
        config.pasta_resumo_avaliacoes
        / 'auditoria_insumos'
        / 'auditoria_pre_validacao_avaliacoes.xlsx',
        config.arquivo_insumos.parent / 'auditoria_pre_validacao_avaliacoes.xlsx',
    ]

    return salvar_auditoria_insumos(arquivos_auditoria, destinos)


def salvar_auditoria_insumos_negativas(config):
    arquivos_auditoria = [
        (
            '02_locais_sem_contratacao',
            config.pasta_resumo_negativas
            / 'exec_02_contratacao'
            / 'exec_02_locais_sem_contratacao.csv',
        ),
        (
            '02_linhas_sem_contratacao',
            config.pasta_resumo_negativas
            / 'exec_02_contratacao'
            / 'exec_02_linhas_sem_contratacao.csv',
        ),
        (
            '03_class_nao_classificados',
            config.pasta_resumo_negativas
            / 'exec_03_classificacao'
            / 'exec_03_classificacao_nao_classificados_detalhado.csv',
        ),
        (
            '04_local_nao_encontrados',
            config.pasta_resumo_negativas
            / 'exec_04_local_editado'
            / 'exec_04_local_editado_nao_encontrados.csv',
        ),
        (
            '02_locais_sem_uf',
            config.pasta_resumo_negativas
            / 'exec_02_contratacao'
            / 'exec_02_locais_sem_uf.csv',
        ),
    ]

    destinos = [
        config.pasta_resumo_negativa
        / 'auditoria_insumos'
        / 'auditoria_pre_validacao_negativas.xlsx',
        config.arquivo_insumos.parent / 'auditoria_pre_validacao_negativas.xlsx',
    ]

    return salvar_auditoria_insumos(arquivos_auditoria, destinos)


def executar_negativas(config):
    if not config.arquivo_entrada_negativas.exists():
        print(
            'AVISO - arquivo de entrada de negativas nao encontrado; '
            f'fluxo de negativas ignorado: {config.arquivo_entrada_negativas}'
        )
        return None

    print('Iniciando pipeline de pre-validacao de negativas...')
    print(f'Entrada bruta negativas: {config.arquivo_entrada_negativas}')
    print(f'Pasta de resumos negativas: {config.pasta_resumo_negativas}')

    df = ler_csv_padronizado(config.arquivo_entrada_negativas)
    registros = []

    resultado_verificacao, duracao = executar_etapa(
        'negativas_exec_00_verificacao_campos',
        processar_verificacao_campos_negativas,
        df,
    )
    df, resumo, artefatos = resultado_verificacao
    salvar_resumos_verificacao_campos_negativas(
        resumo,
        artefatos,
        config.pasta_resumo_negativas / 'exec_00_verificacao_campos',
        config.arquivo_entrada_negativas,
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (df, resumo), duracao = executar_etapa(
        'negativas_exec_01_limpeza',
        processar_limpeza_negativas,
        df,
    )
    salvar_resumos_limpeza_negativas(
        resumo,
        config.pasta_resumo_negativas / 'exec_01_limpeza',
        arquivo_entrada_resumo=config.arquivo_entrada_negativas,
        arquivo_saida_resumo='pipeline_negativas_memoria_exec_01',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    resultado_contratacao, duracao = executar_etapa(
        'negativas_exec_02_contratacao',
        processar_contratacao_negativas,
        df,
        config.arquivo_insumos,
    )
    df, resumo, artefatos = resultado_contratacao
    salvar_resumos_contratacao_negativas(
        resumo,
        artefatos,
        config.pasta_resumo_negativas / 'exec_02_contratacao',
        arquivo_entrada_resumo='pipeline_negativas_memoria_exec_01',
        arquivo_saida_resumo='pipeline_negativas_memoria_exec_02',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    resultado_classificacao, duracao = executar_etapa(
        'negativas_exec_03_classificacao',
        processar_classificacao_negativas,
        df,
        config.arquivo_regras_classificacao,
    )
    df, resumo, artefatos = resultado_classificacao
    salvar_resumos_classificacao_negativas(
        resumo,
        artefatos,
        config.pasta_resumo_negativas / 'exec_03_classificacao',
        arquivo_entrada_resumo='pipeline_negativas_memoria_exec_02',
        arquivo_saida_resumo='pipeline_negativas_memoria_exec_03',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    resultado_local_editado, duracao = executar_etapa(
        'negativas_exec_04_local_editado',
        processar_local_editado_negativas,
        df,
        config.arquivo_insumos,
    )
    df, resumo, artefatos = resultado_local_editado
    salvar_resumos_local_editado_negativas(
        resumo,
        artefatos,
        config.pasta_resumo_negativas / 'exec_04_local_editado',
        arquivo_entrada_resumo='pipeline_negativas_memoria_exec_03',
        arquivo_saida_resumo='pipeline_negativas_memoria_exec_04',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    config.arquivo_csv_final_pre_validacao_negativas.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    salvar_csv_padronizado(df, config.arquivo_csv_final_pre_validacao_negativas)
    caminhos_auditoria = salvar_auditoria_insumos_negativas(config)
    caminho_resumo_mestre = salvar_resumo_mestre_negativas(config, registros)

    print(
        'CSV final da pre-validacao de negativas gerado: '
        f'{config.arquivo_csv_final_pre_validacao_negativas}'
    )
    for caminho_auditoria in caminhos_auditoria:
        print(f'Auditoria de insumos negativas gerada: {caminho_auditoria}')
    print(f'Resumo mestre negativas gerado: {caminho_resumo_mestre}')
    print('Pipeline de pre-validacao de negativas finalizada.')
    return caminho_resumo_mestre


def executar_avaliacoes(config):
    if not config.arquivo_entrada_bruta.exists():
        print(f'ERRO - arquivo de entrada bruta nao encontrado: {config.arquivo_entrada_bruta}')
        return 1

    print('Iniciando pipeline de pre-validacao de avaliacoes...')
    print(f'Entrada bruta: {config.arquivo_entrada_bruta}')
    print(f'Pasta de resumos: {config.pasta_resumo}')

    df = ler_csv_padronizado(config.arquivo_entrada_bruta)
    registros = []

    resultado_verificacao, duracao = executar_etapa(
        'exec_00_verificacao_campos',
        processar_verificacao_campos,
        df,
    )
    df, resumo, artefatos = resultado_verificacao
    salvar_resumos_verificacao_campos(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_00_verificacao_campos',
        config.arquivo_entrada_bruta,
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    resultado_limpeza, duracao = executar_etapa(
        'exec_01_limpeza',
        processar_limpeza,
        df,
    )
    df, resumo, artefatos = resultado_limpeza
    salvar_resumos_limpeza(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_01_limpeza',
        config.arquivo_entrada_bruta,
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (resultado_contratacao), duracao = executar_etapa(
        'exec_02_contratacao',
        processar_contratacao,
        df,
        config.arquivo_insumos,
    )
    df, resumo, artefatos = resultado_contratacao
    salvar_resumos_contratacao(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_02_contratacao',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (df, resumo), duracao = executar_etapa(
        'exec_03_nota',
        processar_nota,
        df,
    )
    salvar_resumos_nota(resumo, config.pasta_resumo / 'exec_03_nota')
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (resultado_classificacao), duracao = executar_etapa(
        'exec_04_classificacao',
        processar_classificacao,
        df,
        config.arquivo_regras_classificacao,
    )
    df, resumo, artefatos = resultado_classificacao
    salvar_resumos_classificacao(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_04_classificacao',
        arquivo_entrada_resumo='pipeline_memoria_exec_03',
        arquivo_saida_resumo='pipeline_memoria_exec_04',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (resultado_local_editado), duracao = executar_etapa(
        'exec_05_local_editado',
        processar_local_editado,
        df,
        config.arquivo_insumos,
    )
    df, resumo, artefatos = resultado_local_editado
    salvar_resumos_local_editado(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_05_local_editado',
        arquivo_entrada_resumo='pipeline_memoria_exec_04',
        arquivo_saida_resumo='pipeline_memoria_exec_05',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (resultado_ajustes), duracao = executar_etapa(
        'exec_06_ajustes_finais',
        processar_ajustes_finais,
        df,
        config.arquivo_regras_ajuste,
    )
    df, resumo, artefatos = resultado_ajustes
    salvar_resumos_ajustes_finais(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_06_ajustes_finais',
        arquivo_entrada_resumo='pipeline_memoria_exec_05',
        arquivo_saida_resumo='pipeline_memoria_exec_06',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (resultado_operadora), duracao = executar_etapa(
        'exec_07_operadora',
        processar_operadora,
        df,
        config.arquivo_regras_operadora,
    )
    df, resumo, artefatos = resultado_operadora
    salvar_resumos_operadora(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_07_operadora',
        arquivo_entrada_resumo='pipeline_memoria_exec_06',
        arquivo_saida_resumo='pipeline_memoria_exec_07',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (resultado_meta), duracao = executar_etapa(
        'exec_08_meta',
        processar_meta,
        df,
        config.arquivo_insumos,
    )
    df, resumo, artefatos = resultado_meta
    salvar_resumos_meta(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_08_meta',
        arquivo_entrada_resumo='pipeline_memoria_exec_07',
        arquivo_saida_resumo='pipeline_memoria_exec_08',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (resultado_unidade), duracao = executar_etapa(
        'exec_09_resultado_unidade',
        processar_resultado_unidade,
        df,
    )
    df, resumo, artefatos = resultado_unidade
    salvar_resumos_resultado_unidade(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_09_resultado_unidade',
        arquivo_entrada_resumo='pipeline_memoria_exec_08',
        arquivo_saida_resumo='pipeline_memoria_exec_09',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    (resultado_status), duracao = executar_etapa(
        'exec_10_status_unidade',
        processar_status_unidade,
        df,
    )
    df, resumo, artefatos = resultado_status
    salvar_resumos_status_unidade(
        resumo,
        artefatos,
        config.pasta_resumo / 'exec_10_status_unidade',
        arquivo_entrada_resumo='pipeline_memoria_exec_09',
        arquivo_saida_resumo='pipeline_memoria_exec_10',
    )
    registros.append({'etapa': resumo['execucao'], 'duracao_segundos': round(duracao, 2)})

    resultado_analise, duracao = executar_etapa(
        'exec_11_analise_dados',
        processar_analise_dados,
        df,
    )
    caminho_excel, caminho_documentacao = salvar_analise_dados(
        resultado_analise,
        config.pasta_resumo / 'exec_11_analise_dados',
    )
    registros.append({
        'etapa': 'exec_11_analise_dados',
        'duracao_segundos': round(duracao, 2),
        'arquivo_excel': str(caminho_excel),
        'arquivo_documentacao': str(caminho_documentacao),
    })

    config.arquivo_csv_final_pre_validacao.parent.mkdir(parents=True, exist_ok=True)
    salvar_csv_padronizado(df, config.arquivo_csv_final_pre_validacao)
    caminhos_auditoria = salvar_auditoria_insumos_avaliacoes(config)
    caminho_resumo_mestre = salvar_resumo_mestre(config, registros)

    print(f'CSV final da pre-validacao gerado: {config.arquivo_csv_final_pre_validacao}')
    for caminho_auditoria in caminhos_auditoria:
        print(f'Auditoria de insumos gerada: {caminho_auditoria}')
    print(f'Resumo mestre gerado: {caminho_resumo_mestre}')
    print('Pipeline de pre-validacao de avaliacoes finalizada.')
    return 0


def main():
    config = criar_config_pre_validacao()

    codigo = executar_avaliacoes(config)
    if codigo != 0:
        return codigo

    executar_negativas(config)
    print('Pipeline de pre-validacao completa finalizada.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
