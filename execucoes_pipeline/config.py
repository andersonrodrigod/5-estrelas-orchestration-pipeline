# -*- coding: utf-8 -*-
import os
from dataclasses import dataclass
from pathlib import Path

# criar um loop


@dataclass(frozen=True)
class PipelineConfig:
    pasta_auditoria: Path
    pasta_resumo_avaliacoes: Path
    pasta_resumo_pipeline: Path
    pasta_resumo_negativa: Path
    pasta_resumo_separacao: Path
    arquivo_entrada_bruta: Path
    arquivo_entrada_negativas: Path
    arquivo_insumos: Path
    arquivo_regras_classificacao: Path
    arquivo_regras_ajuste: Path
    arquivo_regras_operadora: Path
    pasta_resumo: Path
    pasta_resumo_negativas: Path
    pasta_resumo_exec_separacao: Path
    arquivo_csv_final_pre_validacao: Path
    arquivo_csv_final_pre_validacao_negativas: Path
    arquivo_csv_final: Path
    arquivo_nomes_classificacao: Path
    pasta_saida_excel_separacao_local: Path
    pasta_saida_excel_separacao_sharepoint: Path
    arquivo_power_bi: Path
    pasta_resumo_arquivos_bi: Path
    arquivos_excel_power_bi: dict
    pasta_copia_power_bi: Path
    nomes_copia_power_bi: dict


def criar_config_pre_validacao():
    pasta_auditoria = Path('auditoria')
    pasta_resumo_avaliacoes = pasta_auditoria / 'saida_resumo_avaliacoes'
    pasta_resumo_pipeline = pasta_auditoria / 'saida_resumo_pipeline'
    pasta_resumo_negativa = pasta_auditoria / 'saida_resumo_negativa'
    pasta_resumo_separacao = pasta_auditoria / 'saida_resumo_separacao'

    arquivo_entrada_bruta = Path(
        os.environ.get(
            'PIPELINE_AVALIACOES_ENTRADA',
            'data/oracle/5_estrelas_julho.csv',
        )
    )
    arquivo_entrada_negativas = Path(
        os.environ.get(
            'PIPELINE_NEGATIVAS_ENTRADA',
            'data/oracle/5_estrelas_negativas_julho.csv',
        )
    )

    return PipelineConfig(
        pasta_auditoria=pasta_auditoria,
        pasta_resumo_avaliacoes=pasta_resumo_avaliacoes,
        pasta_resumo_pipeline=pasta_resumo_pipeline,
        pasta_resumo_negativa=pasta_resumo_negativa,
        pasta_resumo_separacao=pasta_resumo_separacao,
        arquivo_entrada_bruta=arquivo_entrada_bruta,
        arquivo_entrada_negativas=arquivo_entrada_negativas,
        arquivo_insumos=Path('utils/insumos/insumos 5 estrelas.xlsx'),
        arquivo_regras_classificacao=Path(
            'utils/insumos/regra_classificacao.xlsx'
        ),
        arquivo_regras_ajuste=Path('utils/insumos/regra_ajuste_final.xlsx'),
        arquivo_regras_operadora=Path('utils/insumos/regras_operadora.xlsx'),
        pasta_resumo=pasta_resumo_pipeline / 'exec_pre_validacao_avaliacoes',
        pasta_resumo_negativas=(
            pasta_resumo_pipeline / 'exec_pre_validacao_negativas'
        ),
        pasta_resumo_exec_separacao=(
            pasta_resumo_separacao / 'exec_separacao'
        ),
        arquivo_csv_final_pre_validacao=(
            Path('data_exec/pipeline')
            / 'pipeline_pre_validacao_avaliacoes_base_final.csv'
        ),
        arquivo_csv_final_pre_validacao_negativas=(
            Path('data_exec/pipeline')
            / 'pipeline_pre_validacao_negativas_base_final.csv'
        ),
        arquivo_csv_final=(
            Path('data_exec/pipeline')
            / 'pipeline_final_base_final.csv'
        ),
        arquivo_nomes_classificacao=Path('data/nomes_classificacao.json'),
        pasta_saida_excel_separacao_local=Path('data_exec/separacao'),
        pasta_saida_excel_separacao_sharepoint=Path(
            r'C:\Users\anderson.dossantos\HAPVIDA ASSISTÊNCIA MÉDICA LTDA'
            r'\5 Estrelas - Documentos\Base de Dados 5 Estrelas'
            r'\base de dados julho 26\atualizações das classificações'
        ),
        arquivo_power_bi=Path('data_exec/pipeline/12_base_power_bi.csv'),
        pasta_resumo_arquivos_bi=(
            pasta_resumo_pipeline / 'exec_gerar_arquivos_bi'
        ),
        arquivos_excel_power_bi={
            'TIPO 1 A 3': {
                'arquivo': Path('data_exec/excel_bi/13_base_tipo_1_a_3_power_bi.xlsx'),
                'aba': 'tipos 1 a 3',
            },
            'TIPO 4 A 7': {
                'arquivo': Path('data_exec/excel_bi/13_base_tipo_4_a_7_power_bi.xlsx'),
                'aba': 'tipos 4 a 7',
            },
            'TIPO 8 OU MAIS': {
                'arquivo': Path('data_exec/excel_bi/13_base_tipo_8_ou_mais_power_bi.xlsx'),
                'aba': 'tipos 8 ou mais',
            },
        },
        pasta_copia_power_bi=Path(
            r'G:\Superintendencia de Atendimento\Inteligência de Dados'
            r'\3 - Bases Gerais\3.5 - Base 5 Estrelas Power BI'
            r'\3.5.1 - Bases Consolidadas'
        ),
        nomes_copia_power_bi={
            'TIPO 1 A 3': '5_ESTRELAS_JULHO_26_1.xlsx',
            'TIPO 4 A 7': '5_ESTRELAS_JULHO_26_2.xlsx',
            'TIPO 8 OU MAIS': '5_ESTRELAS_JULHO_26_3.xlsx',
        },
    )
