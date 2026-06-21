# -*- coding: utf-8 -*-
import os
from dataclasses import dataclass
from pathlib import Path

from funcoes_auxiliares.caminhos import resolver_caminho_onedrive_comercial


@dataclass(frozen=True)
class PipelineConfig:
    arquivo_entrada_bruta: Path
    arquivo_entrada_negativas: Path
    arquivo_insumos: Path
    arquivo_regras_classificacao: Path
    arquivo_regras_ajuste: Path
    arquivo_regras_operadora: Path
    pasta_resumo: Path
    pasta_resumo_negativas: Path
    arquivo_csv_final_pre_validacao: Path
    arquivo_csv_final_pre_validacao_negativas: Path
    arquivo_csv_final: Path


def criar_config_pre_validacao():
    arquivo_entrada_bruta = Path(
        os.environ.get(
            'PIPELINE_AVALIACOES_ENTRADA',
            'data/5_estrelas_maio.csv',
        )
    )
    arquivo_entrada_negativas = Path(
        os.environ.get(
            'PIPELINE_NEGATIVAS_ENTRADA',
            'data/5_estrelas_maio_negativo.csv',
        )
    )

    return PipelineConfig(
        arquivo_entrada_bruta=arquivo_entrada_bruta,
        arquivo_entrada_negativas=arquivo_entrada_negativas,
        arquivo_insumos=resolver_caminho_onedrive_comercial(
            Path('utils/insumos/insumos 5 estrelas.xlsx'),
            Path('5 Estrelas/INSUMOS/insumos 5 estrelas.xlsx'),
        ),
        arquivo_regras_classificacao=resolver_caminho_onedrive_comercial(
            Path('utils/insumos/regra_classificacao.xlsx'),
            Path('5 Estrelas/INSUMOS/regra_classificacao.xlsx'),
        ),
        arquivo_regras_ajuste=resolver_caminho_onedrive_comercial(
            Path('utils/insumos/regra_ajuste_final.xlsx'),
            Path('5 Estrelas/INSUMOS/regra_ajuste_final.xlsx'),
        ),
        arquivo_regras_operadora=resolver_caminho_onedrive_comercial(
            Path('utils/insumos/regras_operadora.xlsx'),
            Path('5 Estrelas/INSUMOS/regras_operadora.xlsx'),
        ),
        pasta_resumo=Path('saida_resumo_avaliacoes') / 'pipeline_pre_validacao',
        pasta_resumo_negativas=(
            Path('saida_resumo_negativas') / 'pipeline_pre_validacao_negativas'
        ),
        arquivo_csv_final_pre_validacao=(
            Path('data_exec_indiv/avaliacoes')
            / 'pipeline_pre_validacao_base_final.csv'
        ),
        arquivo_csv_final_pre_validacao_negativas=(
            Path('data_exec_indiv/negativas')
            / 'pipeline_pre_validacao_base_final.csv'
        ),
        arquivo_csv_final=(
            Path('data_exec_indiv/avaliacoes')
            / 'pipeline_final_base_final.csv'
        ),
    )
