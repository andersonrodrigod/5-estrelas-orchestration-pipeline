# -*- coding: utf-8 -*-
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from execucoes_pipeline.config import criar_config_pre_validacao
from execucoes_pipeline.exec_pre_validacao import executar_avaliacoes


def main():
    config = criar_config_pre_validacao()
    return executar_avaliacoes(config)


if __name__ == '__main__':
    sys.exit(main())
