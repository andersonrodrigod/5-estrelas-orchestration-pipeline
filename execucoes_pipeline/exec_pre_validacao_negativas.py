# -*- coding: utf-8 -*-
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from execucoes_pipeline.config import criar_config_pre_validacao
from execucoes_pipeline.exec_pre_validacao import executar_negativas


def main():
    config = criar_config_pre_validacao()
    resultado = executar_negativas(config)
    return 0 if resultado is not None else 1


if __name__ == '__main__':
    sys.exit(main())
