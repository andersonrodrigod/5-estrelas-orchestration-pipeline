# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

from funcoes_auxiliares.padronizacao_csv import salvar_csv_padronizado


def garantir_pasta(caminho):
    Path(caminho).mkdir(parents=True, exist_ok=True)


def salvar_json(caminho, dados):
    caminho = Path(caminho)
    garantir_pasta(caminho.parent)

    with open(caminho, 'w', encoding='utf-8') as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=4)


def salvar_txt(caminho, linhas):
    caminho = Path(caminho)
    garantir_pasta(caminho.parent)

    with open(caminho, 'w', encoding='utf-8') as arquivo:
        arquivo.write('\n'.join(linhas))


def salvar_csv(caminho, df):
    caminho = Path(caminho)
    garantir_pasta(caminho.parent)
    salvar_csv_padronizado(df, caminho)


def salvar_csv_simples(caminho, registros):
    salvar_csv(caminho, pd.DataFrame(registros))
