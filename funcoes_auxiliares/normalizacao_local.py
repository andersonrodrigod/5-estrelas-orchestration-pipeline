# -*- coding: utf-8 -*-
import unicodedata

import pandas as pd


def normalizar_local_comparacao(serie):
    serie = serie.astype('string').str.strip().str.lower()
    serie = serie.str.replace(r'\.+$', '', regex=True).str.strip()
    serie = serie.str.replace(r'\s+', ' ', regex=True)
    serie = serie.str.replace('cl¿ico', 'clinico', regex=False)
    return serie.map(
        lambda valor: ''.join(
            caractere
            for caractere in unicodedata.normalize('NFKD', valor)
            if not unicodedata.combining(caractere)
        ) if not pd.isna(valor) else valor
    )
