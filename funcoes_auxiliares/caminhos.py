# -*- coding: utf-8 -*-
import os
from pathlib import Path


def resolver_caminho_onedrive_comercial(caminho_atual, caminho_onedrive):
    caminho_atual = Path(caminho_atual)
    base_onedrive = os.environ.get('OneDriveCommercial')

    if base_onedrive:
        caminho_comercial = Path(base_onedrive) / caminho_onedrive

        if caminho_comercial.exists():
            print(f'Usando arquivo do OneDrive comercial: {caminho_comercial}')
            return caminho_comercial

    print(f'Usando arquivo local do projeto: {caminho_atual}')
    return caminho_atual
