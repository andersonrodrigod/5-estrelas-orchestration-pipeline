# -*- coding: utf-8 -*-
"""
Exemplo de extracao Oracle para rodar pelo VS Code.

Antes de executar:
    pip install oracledb pandas openpyxl

Como usar:
    1. Copie scripts/.env.oracle.example para scripts/.env.oracle
    2. Preencha usuario, senha, alias/tabela e pasta de saida
    3. Rode: python scripts/extracao_oracle_exemplo.py
"""
import csv
import getpass
import os
from datetime import datetime
from pathlib import Path

import oracledb
import pandas as pd


ARQUIVO_ENV = Path(__file__).with_name(".env.oracle")


def carregar_env(caminho=ARQUIVO_ENV):
    if not caminho.exists():
        return

    for linha in caminho.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue

        chave, valor = linha.split("=", 1)
        os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


carregar_env()


# Caminhos do Oracle Client nesta maquina.
ORACLE_CLIENT_DIR = os.getenv(
    "ORACLE_CLIENT_DIR",
    r"C:\app\client\anderson.dossantos\product\19.0.0\client_1\bin",
)
TNS_ADMIN = os.getenv(
    "TNS_ADMIN",
    r"C:\app\client\anderson.dossantos\product\19.0.0\client_1\network\admin",
)

# Conexao. Use ORACLE_DSN para alias do tnsnames.ora, ou HOST/PORT/SERVICE.
ORACLE_USER = os.getenv("ORACLE_USER", "SEU_USUARIO")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
ORACLE_DSN = os.getenv("ORACLE_DSN", "DBRAWZN")
ORACLE_HOST = os.getenv("ORACLE_HOST", "")
ORACLE_PORT = os.getenv("ORACLE_PORT", "1521")
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "")

# Tabela e destino. Ajuste TABLE_NAME e OUTPUT_FILE_NAME conforme a extracao.
TABLE_NAME = os.getenv(
    "TABLE_NAME",
    "USR_ATENDIMENTO_5ESTRELAS.RAW_LND_5_ESTRELAS_DETALHADO",
)
OUTPUT_FILE_NAME = os.getenv("OUTPUT_FILE_NAME", "5_ESTRELAS_EXTRACAO")
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", r"data\oracle"))
LOG_FILE_NAME = os.getenv("LOG_FILE_NAME", "log_extracao_oracle.xlsx")

FETCH_SIZE = int(os.getenv("FETCH_SIZE", "10000"))
PROGRESS_CADA = int(os.getenv("PROGRESS_CADA", "50000"))

# Opcional: coloque um WHERE sem a palavra WHERE. Exemplo:
# SQL_WHERE=DT_ATENDIMENTO >= DATE '2026-06-01'
SQL_WHERE = os.getenv("SQL_WHERE", "").strip()


def montar_dsn():
    if ORACLE_DSN:
        return ORACLE_DSN

    if not ORACLE_HOST or not ORACLE_SERVICE:
        raise ValueError(
            "Preencha ORACLE_DSN ou ORACLE_HOST + ORACLE_SERVICE no .env.oracle."
        )

    return f"//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"


def montar_sql():
    sql = f"SELECT * FROM {TABLE_NAME}"
    if SQL_WHERE:
        sql += f" WHERE {SQL_WHERE}"
    return sql


def conectar():
    if TNS_ADMIN:
        os.environ["TNS_ADMIN"] = TNS_ADMIN

    oracledb.init_oracle_client(lib_dir=ORACLE_CLIENT_DIR)

    senha = ORACLE_PASSWORD
    if not senha:
        senha = getpass.getpass("Senha Oracle: ")

    return oracledb.connect(
        user=ORACLE_USER,
        password=senha,
        dsn=montar_dsn(),
    )


def caminho_saida_csv():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{OUTPUT_FILE_NAME}.csv"


def caminho_log():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / LOG_FILE_NAME


def salvar_log(registro):
    arquivo_log = caminho_log()
    df_novo = pd.DataFrame([registro])

    if arquivo_log.exists():
        df_log = pd.read_excel(arquivo_log)
        df_log = pd.concat([df_log, df_novo], ignore_index=True)
    else:
        df_log = df_novo

    df_log.to_excel(arquivo_log, index=False)


def extrair():
    inicio = datetime.now()
    arquivo_csv = caminho_saida_csv()
    sql = montar_sql()
    total_linhas = 0

    print(f"Conectando em: {montar_dsn()}")
    print(f"Tabela: {TABLE_NAME}")
    print(f"Arquivo CSV: {arquivo_csv}")

    with conectar() as conn:
        with conn.cursor() as cursor:
            cursor.arraysize = FETCH_SIZE
            cursor.execute(sql)

            colunas = [coluna[0] for coluna in cursor.description] # type: ignore

            with arquivo_csv.open("w", newline="", encoding="utf-8-sig") as saida:
                writer = csv.writer(saida, delimiter=";")
                writer.writerow(colunas)

                while True:
                    linhas = cursor.fetchmany(FETCH_SIZE)
                    if not linhas:
                        break

                    writer.writerows(linhas)
                    total_linhas += len(linhas)

                    if total_linhas % PROGRESS_CADA == 0:
                        print(f"Linhas extraidas: {total_linhas:,}")

    fim = datetime.now()
    registro = {
        "inicio": inicio,
        "fim": fim,
        "duracao_segundos": round((fim - inicio).total_seconds(), 2),
        "usuario": ORACLE_USER,
        "dsn": montar_dsn(),
        "tabela": TABLE_NAME,
        "where": SQL_WHERE,
        "arquivo_csv": str(arquivo_csv),
        "linhas": total_linhas,
    }
    salvar_log(registro)

    print(f"Extracao finalizada. Linhas: {total_linhas:,}")
    print(f"CSV gerado: {arquivo_csv}")
    print(f"Log gerado: {caminho_log()}")


if __name__ == "__main__":
    extrair()
