# -*- coding: utf-8 -*-
"""
Consulta quantidades do 5 Estrelas no Oracle e grava um Excel de conferencia.

Usa o mesmo scripts/.env.oracle da extracao principal para conexao.

Variaveis opcionais no .env.oracle:
    CONSULTA_ANO=2026
    CONSULTA_MES=7
    CONSULTA_OUTPUT_DIR=data\\oracle\\consulta_5_estrelas
    CONSULTA_OUTPUT_FILE_NAME=consulta_5_estrelas_2026_07.xlsx
"""
import getpass
import os
import re
from calendar import monthrange
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


ORACLE_CLIENT_DIR = os.getenv(
    "ORACLE_CLIENT_DIR",
    r"C:\app\client\anderson.dossantos\product\19.0.0\client_1\bin",
)
TNS_ADMIN = os.getenv(
    "TNS_ADMIN",
    r"C:\app\client\anderson.dossantos\product\19.0.0\client_1\network\admin",
)

ORACLE_USER = os.getenv("ORACLE_USER", "SEU_USUARIO")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
ORACLE_DSN = os.getenv("ORACLE_DSN", "DBRAWZN")
ORACLE_HOST = os.getenv("ORACLE_HOST", "")
ORACLE_PORT = os.getenv("ORACLE_PORT", "1521")
ORACLE_SERVICE = os.getenv("ORACLE_SERVICE", "")

TABLE_NAME = os.getenv(
    "TABLE_NAME",
    "USR_ATENDIMENTO_5ESTRELAS.RAW_LND_5_ESTRELAS_DETALHADO",
)

CONSULTA_ANO = int(os.getenv("CONSULTA_ANO", "2026"))
CONSULTA_MES = int(os.getenv("CONSULTA_MES", "7"))
CONSULTA_OUTPUT_DIR = Path(
    os.getenv("CONSULTA_OUTPUT_DIR", r"data\oracle\consulta_5_estrelas")
)
CONSULTA_OUTPUT_FILE_NAME = os.getenv(
    "CONSULTA_OUTPUT_FILE_NAME",
    f"consulta_5_estrelas_{CONSULTA_ANO}_{CONSULTA_MES:02d}.xlsx",
)
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "10000"))


def montar_dsn():
    if ORACLE_DSN:
        return ORACLE_DSN

    if not ORACLE_HOST or not ORACLE_SERVICE:
        raise ValueError(
            "Preencha ORACLE_DSN ou ORACLE_HOST + ORACLE_SERVICE no .env.oracle."
        )

    return f"//{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"


def validar_nome_tabela(nome):
    partes = nome.split(".")
    if not 1 <= len(partes) <= 2:
        raise ValueError(f"TABLE_NAME invalido: {nome}")

    for parte in partes:
        if not parte.replace("_", "").isalnum():
            raise ValueError(f"TABLE_NAME possui caracteres invalidos: {nome}")

    return nome


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


def caminho_saida_excel():
    CONSULTA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    nome = CONSULTA_OUTPUT_FILE_NAME
    if not nome.lower().endswith(".xlsx"):
        nome = f"{nome}.xlsx"

    return CONSULTA_OUTPUT_DIR / nome


def executar_consulta_df(conn, sql, binds):
    nomes_binds = set(re.findall(r":([A-Za-z_][A-Za-z0-9_]*)", sql))
    binds_consulta = {
        nome: valor
        for nome, valor in binds.items()
        if nome in nomes_binds
    }

    with conn.cursor() as cursor:
        cursor.arraysize = FETCH_SIZE
        cursor.execute(sql, binds_consulta)
        colunas = [coluna[0] for coluna in cursor.description]
        linhas = []

        while True:
            lote = cursor.fetchmany(FETCH_SIZE)
            if not lote:
                break
            linhas.extend(lote)

    return pd.DataFrame(linhas, columns=colunas)


def montar_consultas(tabela):
    filtros_validos = """
        ANO = :ano
        AND MES = :mes
        AND NOTA1 NOT IN ('NQA', 'N/A', 'IGN')
    """

    return {
        "contagens_5_estrelas": f"""
            SELECT 'VALIDOS' AS INDICADOR, COUNT(*) AS QUANTIDADE
            FROM {tabela}
            WHERE {filtros_validos}
            UNION ALL
            SELECT 'N/A' AS INDICADOR, COUNT(*) AS QUANTIDADE
            FROM {tabela}
            WHERE ANO = :ano
              AND MES = :mes
              AND NOTA1 = 'N/A'
            UNION ALL
            SELECT 'IGN' AS INDICADOR, COUNT(*) AS QUANTIDADE
            FROM {tabela}
            WHERE ANO = :ano
              AND MES = :mes
              AND NOTA1 = 'IGN'
        """,
        "quantidade_por_dia": f"""
            SELECT
                DIA,
                COUNT(*) AS QUANTIDADE
            FROM (
                SELECT
                    TRUNC(
                        TO_DATE(
                            DT_RESPOSTA DEFAULT NULL ON CONVERSION ERROR,
                            'YYYY-MM-DD HH24:MI:SS'
                        )
                    ) AS DIA
                FROM {tabela}
                WHERE {filtros_validos}
            )
            WHERE DIA IS NOT NULL
            GROUP BY DIA
            ORDER BY DIA
        """,
        "urgencia": """
            SELECT COUNT(*) AS TOTAL_ATENDIMENTOS
            FROM (
                SELECT HTM.HR_FIM_ATENDIMENTO, HTM.HR_ATENDIMENTO
                FROM RAWZN.RAW_HSP_TM_ATENDIMENTO HTM
                WHERE HTM.DT_ATENDIMENTO BETWEEN :data_inicio AND :data_fim
                  AND HTM.CD_MOTIVO_ATENDIMENTO IN (
                      '1', '2', '3', '4', '5', '6', '8', '9', '10', '11'
                  )
                UNION ALL
                SELECT PTM.HR_FIM_ATENDIMENTO, PTM.HR_ATENDIMENTO
                FROM RAWZN.RAW_PSC_TM_ATENDIMENTO PTM
                WHERE PTM.DT_ATENDIMENTO BETWEEN :data_inicio AND :data_fim
                  AND PTM.CD_MOTIVO_ATENDIMENTO IN (
                      '1', '2', '3', '4', '5', '6', '8', '9', '10', '11'
                  )
            ) COMBINED
        """,
        "eletivos": """
            SELECT COUNT(*) AS TOTAL_ATENDIMENTOS
            FROM RAWZN.RAW_HAP_TB_ATENDIMENTO_CONSULTA TC
            WHERE TC.FL_TIPO_ATENDIMENTO = 0
              AND TC.DT_ATENDIMENTO BETWEEN :data_inicio AND :data_fim
        """,
    }


def montar_resumo_unico(dfs):
    contagens = {
        str(linha["INDICADOR"]).strip().upper(): int(linha["QUANTIDADE"])
        for _, linha in dfs["contagens_5_estrelas"].iterrows()
    }
    validos = contagens.get("VALIDOS", 0)
    n_a = contagens.get("N/A", 0)
    ign = contagens.get("IGN", 0)
    urgencia = int(dfs["urgencia"].iloc[0]["TOTAL_ATENDIMENTOS"])
    eletivos = int(dfs["eletivos"].iloc[0]["TOTAL_ATENDIMENTOS"])
    atendimentos = urgencia + eletivos
    percentual = validos / atendimentos if atendimentos else 0

    return pd.DataFrame(
        [
            {
                "ANO": CONSULTA_ANO,
                "MES": CONSULTA_MES,
                "VALIDOS": validos,
                "N/A": n_a,
                "IGN": ign,
                "URGENCIA": urgencia,
                "ELETIVOS": eletivos,
                "ATENDIMENTOS": atendimentos,
                "PERCENTUAL_VALIDOS_SOBRE_ATENDIMENTOS": percentual,
            }
        ]
    )


def escrever_aba_resumo(writer, resumo_df, quantidade_por_dia_df):
    sheet_name = "resumo"
    resumo_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0)
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    percentual_coluna = resumo_df.columns.get_loc(
        "PERCENTUAL_VALIDOS_SOBRE_ATENDIMENTOS"
    ) + 1
    worksheet.cell(row=2, column=percentual_coluna).number_format = "0.00%"

    inicio_quantidade = len(resumo_df) + 3
    worksheet.cell(row=inicio_quantidade, column=1, value="QUANTIDADE POR DIA")
    quantidade_por_dia_df.to_excel(
        writer,
        sheet_name=sheet_name,
        index=False,
        startrow=inicio_quantidade,
    )

    for coluna in worksheet.columns:
        largura = max(len(str(celula.value or "")) for celula in coluna)
        worksheet.column_dimensions[coluna[0].column_letter].width = min(
            max(largura + 2, 12),
            55,
        )


def consultar():
    inicio = datetime.now()
    tabela = validar_nome_tabela(TABLE_NAME)
    ultimo_dia = monthrange(CONSULTA_ANO, CONSULTA_MES)[1]
    data_inicio = datetime(CONSULTA_ANO, CONSULTA_MES, 1)
    data_fim = datetime(CONSULTA_ANO, CONSULTA_MES, ultimo_dia, 23, 59, 59)
    binds = {
        "ano": CONSULTA_ANO,
        "mes": CONSULTA_MES,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
    }
    arquivo_excel = caminho_saida_excel()
    consultas = montar_consultas(tabela)

    print(f"Conectando em: {montar_dsn()}")
    print(f"Tabela: {tabela}")
    print(f"Periodo: ANO={CONSULTA_ANO} MES={CONSULTA_MES}")
    print(f"Excel de saida: {arquivo_excel}")

    with conectar() as conn:
        dfs = {}
        for nome, sql in consultas.items():
            print(f"Executando consulta: {nome}")
            dfs[nome] = executar_consulta_df(conn, sql, binds)
            print(f"Linhas retornadas em {nome}: {len(dfs[nome]):,}")

    fim = datetime.now()
    print(f"Duracao: {round((fim - inicio).total_seconds(), 2)}s")

    resumo_df = montar_resumo_unico(dfs)

    with pd.ExcelWriter(arquivo_excel, engine="openpyxl") as writer:
        escrever_aba_resumo(
            writer,
            resumo_df,
            dfs["quantidade_por_dia"],
        )

    print("Consulta finalizada.")
    print(f"Excel gerado: {arquivo_excel}")


if __name__ == "__main__":
    consultar()
