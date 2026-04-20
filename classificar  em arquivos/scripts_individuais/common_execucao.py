from datetime import datetime
from pathlib import Path
import unicodedata

import pandas as pd


INPUT_BI = "base de dados 5 estrelas janeiro 26 bi.xlsx"
INPUT_NEGATIVAS = "base de dados 5 estrelas janeiro 26 negativas.xlsx"
OUTPUT_PREFIX = "base de dados 5 estrelas janeiro 25 - "
EXPECTED_CLASS_COLUMN = "CLASSIFICACAO"

GROUP_RULES = {
    "diagnostico": [
        "VIDA IMAGEM",
        "LABORATORIO",
    ],
    "hapclinica": [
        "HAPCLINICA",
    ],
    "hospitalar": [
        "HOSPITALAR",
        "INTERNACAO",
    ],
    "med prev e programas especiais": [
        "CASE",
        "GESTAR BEM",
        "INTERNACAO PGC",
        "MED PREV",
        "NASCER BEM",
        "PRODUTO COORDENADO",
        "TEA",
        "TRANSPLANTE RENAL",
    ],
    "odontologia": [
        "ODONTOLOGIA",
    ],
    "rede credenciada": [
        "CRED_ATEND ELETIVO",
        "CRED_ATEND EMERGENCIA",
        "CRED_EXAMES",
        "CRED_INTERNACAO",
        "CRED_LABORATORIO",
        "CRED_TRATAMENTO",
    ],
    "teleconsulta": [
        "HAPCLINICA DIGITAL TELEMEDICINA",
        "TELECONSULTA - DIGITAL",
        "TELEMEDICINA FERNANDES LIMA",
        "TELEMEDICINA LOBO FILHO",
        "TELEMEDICINE RODOLFO FERNANDES",
    ],
}


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def normalize_header(value: object) -> str:
    return normalize_text(value).replace(" ", "").replace("_", "")


def resolve_classification_column(df: pd.DataFrame, file_label: str) -> str:
    target = normalize_header(EXPECTED_CLASS_COLUMN)
    for col in df.columns:
        if normalize_header(col) == target:
            return col
    raise ValueError(
        f"Coluna de classificacao nao encontrada em '{file_label}'. "
        f"Esperado: {EXPECTED_CLASS_COLUMN}"
    )


def run_group(group_name: str) -> None:
    if group_name not in GROUP_RULES:
        raise ValueError(f"Grupo invalido: {group_name}")

    path_bi = Path(INPUT_BI)
    path_neg = Path(INPUT_NEGATIVAS)
    if not path_bi.exists() or not path_neg.exists():
        raise FileNotFoundError("Arquivos de origem nao encontrados.")

    df_bi = pd.read_excel(path_bi)
    df_neg = pd.read_excel(path_neg)

    col_bi = resolve_classification_column(df_bi, INPUT_BI)
    col_neg = resolve_classification_column(df_neg, INPUT_NEGATIVAS)

    selected = {normalize_text(v) for v in GROUP_RULES[group_name]}

    df_bi = df_bi.copy()
    df_neg = df_neg.copy()
    df_bi["__class_norm"] = df_bi[col_bi].apply(normalize_text)
    df_neg["__class_norm"] = df_neg[col_neg].apply(normalize_text)

    bi_filtered = df_bi[df_bi["__class_norm"].isin(selected)].drop(columns=["__class_norm"])
    neg_filtered = df_neg[df_neg["__class_norm"].isin(selected)].drop(columns=["__class_norm"])

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("execucoes_individuais") / group_name / f"execucao_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{OUTPUT_PREFIX}{group_name}.xlsx"
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        bi_filtered.to_excel(writer, sheet_name="avaliacoes", index=False)
        neg_filtered.to_excel(writer, sheet_name="negativas", index=False)

    report = [
        "=" * 70,
        f"EXECUCAO INDIVIDUAL: {group_name}",
        "=" * 70,
        f"Arquivo gerado: {output_file}",
        f"Total avaliacoes enviadas: {len(bi_filtered)}",
        f"Total negativas enviadas: {len(neg_filtered)}",
        f"Total geral enviado: {len(bi_filtered) + len(neg_filtered)}",
    ]

    report_text = "\n".join(report)
    print(report_text)

    report_path = output_dir / "relatorio_execucao.txt"
    report_path.write_text(report_text, encoding="utf-8")
    print(f"Relatorio salvo em: {report_path}")
