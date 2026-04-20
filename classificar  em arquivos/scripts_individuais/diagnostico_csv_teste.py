from datetime import datetime
from pathlib import Path
import unicodedata

import pandas as pd


INPUT_BI = "base de dados 5 estrelas janeiro 26 bi.xlsx"
INPUT_NEGATIVAS = "base de dados 5 estrelas janeiro 26 negativas.xlsx"
GROUP_NAME = "diagnostico"
CLASS_VALUES = ["VIDA IMAGEM", "LABORATORIO"]
EXPECTED_CLASS_COLUMN = "CLASSIFICACAO"


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


def main() -> None:
    start = datetime.now()

    path_bi = Path(INPUT_BI)
    path_neg = Path(INPUT_NEGATIVAS)

    if not path_bi.exists() or not path_neg.exists():
        raise FileNotFoundError("Arquivos de origem nao encontrados.")

    df_bi = pd.read_excel(path_bi)
    df_neg = pd.read_excel(path_neg)

    col_bi = resolve_classification_column(df_bi, INPUT_BI)
    col_neg = resolve_classification_column(df_neg, INPUT_NEGATIVAS)

    class_values_norm = {normalize_text(v) for v in CLASS_VALUES}

    df_bi["__class_norm"] = df_bi[col_bi].apply(normalize_text)
    df_neg["__class_norm"] = df_neg[col_neg].apply(normalize_text)

    bi_filtered = df_bi[df_bi["__class_norm"].isin(class_values_norm)].drop(columns=["__class_norm"])
    neg_filtered = df_neg[df_neg["__class_norm"].isin(class_values_norm)].drop(columns=["__class_norm"])

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("execucoes_individuais_csv") / GROUP_NAME / f"execucao_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    out_bi = output_dir / "avaliacoes.csv"
    out_neg = output_dir / "negativas.csv"

    # utf-8-sig keeps accents compatible with Excel on Windows.
    bi_filtered.to_csv(out_bi, index=False, encoding="utf-8-sig")
    neg_filtered.to_csv(out_neg, index=False, encoding="utf-8-sig")

    end = datetime.now()
    elapsed = end - start

    print("=" * 70)
    print("TESTE CSV - DIAGNOSTICO")
    print("=" * 70)
    print(f"Arquivo avaliacoes: {out_bi}")
    print(f"Arquivo negativas: {out_neg}")
    print(f"Linhas avaliacoes: {len(bi_filtered)}")
    print(f"Linhas negativas: {len(neg_filtered)}")
    print(f"Total linhas: {len(bi_filtered) + len(neg_filtered)}")
    print(f"Tempo total: {elapsed}")


if __name__ == "__main__":
    main()
