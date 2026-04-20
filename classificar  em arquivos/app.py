from datetime import datetime
from pathlib import Path
import unicodedata

import pandas as pd


INPUT_BI = "base de dados 5 estrelas feveireiro 26.xlsx"
INPUT_NEGATIVAS = "base de negativas fevereiro 26.xlsx"
OUTPUT_PREFIX = "base de dados 5 estrelas fevereiro 25 - "
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
        "QUALIVIDA",
        "TRANSPLANTE"
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
        "TELECONSULTA ELETIVA",
        "TELEMEDICINA",
        "TELECONSULTA CASE",
        "TELECONSULTA URGÊNCIA"
    ],
}


def normalize_text(value: object) -> str:
    if pd.isna(value): #type: ignore
        return ""
    text = str(value).strip().upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = " ".join(text.split())
    return text


def normalize_header(value: object) -> str:
    text = normalize_text(value)
    text = text.replace(" ", "").replace("_", "")
    return text


def resolve_classification_column(df: pd.DataFrame, file_label: str) -> str:
    normalized_target = normalize_header(EXPECTED_CLASS_COLUMN)
    for column in df.columns:
        if normalize_header(column) == normalized_target:
            return column
    raise ValueError(
        f"Coluna de classificacao nao encontrada em '{file_label}'. "
        f"Esperado: {EXPECTED_CLASS_COLUMN} (aceita variacao com acento)."
    )


def load_all_sheets(file_path: Path, file_label: str) -> pd.DataFrame:
    sheets = pd.read_excel(file_path, sheet_name=None)
    if not sheets:
        raise ValueError(f"Nenhuma aba encontrada em '{file_label}'.")

    dataframes: list[pd.DataFrame] = []
    for sheet_name, df in sheets.items():
        if df.empty:
            continue
        df_sheet = df.copy()
        df_sheet["__origem_aba"] = sheet_name
        dataframes.append(df_sheet)

    if not dataframes:
        raise ValueError(f"Todas as abas de '{file_label}' estao vazias.")

    return pd.concat(dataframes, ignore_index=True, sort=False)


def build_normalized_rules(
    group_rules: dict[str, list[str]],
) -> tuple[dict[str, set[str]], dict[str, str]]:
    normalized_group_rules: dict[str, set[str]] = {}
    class_to_group: dict[str, str] = {}

    for group_name, classes in group_rules.items():
        normalized_set = {normalize_text(c) for c in classes}
        normalized_group_rules[group_name] = normalized_set

        for cls in normalized_set:
            if cls in class_to_group:
                raise ValueError(
                    f"Classificacao duplicada em grupos diferentes: "
                    f"'{cls}' -> '{class_to_group[cls]}' e '{group_name}'."
                )
            class_to_group[cls] = group_name

    return normalized_group_rules, class_to_group


def build_display_class_map(group_rules: dict[str, list[str]]) -> dict[str, dict[str, str]]:
    display_map: dict[str, dict[str, str]] = {}
    for group_name, classes in group_rules.items():
        group_map: dict[str, str] = {}
        for class_name in classes:
            group_map[normalize_text(class_name)] = class_name
        display_map[group_name] = group_map
    return display_map


def get_unassigned_summary(
    df: pd.DataFrame,
    original_class_column: str,
) -> pd.Series:
    unassigned = df.loc[df["__group"].isna(), original_class_column]
    return unassigned.astype(str).value_counts().sort_index()


def main() -> None:
    path_bi = Path(INPUT_BI)
    path_neg = Path(INPUT_NEGATIVAS)

    if not path_bi.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {path_bi}")
    if not path_neg.exists():
        raise FileNotFoundError(f"Arquivo nao encontrado: {path_neg}")

    df_bi = load_all_sheets(path_bi, INPUT_BI)
    df_neg = load_all_sheets(path_neg, INPUT_NEGATIVAS)

    col_bi = resolve_classification_column(df_bi, INPUT_BI)
    col_neg = resolve_classification_column(df_neg, INPUT_NEGATIVAS)

    normalized_group_rules, class_to_group = build_normalized_rules(GROUP_RULES)
    display_class_map = build_display_class_map(GROUP_RULES)

    df_bi = df_bi.copy()
    df_neg = df_neg.copy()

    df_bi["__class_norm"] = df_bi[col_bi].apply(normalize_text)
    df_neg["__class_norm"] = df_neg[col_neg].apply(normalize_text)
    df_bi["__group"] = df_bi["__class_norm"].map(class_to_group)
    df_neg["__group"] = df_neg["__class_norm"].map(class_to_group)
    bi_class_counts = df_bi.groupby(["__group", "__class_norm"]).size()
    neg_class_counts = df_neg.groupby(["__group", "__class_norm"]).size()

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("execucoes") / f"execucao_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)

    sent_counts: dict[str, dict[str, int]] = {}

    for group_name in GROUP_RULES:
        bi_filtered = df_bi[df_bi["__group"] == group_name].drop(
            columns=["__class_norm", "__group"]
        )
        neg_filtered = df_neg[df_neg["__group"] == group_name].drop(
            columns=["__class_norm", "__group"]
        )

        output_file = output_dir / f"{OUTPUT_PREFIX}{group_name}.xlsx"
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            bi_filtered.to_excel(writer, sheet_name="avaliacoes", index=False)
            neg_filtered.to_excel(writer, sheet_name="negativas", index=False)

        sent_counts[group_name] = {
            "avaliacoes": len(bi_filtered),
            "negativas": len(neg_filtered),
            "total": len(bi_filtered) + len(neg_filtered),
        }

    sent_bi = int(df_bi["__group"].notna().sum())
    sent_neg = int(df_neg["__group"].notna().sum())
    total_sent = sent_bi + sent_neg

    total_bi = len(df_bi)
    total_neg = len(df_neg)
    total_origin = total_bi + total_neg

    unassigned_bi = get_unassigned_summary(df_bi, col_bi)
    unassigned_neg = get_unassigned_summary(df_neg, col_neg)

    report_lines: list[str] = []
    report_lines.append("=" * 70)
    report_lines.append("ARQUIVOS GERADOS")
    report_lines.append("=" * 70)
    report_lines.append(f"Pasta da execucao: {output_dir}")
    for group_name in GROUP_RULES:
        report_lines.append(f"- {OUTPUT_PREFIX}{group_name}.xlsx")

    report_lines.append("")
    report_lines.append("=" * 70)
    report_lines.append("1) CLASSIFICACOES NAO ENVIADAS PARA NENHUM ARQUIVO")
    report_lines.append("=" * 70)
    report_lines.append(f"[Origem: {INPUT_BI}]")
    if unassigned_bi.empty:
        report_lines.append("Todas as classificacoes deste arquivo foram enviadas.")
    else:
        for class_name, qtd in unassigned_bi.items():
            report_lines.append(f"- {class_name}: {qtd}")

    report_lines.append("")
    report_lines.append(f"[Origem: {INPUT_NEGATIVAS}]")
    if unassigned_neg.empty:
        report_lines.append("Todas as classificacoes deste arquivo foram enviadas.")
    else:
        for class_name, qtd in unassigned_neg.items():
            report_lines.append(f"- {class_name}: {qtd}")

    report_lines.append("")
    report_lines.append("=" * 70)
    report_lines.append("2) COMO OS DADOS FORAM SEPARADOS")
    report_lines.append("=" * 70)
    for group_name in GROUP_RULES:
        report_lines.append(f"[Grupo: {group_name}]")
        report_lines.append(
            "Classificacoes da regra: "
            + ", ".join(GROUP_RULES[group_name])
        )
        for class_norm in sorted(normalized_group_rules[group_name]):
            class_label = display_class_map[group_name].get(class_norm, class_norm)
            bi_count = int(bi_class_counts.get((group_name, class_norm), 0))
            neg_count = int(neg_class_counts.get((group_name, class_norm), 0))
            report_lines.append(
                f"- {class_label}: "
                f"avaliacoes={bi_count} | negativas={neg_count} | total={bi_count + neg_count}"
            )
        report_lines.append("")

    report_lines.append("=" * 70)
    report_lines.append("3) QUANTIDADES ENVIADAS POR ARQUIVO")
    report_lines.append("=" * 70)
    for group_name, counts in sent_counts.items():
        report_lines.append(
            f"- {group_name}: "
            f"avaliacoes={counts['avaliacoes']} | "
            f"negativas={counts['negativas']} | "
            f"total={counts['total']}"
        )

    report_lines.append("")
    report_lines.append("=" * 70)
    report_lines.append("TOTAIS GERAIS")
    report_lines.append("=" * 70)
    report_lines.append(f"Total origem avaliacoes ({INPUT_BI}): {total_bi}")
    report_lines.append(f"Total origem negativas ({INPUT_NEGATIVAS}): {total_neg}")
    report_lines.append(f"Total origem (2 arquivos): {total_origin}")
    report_lines.append(f"Total enviado de avaliacoes: {sent_bi}")
    report_lines.append(f"Total enviado de negativas: {sent_neg}")
    report_lines.append(f"Total enviado (geral): {total_sent}")

    report_lines.append("")
    report_lines.append("=" * 70)
    report_lines.append("CONFERENCIA FINAL")
    report_lines.append("=" * 70)
    if total_sent == total_origin:
        report_lines.append("OK: todos os registros foram enviados e os numeros batem.")
    else:
        report_lines.append("ATENCAO: ha diferenca entre origem e enviados.")
        report_lines.append(f"Diferenca (origem - enviados): {total_origin - total_sent}")

    report_text = "\n".join(report_lines)
    print(report_text)

    report_path = output_dir / "relatorio_execucao.txt"
    report_path.write_text(report_text, encoding="utf-8")
    print(f"\nRelatorio salvo em: {report_path}")


if __name__ == "__main__":
    main()
