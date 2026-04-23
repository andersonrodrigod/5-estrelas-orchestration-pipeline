import pandas as pd


ARQUIVO_EXCEL = "base pesquisa 5 estrelas março 26.xlsx"
ARQUIVO_SAIDA = "resumo_classificacao.csv"


print("Etapa 1: Iniciando o processo de resumo por CLASSIFICACAO...")
print(f"Arquivo de entrada: {ARQUIVO_EXCEL}")

# 1) Ler arquivo e identificar abas automaticamente
try:
    excel = pd.ExcelFile(ARQUIVO_EXCEL)
except FileNotFoundError:
    print("Erro: arquivo Excel nao encontrado no diretorio atual.")
    raise

abas = excel.sheet_names
print(f"Etapa 2: Abas encontradas ({len(abas)}): {abas}")

# 2) Ler cada aba, mostrar total de linhas e separar colunas necessarias
lista_dataframes = []
total_linhas_abas = 0

for aba in abas:
    print(f"\nLendo a aba: {aba}")
    df_aba = pd.read_excel(ARQUIVO_EXCEL, sheet_name=aba)

    total_aba = len(df_aba)
    total_linhas_abas += total_aba
    print(f"- Linhas nesta aba: {total_aba}")

    colunas_necessarias = ["CLASSIFICACAO", "NOTA GERAL"]
    colunas_faltando = [col for col in colunas_necessarias if col not in df_aba.columns]

    if colunas_faltando:
        print(f"- Aviso: aba ignorada, faltam colunas: {colunas_faltando}")
        continue

    df_aba = df_aba[colunas_necessarias].copy()
    df_aba["ABA_ORIGEM"] = aba
    lista_dataframes.append(df_aba)

if not lista_dataframes:
    print("Erro: nenhuma aba valida foi encontrada com as colunas esperadas.")
    raise ValueError("Nao ha dados validos para processar.")

# 3) Unificar dados das abas
print("\nEtapa 3: Unificando os dados das abas...")
df_unificado = pd.concat(lista_dataframes, ignore_index=True)
print(f"- Total de linhas somadas das abas lidas: {total_linhas_abas}")
print(f"- Total consolidado apos unificacao: {len(df_unificado)}")

# 4) Tratar CLASSIFICACAO (remover vazios)
print("\nEtapa 4: Tratando a coluna CLASSIFICACAO (removendo vazios)...")
df_unificado["CLASSIFICACAO"] = df_unificado["CLASSIFICACAO"].astype(str).str.strip()
df_unificado.loc[df_unificado["CLASSIFICACAO"].isin(["", "nan", "None"]), "CLASSIFICACAO"] = pd.NA

linhas_antes_classificacao = len(df_unificado)
df_classificacao_valida = df_unificado.dropna(subset=["CLASSIFICACAO"]).copy()
linhas_descartadas_classificacao = linhas_antes_classificacao - len(df_classificacao_valida)

print(f"- Linhas descartadas por CLASSIFICACAO vazia: {linhas_descartadas_classificacao}")
print(f"- Linhas restantes apos filtro de CLASSIFICACAO: {len(df_classificacao_valida)}")

# 5) Tratar NOTA GERAL como numerica e medir descarte
print("\nEtapa 5: Convertendo NOTA GERAL para numerica (errors='coerce')...")
nota_original = df_classificacao_valida["NOTA GERAL"].copy()
df_classificacao_valida["NOTA GERAL"] = pd.to_numeric(df_classificacao_valida["NOTA GERAL"], errors="coerce")

linhas_antes_nota = len(df_classificacao_valida)
df_limpo = df_classificacao_valida.dropna(subset=["NOTA GERAL"]).copy()
linhas_descartadas_nota_total = linhas_antes_nota - len(df_limpo)

# Aqui contamos especificamente erros de conversao (valor existia, mas virou NaN)
erro_conversao_nota = ((nota_original.notna()) & (df_classificacao_valida["NOTA GERAL"].isna())).sum()

print(f"- Linhas descartadas por NOTA GERAL nula/invalida: {linhas_descartadas_nota_total}")
print(f"- Linhas descartadas por erro de conversao da NOTA GERAL: {erro_conversao_nota}")
print(f"- Linhas validas para resumo final: {len(df_limpo)}")

# 6) Contagem e media por CLASSIFICACAO
print("\nEtapa 6: Calculando CONTAGEM e MEDIA_NOTA_GERAL por CLASSIFICACAO...")
df_resultado = (
    df_limpo.groupby("CLASSIFICACAO", as_index=False)
    .agg(
        CONTAGEM=("CLASSIFICACAO", "size"),
        MEDIA_NOTA_GERAL=("NOTA GERAL", "mean")
    )
)

# 7) Ordenar pela maior contagem
df_resultado = df_resultado.sort_values(by="CONTAGEM", ascending=False).reset_index(drop=True)
df_resultado["MEDIA_NOTA_GERAL"] = df_resultado["MEDIA_NOTA_GERAL"].round(2)

print("\nEtapa 7: Resultado final pronto (ordenado por CONTAGEM).")
print(df_resultado)

# 8) Salvar em CSV
print(f"\nEtapa 8: Salvando resultado em '{ARQUIVO_SAIDA}'...")
df_resultado.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8-sig")

print("Processo concluido com sucesso!")
