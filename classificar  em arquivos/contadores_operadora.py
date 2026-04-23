import pandas as pd


ARQUIVO_EXCEL = "base pesquisa 5 estrelas março 26.xlsx"
ARQUIVO_SAIDA = "resumo_operadora.csv"


print("Etapa 1: Iniciando o processo de resumo por OPERADORA...")
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

    colunas_necessarias = ["OPERADORA", "NOTA GERAL"]
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

# 4) Tratar OPERADORA (remover vazios)
print("\nEtapa 4: Tratando a coluna OPERADORA (removendo vazios)...")
df_unificado["OPERADORA"] = df_unificado["OPERADORA"].astype(str).str.strip()
df_unificado.loc[df_unificado["OPERADORA"].isin(["", "nan", "None"]), "OPERADORA"] = pd.NA

linhas_antes_operadora = len(df_unificado)
df_operadora_valida = df_unificado.dropna(subset=["OPERADORA"]).copy()
linhas_descartadas_operadora = linhas_antes_operadora - len(df_operadora_valida)

print(f"- Linhas descartadas por OPERADORA vazia: {linhas_descartadas_operadora}")
print(f"- Linhas restantes apos filtro de OPERADORA: {len(df_operadora_valida)}")

# 5) Tratar NOTA GERAL como numerica e medir descarte
print("\nEtapa 5: Convertendo NOTA GERAL para numerica (errors='coerce')...")
nota_original = df_operadora_valida["NOTA GERAL"].copy()
df_operadora_valida["NOTA GERAL"] = pd.to_numeric(df_operadora_valida["NOTA GERAL"], errors="coerce")

linhas_antes_nota = len(df_operadora_valida)
df_limpo = df_operadora_valida.dropna(subset=["NOTA GERAL"]).copy()
linhas_descartadas_nota_total = linhas_antes_nota - len(df_limpo)

# Conta especificamente erro de conversao (valor existia, mas virou NaN)
erro_conversao_nota = ((nota_original.notna()) & (df_operadora_valida["NOTA GERAL"].isna())).sum()

print(f"- Linhas descartadas por NOTA GERAL nula/invalida: {linhas_descartadas_nota_total}")
print(f"- Linhas descartadas por erro de conversao da NOTA GERAL: {erro_conversao_nota}")
print(f"- Linhas validas para resumo final: {len(df_limpo)}")

# 6) Contagem e media por OPERADORA
print("\nEtapa 6: Calculando CONTAGEM e MEDIA_NOTA_GERAL por OPERADORA...")
df_resultado = (
    df_limpo.groupby("OPERADORA", as_index=False)
    .agg(
        CONTAGEM=("OPERADORA", "size"),
        MEDIA_NOTA_GERAL=("NOTA GERAL", "mean")
    )
)

# 7) Adicionar linha combinada de PROMED + HAPVIDA
print("\nEtapa 7: Calculando linha combinada PROMED + HAPVIDA...")
operadoras_combinadas = ["PROMED", "HAPVIDA"]
df_combo = df_limpo[df_limpo["OPERADORA"].isin(operadoras_combinadas)].copy()

if not df_combo.empty:
    contagem_combo = len(df_combo)
    media_combo = round(df_combo["NOTA GERAL"].mean(), 2)
    nova_linha = pd.DataFrame(
        [{
            "OPERADORA": "PROMED + HAPVIDA",
            "CONTAGEM": contagem_combo,
            "MEDIA_NOTA_GERAL": media_combo,
        }]
    )
    df_resultado = pd.concat([df_resultado, nova_linha], ignore_index=True)
    print(f"- CONTAGEM PROMED + HAPVIDA: {contagem_combo}")
    print(f"- MEDIA PROMED + HAPVIDA: {media_combo}")
else:
    print("- Aviso: nao foi possivel calcular PROMED + HAPVIDA (sem dados validos).")

# 8) Ordenar e arredondar para facilitar leitura
df_resultado["MEDIA_NOTA_GERAL"] = df_resultado["MEDIA_NOTA_GERAL"].round(2)
df_resultado = df_resultado.sort_values(by="CONTAGEM", ascending=False).reset_index(drop=True)

print("\nEtapa 8: Resultado final pronto (ordenado por CONTAGEM).")
print(df_resultado)

# 9) Salvar em CSV
print(f"\nEtapa 9: Salvando resultado em '{ARQUIVO_SAIDA}'...")
df_resultado.to_csv(ARQUIVO_SAIDA, index=False, encoding="utf-8-sig")

# 10) Resumo final
print("Etapa 10: Resumo final")
print(f"- Total de abas lidas: {len(abas)}")
print(f"- Total de linhas unificadas: {len(df_unificado)}")
print(f"- Total de linhas validas para calculo: {len(df_limpo)}")
print(f"- Total de grupos no resultado: {len(df_resultado)}")

print("Processo concluido com sucesso!")
