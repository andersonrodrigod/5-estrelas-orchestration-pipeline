import pandas as pd

df = pd.read_csv('data/5_estrelas_fevereiro.csv')

colunas = ['Nota geral', 'Contratação', 'CLASSIFICACAO', 'Operadora', 'Local editado', 'Meta', 'Resultado da unidade', 'Status unidade']

for coluna in colunas:
    if coluna not in df.columns:
        df[coluna] = None


