import pandas as pd
import numpy as np

# 1. Cargar el archivo (ajusta el nombre del archivo si es .tsv o .csv)
# Eurostat suele usar tabuladores (\t) y a veces comas en la primera columna
file_path = 'estat_tour_occ_nim.tsv' 
df = pd.read_csv(file_path, sep='\t')

# 2. Limpiar los nombres de las columnas
# Eurostat pone mucha info en la primera columna separada por comas
# Ejemplo: "freq,c_resid,unit,nace_r2,geo\TIME_PERIOD"
first_col = df.columns[0]
new_headers = first_col.split(',')
# Limpiamos el último encabezado que suele traer basura como "\TIME_PERIOD"
new_headers[-1] = 'geo'

# Separamos esa primera columna en varias columnas reales
df[new_headers] = df[first_col].str.split(',', expand=True)
df = df.drop(columns=[first_col])

# 3. Transformar de formato ancho a formato largo (Melt)
# Esto pasa las fechas de las columnas a una sola fila por mes
id_vars = new_headers
df_long = df.melt(id_vars=id_vars, var_name='date', value_name='value')

# 4. Limpiar los valores numéricos
# Eliminamos las letras 'e', 'b', 'u' que pone Eurostat y los ':' (nulos)
df_long['value'] = df_long['value'].str.replace(r'[a-zA-Z]', '', regex=True)
df_long['value'] = pd.to_numeric(df_long['value'].replace(':', np.nan), errors='coerce')

# 5. Filtrar por tus destinos del One-Pager (ISO Codes)
# Albania: AL, Japón: JP, Austria: AT (ejemplo), Portugal (Madeira): PT
destinos = ['AL', 'JP', 'KZ', 'PT', 'DK', 'IT'] # Ajusta según necesites
df_filtered = df_long[df_long['geo'].isin(destinos)].copy()

# 6. Filtrar por fechas recientes (Recomendado: 2015 en adelante)
df_filtered = df_filtered[df_filtered['date'] >= '2015-01']

# 7. Ordenar cronológicamente
df_filtered = df_filtered.sort_values(['geo', 'date'])

print(df_filtered.head())

# Guardar el dataset limpio para el siguiente paso del pipeline
df_filtered.to_csv('cleaned_tourism_data.csv', index=False)