import pandas as pd
import warnings
import json
import numpy as np
import psycopg2
import os

urls = []
datasets= []

print('')
print("Iniciando proceso ETL")
print('')

#Lectura del dataset Terrazas
terrazas_url = f"https://raw.githubusercontent.com/lscampov98/lab_GD/main/Fuentes/Terrazas_202104.csv"
urls.append(terrazas_url)
terrazas_df = pd.read_csv(terrazas_url, sep=';', encoding='latin-1')
datasets.append(terrazas_df)

#Lectura del dataset Locales
locales_url = f"https://raw.githubusercontent.com/lscampov98/lab_GD/main/Fuentes/Locales_202104.csv"
urls.append(locales_url)
locales_df = pd.read_csv(locales_url, sep=';', encoding='latin-1')
datasets.append(locales_df)

#Lectura del dataset Licencias
licencias_url = f"https://raw.githubusercontent.com/lscampov98/lab_GD/main/Fuentes/Licencias_Locales_202104.csv"
urls.append(licencias_url)
licencias_df = pd.read_csv(licencias_url ,sep=";", encoding='utf-8')
datasets.append(licencias_df)

#Lectura del dataset Books
books_url = f"https://raw.githubusercontent.com/lscampov98/lab_GD/main/Fuentes/books.json"
urls.append(books_url)
books_df = pd.read_json(books_url, lines=True)
datasets.append(books_df)

#Lectura del dataset Licencias2
licencias_url_2 = f"https://raw.githubusercontent.com/lscampov98/lab_GD/main/Fuentes/Licencias_Locales_202204.csv"
urls.append(licencias_url_2)
licencias_df_2 = pd.read_csv(licencias_url ,sep=";", encoding='utf-8')
datasets.append(licencias_df_2)

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

#TAREA 1
# 1. Función de Filtrado de Registros
def filtrar_registros(df):
    # Filtrar columnas que tienen más del 50% de valores nulos
    threshold = 0.5 * len(df)
    df_filtrado = df.dropna(thresh=threshold, axis=1)
    columnas_eliminadas = df.shape[1] - df_filtrado.shape[1]
    registros_eliminados = columnas_eliminadas*df.shape[0]
    print(f"Columnas eliminadas: {columnas_eliminadas}")
    print(f"Registros eliminados: {registros_eliminados}")
    return df_filtrado

#Aplicación de función de Filtrado de Registros en los datasets
print("Dataset Terrazas")
terrazas_df = filtrar_registros(terrazas_df)
print("Dataset Locales")
locales_df = filtrar_registros(locales_df)
print("Dataset Licencias")
licencias_df = filtrar_registros(licencias_df)
print("Dataset Books")
books_df = filtrar_registros(books_df)


# 2. Función de conversión de coordenadas en float
def conversion_coordenadas(df):
    df['coordenada_x_local'] = pd.to_numeric(df['coordenada_x_local'].str.replace(',', '.') , errors='coerce')
    df['coordenada_y_local'] = pd.to_numeric(df['coordenada_y_local'].str.replace(',', '.') , errors='coerce')
    return df

#Aplicación de conversión de coordenadas en float en los datasets
terrazas_df = conversion_coordenadas(terrazas_df)
locales_df = conversion_coordenadas(locales_df)
licencias_df = conversion_coordenadas(licencias_df)

# 3. Función de conversión de fechas en date
def fechas_conversion(df, col):
    df[col] = pd.to_datetime(df[col], format="%d/%m/%Y")
    return df

#Aplicación de conversión de fechas en date en el dataset Licencias
licencias_df = fechas_conversion(licencias_df, 'Fecha_Dec_Lic')

# 4. Función de normalización de Datos
def normalizar_datos(df):
    # Aplicar logaritmo a columnas numéricas
    num_cols = df.select_dtypes(include=[np.number]).columns
    df_normalizado = df.copy()
    df_normalizado[num_cols] = df_normalizado[num_cols].replace({0: np.nan, -1: np.nan})
    df_normalizado[num_cols] = np.log1p(df_normalizado[num_cols])  # log1p para evitar log(0)
    return df_normalizado

#Aplicación de función de normalización de datos en los datasets
terrazas_df2 = normalizar_datos(terrazas_df)
locales_df2 = normalizar_datos(locales_df)
licencias_df2 = normalizar_datos(licencias_df)
books_df2 = normalizar_datos(books_df)

# 5. Función de creación de Variables Derivadas en nuevo dataset Terrazas_Normalizadas
Terrazas_Normalizadas= terrazas_df.copy()
Terrazas_Normalizadas['Superficie_ES'] = Terrazas_Normalizadas['Superficie_ES'].str.replace(',', '.').astype(float)
Terrazas_Normalizadas['Superficie_TO'] = Terrazas_Normalizadas['Superficie_ES'] + Terrazas_Normalizadas['Superficie_ES']
Terrazas_Normalizadas['ratio_superficie_id'] = Terrazas_Normalizadas['Superficie_TO'] / Terrazas_Normalizadas['id_terraza']

# Guardar el nuevo dataset Terrazas_Normalizadas
#Terrazas_Normalizadas.to_csv("Terrazas_Normalizadas.csv", index=False)


#TAREA 2
# 1. Función de detección y eliminación de duplicados
def eliminar_duplicados(df):
    df_sin_duplicados = df.drop_duplicates(subset=['id_local', 'ref_licencia', 'rotulo'])
    print(f"Filas duplicadas eliminadas: {df.shape[0] - df_sin_duplicados.shape[0]}")
    return df_sin_duplicados

#Aplicación de función de detección y eliminación de duplicados en dataset Licencias_SinDuplicados
Licencias_SinDuplicados = eliminar_duplicados(licencias_df)

# Guardar dataset limpio
#licencias_df.to_csv("Licencias_SinDuplicados.csv", index=False)

# 2. Función de rellenar data en pageCount
def rellenar_pageCount(df):
    mean_page_count = df['pageCount'].replace(0, pd.NA).mean()
    # Reemplazar los valores de 0 en 'pageCount' por el promedio calculado
    df['pageCount'] = df['pageCount'].replace(0, int(mean_page_count))
    return df

#Aplicación de función de rellenar data en pageCount en el dataset Books
books_df = rellenar_pageCount(books_df)

# 3. Función para corregir _id para que sean números secuenciales
def reemplazar_con_secuenciales(df):
    df['_id'] = range(1, len(df) + 1)
    return df

#Aplicación de función para corregir _id para que sean números secuenciales en el dataset Books
books_df = reemplazar_con_secuenciales(books_df)

# 4. Función de transformación de Cadenas de Texto
def limpiar_texto(df):
    # Seleccionar solo las columnas de tipo 'object' (texto)
    cat_cols = df.select_dtypes(include=[object]).columns
    for col in cat_cols:
        # Convertir los valores a string, incluyendo el manejo de valores nulos
        df[col] = df[col].astype(str).fillna('')
        # Aplicar las transformaciones de texto
        df[col] = df[col].str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)  # Normalización
        df[col] = df[col].astype('category')
    return df

#Aplicación de función de transformación de Cadenas de Texto en el dataset Books
Books_Limpio = limpiar_texto(books_df)

# Guardar dataset limpio
#Books_Limpio.to_csv("Books_Limpio.csv", index=False)


# TAREA 3
# 1. Función de realización de JOIN entre Datasets Terrazas_Normalizadas y Licencias_SinDuplicados
Licencias_Terrazas_Integradas = pd.merge(Terrazas_Normalizadas, Licencias_SinDuplicados, on='id_local', how='inner')

#Aplicación de función de transformación de Cadenas de Texto en el dataset Licencias_Terrazas_Integradas
limpiar_texto(Licencias_Terrazas_Integradas)

# Guardar dataset integrado
#Licencias_Terrazas_Integradas.to_csv("Licencias_Terrazas_Integradas.csv", index=False)

# 2. Función para Combinar y Agregar Datos Geográficos
def agregar_por_barrio(df):
    if 'desc_barrio_local' in df.columns and 'Superficie_TO' in df.columns:
        superficies_agregadas = df.groupby('desc_barrio_local')['Superficie_TO'].sum().reset_index()
        max_area = superficies_agregadas.loc[superficies_agregadas['Superficie_TO'].idxmax()]
        min_area = superficies_agregadas.loc[superficies_agregadas['Superficie_TO'].idxmin()]
        return superficies_agregadas
    
#Aplicación de función para Combinar y Agregar Datos Geográficos en dataset Superficies_Agregadas
Superficies_Agregadas = agregar_por_barrio(Terrazas_Normalizadas)

# Guardar el resultado
#Superficies_Agregadas.to_csv("Superficies_Agregadas.csv", index=False)


# TAREA 4
# 1. Función de concatenación de Datasets Licencias
def concatenar_datasets_licencias(github_urls,datasets):
    dataframes = []
    i=0
    for url in github_urls:
        # Extraer el nombre del archivo de la URL
        filename = url.split("/")[-1]
        # Filtrar los archivos que inician con "Licencias_Locales"
        if filename.startswith("Licencias_Locales"):
            try:
                dataframes.append(datasets[i])
                print(f"Archivo leído: {filename}")  # Mostrar el archivo leído
            except pd.errors.ParserError as e:
                print(f"Error al leer el archivo en {url}: {e}")
            except Exception as e:
                print(f"Ocurrió un error con {url}: {e}")
            i+=1
    if len(dataframes)>1:
        datasets_concatenados = pd.concat(dataframes, ignore_index=True)
        # Paso 4: Eliminar registros duplicados
        datasets_concatenados = datasets_concatenados.drop_duplicates()
        return datasets_concatenados

#Aplicación de función para concatenación de Datasets Licencias
Licencias_Concatenadas = concatenar_datasets_licencias(urls, datasets)

# 2. Función de concatenación de Datasets Terrazas
def concatenar_datasets_terrazas(github_urls,datasets):
    dataframes = []
    i=0
    for url in github_urls:
        # Extraer el nombre del archivo de la URL
        filename = url.split("/")[-1]
        # Filtrar los archivos que inician con "Terrazas"
        if filename.startswith("Terrazas"):
            try:
                dataframes.append(datasets[i])
                print(f"Archivo leído: {filename}")  # Mostrar el archivo leído
            except pd.errors.ParserError as e:
                print(f"Error al leer el archivo en {url}: {e}")
            except Exception as e:
                print(f"Ocurrió un error con {url}: {e}")
            i+=1
    if len(dataframes)>1:
        datasets_concatenados = pd.concat(dataframes, ignore_index=True)
        # Paso 4: Eliminar registros duplicados
        datasets_concatenados = datasets_concatenados.drop_duplicates()
        return datasets_concatenados

#Aplicación de función para concatenación de Datasets Terrazas
Terrazas_Concatenadas = concatenar_datasets_terrazas(urls, datasets)

# 3. Función de concatenación de Datasets Locales
def concatenar_datasets_locales(github_urls,datasets):
    dataframes = []
    i=0
    for url in github_urls:
        # Extraer el nombre del archivo de la URL
        filename = url.split("/")[-1]
        # Filtrar los archivos que inician con "Locales"
        if filename.startswith("Locales"):
            try:
                dataframes.append(datasets[i])
                print(f"Archivo leído: {filename}")  # Mostrar el archivo leído
            except pd.errors.ParserError as e:
                print(f"Error al leer el archivo en {url}: {e}")
            except Exception as e:
                print(f"Ocurrió un error con {url}: {e}")
            i+=1
    if len(dataframes)>1:
        datasets_concatenados = pd.concat(dataframes, ignore_index=True)
        # Paso 4: Eliminar registros duplicados
        datasets_concatenados = datasets_concatenados.drop_duplicates()
        return datasets_concatenados

#Aplicación de función para concatenación de Datasets Locales
Locales_Concatenadas = concatenar_datasets_locales(urls, datasets)

# 4. Función de concatenación de Datasets Books
def concatenar_datasets_books(github_urls,datasets):
    dataframes = []
    i=0
    for url in github_urls:
        # Extraer el nombre del archivo de la URL
        filename = url.split("/")[-1]
        # Filtrar los archivos que inician con "Books"
        if filename.startswith("books"):
            try:
                dataframes.append(datasets[i])
                print(f"Archivo leído: {filename}")  # Mostrar el archivo leído
            except pd.errors.ParserError as e:
                print(f"Error al leer el archivo en {url}: {e}")
            except Exception as e:
                print(f"Ocurrió un error con {url}: {e}")
            i+=1
    if len(dataframes)>1:
        datasets_concatenados = pd.concat(dataframes, ignore_index=True)
        # Paso 4: Eliminar registros duplicados
        datasets_concatenados = datasets_concatenados.drop_duplicates()
        return datasets_concatenados

#Aplicación de función para concatenación de Datasets Books
Books_Concatenadas = concatenar_datasets_books(urls, datasets)


#CONEXIÓN A BASE DE DATOS POSTGRESQL
connection = psycopg2.connect(
        host= os.getenv("DB_HOST"),
        database= os.getenv("DB_NAME"),
        user= os.getenv("DB_USER"),
        password= os.getenv("DB_PASSWORD"),
        port= os.getenv("DB_PORT")
)

cursor = connection.cursor()

#Función de Carga de Datos a la Tabla Dim_Fecha
def load_dim_fecha(df, cursor):
    try:
        # Eliminar duplicados y transformar la columna Fecha_Dec_Lic
        fecha_data = df[['Fecha_Dec_Lic']].drop_duplicates()
        fecha_data['fecha'] = pd.to_datetime(fecha_data['Fecha_Dec_Lic'], format='%Y-%m-%d').dt.date
        
        for index, row in fecha_data.iterrows():
            dia = row['fecha'].day
            mes = row['fecha'].month
            anio = row['fecha'].year
            fecha = row['fecha']
            
            cursor.execute("""
                INSERT INTO "Dim_Fecha" (dia, mes, anio, fecha)
                VALUES (%s, %s, %s, %s)
            """, (dia, mes, anio, fecha))
        
        connection.commit()
        print("Registro insertado correctamente a Dim_Fecha")
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error al insertar datos: {error}")

#Aplicación de función de Carga de Datos a la Tabla Dim_Fecha
load_dim_fecha(Licencias_Terrazas_Integradas,cursor)

#Función de Carga de Datos a la Tabla Dim_Licencias
def load_dim_licencias(df, cursor):
    try:
        # Eliminar duplicados
        licencia_data = df[['ref_licencia', 'desc_tipo_licencia', 'desc_tipo_situacion_licencia']].drop_duplicates()

        for index, row in licencia_data.iterrows():            
            cursor.execute("""
                INSERT INTO "Dim_Licencia" (ref_licencia, desc_tipo_licencia, desc_tipo_situacion_licencia)
                VALUES (%s, %s, %s)
            """, (row['ref_licencia'], row['desc_tipo_licencia'], row['desc_tipo_situacion_licencia']))
        
        connection.commit()
        print("Registro insertado correctamente a Dim_Licencias.")
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error al insertar datos: {error}")

#Aplicación de función de Carga de Datos a la Tabla Dim_Licencias
load_dim_licencias(Licencias_Terrazas_Integradas,cursor)

#Función de Carga de Datos a la Tabla Dim_Ubicacion
def load_dim_ubicacion(df, cursor):
    try:
        # Eliminar duplicados
        ubicacion_data = df[['desc_distrito_local_x', 'desc_barrio_local_x', 'desc_vial_edificio_x', 'num_edificio_x', 'Cod_Postal', 'coordenada_x_local_x', 'coordenada_y_local_x']].drop_duplicates()

        for index, row in ubicacion_data.iterrows():
            
            cursor.execute("""
                INSERT INTO "Dim_Ubicacion" (desc_distrito, desc_barrio, desc_vial_edif, num_edif, cod_postal, coordenadas_x, coordenadas_y)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row['desc_distrito_local_x'], row['desc_barrio_local_x'], row['desc_vial_edificio_x'], row['num_edificio_x'], row['Cod_Postal'], row['coordenada_x_local_x'], row['coordenada_y_local_x'])) 
        connection.commit()
        print("Registro insertado correctamente a Dim_Ubicacion.")
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error al insertar datos: {error}")

#Aplicación de función de Carga de Datos a la Tabla Dim_Ubicacion
load_dim_ubicacion(Licencias_Terrazas_Integradas,cursor)

#Función de Carga de Datos a la Tabla Dim_Terraza
def load_dim_terraza(df, cursor):
    try:
        # Eliminar duplicados
        ubicacion_data = df[['id_terraza', 'DESC_NOMBRE', 'desc_situacion_terraza', 'desc_periodo_terraza', 'desc_ubicacion_terraza']].drop_duplicates()

        for index, row in ubicacion_data.iterrows():
            
            cursor.execute("""
                INSERT INTO "Dim_Terraza" (id_terraza, desc_nombre, desc_situacion_terraza, desc_periodo_terraza, desc_ubicacion_terraza)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['id_terraza'], row['DESC_NOMBRE'], row['desc_situacion_terraza'], row['desc_periodo_terraza'], row['desc_ubicacion_terraza'])) 
        connection.commit()
        print("Registro insertado correctamente a Dim_Terraza.")
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error al insertar datos: {error}")

#Aplicación de función de Carga de Datos a la Tabla Dim_Terraza
load_dim_terraza(Licencias_Terrazas_Integradas,cursor)

#Función de Carga de Datos a la Tabla Dim_Local
def load_dim_local(df, cursor):
    try:
        # Eliminar duplicados
        ubicacion_data = df[['id_local', 'rotulo_y', 'desc_situacion_local_x']].drop_duplicates()

        for index, row in ubicacion_data.iterrows():
            
            cursor.execute("""
                INSERT INTO "Dim_Local" (id_local, rotulo, desc_situacion_local)
                VALUES (%s, %s, %s)
            """, (row['id_local'], row['rotulo_y'], row['desc_situacion_local_x'])) 
        connection.commit()
        print("Registro insertado correctamente a Dim_Local.")
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error al insertar datos: {error}")

#Aplicación de función de Carga de Datos a la Tabla Dim_Local
load_dim_local(Licencias_Terrazas_Integradas,cursor)

#Función de Carga de Datos a la Tabla Fact_licencias_terrazas
def load_fact_licencias_terrazas(df, cursor):
    try:
        for index, row in df.iterrows():
            
            cursor.execute('SELECT id_fecha FROM "Dim_Fecha" WHERE fecha = %s', (row['Fecha_Dec_Lic'].date(),))
            id_fecha = cursor.fetchone()

            cursor.execute('SELECT id_ubicacion FROM "Dim_Ubicacion" WHERE desc_distrito = %s AND desc_barrio = %s AND desc_vial_edif = %s AND num_edif = %s AND cod_postal = %s AND coordenadas_x = %s AND coordenadas_y = %s', (row['desc_distrito_local_x'], row['desc_barrio_local_x'], row['desc_vial_edificio_x'], row['num_edificio_x'], row['Cod_Postal'], str(row['coordenada_x_local_x']),str(row['coordenada_y_local_x'])))
            id_ubicacion = cursor.fetchone()

            cursor.execute('SELECT id_licencia FROM "Dim_Licencia" WHERE ref_licencia = %s AND desc_tipo_licencia = %s AND desc_tipo_situacion_licencia = %s', (row['ref_licencia'], row['desc_tipo_licencia'], row['desc_tipo_situacion_licencia']))
            id_licencia = cursor.fetchone()
            
            cursor.execute("""
                INSERT INTO "Fact_Licencias_Terrazas" (id_fecha, id_ubicacion, id_licencia, id_local, id_terraza, superficie_es, superficie_to, mesas, sillas)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_fecha[0],  # id_fecha
                id_ubicacion[0],  # id_ubicacion
                id_licencia[0],
                row['id_local'],
                row['id_terraza'],
                row['Superficie_ES'],
                row['Superficie_TO'],
                row['mesas_es'], 
                row['sillas_es'] 
            ))

        connection.commit()
        print("Registro insertado correctamente a Fact_licencias_terrazas.")
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error al insertar datos: {error}")
        connection.rollback()

#Aplicación de función de Carga de Datos a la Tabla Fact_licencias_terrazas
load_fact_licencias_terrazas(Licencias_Terrazas_Integradas,cursor)
print("")

# Cerrar la conexión a la base de datos
cursor.close()
connection.close()