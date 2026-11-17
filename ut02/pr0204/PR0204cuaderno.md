## 1.- Establece la conexión con el clúster HDFS.


```python
from hdfs import InsecureClient
hdfs_url = 'http://namenode:9870'
client = InsecureClient(hdfs_url, user='root')

print("Conexion HDFS establecida. Estado de la raiz:")
print(client.status('/'))
```

    Conexion HDFS establecida. Estado de la raiz:
    {'accessTime': 0, 'blockSize': 0, 'childrenNum': 4, 'fileId': 16385, 'group': 'supergroup', 'length': 0, 'modificationTime': 1793697302663, 'owner': 'root', 'pathSuffix': '', 'permission': '755', 'replication': 0, 'snapshotEnabled': True, 'storagePolicy': 0, 'type': 'DIRECTORY'}


## 2.- Estructura de directorios


```python
directorios = [
    '/datalake/renfe/raw/estaciones',
    '/datalake/renfe/raw/viajeros',
    '/datalake/renfe/processed',
    '/datalake/renfe/logs'
]

for directorio in directorios:
    client.makedirs(directorio, permission='755')

print("Estructura de carpetas creada. Contenido de /datalake/renfe:")
print(client.list('/datalake/renfe'))
```

    Estructura de carpetas creada. Contenido de /datalake/renfe:
    ['logs', 'processed', 'raw']


## 3.- Ingesta concurrente de datos crudos


```python
client.upload('/datalake/renfe/raw/estaciones', 'data/estaciones_completo.csv', overwrite=True, n_threads=4)
client.upload('/datalake/renfe/raw/estaciones', 'data/estaciones_feve.csv', overwrite=True, n_threads=4)
client.upload('/datalake/renfe/raw/viajeros', 'data/viajeros_tramo.csv', overwrite=True, n_threads=4)

print("Carga de ficheros Raw finalizada con éxito.\n")
print("--- AUDITORÍA DE METADATOS (/raw/estaciones) ---")

archivos_estado = client.list('/datalake/renfe/raw/estaciones', status=True)

for nombre, estado in archivos_estado:
    tamano = estado['length']
    permisos = estado['permission']
    replicacion = estado['replication']
    print(f"Archivo: {nombre} | Tamaño: {tamano} bytes | Permisos: {permisos} | Replicación: {replicacion}")
```

    Carga de ficheros Raw finalizada con éxito.
    
    --- AUDITORÍA DE METADATOS (/raw/estaciones) ---
    Archivo: estaciones_completo.csv | Tamaño: 142155 bytes | Permisos: 644 | Replicación: 3
    Archivo: estaciones_feve.csv | Tamaño: 61795 bytes | Permisos: 644 | Replicación: 3


## 4.- Procesamiento en Streaming y filtrado


```python
provincia_objetivo = 'LEÓN'
contador_estaciones = 0

def procesar_csv_streaming(ruta_origen, provincia):
    global contador_estaciones
    with client.read(ruta_origen, encoding='latin-1', delimiter='\n') as reader:
        for idx, linea in enumerate(reader):
            if idx == 0:
                yield linea + '\n' 
            elif provincia in linea:
                contador_estaciones += 1
                yield linea + '\n' 

ruta_origen = '/datalake/renfe/raw/estaciones/estaciones_completo.csv'
ruta_destino = '/datalake/renfe/processed/estaciones_filtradas.csv'

print(f"Iniciando el filtrado en streaming para la provincia: {provincia_objetivo}...")

client.write(ruta_destino, data=procesar_csv_streaming(ruta_origen, provincia_objetivo), encoding='latin-1', overwrite=True)

print(f"Proceso finalizado. Se han filtrado {contador_estaciones} estaciones.")

print("\n--- TEST: PRIMERAS 5 LÍNEAS DEL RESULTADO EN HDFS ---")
with client.read(ruta_destino, encoding='latin-1', delimiter='\n') as reader:
    for idx, linea in enumerate(reader):
        if idx < 5:
            print(linea.strip())
        else:
            break
```

    Iniciando el filtrado en streaming para la provincia: LEÓN...
    Proceso finalizado. Se han filtrado 45 estaciones.
    
    --- TEST: PRIMERAS 5 LÍNEAS DEL RESULTADO EN HDFS ---
    "CODIGO";"DESCRIPCION";"LATITUD";"LONGITUD";"DIRECION";"CP";"POBLACION";"PROVINCIA";"PAIS";"CERCANIAS";"ANCHO MÉTRICO";"COMUN"
    "05755";"LA LLAMA DE LA GUZPEÑA";"42.7702481";"-5.0412478";"Ctra. Cl - 626 --- Estación Ferrocarril, S/N";"24893";"PRADO DE LA GUZPEÑA";"LEÓN";"ESPAÑA";"SI";"SI";""
    "05756";"LA ESPINA";"42.7868560";"-4.9052025";"Ctra. Cl - 626 --- Estación Ferrocarril, S/N";"24889";"VALDERRUEDA";"LEÓN";"ESPAÑA";"SI";"SI";""
    "05757";"VALCUENDE";"42.7655780";"-4.9292558";"C/ Arriba, S/N";"24889";"VALDERRUEDA";"LEÓN";"ESPAÑA";"SI";"SI";""
    "05758";"PUENTE ALMUHEY";"42.7876320";"-4.9718992";"Ctra. General  S/N- Puente Almuhey";"24880";"VALDERRUEDA";"LEÓN";"ESPAÑA";"SI";"SI";""


## 5.- Registro de auditoría


```python
ruta_log = '/datalake/renfe/logs/ingesta.log'

mensaje_inicio = "[INICIO] Pipeline ejecutado. Archivos crudos cargados en /raw.\n"
client.write(ruta_log, data=mensaje_inicio, encoding='utf-8', overwrite=True)

mensaje_procesado = f"[PROCESADO] Se han filtrado un total de {contador_estaciones} estaciones para la provincia seleccionada ({provincia_objetivo}).\n"

with client.read(ruta_log, encoding='utf-8') as reader:
    contenido_actual = reader.read()

client.write(ruta_log, data=contenido_actual + mensaje_procesado, encoding='utf-8', overwrite=True)

print("--- CONTENIDO COMPLETO DE INGESTA.LOG ---")
with client.read(ruta_log, encoding='utf-8') as reader:
    print(reader.read())
```

    --- CONTENIDO COMPLETO DE INGESTA.LOG ---
    [INICIO] Pipeline ejecutado. Archivos crudos cargados en /raw.
    [PROCESADO] Se han filtrado un total de 45 estaciones para la provincia seleccionada (LEÓN).
    


## 6.- Exportación para analítica y limpieza


```python
client.download('/datalake/renfe/processed/estaciones_filtradas.csv', 'entrega_estaciones.csv', overwrite=True)
print("Archivo 'entrega_estaciones.csv' descargado localmente con éxito.")

client.delete('/datalake/renfe/raw/estaciones/estaciones_feve.csv')
print("\nListado de la carpeta /raw/estaciones tras aplicar limpieza:")
print(client.list('/datalake/renfe/raw/estaciones'))

def reset_datalake():
    """Purga completamente la estructura del proyecto en el Data Lake."""
    if client.status('/datalake/renfe', strict=False):
        client.delete('/datalake/renfe', recursive=True)
        print("\n[RESET] Estructura /datalake/renfe y todo su contenido eliminados.")
```

    Archivo 'entrega_estaciones.csv' descargado localmente con éxito.
    
    Listado de la carpeta /raw/estaciones tras aplicar limpieza:
    ['estaciones_completo.csv']



```python
print(client.list('/datalake/renfe'))
```

    ['logs', 'processed', 'raw']

