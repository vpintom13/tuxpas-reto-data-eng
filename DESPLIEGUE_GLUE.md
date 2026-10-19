# Guía de Despliegue en AWS Glue

Este documento detalla los pasos necesarios para desplegar el pipeline de reporte de margen mensual en AWS Glue utilizando la consola de AWS.

## 1. Preparación de Dependencias
Para que Glue pueda ejecutar la lógica de negocio contenida en el proyecto, es necesario subir el código fuente como una librería.

1. Comprimir la carpeta `src/` en un archivo `.zip` (asegurándose de que la estructura interna comience con la carpeta `src`).
2. Subir el archivo `src.zip` a un bucket de **Amazon S3**.
3. Copiar la ruta de S3 del archivo subido (ej. `s3://mi-bucket/codigo/src.zip`).

## 2. Creación del Job en AWS Glue
Siga estos pasos en la Consola de AWS:

1. Acceder al servicio **AWS Glue**.
2. En el menú lateral, ir a **ETL jobs** $\rightarrow$ **Visual ETL**.
3. Seleccionar la opción **Script Editor** y elegir el motor **Spark**.
4. Subir o pegar el contenido del archivo [glue_job.py](glue_job.py) en el editor.

## 3. Configuración del Job
Acceda a la pestaña de **Job details** y configure los siguientes parámetros:

### Configuración General
- **Name:** Asignar un nombre descriptivo al Job (ej. `reporte_margen_mensual`).
- **IAM Role:** Seleccionar un rol de IAM que tenga permisos de lectura y escritura sobre los buckets de S3 utilizados.
- **Type:** `Spark`.
- **Glue version:** `Glue 5.1` (incluye Spark 3.5 y Python 3).

### Librerías y Dependencias
En la sección **Libraries**:
- **Python library path:** Pegar la ruta de S3 del archivo `src.zip` subido en el paso 1.

### Parámetros del Job (Job Parameters)
En la sección de parámetros, agregar las siguientes claves y valores:

| Key | Value (Ejemplo) | Descripción |
| :--- | :--- | :--- |
| `--DELTA_LAKE_PATH` | `s3://mi-bucket/delta-lake/` | Ruta base del Delta Lake |
| `--INPUT_PATH` | `s3://mi-bucket/datos/lote_1/` | Ruta de los archivos CSV de entrada |
| `--OUTPUT_CSV_PATH` | `s3://mi-bucket/salida/` | Ruta donde se guardará el reporte final |
| `--datalake-formats` | `delta` | Habilita el soporte nativo para Delta Lake en Glue |

## 4. Ejecución
1. Guardar los cambios del Job.
2. Hacer clic en **Run** para iniciar la ejecución.
3. Monitorear el progreso en la pestaña **Runs**.
