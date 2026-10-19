# Reporte de Margen Mensual - Migración a PySpark & Delta Lake

Este proyecto consiste en la migración de un reporte financiero de margen mensual desde SQL Server (T-SQL) hacia una arquitectura moderna basada en **PySpark** y **Delta Lake**, optimizada para su ejecución en **AWS Glue 5.1**.

## Ejecución Local

### Requisitos Previos
- **Python:** 3.11
- **Java:** 17 (con la variable `JAVA_HOME` correctamente configurada).

### Configuración
1. Crear un archivo `.env` en la raíz del proyecto con las siguientes rutas:
   ```env
   INPUT_PATH=datos/lote_1
   DELTA_LAKE_PATH=delta_lake
   OUTPUT_CSV_PATH=output/reporte
   ```
2. Crear y activar un entorno virtual:
   ```bash
   python3.11 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```

### Ejecución
Para procesar la data, ejecutar:
```bash
python run_local.py
```
*   **Lote 1**: Configurar `INPUT_PATH=datos/lote_1` en el `.env`.
*   **Lote 2**: Configurar `INPUT_PATH=datos/lote_2` en el `.env`.

**Resultado:** El reporte se genera en `OUTPUT_CSV_PATH` y se crea una copia simplificada llamada `reporte.csv` en la carpeta padre para facilitar su acceso.

## Arquitectura y Diseño

Se implementó una **Arquitectura Medallion**, dividiendo el pipeline en tres fases claras para mejorar la mantenibilidad y facilitar el debugging:

1.  **Ingesta (Bronze):** Carga de archivos CSV a tablas Delta.
2.  **Transformación (Silver):** Limpieza, estandarización y aplicación de reglas de negocio.
3.  **Reporte (Gold):** Agregaciones finales y generación del CSV de métricas.

### Calidad y Pruebas
Se implementó una estrategia de testing robusta:
- **Unit Tests:** Pruebas independientes por cada capa del pipeline.
- **End-to-End Test:** Validación del flujo completo, verificando la idempotencia y la paridad de resultados frente al sistema legado.

## Estructura del Proyecto

| Carpeta/Archivo | Descripción |
| :--- | :--- |
| `datos/` | Datasets de prueba (Lote 1 y 2). |
| `legado/salida/` | Reportes originales de SQL Server usados para validación. |
| `delta_lake/` | Almacenamiento local de las tablas Delta. |
| `output/` | Directorio de salida de los reportes generados. |
| `src/` | Código fuente modularizado del pipeline. |
| `tests/` | Suite de pruebas unitarias y end-to-end. |
| `run_local.py` | Punto de entrada para ejecución local. |
| `glue_job.py` | Script de orquestación para AWS Glue. |

## Observaciones Técnicas

### Supuestos
- Se detectaron inconsistencias en la data original (espacios en blanco, variaciones de mayúsculas/minúsculas). Se normalizaron los campos afectados bajo el supuesto de que este comportamiento es recurrente.

### Limitaciones y Hallazgos (Fuera de Alcance)
Durante la conciliación con el registro contable, se identificaron causas raíz de diferencias que quedan fuera del alcance de esta migración técnica:
- **Tipo de Cambio:** El sistema actual requiere coincidencia exacta entre `fecha_pedido` y la fecha del tipo de cambio. Ventas realizadas en fechas sin registro de TC resultan en valores `null`, excluyendo la venta del reporte final.
- **Costos Históricos:** El proceso legado utiliza el costo vigente (`vigente_hasta == null`), mientras que lo ideal sería utilizar el costo histórico al momento de la transacción.
- **Despliegue:** La automatización del despliegue (CI/CD) a AWS Glue no fue parte del alcance.
