# AI Log - Bitácora de Desarrollo Asistido

Este documento registra la interacción con la IA durante el proceso de migración del reporte de margen mensual, detallando la metodología, los prompts clave y las lecciones aprendidas.

## Stack Tecnológico de Asistencia
- **Agente:** Claude Code
- **Modelo:** Gemma 4
- **Metodología:** Uso de `Plan Mode` para diseño arquitectónico y validación de pasos antes de la implementación.
- **Herramientas:** MCP (no requeridos para este flujo).

## Historial de Prompts y Evolución

| Fase | Prompt Principal | Resultado / Impacto |
| :--- | :--- | :--- |
| **1. Migración Inicial** | Migrar script SQL a PySpark (v3.5, Python 3) basándose en `ddl.sql` y archivos CSV, manteniendo la lógica original y parametrizando rutas de entrada/salida. | **Plan de Migración:** Ingesta de 6 CSVs, pipeline de transformaciones (filtros, joins, cálculos) y agregación final para métricas por tienda. |
| **2. Validación** | Crear tests para comparar el resultado de PySpark vs el esperado usando `pytest` y `pyspark.testing`. | **Framework de Pruebas:** Ejecución del script por subproceso con outputs temporales y comparación automatizada contra archivos de referencia. |
| **3. Refactorización** | Dividir el script y sus tests en tres capas: Ingesta $\rightarrow$ Transformación $\rightarrow$ Reporte. | **Modularización:** Separación de responsabilidades en 3 scripts independientes, mejorando la mantenibilidad. |
| **4. Orquestación** | Crear script de pipeline y test de flujo completo que valide resultados vs `/legado` e idempotencia. | **Pipeline End-to-End:** Automatización del flujo completo con validación de integridad de datos. |

## Análisis de Errores y Mejoras (Human-in-the-loop)

**Incidencia:** 
La IA propuso generar la `SparkSession` al inicio de cada script individual, lo que provocaba redundancia de código y problemas de eficiencia.

**Solución aplicada:**
Se refactorizó la arquitectura para centralizar la creación de la `SparkSession` en un único entrypoint. Además, para asegurar la compatibilidad con **AWS Glue**, se modificó el diseño para que el pipeline reciba la sesión de Spark como una dependencia externa.

## Conclusión de Colaboración
La IA fue fundamental en la traducción técnica de T-SQL a PySpark, acelerando la escritura del código base. El valor humano se centró en:
- Definición de la arquitectura del proyecto.
- Diseño de la estrategia de despliegue.
- Revisión crítica de código y definición de casos de prueba complejos.
- Aseguramiento de la paridad de datos.
