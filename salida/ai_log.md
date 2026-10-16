```
### 4.6 `AI_LOG.md`

El uso de asistentes de IA está permitido; lo que interesa es ver con qué criterio se usan. Esta bitácora es obligatoria y debe incluir:

1. Las herramientas y los modelos utilizados (Claude, ChatGPT, Copilot, Cursor, agentes, MCP u otros), con su versión cuando se conozca.
2. El flujo de trabajo: si se usó un solo agente o varios, plan mode o MCP, y cómo se coordinaron.
3. Entre tres y cinco prompts clave, con lo que se pidió, un resumen de la respuesta y qué se hizo con ella (aceptarla, modificarla o descartarla), y por qué.
4. Al menos un caso en que la IA se equivocó o propuso algo mejorable, y cómo se detectó y se corrigió.
5. Una autocrítica: qué parte del trabajo es propia, qué parte se debe a la IA y cómo se validó que el resultado es correcto, más allá de que se ejecute sin errores.

No hay una cantidad de IA correcta: se valora el criterio con que se usa, no cuánto se usa.
```

----------

- Claude Code (agent) + Gemma 4 (modelo).
- Solo se uno 1 agente en plan mode. MCP no fueron necesarios
- Promts usados:
    1. Migrar el script SQL a pyspark sin realizar alteraciones en la logica, bajo las versiones de pyspark, python y otros paquetes y siguiendo la definicion de las tablas en el archivo ddl.sql, que corresponden a los csv en la carpeta datos/. Input y output path deben ser parametrizables
    
        Respuesta: Plan de los cambios a realizar. Migracion de reporte desde procedimiento almacenado SQL a script PySpark. Incluye ingesta de 6 archivos csv, pipeline de transformaaciones (filtrado, joins, calculos) y agregacion al final para generar un csv con las metricas de ventas por tienda.

    2. Escribir test para comparar el resultado de la migracion a pyspark vs el esperado usando pytest y pyspark.testing
    
        Respuesta: Plan de los cambios. Ejecucion de script de migracion de PySpark por subproceso con output a carpeta temporal. Comparacion del csv resultando con el archivo de referefencia

