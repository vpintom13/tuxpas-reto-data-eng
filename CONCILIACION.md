# Conciliación de Reporte de Margen Mensual

## Resumen de Ejecución
- **Lote procesado:** Lote 1 y Lote 2
- **Fecha de validación:** 2026-10-19
- **Estado:** Paridad Total

## Comparativa de Totales
| Métrica | SQL Server (Legado) | PySpark (Nuevo) | Diferencia (S/) |
| :--- | :--- | :--- | :--- |
| Conteo | 5013 | 5013 | 0.00 |
| Venta Neta | 4611990.92 | 4611990.92 | 0.00 |
| Costo | 2336206.73 | 2336206.73 | 0.00 |
| Margen | 2275784.19 | 2275784.19 | 0.00 |

## Explicación de Diferencias
No se encontraron diferencias entre el sistema legado (SQL Server) y el nuevo sistema (PySpark).

### Hallazgos en la Conciliación con el Registro Contable
Aunque existe paridad técnica con el sistema legado, durante la validación contra el registro contable se identificaron las siguientes causas raíz de discrepancias ( inherentes a la lógica de negocio preexistente):

1. **Tipo de Cambio:** El sistema requiere una coincidencia exacta entre la `fecha_pedido` y la fecha registrada en la tabla de tipos de cambio. Si una venta ocurre en una fecha sin registro de TC, el valor retorna como `null` y la venta es excluida del reporte final.
2. **Costos Históricos:** Actualmente, el proceso utiliza el costo vigente (`vigente_hasta == null`). Para una conciliación contable exacta, debería utilizarse el costo histórico registrado al momento exacto de la transacción.


## Conclusión
El reporte es apto para producción
