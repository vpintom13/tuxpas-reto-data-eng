from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType

SCHEMA_STG_TIENDAS = StructType([
    StructField("id_tienda", StringType(), False),
    StructField("nombre", StringType(), False),
    StructField("ciudad", StringType(), False),
    StructField("fecha_apertura", DateType(), False),
])

SCHEMA_STG_MAPEO_SKU = StructType([
    StructField("sku_pos", StringType(), False),
    StructField("sku_erp", StringType(), True),
    StructField("sku_web", StringType(), True),
])

SCHEMA_STG_PRODUCTOS = StructType([
    StructField("sku_erp", StringType(), False),
    StructField("nombre", StringType(), False),
    StructField("categoria", StringType(), False),
])

SCHEMA_STG_COSTOS_HISTORICO = StructType([
    StructField("sku_erp", StringType(), False),
    StructField("vigente_desde", DateType(), False),
    StructField("vigente_hasta", DateType(), True),
    StructField("costo_unitario", DecimalType(12, 4), False),
])

SCHEMA_STG_TIPO_CAMBIO = StructType([
    StructField("fecha", DateType(), False),
    StructField("tc", DecimalType(6, 3), False),
])

SCHEMA_STG_VENTAS_POS = StructType([
    StructField("tipo_comprobante", StringType(), False),
    StructField("serie", StringType(), False),
    StructField("numero", IntegerType(), False),
    StructField("item", IntegerType(), False),
    StructField("fecha_emision", TimestampType(), False),
    StructField("id_tienda", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("descripcion", StringType(), False),
    StructField("cantidad", IntegerType(), False),
    StructField("importe_total", DecimalType(12, 2), False),
    StructField("estado", StringType(), True),
    StructField("comprobante_ref", StringType(), True),
    StructField("fecha_actualizacion", TimestampType(), False),
])

SCHEMA_STG_PEDIDOS_WEB = StructType([
    StructField("id_pedido", StringType(), False),
    StructField("linea", IntegerType(), False),
    StructField("fecha_pedido", TimestampType(), False),
    StructField("sku_web", StringType(), False),
    StructField("cantidad", IntegerType(), False),
    StructField("importe", DecimalType(12, 2), False),
    StructField("moneda", StringType(), False),
    StructField("estado", StringType(), False),
    StructField("cliente_email", StringType(), False),
    StructField("cliente_doc", StringType(), False),
    StructField("fecha_actualizacion", TimestampType(), False),
])
