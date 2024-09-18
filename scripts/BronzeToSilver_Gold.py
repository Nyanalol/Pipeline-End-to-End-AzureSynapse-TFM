from pyspark.sql.functions import col, sum, avg, countDistinct

# Definir variables
storage_account_name = 'dlsmde01magac'
data_lake_container = f'abfss://datalake@{storage_account_name}.dfs.core.windows.net'
bronze_folder = 'bronze' 
silver_folder = 'silver'  
gold_folder = 'gold'  

# Función para procesar tablas desde la capa Bronze a Silver
def process_table(table_name):
    source_wildcard = f"{table_name}*.csv"
    source_path = f"{data_lake_container}/{bronze_folder}/{source_wildcard}"
    delta_table_path = f"{data_lake_container}/{silver_folder}/{table_name}"

    # Leer archivo en DataFrame de Spark
    sdf = spark.read.format('csv') \
        .option("recursiveFileLookup", "true") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("quote", '"') \
        .load(source_path)

    # Eliminar duplicados
    if "ID_Producto" in sdf.columns:
        sdf = sdf.dropDuplicates(["ID_Producto"])

    # Guardar la tabla en formato Delta
    sdf.write.format('delta').mode("overwrite").save(delta_table_path)

# Procesar las tablas de la capa Bronze a Silver
process_table("InventarioProductos")
process_table("SoporteTecnico")
process_table("Ventas")



# Definir paths para cada tabla en la capa Silver
ventas_silver_path = f"{data_lake_container}/{silver_folder}/Ventas"
soporte_silver_path = f"{data_lake_container}/{silver_folder}/SoporteTecnico"
inventario_silver_path = f"{data_lake_container}/{silver_folder}/InventarioProductos"



# Leer las tablas en formato Delta desde la capa Silver
ventas_df = spark.read.format("delta").load(ventas_silver_path)
soporte_df = spark.read.format("delta").load(soporte_silver_path)
inventario_df = spark.read.format("delta").load(inventario_silver_path)

# Realizar join entre Ventas y InventarioProductos usando ID_Producto
ventas_inventario_df = ventas_df.join(inventario_df, "ID_Producto", "inner")

# Realizar join entre el resultado anterior y SoporteTecnico usando ID_Cliente
ventas_soporte_df = ventas_inventario_df.join(soporte_df, "ID_Cliente", "left")

# Realizar las agregaciones necesarias
resultado_df = ventas_soporte_df.groupBy(
    "ID_Producto", "Nombre_Producto", "Stock", "Precio_Costo", "Precio_Venta", 
    "Categoria_Producto", "ID_Cliente", "Nombre_Cliente", "Categoria_Problema"
).agg(
    sum("Cantidad").alias("Total_Ventas"),
    countDistinct("ID_Ticket").alias("Total_Problemas_Reportados"),
    avg("Tiempo_Resolucion").alias("Tiempo_Promedio_Resolucion"),
    sum("Total").alias("Ingresos_Totales")
).orderBy(col("Ingresos_Totales").desc())

# Definir el path para guardar el resultado en la capa Gold
gold_output_path = f"{data_lake_container}/{gold_folder}/Analisis.csv"

# Guardar el resultado en formato CSV en la capa Gold
resultado_df.write.format("csv").mode("overwrite").option("header", "true").save(gold_output_path)
