from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round
import pandas as pd

# ---------------------
# 1. Crear sesión de Spark
# ---------------------
spark = SparkSession.builder \
    .appName("Leer y tratar archivos") \
    .getOrCreate()

# ---------------------
# 2. Leer archivo1.xlsx con pandas -> pasar a Spark -> tratar fecha
# ---------------------
df_excel_pd = pd.read_excel("archivo1.xlsx", engine='openpyxl')

df_excel = spark.createDataFrame(df_excel_pd)

# Convertir columna 'fecha' al formato dd/MM/yyyy
df_excel = df_excel.withColumn("fecha_formateada", to_date(col("fecha"), "dd/MM/yyyy"))

print("📘 archivo1.xlsx:")
df_excel.show()
df_excel.printSchema()

# ---------------------
# 3. Leer archivo2.csv con '|' -> convertir edad a entero
# ---------------------
df_csv_pipe = spark.read.option("header", True) \
    .option("delimiter", "|") \
    .csv("archivo2.csv")

df_csv_pipe = df_csv_pipe.withColumn("edad", col("edad").cast("int"))

print("📗 archivo2.csv:")
df_csv_pipe.show()
df_csv_pipe.printSchema()

# ---------------------
# 4. Leer archivo3.csv con ';' -> convertir columnas a entero y calcular porcentaje
# ---------------------
df_csv_semicolon = spark.read.option("header", True) \
    .option("delimiter", ";") \
    .csv("archivo3.csv")

df_csv_semicolon = df_csv_semicolon \
    .withColumn("valor1", col("valor1").cast("int")) \
    .withColumn("valor2", col("valor2").cast("int")) \
    .withColumn("porcentaje", round((col("valor1") / col("valor2")) * 100, 2))

print("📙 archivo3.csv:")
df_csv_semicolon.show()
df_csv_semicolon.printSchema()
