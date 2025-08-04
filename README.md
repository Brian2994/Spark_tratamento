# 📊 Tratamiento de Archivos con PySpark

Este proyecto utiliza **Apache Spark** (PySpark) para leer, tratar y visualizar datos provenientes de tres archivos con diferentes formatos y delimitadores.

---

## 📁 Archivos de entrada

1. `archivo1.xlsx`  
   - Formato: Excel  
   - Columnas: `nombre`, `fecha`  
   - Tratamiento: convertir la columna `fecha` al formato `día/mes/año`

2. `archivo2.csv`  
   - Formato: CSV delimitado por `|`  
   - Columnas: `nombre`, `edad`  
   - Tratamiento: convertir `edad` de decimal (ej. 25.0) a entero

3. `archivo3.csv`  
   - Formato: CSV delimitado por `;`  
   - Columnas: `producto`, `valor1`, `valor2`  
   - Tratamiento: convertir `valor1` y `valor2` a entero, y calcular una nueva columna llamada `porcentaje = (valor1 / valor2) * 100`

---

## ⚙️ Requisitos

- Python 3.8+
- Apache Spark (vía PySpark)
- Pandas
- OpenPyXL (para leer `.xlsx`)

### 🧪 Instalación

```bash
pip install pyspark pandas openpyxl
