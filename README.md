# Autor
Miguel Ángel González-Albo Campillo
70578744R


# Pipeline End-to-End Azure Synapse TFM

Este repositorio contiene el código y los archivos relacionados con el proyecto de fin de máster en Big Data & Data Engineering, cuyo objetivo es construir un pipeline de datos end-to-end utilizando Azure Synapse, Machine Learning con MLFlow, y visualización con Power BI.

## Estructura del repositorio

El documento principal del TFM se encuentra en la carpeta report

- `report/`: Documento PDF con el informe final del TFM.
- `notebooks/`: Contiene los Jupyter Notebooks utilizados para la transformación de datos y la implementación del modelo de Machine Learning.
- `scripts/`: Contiene el script PySpark utilizado para las transformaciones de datos.
- `powerbi/`: Archivo `.pbix` con el dashboard interactivo de Power BI.
- `video/`: Video explicativo del proyecto.

## Notas
- He comentado que uso la curva ROC, pero finalmente no la he incluido.
- En el notebook subido, el primero, he tenido que eliminar completamente la línea donde está la conexión a Azure, ya que GitHub no me permitía subirla
- En el esquema aparece Azure ML, pero al final tuve que descartarlo debido a la cuenta de estudiante y la limitación de vCores
