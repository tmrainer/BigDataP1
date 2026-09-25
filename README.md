# Proyecto Grupal I — Big Data (UTEC)
## Procesamiento de Datos Masivos en la Nube

Análisis distribuido de 500 000 reseñas de Steam sobre **Google Cloud Dataproc**,
comparando cuatro motores de procesamiento (Polars, Dask, Modin y Apache Spark)
y ejecutando tres programas de Hadoop MapReduce sobre HDFS.

**Integrantes:** Castro Gomez, Percy Valentín · Martínez Palomino, Jorge Armando ·
Molina Álvarez, Carla Viviana · Montesinos Damian, Leonardo Alberto ·
Namihas Millan, Yitzhak Abraham

---

## Contenido del repositorio

```
notebooks/     Los 8 notebooks ejecutados, con sus salidas y tiempos
informe/       Código fuente LaTeX del informe técnico
```

Este repositorio contiene **únicamente código fuente**. El dataset (700 MB) no se
versiona: supera el límite de 100 MB por archivo de GitHub y reside en el bucket
del proyecto.

---

## Notebooks ejecutados

Los ocho notebooks están **ejecutados y con sus salidas guardadas**. Cada uno
resuelve las mismas 10 consultas sobre el mismo dataset, cronometrando cada una.

| Notebook | Arquitectura | Motor |
|---|---|---|
| `polars_2_workers.ipynb` / `polars_4_workers.ipynb` | A / B | Polars (Rust, en memoria) |
| `dask_2_workers.ipynb` / `dask_4_workers.ipynb` | A / B | Dask (`LocalCluster`) |
| `modin_2_workers.ipynb` / `modin_4_workers.ipynb` | A / B | Modin sobre Ray |
| `spark_2_workers.ipynb` / `spark_4_workers.ipynb` | A / B | Apache Spark sobre YARN |

- **Arquitectura A:** 1 master `n2-highmem-4` + 2 workers `n2-standard-4`
- **Arquitectura B:** 1 master `n2-highmem-4` + 4 workers `n2-standard-4`
- Imagen de Dataproc `2.3-debian12` · Spark 3.5.3 · Hadoop 3.3.6

### Las 10 consultas

| # | Operación | Tipo |
|---|---|---|
| 1 | Exploración y validación del dataset | Validación |
| 2 | Eliminación de duplicados | Duplicados |
| 3 | Tratamiento de valores nulos | Limpieza |
| 4 | Creación de `review_length` | Transformación de variables |
| 5 | Filtrado de reseñas recomendadas | Filtrado + métrica |
| 6 | Reseñas por videojuego | Agrupación + agregación + orden |
| 7 | Porcentaje de recomendación por juego | Agregación múltiple + métrica |
| 8 | Promedio de horas jugadas por juego | Agregación (media) + orden |
| 9 | Longitud promedio de reseña por juego | Agregación (media) + orden |
| 10 | Ranking final de videojuegos | Agrupación + filtrado + orden múltiple |

### Cómo reproducirlos

Los notebooks se ejecutaron en la terminal de JupyterLab del nodo maestro del
clúster, uno por herramienta y en sesiones independientes, para que cada motor
dispusiera del 100 % de los recursos.

La primera celda de cada notebook descarga el dataset desde el bucket:

```bash
gsutil cp gs://bigdata-2026-02/proyecto01/steam_reviews_500k.csv /tmp/steam_reviews_500k.csv
```

Para ejecutarlos fuera del clúster hay que sustituir esa ruta por un archivo
local. Ten en cuenta que los tiempos no serán comparables, y que el notebook de
Spark pasaría de `spark.master=yarn` a modo local, perdiendo la ejecución
distribuida real que documenta el informe.

---

## Resultados principales

**Tiempo total de las 10 consultas** (Arquitectura A, en segundos):

| Polars | Modin | Dask | Apache Spark |
|---:|---:|---:|---:|
| **0,567** | 21,45 | 203,18 | 376,97 |

Spark resultó 665 veces más lento que Polars ejecutando la misma lógica sobre los
mismos datos. Para 667,7 MiB, el coste de distribuir supera al del cómputo.

**Escalar de 2 a 4 workers no redujo los tiempos** en ninguna de las cuatro
herramientas (variaciones entre −2,16 % y +6,75 %, dentro del ruido de medición).
Las causas se diagnostican con evidencia en la sección 4.4 del informe.

**Indicadores del dataset:**

- Tasa global de recomendación: **84,43 %** (266 640 de 315 809 reseñas limpias)
- 184 191 registros duplicados detectados y eliminados (36,8 % del archivo)
- 22 992 videojuegos distintos; solo 609 alcanzan las 100 reseñas

---

## Dos notas sobre los datos

**1. Muestreo con reposición.** El archivo de trabajo se construyó con
`sample(n=500000, replace=True)` sobre el primer archivo del dataset de Kaggle,
no sobre su totalidad. Eso introdujo 184 191 filas duplicadas que la Consulta 2
detecta y elimina. El análisis completo, con la verificación probabilística,
está en la sección 2.2 del informe.

**2. Conteo de grupos.** En las Consultas 6 a 9, Polars y Spark reportan 22 992
grupos y Dask y Modin reportan 22 991. La causa es que `groupby()` de pandas
descarta las claves nulas por defecto, mientras que `group_by()` de Polars y
`groupBy()` de Spark las conservan como un grupo: son los 22 registros con
`game` nulo. Se analiza en la sección 5.11 del informe.

**3. Tiempos de Spark en la Arquitectura B.** Los CSV exportados a GCS son el
registro de referencia. Las celdas 1 a 4 del notebook `spark_4_workers.ipynb`
muestran una reejecución posterior a esa exportación, con diferencias de pocos
segundos que no alteran ninguna conclusión.

---

## Compilar el informe

```bash
cd informe
pdflatex main.tex && pdflatex main.tex && pdflatex main.tex
```

Tres pasadas para resolver el índice y las referencias cruzadas. También se puede
subir la carpeta `informe/` completa a Overleaf y compilar con **pdfLaTeX**.

---

## Dataset

**100 Million+ Steam Reviews** — <https://www.kaggle.com/datasets/kieranpoc/steam-reviews>

Muestra de trabajo (500 000 registros, 24 columnas, 700 MB):
`gs://bigdata-2026-02/proyecto01/steam_reviews_500k.csv`
