# Portafolio de Data Engineering --- 20 proyectos

![Data
Engineering](https://img.shields.io/badge/Área-Data%20Engineering-1565C0)
![Proyectos](https://img.shields.io/badge/Proyectos-20-2E7D32)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Avanzado-4479A1?logo=postgresql&logoColor=white)
![Cloud](https://img.shields.io/badge/Cloud-AWS%20%7C%20Azure%20%7C%20GCP-orange)

Colección de **20 proyectos prácticos de Data Engineering**, desde nivel
Junior hasta Senior. El objetivo es construir soluciones reproducibles y
documentadas que cubran ingestión, procesamiento batch y streaming,
almacenamiento, modelado, orquestación, calidad, gobierno, seguridad y
despliegue en entornos cloud y multi-cloud.

> **Enfoque:** cada proyecto debe evolucionar desde un ejercicio
> funcional hacia una solución mantenible, observable, segura y
> desplegable. Los servicios cloud pueden generar costos; utilizar
> cuotas gratuitas, presupuestos y recursos temporales cuando
> corresponda.

------------------------------------------------------------------------

## Tabla de contenidos

-   [Objetivos](#objetivos)
-   [Tecnologías](#tecnologías)
-   [Estructura sugerida del
    repositorio](#estructura-sugerida-del-repositorio)
-   [Nivel Junior](#nivel-junior)
    -   [01. Pipeline ETL de ventas
        minoristas](#01-pipeline-etl-de-ventas-minoristas)
    -   [02. Data Lake básico en GCP](#02-data-lake-básico-en-gcp)
    -   [03. Integración con Azure Data
        Factory](#03-integración-con-azure-data-factory)
    -   [04. Data Warehouse minorista](#04-data-warehouse-minorista)
    -   [05. Ingesta de múltiples APIs](#05-ingesta-de-múltiples-apis)
-   [Nivel Semi-Senior](#nivel-semi-senior)
    -   [06. Lakehouse con Databricks y Delta
        Lake](#06-lakehouse-con-databricks-y-delta-lake)
    -   [07. Data Warehouse avanzado en
        BigQuery](#07-data-warehouse-avanzado-en-bigquery)
    -   [08. Procesamiento distribuido con
        Spark](#08-procesamiento-distribuido-con-spark)
    -   [09. Streaming con Kafka y GCP](#09-streaming-con-kafka-y-gcp)
    -   [10. DataOps y orquestación](#10-dataops-y-orquestación)
-   [Nivel Senior](#nivel-senior)
    -   [11. Lakehouse empresarial
        multi-cloud](#11-lakehouse-empresarial-multi-cloud)
    -   [12. Plataforma fintech en tiempo
        real](#12-plataforma-fintech-en-tiempo-real)
    -   [13. Gobierno y calidad de datos
        empresarial](#13-gobierno-y-calidad-de-datos-empresarial)
    -   [14. DevSecOps e infraestructura como
        código](#14-devsecops-e-infraestructura-como-código)
    -   [15. Plataforma de datos empresarial
        end-to-end](#15-plataforma-de-datos-empresarial-end-to-end)
-   [Cinco proyectos avanzados
    adicionales](#cinco-proyectos-avanzados-adicionales)
    -   [16. Data Lake de e-commerce](#16-data-lake-de-e-commerce)
    -   [17. Pipeline IoT en streaming](#17-pipeline-iot-en-streaming)
    -   [18. Data Warehouse financiero](#18-data-warehouse-financiero)
    -   [19. Feature Engineering para Machine
        Learning](#19-feature-engineering-para-machine-learning)
    -   [20. Capstone: plataforma de datos
        empresarial](#20-capstone-plataforma-de-datos-empresarial)
-   [Estándares de arquitectura](#estándares-de-arquitectura)
-   [Calidad, seguridad y
    observabilidad](#calidad-seguridad-y-observabilidad)
-   [Criterios de finalización](#criterios-de-finalización)
-   [Plan de implementación](#plan-de-implementación)
-   [Recursos oficiales](#recursos-oficiales)
-   [Licencia](#licencia)

------------------------------------------------------------------------

## Objetivos

1.  Diseñar pipelines ETL y ELT confiables.
2.  Integrar fuentes estructuradas, semiestructuradas y APIs.
3.  Construir Data Lakes, Data Warehouses y arquitecturas Lakehouse.
4.  Procesar grandes volúmenes de datos con Spark y PySpark.
5.  Implementar procesamiento batch y streaming.
6.  Orquestar procesos, administrar dependencias y manejar reintentos.
7.  Aplicar pruebas, validaciones y controles de calidad.
8.  Incorporar seguridad, gobierno, linaje y observabilidad.
9.  Automatizar infraestructura y despliegues.
10. Documentar decisiones técnicas y demostrar resultados reproducibles.

## Tecnologías

  -----------------------------------------------------------------------
  Área                                Tecnologías y herramientas
  ----------------------------------- -----------------------------------
  Lenguajes                           Python, SQL, Scala, Java, Bash

  Procesamiento                       Pandas, PyArrow, Apache Spark,
                                      PySpark, Apache Flink, Apache Beam

  Cloud                               AWS, Microsoft Azure, Google Cloud
                                      Platform (GCP)

  Data Warehouse                      BigQuery, Amazon Redshift, Azure
                                      SQL, PostgreSQL

  Lakehouse                           Databricks, Delta Lake, Apache
                                      Iceberg

  Almacenamiento                      Amazon S3, Azure Data Lake Storage
                                      Gen2, Google Cloud Storage

  Streaming                           Apache Kafka, Amazon Kinesis,
                                      Google Pub/Sub, Dataflow

  Orquestación                        Apache Airflow, Azure Data Factory

  Transformaciones                    dbt, SQL

  Calidad y gobierno                  Great Expectations, Unity Catalog,
                                      Microsoft Purview, Dataplex

  DevOps                              Docker, Kubernetes, GitHub Actions,
                                      Terraform

  Visualización                       Power BI, Tableau, Looker Studio

  Formatos                            CSV, JSON, Avro, Parquet
  -----------------------------------------------------------------------

No es obligatorio utilizar todas las herramientas en todos los
proyectos. Se debe elegir la tecnología que resuelva el problema y
justificar la decisión.

## Estructura sugerida del repositorio

``` text
data-engineering-portfolio/
├── README.md
├── docs/
│   ├── architecture/
│   ├── decisions/
│   └── images/
├── projects/
│   ├── junior/
│   │   ├── 01-retail-etl/
│   │   ├── 02-gcp-data-lake/
│   │   ├── 03-azure-data-factory/
│   │   ├── 04-retail-data-warehouse/
│   │   └── 05-multi-api-ingestion/
│   ├── semi-senior/
│   │   ├── 06-databricks-lakehouse/
│   │   ├── 07-bigquery-warehouse/
│   │   ├── 08-spark-processing/
│   │   ├── 09-kafka-gcp-streaming/
│   │   └── 10-dataops-orchestration/
│   ├── senior/
│   │   ├── 11-multi-cloud-lakehouse/
│   │   ├── 12-fintech-streaming/
│   │   ├── 13-data-governance-quality/
│   │   ├── 14-devsecops-iac/
│   │   └── 15-enterprise-data-platform/
│   └── advanced/
│       ├── 16-ecommerce-data-lake/
│       ├── 17-iot-streaming/
│       ├── 18-financial-warehouse/
│       ├── 19-ml-feature-engineering/
│       └── 20-enterprise-capstone/
├── scripts/
├── infrastructure/
├── tests/
└── .github/workflows/
```

Cada proyecto debe ser autónomo, con su propio `README.md`,
configuración, código, pruebas y diagrama de arquitectura.

------------------------------------------------------------------------

# Nivel Junior

## 01. Pipeline ETL de ventas minoristas

**Objetivo:** construir un pipeline ETL que lea ventas desde archivos
CSV, limpie los datos y los cargue en una base relacional.

**Tecnologías:** Python, Pandas, PostgreSQL, SQL, Docker.

**Arquitectura:**

``` text
CSV → Extracción Python → Limpieza y validación → PostgreSQL → Consultas SQL
```

**Requerimientos funcionales:** - Leer uno o varios archivos CSV. -
Validar columnas, tipos de datos y valores obligatorios. - Tratar
duplicados, nulos, fechas inválidas y registros inconsistentes. -
Normalizar nombres de columnas y formatos. - Cargar los datos en
PostgreSQL. - Permitir ejecuciones repetibles sin duplicar registros.

**Implementación:** 1. Crear un conjunto de datos de ventas de ejemplo.
2. Diseñar el esquema relacional de destino. 3. Desarrollar módulos
separados para extracción, transformación y carga. 4. Incorporar logs y
manejo de excepciones. 5. Configurar PostgreSQL mediante Docker Compose.
6. Agregar pruebas unitarias y una prueba de integración.

**Entregables:** código fuente, `docker-compose.yml`, SQL de creación de
tablas, datos de ejemplo, pruebas y documentación.

**Criterios de aceptación:** el pipeline se ejecuta desde cero, valida
los datos, registra errores y puede repetirse sin generar duplicados.

## 02. Data Lake básico en GCP

**Objetivo:** crear un Data Lake sencillo en Google Cloud para almacenar
archivos originales y datos procesados.

**Tecnologías:** Google Cloud Storage, BigQuery, Python, PyArrow.

**Arquitectura:**

``` text
CSV / JSON → Python → Cloud Storage (raw)
                         ↓
                Transformación PyArrow
                         ↓
                 Cloud Storage (processed)
                         ↓
                      BigQuery
```

**Requerimientos funcionales:** - Crear zonas `raw` y `processed`. -
Cargar archivos CSV o JSON en Cloud Storage. - Convertir los datos
procesados a Parquet. - Organizar archivos por fecha o categoría. -
Crear tablas externas o cargar los datos en BigQuery. - Documentar
esquema, particiones y convenciones de nombres.

**Implementación:** 1. Crear un proyecto de GCP y un bucket. 2.
Configurar permisos con una cuenta de servicio de privilegios mínimos.
3. Desarrollar el proceso de carga. 4. Convertir los datos a Parquet. 5.
Crear una tabla en BigQuery y ejecutar consultas. 6. Documentar costos
estimados y limpieza de recursos.

**Entregables:** scripts Python, estructura del bucket, esquema de
BigQuery, consultas SQL y guía de configuración.

**Criterios de aceptación:** los archivos se almacenan correctamente,
los datos procesados pueden consultarse y el proceso es reproducible.

## 03. Integración con Azure Data Factory

**Objetivo:** integrar datos de distintas fuentes mediante Azure Data
Factory (ADF).

**Tecnologías:** Azure Data Factory, Azure Data Lake Storage Gen2, Azure
SQL Database, Python.

**Arquitectura:**

``` text
Fuente CSV / SQL → Azure Data Factory → ADLS Gen2
                                          ↓
                                    Transformación
                                          ↓
                                      Azure SQL
```

**Requerimientos funcionales:** - Configurar Linked Services y
Datasets. - Crear pipelines parametrizados. - Copiar archivos desde una
fuente hacia ADLS Gen2. - Validar la ejecución y registrar fallos. -
Cargar datos transformados en Azure SQL. - Incorporar parámetros para
rutas y fechas de procesamiento.

**Implementación:** 1. Crear los recursos necesarios en Azure. 2.
Configurar conexiones y credenciales seguras. 3. Diseñar un pipeline de
copia. 4. Agregar actividades de validación y control. 5. Configurar
triggers manuales o programados. 6. Documentar monitoreo, permisos y
costos.

**Entregables:** definición de pipelines, datasets, Linked Services,
scripts auxiliares y documentación.

**Criterios de aceptación:** el pipeline mueve datos de extremo a
extremo y permite identificar ejecuciones exitosas y fallidas.

## 04. Data Warehouse minorista

**Objetivo:** diseñar un Data Warehouse para analizar ventas, productos,
clientes y sucursales.

**Tecnologías:** PostgreSQL o SQL Server, SQL, Python, Power BI.

**Arquitectura:**

``` text
Fuentes operacionales → ETL Python / SQL → Modelo dimensional
                                             ↓
                                      Data Warehouse
                                             ↓
                                         Power BI
```

**Requerimientos funcionales:** - Diseñar un modelo dimensional. - Crear
una tabla de hechos de ventas. - Crear dimensiones de producto, cliente,
sucursal y fecha. - Definir claves sustitutas y relaciones. -
Implementar cargas incrementales. - Crear consultas para ventas, ticket
promedio y evolución temporal.

**Implementación:** 1. Definir el proceso de negocio y el nivel de
granularidad. 2. Diseñar el esquema estrella. 3. Crear tablas y
restricciones. 4. Implementar las transformaciones y cargas. 5. Validar
integridad y consistencia. 6. Crear un dashboard de análisis.

**Entregables:** diagrama entidad-relación, modelo dimensional, scripts
SQL, ETL y dashboard.

**Criterios de aceptación:** el modelo responde correctamente consultas
analíticas y conserva la integridad de las relaciones.

## 05. Ingesta de múltiples APIs

**Objetivo:** desarrollar un sistema que consuma varias APIs REST,
unifique sus datos y los almacene en una base central.

**Tecnologías:** Python, REST APIs, PostgreSQL, BigQuery.

**Arquitectura:**

``` text
API A ─┐
API B ─┼→ Cliente Python → Normalización → Almacenamiento
API C ─┘                                  ├→ PostgreSQL
                                         └→ BigQuery
```

**Requerimientos funcionales:** - Consumir al menos tres endpoints. -
Gestionar autenticación y variables de entorno. - Implementar
paginación, timeout y reintentos. - Respetar límites de solicitudes y
códigos HTTP. - Normalizar estructuras JSON diferentes. - Registrar
origen, fecha de extracción y estado de carga.

**Implementación:** 1. Seleccionar APIs públicas o de prueba. 2. Definir
un contrato de datos común. 3. Crear clientes independientes por API. 4.
Implementar extracción, normalización y persistencia. 5. Agregar logs y
control de errores. 6. Documentar límites y condiciones de uso de cada
API.

**Entregables:** conectores, esquema unificado, base de datos,
configuración de ejemplo y pruebas.

**Criterios de aceptación:** las fuentes se integran sin bloquear todo
el proceso ante el fallo aislado de una API.

------------------------------------------------------------------------

# Nivel Semi-Senior

## 06. Lakehouse con Databricks y Delta Lake

**Objetivo:** implementar una arquitectura Lakehouse con capas Medallion
y control transaccional.

**Tecnologías:** Azure Databricks, PySpark, Delta Lake, Unity Catalog,
ADLS Gen2.

**Arquitectura:**

``` text
Fuentes → ADLS Gen2 → Bronze → Silver → Gold → SQL / BI
                         Databricks + Delta Lake
                                      ↓
                                Unity Catalog
```

**Requerimientos funcionales:** - Implementar capas Bronze, Silver y
Gold. - Ingerir datos batch. - Aplicar limpieza, deduplicación y reglas
de negocio. - Utilizar Delta Lake para operaciones ACID. - Configurar
particiones y estrategias de optimización. - Administrar permisos y
metadatos con Unity Catalog.

**Implementación:** 1. Configurar workspace y almacenamiento. 2. Definir
catálogos, esquemas y permisos. 3. Crear notebooks o jobs PySpark. 4.
Implementar transformaciones por capa. 5. Aplicar controles de calidad.
6. Medir rendimiento antes y después de optimizar.

**Entregables:** notebooks, jobs, tablas Delta, configuración de
gobierno y diagramas.

**Criterios de aceptación:** el flujo completo puede ejecutarse
nuevamente, mantiene trazabilidad y aplica controles de acceso.

## 07. Data Warehouse avanzado en BigQuery

**Objetivo:** construir un Data Warehouse analítico escalable utilizando
BigQuery y dbt.

**Tecnologías:** GCP, BigQuery, dbt, Cloud Storage, Looker Studio.

**Arquitectura:**

``` text
Fuentes → Cloud Storage → BigQuery (staging)
                              ↓
                         dbt models
                              ↓
                    Data Marts / BigQuery
                              ↓
                        Looker Studio
```

**Requerimientos funcionales:** - Diseñar zonas de staging,
transformación y marts. - Implementar modelos dbt. - Crear tests de
unicidad, relaciones y valores no nulos. - Utilizar particionamiento y
clustering cuando corresponda. - Documentar modelos y dependencias. -
Crear indicadores y visualizaciones.

**Implementación:** 1. Crear datasets y permisos. 2. Cargar datos de
origen. 3. Configurar el proyecto dbt. 4. Desarrollar modelos
incrementales cuando sean convenientes. 5. Ejecutar tests y generar
documentación. 6. Construir un dashboard conectado a BigQuery.

**Entregables:** proyecto dbt, modelos SQL, documentación, tests y
dashboard.

**Criterios de aceptación:** los modelos se ejecutan de forma
reproducible, pasan las pruebas y permiten rastrear dependencias.

## 08. Procesamiento distribuido con Spark

**Objetivo:** procesar grandes volúmenes de datos utilizando Apache
Spark y evaluar el impacto de la distribución.

**Tecnologías:** Apache Spark, PySpark, Parquet, AWS EMR o Databricks.

**Arquitectura:**

``` text
Archivos / Data Lake → Spark / PySpark → Transformaciones distribuidas
                                              ↓
                                     Parquet particionado
                                              ↓
                                      Consultas analíticas
```

**Requerimientos funcionales:** - Generar o utilizar un dataset de
volumen significativo. - Leer CSV, JSON y Parquet. - Aplicar joins,
agregaciones y ventanas. - Evaluar particionamiento y skew. - Comparar
operaciones optimizadas y no optimizadas. - Medir tiempo, recursos y
volumen procesado.

**Implementación:** 1. Preparar el entorno Spark local o cloud. 2.
Diseñar un conjunto de datos reproducible. 3. Implementar
transformaciones con DataFrames. 4. Revisar el plan de ejecución. 5.
Optimizar particiones y operaciones costosas. 6. Exportar resultados a
Parquet.

**Entregables:** notebooks, scripts, dataset o generador, resultados de
benchmarks y documentación.

**Criterios de aceptación:** el procesamiento es correcto y se presenta
una comparación técnica de las decisiones de optimización.

## 09. Streaming con Kafka y GCP

**Objetivo:** construir un pipeline de eventos en tiempo real que
procese mensajes y los almacene para análisis.

**Tecnologías:** Apache Kafka, Google Pub/Sub, Dataflow, BigQuery.

**Arquitectura:**

``` text
Productores → Kafka / Pub/Sub → Dataflow → BigQuery
                    ↓
              Validación de eventos
```

**Requerimientos funcionales:** - Generar eventos con un productor. -
Definir topics, esquemas y claves de partición. - Consumir eventos
continuamente. - Validar estructura y campos obligatorios. - Gestionar
mensajes inválidos y reintentos. - Almacenar eventos procesados en
BigQuery. - Medir latencia, throughput y errores.

**Implementación:** 1. Diseñar el contrato de eventos. 2. Configurar
productores y consumidores. 3. Implementar el procesamiento con Dataflow
o una alternativa compatible. 4. Definir estrategia de ventanas si el
caso lo requiere. 5. Persistir eventos en BigQuery. 6. Crear métricas y
alertas operativas.

**Entregables:** productores, consumidores, pipeline, esquema de
eventos, consultas y métricas.

**Criterios de aceptación:** los eventos se procesan continuamente y se
documenta cómo se manejan fallos, duplicados y retrasos.

## 10. DataOps y orquestación

**Objetivo:** automatizar pipelines de datos mediante orquestación,
pruebas y CI/CD.

**Tecnologías:** Apache Airflow, Docker, dbt, PostgreSQL, GitHub
Actions.

**Arquitectura:**

``` text
GitHub → CI (lint + tests) → Airflow DAG
                               ↓
                       Extracción / dbt
                               ↓
                           PostgreSQL
                               ↓
                     Logs + alertas + métricas
```

**Requerimientos funcionales:** - Crear DAGs con dependencias
explícitas. - Configurar schedules, retries, timeouts y backfills. -
Ejecutar transformaciones dbt. - Incorporar pruebas automáticas. - Crear
pipeline CI con GitHub Actions. - Gestionar secretos fuera del
repositorio.

**Implementación:** 1. Levantar Airflow con Docker Compose. 2. Crear un
DAG modular. 3. Integrar tareas de extracción y transformación. 4.
Incorporar tests y validaciones. 5. Configurar CI para cada Pull
Request. 6. Documentar recuperación ante fallos.

**Entregables:** DAGs, configuración Docker, proyecto dbt, workflows CI
y manual operativo.

**Criterios de aceptación:** los pipelines pueden ejecutarse bajo
demanda y por calendario, y los cambios pasan por controles
automatizados.

------------------------------------------------------------------------

# Nivel Senior

## 11. Lakehouse empresarial multi-cloud

**Objetivo:** diseñar una arquitectura Lakehouse portable que integre
servicios de AWS, Azure y GCP.

**Tecnologías:** Databricks, AWS, Azure, GCP, Delta Lake, Apache
Iceberg, Terraform.

**Arquitectura de referencia:**

``` text
AWS S3 ─────┐
ADLS Gen2 ──┼→ Capa de almacenamiento → Lakehouse → Consumo analítico
GCS ────────┘                         Delta / Iceberg
                                           ↓
                              Catálogo y gobierno central
```

**Requerimientos funcionales:** - Definir una arquitectura de referencia
multi-cloud. - Separar almacenamiento, cómputo y consumo. - Evaluar
Delta Lake y Apache Iceberg según el caso de uso. - Definir
interoperabilidad y catálogo. - Automatizar recursos mediante
Terraform. - Establecer identidad, redes y políticas de acceso.

**Implementación:** 1. Documentar requisitos funcionales y no
funcionales. 2. Diseñar arquitectura lógica y física. 3. Crear módulos
de infraestructura. 4. Implementar un flujo de datos de demostración. 5.
Evaluar portabilidad, costos y dependencia de proveedor. 6. Documentar
recuperación y continuidad.

**Entregables:** diagramas, módulos Terraform, pipelines de ejemplo,
matriz de decisiones y estimación de costos.

**Criterios de aceptación:** la arquitectura está documentada, puede
desplegarse de forma controlada y justifica sus decisiones de
interoperabilidad.

## 12. Plataforma fintech en tiempo real

**Objetivo:** procesar transacciones financieras en tiempo real y
detectar patrones anómalos.

**Tecnologías:** Apache Kafka, Apache Flink, Databricks, Delta Lake,
BigQuery o Redshift.

**Arquitectura:**

``` text
Transacciones → Kafka → Flink → Reglas / detección
                            ↓
                      Delta Lake
                            ↓
                  BigQuery / Redshift
                            ↓
                     Alertas y BI
```

**Requerimientos funcionales:** - Simular eventos de transacciones. -
Procesar eventos con baja latencia. - Definir ventanas temporales y
reglas de negocio. - Detectar duplicados y eventos fuera de orden. -
Implementar idempotencia y manejo de estado. - Mantener datos históricos
para auditoría. - Generar alertas ante patrones sospechosos.

**Implementación:** 1. Definir esquema y semántica de eventos. 2.
Configurar Kafka y Flink. 3. Desarrollar reglas de procesamiento. 4.
Implementar persistencia histórica. 5. Diseñar estrategia de
re-procesamiento. 6. Medir latencia, throughput y precisión de reglas.

**Entregables:** generador de transacciones, jobs, reglas,
almacenamiento, dashboard y pruebas de carga.

**Criterios de aceptación:** el sistema procesa eventos de forma
continua y documenta garantías de entrega, consistencia y recuperación.

> Utilizar datos sintéticos o anonimizados. No incluir información
> financiera real ni credenciales en el repositorio.

## 13. Gobierno y calidad de datos empresarial

**Objetivo:** implementar una plataforma de gobierno que permita
administrar catálogo, calidad, linaje, accesos y políticas.

**Tecnologías:** Unity Catalog, Microsoft Purview, Dataplex, dbt, Great
Expectations.

**Arquitectura:**

``` text
Fuentes → Catálogo / Metadatos → Reglas de calidad
                  ↓                    ↓
                Linaje             Resultados
                  ↓                    ↓
             Gobierno y políticas de acceso
```

**Requerimientos funcionales:** - Inventariar datasets y propietarios. -
Definir clasificación y sensibilidad. - Implementar reglas de calidad. -
Registrar linaje de datos. - Definir roles y permisos. - Documentar
políticas de retención y auditoría. - Generar reportes de cumplimiento.

**Implementación:** 1. Seleccionar datasets de ejemplo. 2. Definir un
glosario de negocio. 3. Registrar metadatos y responsables. 4. Crear
reglas de calidad con Great Expectations. 5. Configurar catálogo y
permisos según la plataforma. 6. Generar reportes de calidad y linaje.

**Entregables:** catálogo, glosario, reglas, reportes, matriz de accesos
y documentación de políticas.

**Criterios de aceptación:** cada dataset relevante cuenta con
propietario, descripción, clasificación, reglas de calidad y permisos
documentados.

## 14. DevSecOps e infraestructura como código

**Objetivo:** automatizar el despliegue seguro de una plataforma de
datos mediante infraestructura como código y controles DevSecOps.

**Tecnologías:** Terraform, Kubernetes, Docker, GitHub Actions,
AWS/Azure/GCP.

**Arquitectura:**

``` text
Git → Pull Request → CI (tests + análisis de seguridad)
                         ↓
                  Terraform Plan
                         ↓
                    Aprobación
                         ↓
                  Terraform Apply
                         ↓
                Plataforma desplegada
```

**Requerimientos funcionales:** - Crear infraestructura mediante
Terraform. - Modularizar recursos y variables. - Separar ambientes dev,
staging y producción. - Ejecutar análisis estático y escaneo de
dependencias. - Gestionar secretos con servicios especializados. -
Aplicar RBAC y mínimo privilegio. - Incorporar políticas de revisión y
aprobación.

**Implementación:** 1. Definir módulos Terraform. 2. Crear configuración
por ambiente. 3. Configurar backend de estado remoto y bloqueo. 4.
Implementar CI/CD. 5. Integrar validaciones de seguridad. 6. Documentar
rollback y recuperación.

**Entregables:** módulos IaC, workflows, políticas, reportes de análisis
y manual de despliegue.

**Criterios de aceptación:** la infraestructura puede versionarse y
desplegarse de forma reproducible, con controles y trazabilidad.

## 15. Plataforma de datos empresarial end-to-end

**Objetivo:** integrar ingestión, almacenamiento, procesamiento,
consumo, gobierno y operación en una plataforma de datos completa.

**Tecnologías:** AWS, Azure o GCP; Spark, Airflow, dbt, Kafka, Docker,
Terraform, herramientas BI.

**Arquitectura:**

``` text
Fuentes batch y streaming
          ↓
   Ingesta y validación
          ↓
       Data Lake
          ↓
 Bronze → Silver → Gold
          ↓
 Data Warehouse / Lakehouse
          ↓
 APIs / BI / Data Science
          ↓
 Gobierno + Calidad + Observabilidad
```

**Requerimientos funcionales:** - Integrar al menos dos fuentes
heterogéneas. - Implementar cargas batch y streaming. - Definir
arquitectura Medallion o equivalente. - Orquestar procesos. - Incorporar
pruebas y controles de calidad. - Aplicar gobierno, seguridad y
auditoría. - Implementar monitoreo y alertas. - Automatizar
infraestructura y despliegue.

**Implementación:** 1. Relevar necesidades y definir SLA/SLO. 2. Diseñar
arquitectura y contratos de datos. 3. Implementar almacenamiento e
ingesta. 4. Desarrollar transformaciones y modelos. 5. Orquestar y
automatizar pruebas. 6. Configurar monitoreo, seguridad y gobierno. 7.
Ejecutar pruebas de carga y recuperación.

**Entregables:** plataforma desplegable, código, infraestructura,
pipelines, dashboards operativos, documentación y análisis de costos.

**Criterios de aceptación:** existe un flujo integral reproducible, con
monitoreo, seguridad, documentación y procedimientos de operación.

------------------------------------------------------------------------

# Cinco proyectos avanzados adicionales

## 16. Data Lake de e-commerce

**Objetivo:** procesar aproximadamente un millón de registros de
e-commerce, almacenarlos en formato columnar y habilitar consultas
analíticas.

**Tecnologías:** AWS S3, AWS Glue, PySpark, Parquet, Amazon Athena.

**Arquitectura:**

``` text
CSV (≈1 millón de registros) → S3 Raw → AWS Glue / PySpark
                                            ↓
                                      Parquet particionado
                                            ↓
                                        S3 Curated
                                            ↓
                                         Athena
```

**Requerimientos funcionales:** - Ingerir datos de ventas, productos y
clientes. - Mantener una copia de los datos originales. - Transformar y
validar con Glue. - Generar Parquet comprimido. - Particionar por fecha
u otra dimensión apropiada. - Consultar mediante Athena y comparar
costos/tiempos.

**Implementación:** 1. Preparar dataset sintético o público. 2. Diseñar
buckets y convenciones de rutas. 3. Crear job ETL con Glue. 4.
Implementar validaciones y particionamiento. 5. Configurar catálogo y
consultas Athena. 6. Comparar CSV frente a Parquet.

**Entregables:** job Glue, estructura S3, catálogo, consultas SQL y
benchmark.

**Criterios de aceptación:** el pipeline procesa el dataset, conserva
datos raw y produce resultados consultables con evidencia de
optimización.

## 17. Pipeline IoT en streaming

**Objetivo:** simular 100 sensores que transmiten eventos y detectar
anomalías en tiempo real.

**Tecnologías:** Amazon Kinesis, AWS Lambda, Amazon S3, Amazon SNS.

**Arquitectura:**

``` text
100 sensores simulados → Kinesis → Lambda
                                    ├→ Detección de anomalías
                                    ├→ S3 (histórico)
                                    └→ SNS (alertas)
```

**Requerimientos funcionales:** - Simular sensores con identificador,
timestamp y medición. - Enviar eventos a Kinesis. - Procesar mensajes
con Lambda. - Detectar valores fuera de umbrales configurables. -
Almacenar histórico en S3. - Enviar alertas mediante SNS. - Registrar
errores y eventos no procesables.

**Implementación:** 1. Desarrollar simulador de sensores. 2. Crear
stream de Kinesis. 3. Configurar función Lambda. 4. Implementar reglas
de anomalías. 5. Persistir eventos y configurar SNS. 6. Probar carga,
retrasos y fallos.

**Entregables:** simulador, función, configuración cloud, histórico,
alertas y documentación.

**Criterios de aceptación:** los eventos se reciben, procesan y
almacenan; las anomalías generan alertas verificables.

## 18. Data Warehouse financiero

**Objetivo:** construir un Data Warehouse financiero con modelo
dimensional y consultas analíticas optimizadas.

**Tecnologías:** Amazon Redshift, Amazon S3, Redshift Spectrum, SQL, BI.

**Arquitectura:**

``` text
Archivos financieros → S3 → COPY → Redshift
                                      ↓
                              Modelo dimensional
                                      ↓
                               Spectrum / BI
```

**Requerimientos funcionales:** - Diseñar un esquema estrella. - Crear
hechos de movimientos y dimensiones de cuenta, cliente, fecha y tipo. -
Cargar datos desde S3 mediante COPY. - Consultar datos externos con
Redshift Spectrum. - Analizar distribución, sort keys y estrategias de
consulta. - Validar conciliaciones y agregados.

**Implementación:** 1. Definir granularidad y reglas de negocio. 2.
Diseñar modelo dimensional. 3. Preparar archivos de carga. 4. Configurar
Redshift y permisos S3. 5. Implementar cargas y consultas. 6. Medir y
optimizar rendimiento.

**Entregables:** DDL, scripts de carga, consultas, modelo dimensional,
benchmark y documentación.

**Criterios de aceptación:** las cargas y consultas son reproducibles y
se documentan decisiones de modelado y rendimiento.

## 19. Feature Engineering para Machine Learning

**Objetivo:** crear un pipeline distribuido de ingeniería de
características para modelos de Machine Learning.

**Tecnologías:** AWS EMR, Apache Spark, PySpark, SageMaker Feature
Store.

**Arquitectura:**

``` text
Datos históricos → S3 → EMR / Spark
                            ↓
                   Feature Engineering
                     ├→ RFM
                     ├→ Variables temporales
                     └→ Agregaciones
                            ↓
                  SageMaker Feature Store
                            ↓
                     Entrenamiento ML
```

**Requerimientos funcionales:** - Procesar datos históricos con Spark. -
Crear variables RFM (recencia, frecuencia y monto). - Generar features
temporales y agregaciones. - Definir esquemas y tipos. - Evitar leakage
entre entrenamiento y evaluación. - Publicar features para consumo de
modelos. - Documentar consistencia entre entrenamiento e inferencia.

**Implementación:** 1. Seleccionar dataset transaccional. 2. Definir
entidad y granularidad de features. 3. Desarrollar transformaciones
PySpark. 4. Validar distribuciones y valores. 5. Publicar features en
Feature Store. 6. Demostrar consumo desde un flujo de entrenamiento.

**Entregables:** jobs Spark, catálogo de features, validaciones,
integración con Feature Store y documentación.

**Criterios de aceptación:** las features son reproducibles, están
documentadas y pueden consumirse de manera consistente.

## 20. Capstone: plataforma de datos empresarial

**Objetivo:** desarrollar una plataforma integral que combine batch,
streaming, Data Warehouse, infraestructura como código, CI/CD y
monitoreo.

**Duración estimada:** 40 horas para una primera implementación acotada.
Una versión productiva requerirá más tiempo, según alcance y requisitos.

**Tecnologías:** elegir entre AWS, Azure o GCP; Spark, Kafka o servicio
equivalente, Airflow, dbt, Terraform, Docker y herramienta de BI.

**Arquitectura:**

``` text
                Fuentes batch y streaming
                           ↓
                    Ingesta de datos
                           ↓
                 Data Lake / Lakehouse
                           ↓
                  Procesamiento y ETL
                           ↓
                 Data Warehouse / Marts
                           ↓
                       Consumo BI

       CI/CD + IaC + Calidad + Seguridad + Monitoreo
```

**Requerimientos funcionales:** - Integrar al menos una fuente batch y
una streaming. - Diseñar almacenamiento por capas. - Implementar
transformaciones y modelo analítico. - Orquestar procesos. - Incorporar
pruebas automatizadas. - Desplegar infraestructura mediante IaC. - Crear
monitoreo, logs y alertas. - Documentar arquitectura, costos y
operación.

**Plan de trabajo sugerido:** 1. **Análisis y diseño:** alcance,
requisitos, arquitectura y contratos. 2. **Infraestructura:**
almacenamiento, red, permisos y entornos. 3. **Ingesta:** conectores
batch y streaming. 4. **Procesamiento:** transformaciones, calidad y
modelado. 5. **Orquestación:** dependencias, retries y backfills. 6.
**Operación:** CI/CD, observabilidad, seguridad y documentación.

**Entregables:** repositorio completo, diagramas, infraestructura,
pipelines, pruebas, dashboard, manual operativo y estimación de costos.

**Criterios de aceptación:** una ejecución de extremo a extremo
demuestra la ingesta, transformación, disponibilidad y monitoreo de los
datos.

------------------------------------------------------------------------

## Estándares de arquitectura

Todos los proyectos deberían aplicar, de acuerdo con su complejidad, los
siguientes principios:

-   **Separación de responsabilidades:** extracción, transformación,
    carga y consumo en módulos independientes.
-   **Idempotencia:** repetir una ejecución no debe producir efectos
    duplicados o inconsistentes.
-   **Reprocesamiento:** permitir recuperar datos por fecha, partición o
    lote.
-   **Escalabilidad:** identificar límites de volumen, concurrencia y
    capacidad.
-   **Portabilidad:** evitar dependencias innecesarias de un proveedor
    cuando el caso requiera interoperabilidad.
-   **Configuración externa:** separar parámetros, secretos y código.
-   **Contratos de datos:** documentar esquemas, campos obligatorios y
    compatibilidad.
-   **Arquitectura evolutiva:** registrar decisiones técnicas y sus
    alternativas.

## Calidad, seguridad y observabilidad

### Calidad de datos

Incorporar controles de: - Completitud y valores nulos. - Unicidad y
duplicados. - Validez de tipos y rangos. - Integridad referencial. -
Consistencia entre fuentes. - Frescura y volumen esperado. - Detección y
cuarentena de registros inválidos.

### Seguridad

-   No subir claves, tokens ni contraseñas al repositorio.
-   Utilizar variables de entorno o gestores de secretos.
-   Aplicar mínimo privilegio y RBAC.
-   Cifrar datos en tránsito y en reposo.
-   Enmascarar o anonimizar datos sensibles.
-   Mantener auditoría de accesos y operaciones.
-   Definir políticas de retención y eliminación.

### Observabilidad

Cada pipeline debería registrar: - Identificador de ejecución y lote. -
Fecha de inicio y finalización. - Registros leídos, procesados,
rechazados y escritos. - Duración y estado. - Errores y reintentos. -
Métricas de latencia y volumen. - Alertas ante fallos o incumplimiento
de SLA/SLO.

## Criterios de finalización

Un proyecto se considera completo cuando incluye:

-   [ ] README propio con objetivo, alcance y requisitos.
-   [ ] Diagrama de arquitectura y flujo de datos.
-   [ ] Instrucciones de instalación y configuración.
-   [ ] Código organizado y versionado.
-   [ ] Datos de ejemplo o instrucciones para obtenerlos.
-   [ ] Pruebas unitarias y, cuando corresponda, de integración.
-   [ ] Validaciones de calidad de datos.
-   [ ] Manejo de errores, logs y reintentos.
-   [ ] Configuración segura sin secretos expuestos.
-   [ ] Instrucciones de ejecución y despliegue.
-   [ ] Evidencia de resultados y consultas de ejemplo.
-   [ ] Limitaciones conocidas y posibles mejoras.
-   [ ] Estimación de costos para recursos cloud.

## Plan de implementación

Orden recomendado para construir el portafolio:

  -----------------------------------------------------------------------
  Etapa                   Proyectos               Resultado esperado
  ----------------------- ----------------------- -----------------------
  Fundamentos             01--05                  ETL, SQL, APIs,
                                                  almacenamiento y
                                                  modelado

  Procesamiento y cloud   06--10                  Lakehouse, BigQuery,
                                                  Spark, streaming y
                                                  DataOps

  Arquitectura            11--15                  Multi-cloud, fintech,
  empresarial                                     gobierno, DevSecOps y
                                                  plataforma integral

  Especialización         16--20                  E-commerce, IoT,
                                                  finanzas, ML features y
                                                  capstone
  -----------------------------------------------------------------------

No es necesario completar todos los proyectos con la misma profundidad.
Es preferible presentar menos proyectos con código ejecutable, pruebas,
diagramas, decisiones técnicas y resultados verificables que muchos
repositorios incompletos.

## Recursos oficiales

-   [Python](https://docs.python.org/3/)
-   [PostgreSQL](https://www.postgresql.org/docs/)
-   [Apache Spark](https://spark.apache.org/docs/latest/)
-   [Apache Airflow](https://airflow.apache.org/docs/)
-   [Apache Kafka](https://kafka.apache.org/documentation/)
-   [Databricks](https://docs.databricks.com/)
-   [Delta Lake](https://docs.delta.io/)
-   [Google Cloud BigQuery](https://cloud.google.com/bigquery/docs)
-   [Google Cloud Storage](https://cloud.google.com/storage/docs)
-   [Azure Data
    Factory](https://learn.microsoft.com/azure/data-factory/)
-   [Azure Data Lake
    Storage](https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-introduction)
-   [AWS Glue](https://docs.aws.amazon.com/glue/)
-   [Amazon S3](https://docs.aws.amazon.com/s3/)
-   [Amazon Redshift](https://docs.aws.amazon.com/redshift/)
-   [Terraform](https://developer.hashicorp.com/terraform/docs)
-   [dbt](https://docs.getdbt.com/)
-   [Great Expectations](https://docs.greatexpectations.io/)

## Licencia

Este repositorio puede publicarse bajo la licencia **MIT** si se desea
permitir reutilización y modificación. Antes de agregarla, crear el
archivo `LICENSE` y verificar que los datasets, ejemplos y recursos
externos utilizados permitan ese uso.

------------------------------------------------------------------------

## Autor

**Luciano --- Data Science / Machine Learning / Data Engineering**

-   GitHub: [@luchoxiii](https://github.com/luchoxiii)
-   Portfolio: [luchoxiii.github.io](https://luchoxiii.github.io)

> Este portafolio tiene fines educativos y de demostración técnica. Los
> diagramas representan arquitecturas de referencia; cada implementación
> debe ajustarse a los requisitos, límites, costos y condiciones de
> seguridad del entorno elegido.
