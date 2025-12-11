# gino-etl-beam: HRL Data Processing Pipeline

Este proyecto implementa un **pipeline ETL completo** utilizando **Apache Beam** en Python. El sistema está diseñado para procesar datos de interacción de aficionados (*fan engagement*) de la Liga de Carreras de Helicópteros (HRL), normalizando, filtrando y enriqueciendo los eventos con información demográfica para generar un dataset optimizado para análisis.

## Descripción del Pipeline

El flujo de trabajo cubre el ciclo de vida completo de los datos, desde la ingestión hasta la persistencia, abordando las siguientes etapas lógicas:

### 1. Ingesta de Datos
- **Entrada Principal:** Lectura masiva de archivos JSON que contienen eventos de interacción.
- **Datos Maestros:** Carga de datos de referencia geográfica desde archivos CSV (`country_data_v2.csv`).

### 2. Normalización y Filtrado
- **Estandarización de Identificadores:** Transformación del campo `RaceID` al formato canónico `<string><número>` en minúsculas.
- **Depuración:** Exclusión de registros donde el `DeviceType` está clasificado como `"Other"`.

### 3. Enriquecimiento
- **Cruce de Datos:** Integración de información demográfica basada en el campo `ViewerLocationCountry`.
- **Estructuración:** Generación de un objeto anidado `LocationData` que incluye:
  - `country`
  - `capital`
  - `continent`
  - `official language`
  - `currency`

### 4. Salida
- **Persistencia:** Serialización de los registros procesados en formato **JSON Lines (.jsonl)** en la ruta especificada.

---

## Requisitos del Sistema

Para la ejecución correcta del entorno, se requiere:

- **Lenguaje:** Python **3.12.10** (versión verificada).
- **Sistema Operativo:** Windows (soporte documentado).
- **Gestor de Paquetes:** `pip`.

---

## Instalación y Configuración

Siga los siguientes pasos para configurar el entorno de desarrollo local.

### Clonar el repositorio
Si dispone de Git, clone el repositorio. De lo contrario, descargue el código fuente manualmente.

```
git clone [https://github.com/ginosaurio/gino-etl-beam.git](https://github.com/ginosaurio/gino-etl-beam.git)
cd gino-etl-beam
```

Se recomienda aislar las dependencias del proyecto. Ejecute los siguientes comandos en PowerShell:

### Crear el entorno virtual
```
python -m venv .venv
```

### Activar el entorno
```
.\.venv\Scripts\Activate.ps1
```
### Actualizar herramientas de empaquetado e instalar dependencias
```
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
```


## Ejecución del Pipeline

Para iniciar el proceso de transformación de datos, ejecute el script principal pipeline.py definiendo los patrones de entrada y salida:
```
python pipeline.py --json_input_pattern ".\data\*.json" --country_csv ".\data\country_data_v2.csv" --output_prefix ".\output\hrl_enriched" --runner DirectRunner
```
