import argparse
import json
import csv
import io
from typing import Dict, Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems


def load_country_data(csv_path: str) -> Dict[str, dict]:
    """
    Carga el CSV de países en memoria y devuelve un dict {Country: info_relevante}.
    Soporta rutas locales (./data/...) y esquemas manejados por FileSystems.
    """
    country_dict: Dict[str, dict] = {}

    # FileSystems.open permite leer tanto local como remoto (file://, azfs://, etc.)
    with FileSystems.open(csv_path) as f:
        # Leemos todo (el archivo es pequeño) y decodificamos con UTF-8 con BOM
        text = f.read().decode("utf-8-sig")

    # Detectar delimitador automáticamente
    sample = text[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample)
    except csv.Error:
        dialect = csv.get_dialect("excel")

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)

    # Normalizar nombres de columnas (strip)
    if reader.fieldnames:
        reader.fieldnames = [name.strip() for name in reader.fieldnames]

    for row in reader:
        # Limpiar espacios en claves y valores
        row = {
            k.strip(): (v.strip() if isinstance(v, str) else v)
            for k, v in row.items()
        }

        country_name = row.get("Country")
        if not country_name:
            continue

        # Tomar el idioma oficial desde la columna real del CSV
        raw_lang = row.get("Main_Official_Language")

        clean_lang = None
        if raw_lang:
            # Si hay varios idiomas separados por coma, usamos el primero
            clean_lang = raw_lang.split(",")[0].strip()

        # Caso especial: South Africa → usar English como idioma representativo
        if country_name == "South Africa":
            clean_lang = "English"

        country_dict[country_name] = {
            "country": row.get("Country"),
            "capital": row.get("Capital"),
            "continent": row.get("Continent"),
            "official language": clean_lang,
            "currency": row.get("Currency"),
        }

    print(f"[DEBUG] Países cargados desde CSV: {len(country_dict)}")
    return country_dict


def normalize_race_id(race_id: str) -> str:
    """
    Normaliza el campo RaceID al formato <string><numero> con la parte string en minúsculas.
    Ejemplos:
        "Cup 25"  -> "cup25"
        "race 11" -> "race11"
        "LEAGUE 4"-> "league4"
    """
    if not race_id:
        return race_id

    # Quitar espacios extra y colapsar múltiples espacios internos
    cleaned = " ".join(race_id.split())
    parts = cleaned.split(" ")
    if len(parts) == 1:
        # Algo como "cup25" ya viene correcto
        return parts[0].lower()

    # Primer token = prefijo string, resto concatenado como sufijo
    prefix = parts[0].lower()
    suffix = "".join(parts[1:])
    return f"{prefix}{suffix}"


def process_record(record: Dict[str, Any], country_dict: Dict[str, dict]) -> Dict[str, Any] | None:
    """
    Lógica de transformación:
    - Filtra DeviceType == "Other".
    - Normaliza RaceID.
    - Enriquese con datos de país en LocationData.
    Devuelve None si el registro debe ser descartado.
    """
    # Filtrado por tipo de dispositivo
    if record.get("DeviceType") == "Other":
        return None

    # Normalizar RaceID
    race_id = record.get("RaceID")
    if race_id is not None:
        record["RaceID"] = normalize_race_id(race_id)

    # Enriquecer con datos del país
    country_name = record.pop("ViewerLocationCountry", None)
    location_info = country_dict.get(country_name)
    if location_info:
        record["LocationData"] = location_info
    else:
        # Si no hay match en el CSV, igualmente incluimos la clave con None
        record["LocationData"] = None

    return record


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--json_input_pattern",
        required=True,
        help=(
            "Patrón de archivos JSON de entrada, por ejemplo: "
            "'./data/*.json'"
        ),
    )
    parser.add_argument(
        "--country_csv",
        required=True,
        help=(
            "Ruta al archivo CSV de países (country_data_v2.csv), "
            "por ejemplo: './data/country_data_v2.csv'"
        ),
    )
    parser.add_argument(
        "--output_prefix",
        required=True,
        help=(
            "Prefijo de salida para el archivo .jsonl, por ejemplo: "
            "'./output/hrl_enriched'"
        ),
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # Cargar diccionario de países como side data (pocas filas, se carga en memoria)
    country_dict = load_country_data(known_args.country_csv)

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # 1. Leer todos los JSON que cumplan el patrón
            | "Leer JSON (texto)" >> beam.io.ReadFromText(known_args.json_input_pattern)
            | "Parsear JSON" >> beam.Map(json.loads)
            # 2. Transformar y enriquecer
            | "Transformar y Enriquecer" >> beam.Map(
                process_record,
                country_dict=country_dict
            )
            | "Filtrar Nulos" >> beam.Filter(lambda x: x is not None)
            # 3. Serializar a JSON Lines
            | "A JSON Lines" >> beam.Map(json.dumps)
            | "Escribir Salida" >> beam.io.WriteToText(
                known_args.output_prefix,
                file_name_suffix=".jsonl",
                num_shards=1,
            )
        )


if __name__ == "__main__":
    run()
