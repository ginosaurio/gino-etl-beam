import csv
import random
import uuid
from datetime import datetime, timedelta
import os

def random_date(start, end):
    delta = end - start
    random_second = random.randrange(int(delta.total_seconds()))
    return start + timedelta(seconds=random_second)

def generate_fake_runs(
    num_runs: int = 500,
    etl_names = ["ventas_diarias", "inventario_mensual", "usuarios_activos", "resumen_finanzas"],
    estados = ["Exito", "Fallo", "Saltado", "En Ejecucion"],
    min_duration = 5,      # segundos
    max_duration = 30,    # segundos
    min_rows = 0,
    max_rows = 10000
):
    runs = []
    now = datetime.now()
    one_year_ago = now - timedelta(days=365)
    for _ in range(num_runs):
        start_ts = random_date(one_year_ago, now)
        duration = random.uniform(min_duration, max_duration)
        rows = random.randint(min_rows, max_rows)
        estado = random.choices(
            estados,
            weights=[0.7, 0.2, 0.05, 0.05],
            k=1
        )[0]
        if estado != "Exito":
            rows = 0

        run = {
            "id_ejecucion": str(uuid.uuid4()),
            "fecha_inicio": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "nombre_etl": random.choice(etl_names),
            "duracion_segundos": round(duration, 2),
            "estado": estado,
            "datos_procesados": rows
        }
        runs.append(run)
    return runs

def save_to_csv(runs, output_folder="output", filename="etls_ejecutadas.csv"):
    os.makedirs(output_folder, exist_ok=True)
    path = os.path.join(output_folder, filename)
    with open(path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(runs[0].keys()))
        writer.writeheader()
        for r in runs:
            writer.writerow(r)
    print(f"CSV generado: {path} ({len(runs)} filas)")

if __name__ == "__main__":
    runs = generate_fake_runs(num_runs=1000)
    save_to_csv(runs, output_folder="./output")
