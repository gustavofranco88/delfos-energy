import sys
import os
import json
import httpx
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import text
from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext

root_path = str(Path(__file__).parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from orchestration.resources import PostgresResource, PostgresAlvoResource

# Configuração de Partição (Últimos 10 dias)
data_inicio = (datetime.now() - timedelta(days=10)).strftime('%Y-%m-%d')
daily_partition = DailyPartitionsDefinition(start_date=data_inicio)

@asset(
    partitions_def=daily_partition
)
def hive_data_transformed(
    context: AssetExecutionContext, 
    source_db: PostgresResource, 
    target_db: PostgresAlvoResource
):
    partition_date = context.partition_key
    context.log.info(f"Iniciando ETL para a data: {partition_date}")
    
    target_db.init_db_structure()

    API_URL = "http://127.0.0.1:8000/data"
    headers = {"X-API-Key": "5fb8b21c-7f3e-4d8a-9b2c-1a2b3c4d5e6f"}
    todos_os_registros = []

    try:
        with httpx.Client(timeout=60.0) as client:
            pagina = 1
            while True:
                params = {
                    "start": partition_date, 
                    "end": partition_date, 
                    "page": pagina, 
                    "size": 1000, 
                    "wind_speed": True, 
                    "power": True
                }
                
                response = client.get(API_URL, headers=headers, params=params)
                
                if response.status_code == 422:
                    context.log.error(f"Erro de validação na API (422): {response.text}")
                
                response.raise_for_status()
                
                dados = response.json().get("results", [])
                todos_os_registros.extend(dados)
                
                if len(dados) < 1000: 
                    break
                pagina += 1

        if not todos_os_registros:
            context.log.warning(f"Nenhum dado retornado pela API para a data {partition_date}")
            return

        df_raw = pd.DataFrame(todos_os_registros)
        df_raw['timestamp'] = pd.to_datetime(df_raw['timestamp'])
        
        df_raw['ts_helper'] = df_raw['timestamp'] 
        df_raw = df_raw.set_index('timestamp').sort_index()

        res = df_raw.resample('10min').agg({
            'wind_speed': ['mean', 'min', 'max', 'std'],
            'power': ['mean', 'min', 'max', 'std'],
            'ts_helper': ['min', 'max'] 
        })

        res.columns = [f"{c[0]}_{c[1]}" for c in res.columns]
        res = res.rename(columns={
            'ts_helper_min': 'timestamp_min',
            'ts_helper_max': 'timestamp_max'
        })
        res = res.reset_index().dropna(subset=['timestamp_min'])

        engine = target_db.get_engine()
        with engine.begin() as conn:
            context.log.info(f"A processar carga de {len(res)} intervalos para o banco alvo...")
            
            for var in ['wind_speed', 'power']:
                for _, row in res.iterrows():
                    # Formatação de timestamps para o ID e JSON
                    ts_i = pd.to_datetime(row['timestamp_min']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    ts_f = pd.to_datetime(row['timestamp_max']).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    
                    signal_id = f"{ts_i}|{ts_f}|{var}"

                    conn.execute(
                        text("INSERT INTO signal (id, name) VALUES (:id, :name) ON CONFLICT (id) DO NOTHING"),
                        {"id": signal_id, "name": var}
                    )

                    ts_json = json.dumps({"inicio": ts_i, "fim": ts_f})
                    val_json = json.dumps({
                        "media": float(row[f"{var}_mean"]),
                        "minimo": float(row[f"{var}_min"]),
                        "maximo": float(row[f"{var}_max"]),
                        "desvio_padrao": float(row[f"{var}_std"]) if pd.notnull(row[f"{var}_std"]) else 0.0
                    })

                    conn.execute(
                        text("INSERT INTO data (timestamp, signal_id, value) VALUES (:ts, :sid, :val)"),
                        {"ts": ts_json, "sid": signal_id, "val": val_json}
                    )
        
        context.log.info(f"Carga da partição {partition_date} concluída com sucesso.")

    except Exception as e:
        context.log.error(f"Erro ao processar a partição {partition_date}: {str(e)}")
        raise e