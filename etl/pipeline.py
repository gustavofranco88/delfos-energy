import httpx
import pandas as pd
import json
from datetime import datetime
from sqlalchemy import create_engine, text

API_URL = "http://127.0.0.1:8000/data"
API_KEY = "5fb8b21c-7f3e-4d8a-9b2c-1a2b3c4d5e6f"
DATABASE_URL = "postgresql://user:password@127.0.0.1:5433/db_alvo"

def load_to_db(df: pd.DataFrame):

    engine = create_engine(DATABASE_URL)
    
    print(f"\n[3/3] Iniciando carga no banco de dados...")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("INSERT INTO signal (id, name) VALUES (:id, :name)"),
                {"id": row['id'], "name": row['nome']}
            )

            ts_dict = json.dumps({
                "inicio": row['timestamp_inicio'],
                "fim": row['timestamp_fim']
            })
            
            val_dict = json.dumps({
                "media": row['Media'],
                "minimo": row['Minimo'],
                "maximo": row['Maximo'],
                "desvio_padrao": row['Desvio_Padrao']
            })

            conn.execute(
                text("""
                    INSERT INTO data (timestamp, signal_id, value) 
                    VALUES (:ts, :sid, :val)
                """),
                {"ts": ts_dict, "sid": row['id'], "val": val_dict}
            )

    print("Carga finalizada com sucesso no banco alvo.")

def run_pipeline(data_filtro: str):
    try:
        datetime.strptime(data_filtro, "%Y-%m-%d")
    except ValueError:
        print(f"Erro: Use o formato YYYY-MM-DD.")
        return

    print(f"\n[1/3] Extraindo dados para: {data_filtro}")
    todos_os_registros = []
    headers = {"X-API-Key": API_KEY}

    try:
        with httpx.Client(timeout=60.0) as client:
            pagina = 1
            while True:
                params = {"start": data_filtro, "end": data_filtro, "page": pagina, "size": 1000, "wind_speed": True, "power": True}
                r = client.get(API_URL, headers=headers, params=params)
                if r.status_code != 200: break
                reg = r.json().get("results", [])
                todos_os_registros.extend(reg)
                if len(reg) < 1000: break
                pagina += 1

        if not todos_os_registros:
            print("Sem dados.")
            return

        # --- TRANSFORMAÇÃO ---
        df_raw = pd.DataFrame(todos_os_registros)
        df_raw['ts_dt'] = pd.to_datetime(df_raw['timestamp'])
        df_raw = df_raw.set_index('ts_dt').sort_index()

        res = df_raw.resample('10min').agg({
            'wind_speed': ['mean', 'min', 'max', 'std'],
            'power': ['mean', 'min', 'max', 'std'],
            'timestamp': ['min', 'max'] 
        })
        res.columns = [f"{c[0]}_{c[1]}" for c in res.columns]
        res = res.reset_index().dropna(subset=['timestamp_min'])

        # Normalização Longa
        dfs = []
        for var in ['wind_speed', 'power']:
            temp = res[['timestamp_min', 'timestamp_max', f'{var}_mean', f'{var}_min', f'{var}_max', f'{var}_std']].copy()
            temp['nome'] = var
            temp.columns = ['timestamp_inicio', 'timestamp_fim', 'Media', 'Minimo', 'Maximo', 'Desvio_Padrao', 'nome']
            dfs.append(temp)
        
        df_final = pd.concat(dfs, ignore_index=True)
        df_final['timestamp_inicio'] = pd.to_datetime(df_final['timestamp_inicio']).dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
        df_final['timestamp_fim'] = pd.to_datetime(df_final['timestamp_fim']).dt.strftime('%Y-%m-%d %H:%M:%S.%f').str[:-3]
        df_final['id'] = df_final['timestamp_inicio'] + "|" + df_final['timestamp_fim'] + "|" + df_final['nome']

        # --- CARGA ---
        load_to_db(df_final)

    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    entrada = input("Digite a data de filtro (Ex: 2026-03-21): ")
    run_pipeline(entrada)