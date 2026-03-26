from fastapi import FastAPI, Depends, Query, HTTPException, Header
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional
from datetime import date, datetime
from api.database import get_db
import traceback

app = FastAPI(title="Delfos Energy Monitoring API")

API_KEY_CREDENTIAL = "5fb8b21c-7f3e-4d8a-9b2c-1a2b3c4d5e6f"

def verify_token(api_key: str = Header(..., alias="X-API-Key")):
    if api_key != API_KEY_CREDENTIAL:
        raise HTTPException(status_code=401, detail="Acesso negado: Token invalido")

@app.get("/data", dependencies=[Depends(verify_token)])
def read_data(
    start: date = Query(..., description="Data inicio (AAAA-MM-DD)"),
    end: date = Query(..., description="Data fim (AAAA-MM-DD)"),
    wind_speed: bool = Query(True, description="Incluir Velocidade do Vento"),
    power: bool = Query(True, description="Incluir Potencia"),
    ambient_temperature: bool = Query(True, description="Incluir Temperatura Ambiente"),
    page: int = Query(1, ge=1, description="Numero da pagina"),
    size: int = Query(1000, ge=1, le=1000, description="Linhas por pagina (Max 1000)"),
    db: Session = Depends(get_db)
):
    try:
        columns_to_select = ["timestamp"]
        
        if wind_speed: columns_to_select.append("wind_speed")
        if power: columns_to_select.append("power")
        if ambient_temperature: columns_to_select.append("ambient_temperature")
        
        cols_sql = ", ".join(columns_to_select)
        skip = (page - 1) * size
        
        query_str = f"""
            SELECT {cols_sql} 
            FROM data 
            WHERE timestamp >= :start AND timestamp <= :end
            ORDER BY timestamp ASC 
            LIMIT :limit OFFSET :offset
        """
        
        params = {
            "start": datetime.combine(start, datetime.min.time()),
            "end": datetime.combine(end, datetime.max.time()),
            "limit": size,
            "offset": skip
        }

        result = db.execute(text(query_str), params)
        rows = [dict(row._mapping) for row in result]

        return {
            "metadata": {
                "page": page, 
                "size": size, 
                "records_in_page": len(rows),
                "columns_returned": columns_to_select
            },
            "results": rows
        }

    except Exception as e:
        traceback.print_exc()
        return {"error": str(e)}