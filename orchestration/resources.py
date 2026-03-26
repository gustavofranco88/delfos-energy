import sys
import os
from pathlib import Path

root_path = str(Path(__file__).parent.parent)
if root_path not in sys.path:
    sys.path.append(root_path)

from dagster import ConfigurableResource
from sqlalchemy import create_engine, text
from etl.models import Base  
import pandas as pd 

class PostgresResource(ConfigurableResource):
    connection_string: str

    def get_engine(self):
        return create_engine(self.connection_string)

    def query_as_df(self, sql_query: str, params: dict = None) -> pd.DataFrame:
        engine = self.get_engine()
        with engine.connect() as conn:
            return pd.read_sql(text(sql_query), conn, params=params)

class PostgresAlvoResource(ConfigurableResource):
    connection_string: str

    def get_engine(self):
        return create_engine(self.connection_string)

    def init_db_structure(self):
        """Cria as tabelas no banco alvo usando a definição do models.py"""
        engine = self.get_engine()
        print("Verificando/Criando estrutura no banco alvo...")
        Base.metadata.create_all(engine)
        return "Estrutura do Banco Alvo pronta."

    def execute_query(self, query: str, params: dict = None):
        engine = self.get_engine()
        with engine.begin() as conn:
            conn.execute(text(query), params)

source_db_resource = PostgresResource(
    connection_string="postgresql://user:password@localhost:5434/db_fonte"
)

target_db_resource = PostgresAlvoResource(
    connection_string="postgresql://user:password@localhost:5433/db_alvo"
)