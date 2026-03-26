from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
import os

DATABASE_URL = "postgresql://user:password@localhost:5433/db_alvo"
engine = create_engine(DATABASE_URL)

def init_db():
    
    Base.metadata.create_all(engine)
    

if __name__ == "__main__":
    init_db()