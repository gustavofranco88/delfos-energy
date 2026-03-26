from sqlalchemy import Column, String, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.orm import relationship

Base = declarative_base()

class Signal(Base):
    __tablename__ = 'signal'
    id = Column(String, primary_key=True)
    name = Column(String(100), nullable=False)
    
    data = relationship('Data', back_populates='signal')

class Data(Base):
    __tablename__ = 'data'
    signal_id = Column(String, ForeignKey('signal.id'), primary_key=True)
    timestamp = Column(JSON, nullable=False)
    value = Column(JSON, nullable=False)
    
    signal = relationship('Signal', back_populates='data')