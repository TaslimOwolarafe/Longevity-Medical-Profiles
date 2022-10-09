from email.policy import default
import json
import os
from datetime import datetime

from sqlalchemy import (create_engine, inspect, 
        Column, Integer, String, Boolean, DateTime)
import pandas as pd

from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class Patient(Base):
    __tablename__ = 'patients'
    id = Column(Integer(), primary_key=True)
    name = Column(String(20), nullable=True)
    age = Column(Integer(), nullable=False)
    gender = Column(String(10), nullable=False)
    weight = Column(Integer(), nullable=False)
    height = Column(Integer(), nullable=False)
    BMI = Column(Integer(), default=None)
    waist = Column(Integer(), nullable=False)
    systolic_bp = Column(Integer(), nullable=False)
    diastolic_bp = Column(Integer(), nullable=False)
    total_cholesterol = Column(Integer(), nullable=False)
    PVD = Column(Boolean(), nullable=False)
    alcohol_consumption = Column(Boolean(), nullable=False)
    hypertension = Column(Boolean(), nullable=False)
    diabetes = Column(Boolean(), nullable=False)
    hepatitis = Column(Boolean(), nullable=False)
    family_hepatitis = Column(Boolean(), nullable=False)
    chronic_fatigue = Column(Boolean(), nullable=False)
    date_created = Column(DateTime(), default=datetime.utcnow)

    @property
    def BMI(self):
        self.BMI = self.weight/(self.height**2)
        return self.weight/(self.height**2)

    def __repr__(self) -> str:
        return f"<User {self.name}, {self.age}, {self.gender}>"
    
patient1 = ()
if os.path.exists('db_config.json'):
    with open('db_config.json') as f:
        config = json.load(f)
else:
    print("No config file found. Please provide correct config file.")
    quit()

tables_info = config['tables']

conn = create_engine(config['conn'])
Session = sessionmaker()
local_session = Session(bind=conn)

patient1 = Patient(name="Mark", age=18, gender='male', weight=45, height=125, waist=36, systolic_bp=True, 
    diastolic_bp=True, total_cholesterol=170, PVD=False, alcohol_consumption=True, 
    hypertension=False, diabetes=False, hepatitis=True, family_hepatitis=False, chronic_fatigue=False)

Base.metadata.create_all(conn)
local_session.add(patient1)
local_session.commit()

insp = inspect(conn)

tables = insp.get_table_names()

os.makedirs('out', exist_ok=True)

for table in tables:
    select_cols = list(map(lambda x: x['name'],  insp.get_columns(table)))
    if table in tables_info:
        drop_cols = tables_info[table]
        select_cols = list(filter(lambda x: x in drop_cols, select_cols))
    if len(select_cols) > 0:
        df = pd.read_sql(f'SELECT {",".join(select_cols)} FROM {table}', conn)
        df.to_csv(f'out/{table}.csv', index=None)