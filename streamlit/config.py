# streamlit/config.py
from dataclasses import dataclass

@dataclass(frozen=True)
class DBConfig:
    host: str = "postgres"
    port: int = 5432
    database: str = "mimic_dw"
    user: str = "dev"
    password: str = "devpassword"
