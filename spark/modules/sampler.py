from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class DataSampler:
    """
    Handles data sampling strategies for High-Velocity Healthcare Data.
    Designed for MIMIC-IV to ensure critical events are not discarded 
    during load shedding.
    """
        
    def __init__(self):
        pass

    def uniform_sample(self, df: DataFrame, fraction: float = 0.1) -> DataFrame:
        return df.filter(F.rand() < fraction)

    def conditional_sample(self, df: DataFrame, condition_col: str, 
                           pass_rate: float = 0.1) -> DataFrame:
        return df.filter(
            (F.col(condition_col) == True) | (F.rand() < pass_rate)
        )

    def hash_sample(self, df: DataFrame, id_col: str, modulus: int = 10) -> DataFrame:
        return df.filter(F.abs(F.hash(F.col(id_col))) % modulus == 0)