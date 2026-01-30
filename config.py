from __future__ import annotations
from dataclasses import dataclass
import os

import dotenv


@dataclass
class DBConnectionConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @staticmethod
    def load(prefix: str) -> DBConnectionConfig:
        dotenv.load_dotenv()

        def get_var(name: str) -> str:
            actual_name = prefix + name
            value = os.getenv(actual_name)
            if not value:
                raise ValueError(f"Parameter {actual_name} is not set")
            return value

        cfg = DBConnectionConfig(
            host=get_var("DB_HOST"),
            port=int(get_var("DB_PORT")),
            user=get_var("DB_USER"),
            password=get_var("DB_PASSWORD"),
            database=get_var("DB_NAME"),
        )
        return cfg

    def connection_str(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class DataFetcherConfig:
    logdb_cfg: DBConnectionConfig
    evpdb_cfg: DBConnectionConfig

    @staticmethod
    def load() -> DataFetcherConfig:
        dotenv.load_dotenv()

        cfg = DataFetcherConfig(
            logdb_cfg=DBConnectionConfig.load("LOG_"),
            evpdb_cfg=DBConnectionConfig.load("EVP_"),
        )

        return cfg
