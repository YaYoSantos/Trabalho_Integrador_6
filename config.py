"""Centralized configuration — all values sourced from .env."""
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    gcp_project_id: str
    pnad_uf_codes: str
    pnad_uf_siglas: list[str]
    ano_inicio: int
    ano_fim: int
    caged_ano_teste: int | None   # when set, CAGED fetches only this year
    rais_ano_teste: int | None    # when set, RAIS fetches only this year
    bq_limite_teste: int | None   # when set, adds LIMIT N to all BigQuery queries
    dados_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path

    @classmethod
    def from_env(cls) -> "Config":
        gcp = os.environ.get("GCP_PROJECT_ID")
        if not gcp:
            raise EnvironmentError("GCP_PROJECT_ID nao definido no .env")

        raw_caged = os.getenv("CAGED_ANO_TESTE")
        raw_rais = os.getenv("RAIS_ANO_TESTE")
        raw_limite = os.getenv("BQ_LIMITE_TESTE")
        dados_dir = Path(os.getenv("DADOS_DIR", "dados"))
        return cls(
            gcp_project_id=gcp,
            pnad_uf_codes=os.getenv("PNAD_UF_CODES", "41,42,43"),
            pnad_uf_siglas=os.getenv("PNAD_UF_SIGLAS", "PR,SC,RS").split(","),
            ano_inicio=int(os.getenv("ANO_INICIO", "2020")),
            ano_fim=int(os.getenv("ANO_FIM", "2025")),
            caged_ano_teste=int(raw_caged) if raw_caged else None,
            rais_ano_teste=int(raw_rais) if raw_rais else None,
            bq_limite_teste=int(raw_limite) if raw_limite else None,
            dados_dir=dados_dir,
            bronze_dir=dados_dir / "bronze",
            silver_dir=dados_dir / "silver",
            gold_dir=dados_dir / "gold",
        )


config = Config.from_env()
