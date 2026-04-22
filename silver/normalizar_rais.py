"""Silver normalizer — RAIS (one CSV per year → single clean CSV)."""
from pathlib import Path

import pandas as pd

from .base import BaseNormalizer

_SEXO_MAP = {1: "Masculino", 2: "Feminino"}

_FAIXA_BINS = [0, 17, 24, 29, 39, 49, 59, 120]
_FAIXA_LABELS = ["<18", "18-24", "25-29", "30-39", "40-49", "50-59", "60+"]


class RaisNormalizer(BaseNormalizer):
    """Normalizes rais_{ano}.csv files from Bronze into a single Silver CSV."""

    def normalize(self) -> pd.DataFrame:
        frames = []
        for csv_path in sorted(self.bronze_dir.glob("rais_*.csv")):
            df = pd.read_csv(csv_path, dtype={"cbo_2002": str, "cnae_2_subclasse": str})
            frames.append(df)

        if not frames:
            raise FileNotFoundError("Nenhum arquivo rais_*.csv encontrado em bronze/")

        df = pd.concat(frames, ignore_index=True)

        df["sexo"] = df["sexo"].map(_SEXO_MAP)

        # Build faixa_etaria from raw idade when available
        if "idade" in df.columns:
            df["faixa_etaria_calculada"] = pd.cut(
                df["idade"], bins=_FAIXA_BINS, labels=_FAIXA_LABELS, right=True
            ).astype(str)

        # vinculo_ativo_3112 already filtered at collection — safe to drop
        df = df.drop(columns=["vinculo_ativo_3112"], errors="ignore")

        out_path = self.silver_dir / "rais_limpo.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"  [rais silver] {len(df):,} linhas → {out_path}")
        return df
