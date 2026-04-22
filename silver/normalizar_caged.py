"""Silver normalizer — CAGED (per-year CSVs → single clean CSV)."""
from pathlib import Path

import pandas as pd

from .base import BaseNormalizer

_SEXO_MAP = {1: "Masculino", 3: "Feminino"}  # CAGED Novo: 1=M, 3=F (not 2)

_FAIXA_BINS = [0, 17, 24, 29, 39, 49, 59, 120]
_FAIXA_LABELS = ["<18", "18-24", "25-29", "30-39", "40-49", "50-59", "60+"]


class CagedNormalizer(BaseNormalizer):
    """Normalizes caged_{ano}.csv files from Bronze into a single Silver CSV."""

    def normalize(self) -> pd.DataFrame:
        paths = sorted(self.bronze_dir.glob("caged_[0-9]*.csv"))
        if not paths:
            raise FileNotFoundError("Nenhum arquivo caged_*.csv encontrado em bronze/")

        df = pd.concat(
            [pd.read_csv(p, dtype={"cbo_2002": str, "cnae_2_subclasse": str}) for p in paths],
            ignore_index=True,
        )

        df["sexo"] = df["sexo"].map(_SEXO_MAP)

        # Build faixa_etaria from raw idade (§5.4 / §7.6)
        df["faixa_etaria"] = pd.cut(
            df["idade"], bins=_FAIXA_BINS, labels=_FAIXA_LABELS, right=True
        ).astype(str)

        df = df.drop(columns=["idade"])

        out_path = self.silver_dir / "caged_limpo.csv"
        df.to_csv(out_path, index=False, encoding="utf-8")
        print(f"  [caged silver] {len(df):,} linhas → {out_path}")
        return df
