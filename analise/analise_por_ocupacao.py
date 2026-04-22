"""Gold analyzer — occupational distribution and wage by gender (RAIS)."""
from pathlib import Path

import pandas as pd

from .base import BaseAnalyzer


class OcupacaoAnalyzer(BaseAnalyzer):
    """Computes female share and mean wage by CBO occupation from RAIS Silver."""

    def analyze(self) -> pd.DataFrame:
        rais_path = self.silver_dir / "rais_limpo.csv"
        if not rais_path.exists():
            raise FileNotFoundError(f"Silver RAIS não encontrado: {rais_path}")

        df = pd.read_csv(rais_path, dtype={"cbo_2002": str}, encoding="utf-8")
        df = df.dropna(subset=["cbo_2002", "valor_remuneracao_media", "sexo"])

        # Group by CBO + year + UF + gender
        agg = (
            df.groupby(["ano", "sigla_uf", "cbo_2002", "sexo"])
            .agg(
                n_vinculos=("valor_remuneracao_media", "count"),
                salario_medio=("valor_remuneracao_media", "mean"),
            )
            .reset_index()
        )

        # Female share per CBO
        total_cbo = (
            agg.groupby(["ano", "sigla_uf", "cbo_2002"])["n_vinculos"]
            .sum()
            .rename("total_vinculos")
        )
        agg = agg.join(total_cbo, on=["ano", "sigla_uf", "cbo_2002"])
        agg["share_pct"] = (agg["n_vinculos"] / agg["total_vinculos"] * 100).round(2)

        out_path = self.gold_dir / "distribuicao_ocupacional.csv"
        agg.to_csv(out_path, index=False, encoding="utf-8")
        print(f"  [ocupacao] {len(agg):,} linhas → {out_path}")
        return agg
