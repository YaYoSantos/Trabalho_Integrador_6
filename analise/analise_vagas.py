"""Gold analyzer — employment balance (saldo) by UF, gender, and age group (CAGED)."""
from pathlib import Path

import pandas as pd

from .base import BaseAnalyzer

_DIMS = ["ano", "mes", "sigla_uf", "sexo", "faixa_etaria"]


class VagasAnalyzer(BaseAnalyzer):
    """Aggregates CAGED Silver to produce monthly employment balance by profile."""

    def analyze(self) -> pd.DataFrame:
        caged_path = self.silver_dir / "caged_limpo.csv"
        if not caged_path.exists():
            raise FileNotFoundError(f"Silver CAGED não encontrado: {caged_path}")

        df = pd.read_csv(caged_path, dtype={"cbo_2002": str, "cnae_2_subclasse": str}, encoding="utf-8")

        agg = (
            df.groupby(_DIMS)
            .agg(
                admissoes=("saldo_movimentacao", lambda s: (s == 1).sum()),
                desligamentos=("saldo_movimentacao", lambda s: (s == -1).sum()),
                saldo=("saldo_movimentacao", "sum"),
            )
            .reset_index()
        )

        # Compute admission salary separately — avoids referencing outer df inside agg lambda
        salario = (
            df[df["saldo_movimentacao"] == 1]
            .groupby(_DIMS)["salario_mensal"]
            .mean()
            .rename("salario_medio_admissao")
            .reset_index()
        )
        agg = agg.merge(salario, on=_DIMS, how="left")

        out_path = self.gold_dir / "vagas_saldo_por_uf_perfil.csv"
        agg.to_csv(out_path, index=False, encoding="utf-8")
        print(f"  [vagas] {len(agg):,} linhas → {out_path}")
        return agg
