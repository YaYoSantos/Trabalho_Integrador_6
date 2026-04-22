"""Silver normalizer — PNAD Contínua (apisidra JSON → clean CSV)."""
import json
import unicodedata
from pathlib import Path

import pandas as pd

from .base import BaseNormalizer

_AUSENTES_IBGE = {"-", "...", "X", "x", "C", ""}

_SEXO_MAP = {"4": "Masculino", "5": "Feminino"}


def _strip_accents(s: str) -> str:
    return unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().lower()


def _build_col_map(header: dict) -> dict[str, str]:
    """Derive rename mapping from SIDRA header row (row 0).

    SIDRA uses MC/MN for unit-of-measure and D1C…DnC for the actual
    dimensions. The dimension order varies per table, so we read the
    descriptions in row 0 to identify UF, period, and sex columns.
    """
    rename: dict[str, str] = {"V": "valor_str"}
    for key, desc in header.items():
        if not key.startswith("D"):
            continue
        d = _strip_accents(desc)
        is_cod = key.endswith("C")
        if "unidade da federacao" in d:
            rename[key] = "uf_cod" if is_cod else "uf_nome"
        elif "grande regiao" in d:
            rename[key] = "regiao_cod" if is_cod else "regiao_nome"
        elif is_cod and ("trimestre" in d or d.startswith("ano ")):
            rename[key] = "periodo"
        elif "sexo" in d:
            rename[key] = "sexo_cod" if is_cod else "sexo_nome"
    return rename


class PnadNormalizer(BaseNormalizer):
    """Normalizes all pnad_*.json files in Bronze into a single Silver CSV."""

    def normalize(self) -> pd.DataFrame:
        frames = []
        for json_path in sorted(self.bronze_dir.glob("pnad_*.json")):
            tabela_id = json_path.stem.replace("pnad_", "")
            df = self._normalize_file(json_path, tabela_id)
            frames.append(df)

        if not frames:
            raise FileNotFoundError("Nenhum arquivo pnad_*.json encontrado em bronze/")

        result = pd.concat(frames, ignore_index=True)
        out_path = self.silver_dir / "pnad_limpo.csv"
        result.to_csv(out_path, index=False, encoding="utf-8")
        print(f"  [pnad silver] {len(result):,} linhas → {out_path}")
        return result

    def _normalize_file(self, path: Path, tabela_id: str) -> pd.DataFrame:
        dados = json.loads(path.read_text(encoding="utf-8"))

        # Row 0 is SIDRA metadata header — use it to derive column semantics, then drop (§5.4)
        col_map = _build_col_map(dados[0])
        df = pd.DataFrame(dados[1:])
        df = df.rename(columns=col_map)

        df["valor"] = pd.to_numeric(
            df["valor_str"].str.replace(",", ".", regex=False),
            errors="coerce",
        ).where(~df["valor_str"].isin(_AUSENTES_IBGE))

        df["sexo"] = df["sexo_cod"].map(_SEXO_MAP) if "sexo_cod" in df.columns else None
        df["tabela_id"] = tabela_id

        keep = [c for c in [
            "tabela_id", "uf_cod", "uf_nome", "regiao_cod", "regiao_nome",
            "periodo", "sexo_cod", "sexo", "valor",
        ] if c in df.columns]
        return df[keep]
