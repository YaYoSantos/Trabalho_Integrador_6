"""Bronze collector — Censo Demográfico 2022 via apisidra.ibge.gov.br."""
import json
from pathlib import Path

from .base import BaseCollector
from ._ibge_http import fetch_apisidra, fetch_metadata

TABELAS_CENSO: dict[str, dict] = {
    "10264": {"descricao": "Ocupação (CBO 2002) por sexo e UF"},
    "10266": {"descricao": "Atividade econômica (CNAE) por sexo"},
    "10268": {"descricao": "Rendimento médio por UF e sexo"},
}

PERIODO_CENSO = "2022"


class CensoCollector(BaseCollector):
    """Collects Censo 2022 tables from apisidra and saves raw JSON to Bronze."""

    def __init__(self, bronze_dir: Path, uf_codes: str) -> None:
        super().__init__(bronze_dir)
        self.uf_codes = uf_codes

    def collect(self) -> None:
        for tabela, cfg in TABELAS_CENSO.items():
            self._collect_table(tabela, cfg)

    def _collect_table(self, tabela: str, cfg: dict) -> None:
        out_path = self.bronze_dir / f"censo_{tabela}.json"
        if out_path.exists():
            print(f"  [censo] tabela {tabela} já existe — pulando")
            return

        meta = fetch_metadata(tabela)
        nivel = self._resolve_nivel(meta)
        # Build classifications from metadata (§7.7) — avoids hardcoding codes
        # that may not exist in the table and cause HTTP 400.
        classificacoes = self._build_classificacoes(meta)

        path = (
            f"/values/t/{tabela}"
            f"/{nivel}/{self.uf_codes}"
            f"/p/{PERIODO_CENSO}"
            f"/v/allxp"
            f"{classificacoes}"
        )

        print(f"  [censo] coletando tabela {tabela} ({cfg['descricao']}) …")
        dados = fetch_apisidra(path)

        out_path.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  [censo] salvo → {out_path}  ({len(dados) - 1} registros)")

    @staticmethod
    def _resolve_nivel(meta: dict) -> str:
        niveis = meta.get("nivelTerritorial", {}).get("Administrativo", [])
        for nivel in niveis:
            if nivel == "N3":
                return "n3"
        return "n1"

    @staticmethod
    def _build_classificacoes(meta: dict) -> str:
        """Derive classification path from table metadata.

        C2 (Sexo) is filtered to Homens(4) + Mulheres(5) only.
        All other classifications use /all so we get full breakdowns.
        """
        parts = []
        for c in meta.get("classificacoes", []):
            cid = c["id"]
            if cid == 2:
                parts.append("c2/4,5")
            else:
                parts.append(f"c{cid}/all")
        return ("/" + "/".join(parts)) if parts else ""
