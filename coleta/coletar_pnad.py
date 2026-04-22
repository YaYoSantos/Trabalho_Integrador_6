"""Bronze collector — PNAD Contínua via apisidra.ibge.gov.br."""
import json
from pathlib import Path

from .base import BaseCollector
from ._ibge_http import expand_period_range, fetch_apisidra

# Table definitions matching ARQUITETURA.md §5.1.
# nivel is kept explicit here — metadata-based detection was unreliable
# because the IBGE servicodados API returns descriptive strings, not "N3" codes.
TABELAS_PNAD: dict[str, dict] = {
    "4093": {
        # N3 trimestral — Força de trabalho (ocupados, desocupados) por sexo e UF
        "descricao": "Forca de trabalho por sexo — UF",
        "periodos": "202001-202504",
        "nivel": "n3",
        "uf_codes": True,  # use configured UF codes at this level
        "classificacoes": "/c2/4,5",
    },
    "5436": {
        # N3 trimestral — Rendimento médio real por sexo e UF
        "descricao": "Rendimento medio real por sexo — UF",
        "periodos": "202001-202504",
        "nivel": "n3",
        "uf_codes": True,
        "classificacoes": "/c2/4,5",
    },
    "7322": {
        # N2 anual — Rendimento por sexo e nivel de instrucao — regiao Sul (N2 cod 4)
        # Metadata confirmed: only N1/N2 supported; classification c1568=nivel instrucao
        "descricao": "Rendimento por sexo e nivel de instrucao — regiao Sul",
        "periodos": "2020,2021,2022,2023,2024",
        "nivel": "n2",
        "nivel_cod": "4",  # 4 = Regiao Sul
        "classificacoes": "/c2/4,5/c1568/all",
    },
}


class PnadCollector(BaseCollector):
    """Collects PNAD Contínua tables and saves raw JSON to Bronze."""

    def __init__(self, bronze_dir: Path, uf_codes: str) -> None:
        super().__init__(bronze_dir)
        self.uf_codes = uf_codes

    def collect(self) -> None:
        for tabela, cfg in TABELAS_PNAD.items():
            self._collect_table(tabela, cfg)

    def _collect_table(self, tabela: str, cfg: dict) -> None:
        out_path = self.bronze_dir / f"pnad_{tabela}.json"
        if out_path.exists():
            print(f"  [pnad] tabela {tabela} ja existe — pulando")
            return

        # nivel_cod overrides uf_codes when the table doesn't support N3
        geo_code = cfg.get("nivel_cod") if "nivel_cod" in cfg else self.uf_codes
        periodos = expand_period_range(cfg["periodos"])
        path = (
            f"/values/t/{tabela}"
            f"/{cfg['nivel']}/{geo_code}"
            f"/p/{periodos}"
            f"/v/allxp"
            f"{cfg['classificacoes']}"
        )

        print(f"  [pnad] coletando tabela {tabela} ({cfg['descricao']}) ...")
        print(f"  [pnad] path: {path}")
        dados = fetch_apisidra(path)

        out_path.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"  [pnad] salvo -> {out_path}  ({len(dados) - 1} registros)")
