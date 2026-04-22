"""Bronze collector — RAIS via BigQuery Storage API (parallel by year)."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from ._bq_client import bq_to_df, bq_warmup_auth
from .base import BaseCollector

_MAX_WORKERS = 4


class RaisCollector(BaseCollector):
    """Downloads RAIS microdados one year per file, years fetched in parallel."""

    def __init__(self, bronze_dir: Path, gcp_project_id: str,
                 uf_siglas: list[str], ano_inicio: int, ano_fim: int,
                 ano_teste: int | None = None,
                 limite_teste: int | None = None) -> None:
        super().__init__(bronze_dir)
        self.gcp_project_id = gcp_project_id
        self.uf_siglas = uf_siglas
        self.ano_inicio = ano_inicio
        self.ano_fim = ano_fim
        self.ano_teste = ano_teste
        self.limite_teste = limite_teste

    def collect(self) -> None:
        anos = [self.ano_teste] if self.ano_teste else list(range(self.ano_inicio, self.ano_fim + 1))
        if not self.ano_teste:
            # RAIS publication lag is ~18 months; years after 2022 are likely absent in BDdB
            print(f"  [rais] coletando {self.ano_inicio}–{self.ano_fim} (anos sem dados serão pulados)")

        # Authenticate in main thread before spawning workers
        bq_warmup_auth(self.gcp_project_id)

        workers = min(_MAX_WORKERS, len(anos))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self._collect_year, ano): ano for ano in anos}
            for future in as_completed(futures):
                ano = futures[future]
                exc = future.exception()
                if exc:
                    print(f"  [rais] {ano} falhou: {exc}")

    def _collect_year(self, ano: int) -> None:
        out_path = self.bronze_dir / f"rais_{ano}.csv"
        if out_path.exists():
            print(f"  [rais] {ano} já existe — pulando")
            return

        ufs = ", ".join(f"'{s}'" for s in self.uf_siglas)
        limit_clause = f"LIMIT {self.limite_teste}" if self.limite_teste else ""

        # faixa_etaria omitted — silver recomputes it from idade with consistent bins
        # tipo_vinculo and valor_remuneracao_dezembro not used by any analyzer
        # vinculo_ativo_3112 is STRING '1', not integer 1 (§7.5)
        query = f"""
            SELECT
                ano, sigla_uf,
                sexo, idade,
                cbo_2002, cnae_2_subclasse,
                valor_remuneracao_media,
                grau_instrucao_apos_2005
            FROM `basedosdados.br_me_rais.microdados_vinculos`
            WHERE ano = {ano}
              AND sigla_uf IN ({ufs})
              AND vinculo_ativo_3112 = '1'
            {limit_clause}
        """

        print(f"  [rais] coletando {ano} ...")
        df = bq_to_df(query, self.gcp_project_id)

        if df.empty:
            print(f"  [rais] {ano} sem dados — ano ainda não disponível no BDdB")
            return

        df.to_csv(out_path, index=False)
        print(f"  [rais] salvo → {out_path}  ({len(df):,} linhas)")
