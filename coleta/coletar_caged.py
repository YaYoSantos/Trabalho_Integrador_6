"""Bronze collector — Novo CAGED via BigQuery Storage API (parallel by year)."""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from ._bq_client import bq_to_df, bq_warmup_auth
from .base import BaseCollector

_MAX_WORKERS = 4


class CagedCollector(BaseCollector):
    """Downloads CAGED microdados one year per file, years fetched in parallel.

    Output files: caged_{ano}.csv  (one per year, idempotent)
    Column names confirmed in POC (§2.3 / §7.6 ARQUITETURA.md).
    """

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
        if self.ano_teste:
            print(f"  [caged] modo teste: ano={self.ano_teste}")
        if self.limite_teste:
            print(f"  [caged] modo teste: limite={self.limite_teste:,} linhas por ano")

        # Authenticate in main thread before spawning workers
        bq_warmup_auth(self.gcp_project_id)

        workers = min(_MAX_WORKERS, len(anos))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(self._collect_year, ano): ano for ano in anos}
            for future in as_completed(futures):
                ano = futures[future]
                exc = future.exception()
                if exc:
                    print(f"  [caged] {ano} falhou: {exc}")

    def _collect_year(self, ano: int) -> None:
        out_path = self.bronze_dir / f"caged_{ano}.csv"
        if out_path.exists():
            print(f"  [caged] {ano} já existe — pulando")
            return

        ufs = ", ".join(f"'{s}'" for s in self.uf_siglas)
        limit_clause = f"LIMIT {self.limite_teste}" if self.limite_teste else ""

        query = f"""
            SELECT
                ano, mes, sigla_uf,
                sexo,
                idade,
                grau_instrucao,
                cbo_2002,
                cnae_2_subclasse,
                tipo_movimentacao,
                saldo_movimentacao,
                salario_mensal
            FROM `basedosdados.br_me_caged.microdados_movimentacao`
            WHERE ano = {ano}
              AND sigla_uf IN ({ufs})
            {limit_clause}
        """

        print(f"  [caged] coletando {ano} ...")
        df = bq_to_df(query, self.gcp_project_id)

        if df.empty:
            print(f"  [caged] {ano} sem dados")
            return

        df.to_csv(out_path, index=False)
        print(f"  [caged] salvo → {out_path}  ({len(df):,} linhas)")
