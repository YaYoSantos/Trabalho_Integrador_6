"""Gold analyzer — gender wage gap.

Sources:
  PNAD 5436 → gap by UF and period (trimestral)
  PNAD 7322 → female employment share by education level (South region)
  RAIS       → gap by education level and by economic sector
"""
import json

import pandas as pd

from .base import BaseAnalyzer

_TABELA_RENDIMENTO = "5436"

_AUSENTES_IBGE: frozenset[str] = frozenset({"-", "...", "X", "x", "C", ""})
_SEXO_SIDRA = {"4": "Masculino", "5": "Feminino"}

# SIDRA c1568 codes → canonical labels (table 7322)
_INSTRUCAO_7322: dict[str, str] = {
    "18837":  "Analfabeto",
    "120712": "Fund. incompleto",
    "120713": "Fund. completo",
    "120714": "Médio incompleto",
    "120715": "Médio completo",
    "99712":  "Superior incompleto",
    "99713":  "Superior completo",
    # 7322 has no Pós-graduação breakdown
}


def _pnad_participacao_por_instrucao(bronze_dir) -> pd.DataFrame:
    """Read pnad_7322 bronze JSON → female employment share by education level.

    Variable 140 = count of people 10+ years old (Mil pessoas).
    Sums across all available years so the share reflects the full period.
    Returns columns: escolaridade, participacao_feminina_pct.
    Empty DataFrame when the file is absent.
    """
    path = bronze_dir / "pnad_7322.json"
    if not path.exists():
        return pd.DataFrame()

    data = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(data[1:])  # row 0 is the header dict

    df = df[df["D3C"] == "140"]              # variable 140 = headcount
    df = df[df["D5C"] != "120704"]           # drop Total instrucao row
    df = df[~df["V"].str.strip().isin(_AUSENTES_IBGE)]

    df["valor"] = pd.to_numeric(df["V"], errors="coerce")
    df["sexo"] = df["D4C"].map(_SEXO_SIDRA)
    df["escolaridade"] = df["D5C"].map(_INSTRUCAO_7322)
    df = df.dropna(subset=["valor", "sexo", "escolaridade"])
    df = df[df["sexo"].isin(["Masculino", "Feminino"])]

    # Sum across all years then compute female share
    by_esc_sex = df.groupby(["escolaridade", "sexo"])["valor"].sum().reset_index()
    total = by_esc_sex.groupby("escolaridade")["valor"].sum().rename("total")
    by_esc_sex = by_esc_sex.join(total, on="escolaridade")
    by_esc_sex["participacao_pct"] = (by_esc_sex["valor"] / by_esc_sex["total"] * 100).round(2)

    return (
        by_esc_sex[by_esc_sex["sexo"] == "Feminino"]
        [["escolaridade", "participacao_pct"]]
        .rename(columns={"participacao_pct": "participacao_feminina_pct"})
        .reset_index(drop=True)
    )

# RAIS grau_instrucao_apos_2005 codes → grouped labels (ordered low→high)
_ESCOLARIDADE: dict[int, str] = {
    1: "Analfabeto",
    2: "Fund. incompleto",
    3: "Fund. incompleto",
    4: "Fund. incompleto",
    5: "Fund. completo",
    6: "Médio incompleto",
    7: "Médio completo",
    8: "Superior incompleto",
    9: "Superior completo",
    10: "Pós-graduação",
    11: "Pós-graduação",
}

ESCOLARIDADE_ORDEM = [
    "Analfabeto",
    "Fund. incompleto",
    "Fund. completo",
    "Médio incompleto",
    "Médio completo",
    "Superior incompleto",
    "Superior completo",
    "Pós-graduação",
]


def _cnae_section(code: str) -> str:
    """Map CNAE 2.0 subclasse code (7-digit str) to section name."""
    try:
        div = int(str(code)[:2])
    except (ValueError, TypeError):
        return "Desconhecido"
    if div <= 3:  return "Agropecuária"
    if div <= 9:  return "Ind. extrativa"
    if div <= 33: return "Ind. transformação"
    if div == 35: return "Eletricidade e gás"
    if div <= 39: return "Água e saneamento"
    if div <= 43: return "Construção"
    if div <= 47: return "Comércio"
    if div <= 53: return "Transporte"
    if div <= 56: return "Alojamento e alimentação"
    if div <= 63: return "TIC"
    if div <= 66: return "Financeiro"
    if div == 68: return "Imobiliário"
    if div <= 75: return "Prof. e técnico"
    if div <= 82: return "Administrativo"
    if div == 84: return "Adm. pública"
    if div == 85: return "Educação"
    if div <= 88: return "Saúde"
    if div <= 93: return "Artes e cultura"
    if div <= 96: return "Outros serviços"
    if div == 97: return "Serv. domésticos"
    return "Outros"


def _wage_gap_pivot(df: pd.DataFrame, index: list[str], wage_col: str) -> pd.DataFrame:
    """Pivot M/F wages and compute gap_pct. Returns empty DF if either sex missing."""
    pivot = (
        df.pivot_table(index=index, columns="sexo", values=wage_col, aggfunc="mean")
        .reset_index()
    )
    pivot.columns.name = None
    if "Masculino" not in pivot.columns or "Feminino" not in pivot.columns:
        return pd.DataFrame()
    pivot["gap_pct"] = (
        (pivot["Masculino"] - pivot["Feminino"]) / pivot["Feminino"] * 100
    ).round(2)
    return pivot.rename(columns={"Masculino": "salario_masculino", "Feminino": "salario_feminino"})


class GapSalarialAnalyzer(BaseAnalyzer):
    """Computes gender wage gap from PNAD (UF level) and RAIS (education + sector)."""

    # ── 4.1a — PNAD: gap by UF and trimester ─────────────────────────────────

    def analyze(self) -> pd.DataFrame:
        pnad_path = self.silver_dir / "pnad_limpo.csv"
        if not pnad_path.exists():
            raise FileNotFoundError(f"Silver PNAD não encontrado: {pnad_path}")

        df = pd.read_csv(pnad_path, dtype={"tabela_id": str}, encoding="utf-8")
        df = df[df["tabela_id"] == _TABELA_RENDIMENTO].dropna(subset=["valor"])
        df = df.rename(columns={"valor": "valor_remuneracao_media"})

        result = _wage_gap_pivot(df, ["uf_cod", "uf_nome", "periodo"], "valor_remuneracao_media")
        if result.empty:
            raise ValueError(
                "Colunas Masculino/Feminino ausentes — "
                "verificar se tabela 5436 foi coletada e silver regenerado."
            )

        out = self.gold_dir / "gap_salarial_por_uf_sexo_idade.csv"
        result.to_csv(out, index=False, encoding="utf-8")
        print(f"  [gap-uf] {len(result):,} linhas → {out}")
        return result

    # ── 4.1b — RAIS: gap by education level ──────────────────────────────────

    def analyze_by_escolaridade(self) -> pd.DataFrame:
        rais_path = self.silver_dir / "rais_limpo.csv"
        if not rais_path.exists():
            raise FileNotFoundError(f"Silver RAIS não encontrado: {rais_path}")

        df = pd.read_csv(rais_path, encoding="utf-8")
        df = df.dropna(subset=["grau_instrucao_apos_2005", "valor_remuneracao_media", "sexo"])
        df = df[df["sexo"].isin(["Masculino", "Feminino"])]

        df["escolaridade"] = (
            df["grau_instrucao_apos_2005"].astype(int).map(_ESCOLARIDADE).fillna("Desconhecido")
        )

        result = _wage_gap_pivot(df, ["sigla_uf", "ano", "escolaridade"], "valor_remuneracao_media")
        if result.empty:
            print("  [gap-escolaridade] dados insuficientes")
            return result

        result["escolaridade"] = pd.Categorical(
            result["escolaridade"], categories=ESCOLARIDADE_ORDEM, ordered=True
        )
        result = result.sort_values(["sigla_uf", "ano", "escolaridade"])

        # Supplement with PNAD 7322 female participation share (South region aggregate)
        bronze_dir = self.silver_dir.parent / "bronze"
        participacao = _pnad_participacao_por_instrucao(bronze_dir)
        if not participacao.empty:
            result = result.merge(participacao, on="escolaridade", how="left")
            print(f"  [gap-escolaridade] PNAD 7322 suplementado: participacao_feminina_pct adicionada")

        out = self.gold_dir / "gap_salarial_por_escolaridade.csv"
        result.to_csv(out, index=False, encoding="utf-8")
        print(f"  [gap-escolaridade] {len(result):,} linhas → {out}")
        return result

    # ── 4.1c — RAIS: gap + employment share by CNAE sector ───────────────────

    def analyze_by_setor(self) -> pd.DataFrame:
        rais_path = self.silver_dir / "rais_limpo.csv"
        if not rais_path.exists():
            raise FileNotFoundError(f"Silver RAIS não encontrado: {rais_path}")

        df = pd.read_csv(rais_path, dtype={"cnae_2_subclasse": str}, encoding="utf-8")
        df = df.dropna(subset=["cnae_2_subclasse", "valor_remuneracao_media", "sexo"])
        df = df[df["sexo"].isin(["Masculino", "Feminino"])]

        df["setor"] = df["cnae_2_subclasse"].apply(_cnae_section)

        # Employment count + mean wage by sector and sex
        emprego = (
            df.groupby(["sigla_uf", "ano", "setor", "sexo"])
            .agg(
                n_vinculos=("valor_remuneracao_media", "count"),
                salario_medio=("valor_remuneracao_media", "mean"),
            )
            .reset_index()
        )

        # Female/male share within each sector
        total = (
            emprego.groupby(["sigla_uf", "ano", "setor"])["n_vinculos"]
            .sum()
            .rename("total_vinculos")
        )
        emprego = emprego.join(total, on=["sigla_uf", "ano", "setor"])
        emprego["share_pct"] = (emprego["n_vinculos"] / emprego["total_vinculos"] * 100).round(2)

        # Wage gap per sector (merged in)
        gap = _wage_gap_pivot(df, ["sigla_uf", "ano", "setor"], "valor_remuneracao_media")
        if not gap.empty:
            emprego = emprego.merge(
                gap[["sigla_uf", "ano", "setor", "gap_pct"]],
                on=["sigla_uf", "ano", "setor"],
                how="left",
            )

        out = self.gold_dir / "gap_salarial_por_setor.csv"
        emprego.to_csv(out, index=False, encoding="utf-8")
        print(f"  [gap-setor] {len(emprego):,} linhas → {out}")
        return emprego
