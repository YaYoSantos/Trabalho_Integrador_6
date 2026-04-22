"""Merge Gold CSVs into the final consolidated .xlsx (§5.8 ARQUITETURA.md)."""
from pathlib import Path

import pandas as pd


def consolidar(gold_dir: Path) -> None:
    """Write mercado_trabalho_consolidado.xlsx with one sheet per theme."""
    out_path = gold_dir / "mercado_trabalho_consolidado.xlsx"

    sheets: dict[str, Path] = {
        "Gap Salarial UF": gold_dir / "gap_salarial_por_uf_sexo_idade.csv",
        "Gap Escolaridade": gold_dir / "gap_salarial_por_escolaridade.csv",
        "Gap Setor": gold_dir / "gap_salarial_por_setor.csv",
        "Ocupacao": gold_dir / "distribuicao_ocupacional.csv",
        "Vagas Saldo": gold_dir / "vagas_saldo_por_uf_perfil.csv",
    }

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, csv_path in sheets.items():
            if not csv_path.exists():
                print(f"  [gold] {csv_path.name} não encontrado — aba ignorada")
                continue
            df = pd.read_csv(csv_path, encoding="utf-8")
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            print(f"  [gold] aba '{sheet_name}' → {len(df):,} linhas")

    print(f"\n  [gold] consolidado → {out_path}")
