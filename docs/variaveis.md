# Variáveis do Pipeline — Bronze → Silver → Gold

Mapeamento das variáveis utilizadas em cada fonte de dados e as chaves de cruzamento entre tabelas.

---

## 1. Variáveis por Fonte

### 1.1 PNAD Contínua — `pnad_limpo.csv`
Alimenta o gap salarial por UF e trimestre (seção 5.1) e a participação feminina por escolaridade (suplemento de 5.3).

| Variável | Tipo | Valores de exemplo | Papel na análise |
|---|---|---|---|
| `tabela_id` | int | `5436`, `7322` | filtra entre tabela de renda (5436) e participação por instrução (7322) |
| `uf_cod` | int | `41`, `42`, `43` | código IBGE da UF (PR, SC, RS) |
| `uf_nome` | str | `Paraná` | nome da UF |
| `periodo` | int | `202001` | trimestre no formato YYYYTT |
| `sexo` | str | `Masculino`, `Feminino` | sexo do respondente |
| `valor` | float | `4529.0` | rendimento médio mensal (R$) — tabela 5436 |

---

### 1.2 CAGED — `caged_limpo.csv`
Alimenta o saldo líquido de empregos por perfil (seção 5.2).

| Variável | Tipo | Valores de exemplo | Papel na análise |
|---|---|---|---|
| `ano` | int | `2020` | ano de referência |
| `mes` | int | `1`–`12` | mês de referência |
| `sigla_uf` | str | `PR`, `SC`, `RS` | UF do estabelecimento |
| `sexo` | str | `Masculino`, `Feminino` | sexo do trabalhador |
| `faixa_etaria` | str | `18-24`, `30-39`, `60+` | faixa etária (derivada de `idade`) |
| `saldo_movimentacao` | int | `+1`, `-1` | **+1** = admissão / **-1** = desligamento |
| `salario_mensal` | float | `1630.24` | salário na admissão (R$) |
| `grau_instrucao` | int | `7` | código de escolaridade CAGED (disponível, não usado nos gráficos atuais) |
| `cnae_2_subclasse` | int | `154700` | subclasse CNAE 2.0 (disponível, não usada nos gráficos atuais) |
| `cbo_2002` | int | `623015` | código de ocupação CBO (disponível, não usado nos gráficos atuais) |

---

### 1.3 RAIS — `rais_limpo.csv`
Alimenta o gap por escolaridade (5.3) e o gap + participação por setor (5.4 / 5.5).

| Variável | Tipo | Valores de exemplo | Papel na análise |
|---|---|---|---|
| `ano` | int | `2020` | ano de referência |
| `sigla_uf` | str | `PR`, `SC`, `RS` | UF do estabelecimento |
| `sexo` | str | `Masculino`, `Feminino` | sexo do trabalhador |
| `grau_instrucao_apos_2005` | int | `1`–`11` | código de escolaridade RAIS (agrupado em `escolaridade`) |
| `cnae_2_subclasse` | int | `161099` | subclasse CNAE 2.0 (mapeada para `setor`) |
| `valor_remuneracao_media` | float | `439.43` | remuneração média mensal do vínculo (R$) |
| `faixa_etaria_calculada` | str | `30-39` | faixa etária (derivada de `idade`; disponível, não usada nos gráficos atuais) |
| `cbo_2002` | int | `623110` | código de ocupação CBO (usado em `OcupacaoAnalyzer`) |

---

## 2. Variáveis Derivadas na Camada Gold

Criadas pelos analyzers em `analise/`; não existem no silver.

| Variável derivada | Fórmula / Origem | Arquivo gold |
|---|---|---|
| `gap_pct` | `(salário_M − salário_F) / salário_F × 100` | todos os arquivos gold |
| `escolaridade` | dict `_ESCOLARIDADE` em `gap_salarial.py:71` — agrupa códigos 1–11 em 8 níveis | `gap_salarial_por_escolaridade.csv` |
| `setor` | função `_cnae_section()` em `gap_salarial.py:97` — mapeia os 2 primeiros dígitos do CNAE para 21 setores | `gap_salarial_por_setor.csv` |
| `share_pct` | `n_vinculos / total_vinculos × 100` | `gap_salarial_por_setor.csv` |
| `admissoes` | contagem de `saldo_movimentacao == 1` | `vagas_saldo_por_uf_perfil.csv` |
| `desligamentos` | contagem de `saldo_movimentacao == -1` | `vagas_saldo_por_uf_perfil.csv` |
| `saldo` | `sum(saldo_movimentacao)` = admissões − desligamentos | `vagas_saldo_por_uf_perfil.csv` |
| `periodo_label` | `f"T{trimestre} {ano}"` — ex: `T1 2020` | calculado no notebook 5.1 |

---

## 3. Cruzamento entre Tabelas

Há apenas um join implementado no pipeline. Todas as demais análises operam sobre uma única fonte.

**RAIS + PNAD 7322** → `gap_salarial_por_escolaridade.csv`

- O gold de escolaridade é calculado inteiramente da RAIS (`rais_limpo.csv`)
- Ao final, `GapSalarialAnalyzer.analyze_by_escolaridade()` faz um `merge` com os dados do PNAD tabela 7322 (`pnad_7322.json`) para acrescentar a coluna `participacao_feminina_pct`
- Chave do join: `escolaridade` (rótulo textual — ex: `"Médio completo"`)

As demais análises não cruzam fontes:

| Gold gerado | Fonte única |
|---|---|
| `gap_salarial_por_uf_sexo_idade.csv` | PNAD tabela 5436 |
| `gap_salarial_por_escolaridade.csv` | RAIS + PNAD 7322 (join acima) |
| `gap_salarial_por_setor.csv` | RAIS |
| `vagas_saldo_por_uf_perfil.csv` | CAGED |
| `distribuicao_ocupacional.csv` | RAIS |
