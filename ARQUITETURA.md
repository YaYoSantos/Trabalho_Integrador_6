# Arquitetura do Projeto — Análise de Mercado de Trabalho Brasileiro
## Desigualdade Salarial de Gênero por Estado, Ocupação e Faixa Etária

**Versão:** 2.0  
**Data inicial:** 2026-04-06 · **Última atualização:** 2026-04-07  
**Status:** ✅ Fase 1 concluída — Iniciando Fase 2 (Coleta Bronze)

---

## 1. Visão Geral do Projeto

### 1.1 Objetivo

Construir um pipeline de coleta, processamento e análise de dados do mercado de trabalho brasileiro com foco nas seguintes variáveis:

| Variável | Descrição |
|---|---|
| **Estado (UF)** | Desagregação geográfica por Unidade da Federação |
| **Gênero** | Sexo do trabalhador (masculino / feminino) |
| **Rendimento médio** | Salário médio mensal real por grupo |
| **Ocupação** | Grupamento ocupacional ou CBO |
| **Faixa etária** | Grupos de idade do trabalhador |
| **Vagas / saldo de emprego** | Admissões, desligamentos e saldo líquido de postos formais |

### 1.2 Produto Final

- Dataset consolidado e limpo, pronto para análise estatística
- Scripts reproduzíveis para recoleta futura
- Base para análise de gap salarial de gênero (similar ao artigo Dums & Campos, 2025)
- Exportação em `.xlsx` e `.csv` para uso em visualizações ou modelos

### 1.3 Período de Análise

**Alvo:** 2020 – 2025

Disponibilidade confirmada por fonte após execução das POCs de cobertura:

| Fonte | Período confirmado | Observação |
|---|---|---|
| PNAD Contínua | 2020 – 2025 | Trimestral — POC de cobertura aprovada |
| CAGED | 2020 – 2025 | 68 competências mensais confirmadas |
| RAIS | 2020 – 2022/2023 | Defasagem 1–2 anos — complementar com CAGED |

---

## 2. Fontes de Dados

### 2.1 Mapa de variáveis × fontes

```
Variável              PNAD Contínua   RAIS        CAGED          Censo 2022
─────────────────────────────────────────────────────────────────────────────
Estado (UF)                ✓            ✓           ✓               ✓
Gênero                     ✓            ✓           ✓               ✓
Rendimento médio           ✓            ✓           ✓ (admissão)    ✓
Ocupação (grupo)           ✓            ✓ (CBO)     ✓               ✓
Faixa etária               ✓            ✓           ✓ (idade bruta) ✓
Vagas / saldo              ✗            ✗           ✓ (único)       ✗
─────────────────────────────────────────────────────────────────────────────
Cobertura                formal+inf.   só formal    só formal       censo
Periodicidade             trimestral   anual        mensal          pontual
Série histórica           2012–atual   2007–atual   2020–atual      2022
POC validada              ✅            ✅            ✅               ✅
```

---

### 2.2 Fonte 1 — PNAD Contínua (IBGE)

**Status:** ✅ POC validada · Cobertura 2020–2025 confirmada

**O que é:**  
Pesquisa Nacional por Amostra de Domicílios Contínua. Principal pesquisa de mercado de trabalho do Brasil, cobrindo formal e informal. Periodicidade **trimestral**.

**Host de dados:** `apisidra.ibge.gov.br`  
**Host de metadados:** `servicodados.ibge.gov.br`

> ⚠️ **Não confundir os dois hosts.** `servicodados` serve apenas metadados (variáveis, classificações, níveis territoriais). Os dados reais vêm exclusivamente de `apisidra`.

**Sintaxe confirmada no POC v3:**
```
https://apisidra.ibge.gov.br/values
  /t/{tabela}
  /n3/41,42,43      ← UFs separadas por VÍRGULA, sem colchetes
  /p/{periodo}      ← formato AAAASS para tabelas trimestrais (ex: 202001)
  /v/allxp
  /c2/4,5           ← classificação Sexo: 4=Homens, 5=Mulheres
```

**⚠️ Formato de período — correção crítica descoberta nas POCs:**

A tabela 4093 (e outras da PNAD Contínua) é **trimestral**. O período usa o código de 6 dígitos `AAAASS` (ano + sequência do trimestre).

```
❌ /p/2020          → HTTP 400 — período inválido para tabela trimestral
✅ /p/202001        → 1º trimestre de 2020
✅ /p/202001,202101 → Q1/2020 e Q1/2021
✅ /p/last          → último período disponível
✅ /p/last 12       → últimos 12 trimestres
```

**Passo obrigatório antes de qualquer coleta:** inspecionar metadados da tabela para confirmar nível territorial, período e classificações suportadas:
```python
# GET via http.client — host: servicodados.ibge.gov.br
path = f"/api/v3/agregados/{tabela}/metadados"
# Lê: nivelTerritorial.Administrativo → define se usa n1, n2 ou n3
# Lê: periodicidade.frequencia → define formato do período
```

**Tabelas alvo:**

| Tabela | Conteúdo | Periodicidade | Classificações |
|---|---|---|---|
| **4093** | Força de trabalho, ocupados, desocupados, informalidade por sexo | Trimestral | C2 (Sexo) |
| **5436** | Rendimento médio real por sexo | Trimestral | C2 (Sexo) |
| **7322** | Rendimento por UF, sexo e faixa etária | Anual | C2 · C58 |
| **7323** | Rendimento por UF e grupamento ocupacional | Anual | C1 |
| **7463** | Ocupados por grupamento e sexo | Trimestral | C2 · C1 |

**Códigos de UF:**
```
41=Paraná | 42=Santa Catarina | 43=Rio Grande do Sul
```

---

### 2.3 Fonte 2 — CAGED / Novo CAGED (Ministério do Trabalho)

**Status:** ✅ POC validada · Cobertura 2020–2025 confirmada (68 competências)

**O que é:**  
Registro mensal de admissões e desligamentos CLT. **Única fonte oficial com saldo de vagas por UF, sexo e faixa etária.**

**Acesso adotado:** Base dos Dados via BigQuery  
`basedosdados.br_me_caged.microdados_movimentacao`

> A API PDET (`api.pdet.mte.gov.br`) foi descartada. A Base dos Dados entrega os dados já tratados e tipados, eliminando o trabalho de parsear CSVs brutos do MTE.

**Schema real confirmado no POC (25 colunas):**

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | INT | Ano de referência |
| `mes` | INT | Mês (1–12) |
| `sigla_uf` | STRING | UF (ex: 'PR') |
| `id_municipio` | STRING | Código IBGE |
| `sexo` | INT | **1=Masculino · 2=Feminino** |
| `idade` | INT | Idade em anos (**coluna correta para faixa etária**) |
| `grau_instrucao` | INT | Escolaridade (código) |
| `cbo_2002` | STRING | Ocupação CBO |
| `cnae_2_secao` | STRING | Seção CNAE |
| `cnae_2_subclasse` | STRING | Subclasse CNAE |
| `categoria` | INT | Categoria do trabalhador |
| `tipo_movimentacao` | INT | Tipo de movimentação (**coluna correta**) |
| `saldo_movimentacao` | INT | **1=Admissão · -1=Desligamento** |
| `salario_mensal` | FLOAT | Salário (R$) |
| `horas_contratuais` | FLOAT | Horas semanais |
| `raca_cor` | INT | Raça/cor |
| `origem_informacao` | INT | Origem do registro |
| `indicador_aprendiz` | INT | Flag (0/1) |
| `indicador_trabalho_parcial` | INT | Flag (0/1) |
| `indicador_trabalho_intermitente` | INT | Flag (0/1) |
| `indicador_fora_prazo` | INT | Flag (0/1) |
| `tipo_empregador` | INT | Tipo de empregador |
| `tipo_estabelecimento` | INT | Tipo de estabelecimento |
| `tamanho_estabelecimento_janeiro` | INT | Porte |
| `tipo_deficiencia` | INT | Tipo de deficiência |

**⚠️ Colunas que NÃO existem (descoberto no POC — HTTP 400):**
```
❌ faixa_etaria_dezena          → usar: idade  (INT bruto em anos)
❌ tipo_movimentacao_desagregado → usar: tipo_movimentacao (INT)
```

---

### 2.4 Fonte 3 — RAIS (Ministério do Trabalho)

**Status:** ✅ POC validada · Cobertura real a confirmar (defasagem 1–2 anos esperada)

**O que é:**  
Censo anual do emprego formal. Captura o **estoque** de vínculos ativos em 31/12 — mais completa que o CAGED para análise estrutural anual.

**Acesso:** Base dos Dados via BigQuery  
`basedosdados.br_me_rais.microdados_vinculos`

> ⚠️ **Tabela ~250 GB — toda query DEVE ter `WHERE ano = X`.**

**⚠️ Correção crítica descoberta no POC:**
```python
❌  WHERE vinculo_ativo_3112 = 1    # INTEGER → HTTP 400 no BigQuery
✅  WHERE vinculo_ativo_3112 = '1'  # STRING → correto
```

**Passo obrigatório:** fazer `SELECT * WHERE ano=X LIMIT 1` antes de coletar para detectar tipos reais das colunas automaticamente.

**Principais colunas:**

| Coluna | Tipo confirmado | Observação |
|---|---|---|
| `ano` | INT | Ano de referência |
| `sigla_uf` | STRING | UF |
| `sexo` | INT | 1=Masc · 2=Fem |
| `idade` | INT | Idade em 31/12 |
| `faixa_etaria` | STRING/INT | Verificar tipo no schema |
| `cbo_2002` | STRING | Ocupação CBO 6 dígitos |
| `cnae_2_subclasse` | STRING | Setor econômico |
| `vinculo_ativo_3112` | **STRING** | **'1'=ativo — filtrar com aspas** |
| `valor_remuneracao_media` | FLOAT | Remuneração média mensal (R$) |
| `valor_remuneracao_dezembro` | FLOAT | Remuneração em dezembro (R$) |
| `grau_instrucao_apos_2005` | INT | Escolaridade |
| `tipo_vinculo` | INT | CLT, estatutário, etc. |

**Estratégia de cobertura:** RAIS para anos disponíveis + CAGED para completar 2024–2025.

---

### 2.5 Fonte 4 — Censo Demográfico 2022 (IBGE) — Complementar

**API:** `apisidra.ibge.gov.br` — mesma sintaxe validada da PNAD.

**Quando usar:** Análise cross-sectional de 2022 com máxima desagregação geográfica (nível município). Dado pontual, sem série histórica.

| Tabela | Conteúdo |
|---|---|
| **10264** | Ocupação (CBO 2002) por sexo e UF |
| **10266** | Atividade econômica (CNAE) por sexo |
| **10268** | Rendimento médio por UF e sexo |

---

## 3. Arquitetura Técnica

### 3.1 Stack tecnológica

```
Linguagem principal      : Python 3.12
HTTP sem URL encoding    : http.client (stdlib) — obrigatório para apisidra
Descompressão gzip       : gzip (stdlib) — respostas IBGE vêm comprimidas
BigQuery / Base dos Dados: basedosdados 2.0.3  ✅ instalado e autenticado
Manipulação de dados     : pandas
Export                   : openpyxl (Excel) · csv (stdlib)
Ambiente                 : Jupyter Notebook
```

**Projeto GCP configurado:** `stoked-keyword-489423-u2`

**Dependências:**
```bash
pip install basedosdados pandas openpyxl
```

---

### 3.2 Estrutura de pastas do projeto

```
projeto_mercado_trabalho/
│
├── 📁 pocs/                              # Todos aprovados ✅
│   ├── poc_v3_apisidra.py               # ✅ apisidra — http.client, gzip
│   ├── poc_basedosdados_v2.py           # ✅ CAGED — BigQuery autenticado
│   ├── poc_rais.py                      # ✅ RAIS — schema inspecionado
│   ├── poc_pnad_cobertura_v2.py         # ✅ PNAD 2020–2025 — formato AAAASS
│   ├── poc_caged_cobertura.py           # ✅ CAGED 2020–2025 — 68 meses
│   └── poc_rais_cobertura_v2.py         # ✅ RAIS — auto-schema + '1' STRING
│
├── 📁 coleta/                           # Fase 2 — a implementar
│   ├── coletar_pnad.py
│   ├── coletar_caged.py
│   ├── coletar_rais.py
│   └── coletar_censo.py
│
├── 📁 dados/
│   ├── 📁 bronze/                       # Dados brutos preservados
│   │   ├── pnad_{tabela}_{periodo}.json
│   │   ├── caged_{ano}.csv
│   │   └── rais_{ano}.csv
│   │
│   ├── 📁 silver/                       # Normalizados e tipados
│   │   ├── pnad_limpo.csv
│   │   ├── caged_limpo.csv
│   │   └── rais_limpo.csv
│   │
│   └── 📁 gold/                         # Dataset analítico final
│       ├── mercado_trabalho_consolidado.xlsx
│       ├── gap_salarial_por_uf_sexo_idade.csv
│       ├── distribuicao_ocupacional.csv
│       └── vagas_saldo_por_uf_perfil.csv
│
├── 📁 analise/
│   ├── gap_salarial.py
│   ├── analise_por_ocupacao.py
│   └── analise_vagas.py
│
├── 📁 docs/
│   └── ARQUITETURA.md
│
├── requirements.txt
└── README.md
```

---

## 4. Pipeline de Dados — Camadas (Medallion)

```
┌──────────────────────────────────────────────────────────────────┐
│  FONTES EXTERNAS                                                 │
│  apisidra.ibge.gov.br        basedosdados → BigQuery (GCP)      │
│  PNAD Contínua / Censo 2022  CAGED · RAIS                       │
└──────────────┬───────────────────────────┬───────────────────────┘
               │                           │
               ▼                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE — dados brutos preservados                              │
│  JSON (PNAD/Censo) · CSV (CAGED/RAIS via BigQuery)              │
│  Sem nenhuma transformação — reprocessar sem re-bater na API    │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│  SILVER — dados normalizados                                    │
│  Tipos corretos · colunas renomeadas · ausentes tratados        │
│  Linha 0 do SIDRA removida · sexo mapeado (1→Masc, 2→Fem)      │
│  Faixas etárias criadas a partir de `idade` (CAGED/RAIS)       │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│  GOLD — dataset analítico                                       │
│  Join entre fontes · gap salarial calculado                     │
│  Formato final: .xlsx (múltiplas abas) + .csv por dimensão      │
└──────────────────────────────────────────────────────────────────┘
```

---

## 5. Detalhamento de Cada Componente

### 5.1 `coletar_pnad.py`

**Responsabilidades:**
- Inspecionar metadados antes de cada coleta (`servicodados` → confirmar nível e período)
- Requisições via `http.client` (sem encoding)
- Descomprimir gzip quando necessário
- Salvar JSON bruto na camada Bronze

**Configuração das tabelas:**
```python
TABELAS_PNAD = {
    "4093": {
        "descricao":      "Força de trabalho por sexo",
        "periodicidade":  "trimestral",        # formato AAAASS
        "periodos":       "202001-202504",
        "classificacoes": "/c2/4,5",
        "nivel":          "/n3/41,42,43",
    },
    "5436": {
        "descricao":      "Rendimento médio real por sexo",
        "periodicidade":  "trimestral",
        "periodos":       "202001-202504",
        "classificacoes": "/c2/4,5",
        "nivel":          "/n3/41,42,43",
    },
    "7322": {
        "descricao":      "Rendimento por UF, sexo e faixa etária",
        "periodicidade":  "anual",             # formato AAAA
        "periodos":       "2020,2021,2022,2023,2024",
        "classificacoes": "/c2/4,5/c58/all",
        "nivel":          "/n3/41,42,43",
    },
    "7463": {
        "descricao":      "Ocupados por grupamento e sexo",
        "periodicidade":  "trimestral",
        "periodos":       "202001-202504",
        "classificacoes": "/c2/4,5/c1/all",
        "nivel":          "/n3/41,42,43",
    },
}
```

---

### 5.2 `coletar_caged.py`

**Query de produção (colunas reais confirmadas no POC):**
```python
query = """
    SELECT
        ano, mes, sigla_uf,
        sexo,                       -- INT: 1=Masc, 2=Fem
        idade,                      -- INT: idade bruta (não faixa_etaria_dezena)
        grau_instrucao,
        cbo_2002, cnae_2_subclasse,
        tipo_movimentacao,          -- INT (não tipo_movimentacao_desagregado)
        saldo_movimentacao,         -- 1=admissão, -1=desligamento
        salario_mensal
    FROM `basedosdados.br_me_caged.microdados_movimentacao`
    WHERE ano BETWEEN 2020 AND 2025
      AND sigla_uf IN ('PR', 'SC', 'RS')
"""
```

---

### 5.3 `coletar_rais.py`

**Query de produção (com correções do POC):**
```python
query = f"""
    SELECT
        ano, sigla_uf, sexo, idade, faixa_etaria,
        cbo_2002, cnae_2_subclasse, tipo_vinculo,
        valor_remuneracao_media, valor_remuneracao_dezembro,
        grau_instrucao_apos_2005
    FROM `basedosdados.br_me_rais.microdados_vinculos`
    WHERE ano = {ano}
      AND sigla_uf IN ('PR', 'SC', 'RS')
      AND vinculo_ativo_3112 = '1'   -- STRING, não INTEGER
"""
# Executar por ano individualmente (tabela ~250 GB)
```

---

### 5.4 Módulo Silver — Normalização

**Transformações por fonte:**

```python
# ── PNAD (JSON apisidra) ──────────────────────────────
df = pd.DataFrame(dados[1:])   # linha 0 = metadados SIDRA — remover

MAPA_COLUNAS_PNAD = {
    "MC": "uf_cod",   "MN": "uf_nome",
    "D2C": "periodo", "D3C": "sexo_cod", "D3N": "sexo_nome",
    "V": "valor",
}
AUSENTES_IBGE = {"-", "...", "X", "x", "C", ""}
df["valor"] = pd.to_numeric(
    df["V"].str.replace(",", "."), errors="coerce"
).where(~df["V"].isin(AUSENTES_IBGE))
df["sexo"] = df["sexo_cod"].map({"4": "Masculino", "5": "Feminino"})

# ── CAGED (CSV BigQuery) ──────────────────────────────
df["sexo"] = df["sexo"].map({1: "Masculino", 2: "Feminino"})

# Criar faixa etária a partir da idade bruta
bins   = [0, 17, 24, 29, 39, 49, 59, 120]
labels = ["<18", "18-24", "25-29", "30-39", "40-49", "50-59", "60+"]
df["faixa_etaria"] = pd.cut(df["idade"], bins=bins, labels=labels)

# ── RAIS (CSV BigQuery) ───────────────────────────────
df["sexo"] = df["sexo"].map({1: "Masculino", 2: "Feminino"})
# vinculo_ativo_3112 já filtrado na coleta — pode dropar
df = df.drop(columns=["vinculo_ativo_3112"], errors="ignore")
```

---

### 5.5 – 5.7 Módulos de Análise Gold

**`gap_salarial.py`**
```
gap_pct = (rendimento_homens - rendimento_mulheres) / rendimento_mulheres × 100
```
Saída: `gap_salarial_por_uf_sexo_idade.csv`

**`analise_por_ocupacao.py`**  
Proporção feminina por CBO · média salarial por ocupação e sexo.  
Saída: `distribuicao_ocupacional.csv`

**`analise_vagas.py`**  
Saldo mensal por UF · decomposição por sexo e faixa etária.  
Saída: `vagas_saldo_por_uf_perfil.csv`

---

### 5.8 Dataset Gold — Consolidado

**Granularidade:** UF × ano × sexo × faixa etária × ocupação

```
uf_cod | uf_nome | ano | trimestre | sexo | faixa_etaria | cbo_grupamento
rendimento_medio | pessoas_ocupadas | taxa_informalidade
gap_salarial_pct | saldo_empregos   | salario_medio_admissao
```

**Formato:** `.xlsx` com abas por tema + `.csv` por dimensão

---

## 6. Plano de Execução — Etapas

### Fase 1 — Validação de Conectividade ✅ CONCLUÍDA

| Etapa | Descrição | Status |
|---|---|---|
| 1.1 | POC apisidra — PNAD/Censo | ✅ Aprovado |
| 1.2 | POC CAGED — Base dos Dados (BigQuery) | ✅ Aprovado |
| 1.3 | POC RAIS — Base dos Dados (BigQuery) | ✅ Aprovado |
| 1.4 | POC cobertura PNAD 2020–2025 | ✅ Aprovado |
| 1.5 | POC cobertura CAGED 2020–2025 | ✅ Aprovado (68 meses) |
| 1.6 | POC cobertura RAIS 2020–2025 | ✅ Aprovado (cobertura real via auto-schema) |

### Fase 2 — Coleta Bronze 🔲 PRÓXIMA

| Etapa | Descrição | Dependência |
|---|---|---|
| 2.1 | Coletar PNAD — tabelas 4093, 5436, 7322, 7463 | Fase 1 ✅ |
| 2.2 | Coletar CAGED 2020–2025 | Fase 1 ✅ |
| 2.3 | Coletar RAIS nos anos com cobertura | Fase 1 ✅ |
| 2.4 | Coletar Censo 2022 (se necessário) | Fase 1 ✅ |

### Fase 3 — Normalização Silver 🔲

| Etapa | Descrição | Dependência |
|---|---|---|
| 3.1 | Normalizar PNAD | Fase 2.1 |
| 3.2 | Normalizar CAGED (criar faixas etárias de `idade`) | Fase 2.2 |
| 3.3 | Normalizar RAIS | Fase 2.3 |
| 3.4 | Validação cruzada com publicações IBGE/MTE | Fase 3.1–3.3 |

### Fase 4 — Análise Gold 🔲

| Etapa | Descrição | Dependência |
|---|---|---|
| 4.1 | Gap salarial por UF, sexo, faixa etária | Fase 3.1 |
| 4.2 | Distribuição por ocupação e sexo | Fase 3.1 + 3.3 |
| 4.3 | Saldo de vagas por perfil | Fase 3.2 |
| 4.4 | Consolidar dataset Gold | Fase 4.1–4.3 |

### Fase 5 — Exportação 🔲

| Etapa | Descrição |
|---|---|
| 5.1 | Gerar `.xlsx` com abas por tema |
| 5.2 | Gerar `.csv` por dimensão |
| 5.3 | Documentar dicionário de variáveis |

---

## 7. Decisões Técnicas Registradas

### 7.1 `http.client` em vez de `requests`
`requests` (via `urllib3`) codifica `|` → `%7C` e `[` → `%5B`, causando HTTP 500 na `apisidra`. O `http.client` envia o path literalmente. Validado no POC v3.

### 7.2 `sys.exit()` quebra o kernel Jupyter
`sys.exit()` lança `SystemExit` que o IPython tenta capturar mas falha em `inspect.getinnerframes` com `AttributeError`. Padrão correto para Jupyter: `raise RuntimeError("mensagem")`.

### 7.3 Gzip nas respostas do IBGE
O servidor IBGE comprime as respostas mesmo quando `Accept-Encoding: identity` é enviado. Detectar pelo magic byte e descomprimir:
```python
if body[:2] == b'\x1f\x8b':
    body = gzip.decompress(body)
```

### 7.4 Formato de período PNAD — crítico
Tabelas trimestrais usam `AAAASS` (6 dígitos). `/p/2020` retorna HTTP 400. Correto: `/p/202001`. Verificar `periodicidade.frequencia` nos metadados antes de montar a URL.

### 7.5 `vinculo_ativo_3112` na RAIS é STRING
A coluna armazena `'0'` e `'1'` como texto. `WHERE vinculo_ativo_3112 = 1` causa HTTP 400 no BigQuery. Correto: `WHERE vinculo_ativo_3112 = '1'`. Descoberto ao inspecionar o schema real via `SELECT * LIMIT 1`.

### 7.6 Colunas inexistentes no CAGED

| ❌ Esperado (incorreto) | ✅ Nome real confirmado no POC |
|---|---|
| `faixa_etaria_dezena` | `idade` (INT bruto em anos) |
| `tipo_movimentacao_desagregado` | `tipo_movimentacao` (INT) |

### 7.7 Inspecionar schema antes de coletar
Padrão estabelecido: sempre fazer `SELECT * LIMIT 1` ou buscar metadados antes de montar queries de produção. Detecta tipos reais de coluna e nomes — evita HTTP 400 por type mismatch ou coluna inexistente.

### 7.8 Por que Bronze / Silver / Gold?
Bronze preserva dados brutos para reprocessamento sem re-bater nas APIs. Silver isola a lógica de limpeza. Gold isola a lógica de negócio. Padrão alinhado com a arquitetura medallion já usada no ambiente Databricks.

### 7.9 RAIS complementa CAGED, não substitui
RAIS = **estoque** (vínculos ativos em 31/12) com CBO de 6 dígitos — ideal para análise estrutural. CAGED = **fluxo** mensal (admissões, desligamentos, saldo). Para 2024–2025 onde a RAIS ainda não disponível, o CAGED cobre a lacuna.

### 7.10 Base dos Dados em vez de API PDET
A API PDET exigiria parsear CSVs brutos com problemas históricos de encoding. A Base dos Dados entrega dados tipados, tratados e com schema documentado no BigQuery — mesmo custo de infraestrutura (projeto GCP já configurado).

---

## 8. Próximos Passos

```
[x] Fase 1 — Todos os POCs aprovados

[ ] Fase 2.1 — Implementar coletar_pnad.py
      Tabelas: 4093, 5436, 7322, 7463
      Período: 202001–202504 (trimestral) e 2020–2024 (anual)
      Nível: n3/41,42,43 (Região Sul)

[ ] Fase 2.2 — Implementar coletar_caged.py
      Colunas corretas: idade, tipo_movimentacao, sexo (INT)
      Período: 2020–2025

[ ] Fase 2.3 — Implementar coletar_rais.py
      Filtro: vinculo_ativo_3112 = '1' (STRING)
      Coleta por ano com WHERE ano = X
      Anos disponíveis: confirmar com output do poc_rais_cobertura_v2
```

---

## 9. Referências

| Recurso | URL |
|---|---|
| API SIDRA — documentação oficial | https://apisidra.ibge.gov.br/home/ajuda |
| SIDRA — metadados via servicodados | https://servicodados.ibge.gov.br/api/docs/agregados?versao=3 |
| PNAD Contínua — tabelas SIDRA | https://sidra.ibge.gov.br |
| Base dos Dados — CAGED | https://basedosdados.org/dataset/562b56a3-0b01-4735-a049-eeac5681f056 |
| Base dos Dados — RAIS | https://basedosdados.org/dataset/3e7c4d58-96ba-448e-b053-d385a829ef00 |
| BigQuery Console | https://console.cloud.google.com/bigquery |
| Dums & Campos (2025) — artigo base | DOI: 10.61164/rmnm.v6i1.3724 |

---

## 10. Registro de POCs Executadas

| POC | Arquivo | Status | Data | Principais aprendizados |
|---|---|---|---|---|
| apisidra v3 | `poc_v3_apisidra.py` | ✅ | 2026-04-06 | `http.client`, gzip, path-based sem `[]` ou `\|` |
| CAGED Base dos Dados | `poc_basedosdados_v2.py` | ✅ | 2026-04-06 | 25 colunas confirmadas, schema inspecionado |
| RAIS Base dos Dados | `poc_rais.py` | ✅ | 2026-04-06 | `vinculo_ativo_3112='1'` é STRING |
| PNAD cobertura 2020–2025 | `poc_pnad_cobertura_v2.py` | ✅ | 2026-04-07 | Período `AAAASS`, metadados antes de coletar |
| CAGED cobertura 2020–2025 | `poc_caged_cobertura.py` | ✅ | 2026-04-07 | 2020→2025 confirmado, 68 meses |
| RAIS cobertura 2020–2025 | `poc_rais_cobertura_v2.py` | ✅ | 2026-04-07 | Auto-detecção de tipo de coluna no schema |
