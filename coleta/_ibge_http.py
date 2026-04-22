"""
Shared HTTP utilities for IBGE APIs.

http.client is mandatory — requests/urllib3 encode '|' and '[' which
causes HTTP 500 on apisidra (see ARQUITETURA.md §7.1).
"""
import gzip
import http.client
import json


def fetch_apisidra(path: str) -> list[dict]:
    """GET from apisidra.ibge.gov.br and return parsed JSON.

    Handles gzip-compressed responses transparently (§7.3).
    """
    conn = http.client.HTTPSConnection("apisidra.ibge.gov.br")
    conn.request("GET", path, headers={"Accept": "application/json"})
    resp = conn.getresponse()
    body = resp.read()
    conn.close()

    if resp.status != 200:
        detail = body[:500].decode("utf-8", errors="replace")
        raise RuntimeError(f"apisidra HTTP {resp.status} para path: {path}\nResposta: {detail}")

    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)

    return json.loads(body.decode("utf-8"))


def fetch_metadata(tabela: str) -> dict:
    """GET table metadata from servicodados.ibge.gov.br."""
    conn = http.client.HTTPSConnection("servicodados.ibge.gov.br")
    conn.request("GET", f"/api/v3/agregados/{tabela}/metadados")
    resp = conn.getresponse()
    body = resp.read()
    conn.close()

    if resp.status != 200:
        raise RuntimeError(f"servicodados HTTP {resp.status} — tabela {tabela}")

    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)

    return json.loads(body.decode("utf-8"))


def expand_period_range(spec: str) -> str:
    """Convert 'AAAASS-AAAASS' or 'AAAA-AAAA' range into comma list.

    Trimestral (6-digit): '202001-202504' → '202001,202002,...,202504'
    Anual (4-digit):      '2020-2024'     → '2020,2021,2022,2023,2024'
    Already a list:       '2020,2021'     → unchanged
    """
    if "-" not in spec:
        return spec

    start, end = spec.split("-", 1)

    if len(start) == 6:  # trimestral
        ano_i, q_i = int(start[:4]), int(start[4:])
        ano_f, q_f = int(end[:4]), int(end[4:])
        parts = []
        ano, q = ano_i, q_i
        while (ano, q) <= (ano_f, q_f):
            parts.append(f"{ano}{q:02d}")
            q += 1
            if q > 4:
                q, ano = 1, ano + 1
        return ",".join(parts)

    # anual
    return ",".join(str(a) for a in range(int(start), int(end) + 1))
