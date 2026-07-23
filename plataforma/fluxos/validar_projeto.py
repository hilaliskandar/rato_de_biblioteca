from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from prefect import flow, task
from prefect.logging import get_run_logger


RAIZ_PLATAFORMA = Path(__file__).resolve().parents[2]
PASTA_AUDITORIA = RAIZ_PLATAFORMA / "auditoria"

PADRAO_ID_PROJETO = re.compile(r"^PRJ-\d{4}$")
PADRAO_NOME_TECNICO = re.compile(r"^[a-z0-9_]+$")


def calcular_hash(arquivo: Path) -> str:
    return hashlib.sha256(arquivo.read_bytes()).hexdigest()


def validar_chaves_normalizadas(
    objeto: Any,
    caminho: str = "raiz",
) -> list[str]:
    erros: list[str] = []

    if isinstance(objeto, dict):
        for chave, valor in objeto.items():
            if not isinstance(chave, str):
                erros.append(
                    f"{caminho}: chave nao textual: {chave!r}"
                )
                continue

            if not PADRAO_NOME_TECNICO.fullmatch(chave):
                erros.append(
                    f"{caminho}: chave fora do padrao: {chave}"
                )

            erros.extend(
                validar_chaves_normalizadas(
                    valor,
                    f"{caminho}.{chave}",
                )
            )

    elif isinstance(objeto, list):
        for indice, valor in enumerate(objeto):
            erros.extend(
                validar_chaves_normalizadas(
                    valor,
                    f"{caminho}[{indice}]",
                )
            )

    return erros


@task(name="carregar-projeto")
def carregar_projeto(
    caminho_projeto: str,
) -> tuple[dict[str, Any], str]:
    logger = get_run_logger()

    arquivo = Path(caminho_projeto).resolve()

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo de projeto nao encontrado: {arquivo}"
        )

    dados = yaml.safe_load(
        arquivo.read_text(encoding="utf-8-sig")
    )

    if not isinstance(dados, dict):
        raise ValueError(
            "O arquivo de projeto deve conter um objeto YAML."
        )

    logger.info("Projeto carregado: %s", arquivo)

    return dados, str(arquivo)


@task(name="validar-estrutura-projeto")
def validar_estrutura_projeto(
    dados: dict[str, Any],
) -> list[str]:
    logger = get_run_logger()
    erros: list[str] = []

    campos_obrigatorios = [
        "id_projeto",
        "nome_curto",
        "titulo",
        "descricao",
        "situacao",
        "tipos_revisao",
        "fontes_descoberta",
        "politica_acesso",
        "padroes_relato",
        "controle_humano",
    ]

    for campo in campos_obrigatorios:
        if campo not in dados:
            erros.append(
                f"Campo obrigatorio ausente: {campo}"
            )

    id_projeto = dados.get("id_projeto")

    if (
        not isinstance(id_projeto, str)
        or not PADRAO_ID_PROJETO.fullmatch(id_projeto)
    ):
        erros.append(
            "id_projeto deve seguir o padrao PRJ-0000."
        )

    nome_curto = dados.get("nome_curto")

    if (
        not isinstance(nome_curto, str)
        or not PADRAO_NOME_TECNICO.fullmatch(nome_curto)
    ):
        erros.append(
            "nome_curto deve usar apenas a-z, 0-9 e underscore."
        )

    erros.extend(
        validar_chaves_normalizadas(dados)
    )

    logger.info(
        "Validacao estrutural encontrou %s erro(s).",
        len(erros),
    )

    return erros


@task(name="validar-politica-acesso-aberto")
def validar_politica_acesso_aberto(
    dados: dict[str, Any],
) -> list[str]:
    logger = get_run_logger()
    erros: list[str] = []

    politica = dados.get("politica_acesso", {})

    regras_obrigatorias = {
        "somente_acesso_aberto": True,
        "texto_integral_obrigatorio": True,
        "acesso_institucional_aceito": False,
        "copia_privada_aceita": False,
        "verificar_localizacao_aberta": True,
    }

    for campo, valor_esperado in regras_obrigatorias.items():
        valor_atual = politica.get(campo)

        if valor_atual is not valor_esperado:
            erros.append(
                f"politica_acesso.{campo} deve ser "
                f"{valor_esperado}."
            )

    logger.info(
        "Validacao da politica de acesso encontrou %s erro(s).",
        len(erros),
    )

    return erros


@task(name="registrar-evento-auditoria")
def registrar_evento_auditoria(
    dados: dict[str, Any],
    caminho_arquivo: str,
    erros: list[str],
) -> str:
    logger = get_run_logger()

    PASTA_AUDITORIA.mkdir(
        parents=True,
        exist_ok=True,
    )

    arquivo_origem = Path(caminho_arquivo)
    arquivo_auditoria = (
        PASTA_AUDITORIA
        / "eventos_auditoria.jsonl"
    )

    evento = {
        "id_evento": (
            "EVT-"
            + datetime.now(timezone.utc).strftime(
                "%Y%m%dT%H%M%S%fZ"
            )
        ),
        "data_hora_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "tipo_ator": "script",
        "ator": "validar_projeto",
        "acao": "validar_cadastro_projeto",
        "id_projeto": dados.get("id_projeto"),
        "arquivo_relativo": arquivo_origem.relative_to(
            RAIZ_PLATAFORMA
        ).as_posix(),
        "hash_arquivo": calcular_hash(arquivo_origem),
        "situacao": (
            "validado"
            if not erros
            else "reprovado"
        ),
        "quantidade_erros": len(erros),
        "erros": erros,
    }

    with arquivo_auditoria.open(
        "a",
        encoding="utf-8",
    ) as destino:
        destino.write(
            json.dumps(
                evento,
                ensure_ascii=False,
            )
            + "\n"
        )

    logger.info(
        "Evento de auditoria registrado: %s",
        evento["id_evento"],
    )

    return evento["id_evento"]


@flow(name="validar-cadastro-de-projeto")
def validar_projeto(
    caminho_projeto: str,
) -> dict[str, Any]:
    logger = get_run_logger()

    dados, caminho_arquivo = carregar_projeto(
        caminho_projeto
    )

    erros_estrutura = validar_estrutura_projeto(
        dados
    )

    erros_acesso = validar_politica_acesso_aberto(
        dados
    )

    erros = erros_estrutura + erros_acesso

    id_evento = registrar_evento_auditoria(
        dados,
        caminho_arquivo,
        erros,
    )

    resultado = {
        "id_projeto": dados.get("id_projeto"),
        "situacao": (
            "validado"
            if not erros
            else "reprovado"
        ),
        "quantidade_erros": len(erros),
        "erros": erros,
        "id_evento_auditoria": id_evento,
    }

    if erros:
        logger.error(
            "Projeto reprovado: %s",
            erros,
        )
    else:
        logger.info(
            "Projeto validado com sucesso: %s",
            dados.get("id_projeto"),
        )

    return resultado


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit(
            "Uso: python validar_projeto.py "
            "<caminho_projeto.yaml>"
        )

    resultado_final = validar_projeto(
        sys.argv[1]
    )

    print()
    print("RESULTADO DA VALIDACAO")
    print(
        json.dumps(
            resultado_final,
            ensure_ascii=False,
            indent=2,
        )
    )

    if resultado_final["situacao"] != "validado":
        raise SystemExit(1)
