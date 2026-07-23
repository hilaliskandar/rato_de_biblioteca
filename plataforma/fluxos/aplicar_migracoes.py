from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
from prefect import flow, task
from prefect.logging import get_run_logger


RAIZ_PLATAFORMA = Path(__file__).resolve().parents[2]

CAMINHO_BANCO = (
    RAIZ_PLATAFORMA
    / "plataforma"
    / "banco_dados"
    / "rato_de_biblioteca.duckdb"
)

PASTA_MIGRACOES = (
    RAIZ_PLATAFORMA
    / "plataforma"
    / "banco_dados"
    / "migracoes"
)

CAMINHO_AUDITORIA = (
    RAIZ_PLATAFORMA
    / "auditoria"
    / "eventos_auditoria.jsonl"
)


def calcular_hash(arquivo: Path) -> str:
    return hashlib.sha256(
        arquivo.read_bytes()
    ).hexdigest()


def obter_id_migracao(arquivo: Path) -> str:
    correspondencia = re.match(
        r"^(\d{4})_",
        arquivo.name,
    )

    if not correspondencia:
        raise ValueError(
            f"Nome de migracao invalido: {arquivo.name}"
        )

    return f"MIG-{correspondencia.group(1)}"


def gerar_id_evento() -> str:
    return (
        "EVT-"
        + datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
    )


@task(name="listar-migracoes")
def listar_migracoes() -> list[str]:
    logger = get_run_logger()

    arquivos = sorted(
        PASTA_MIGRACOES.glob("*.sql")
    )

    if not arquivos:
        raise RuntimeError(
            "Nenhuma migracao SQL foi encontrada."
        )

    logger.info(
        "%s migracao(oes) encontrada(s).",
        len(arquivos),
    )

    return [
        str(arquivo)
        for arquivo in arquivos
    ]


@task(name="aplicar-migracao-individual")
def aplicar_migracao_individual(
    caminho_migracao: str,
) -> dict[str, Any]:
    logger = get_run_logger()

    arquivo = Path(caminho_migracao)
    id_migracao = obter_id_migracao(arquivo)
    hash_arquivo = calcular_hash(arquivo)

    sql = arquivo.read_text(
        encoding="utf-8-sig"
    )

    CAMINHO_BANCO.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    conexao = duckdb.connect(
        str(CAMINHO_BANCO)
    )

    try:
        conexao.execute(
            """
            CREATE TABLE IF NOT EXISTS migracoes_banco (
                id_migracao VARCHAR PRIMARY KEY,
                descricao VARCHAR NOT NULL,
                hash_arquivo VARCHAR NOT NULL,
                aplicada_em_utc TIMESTAMPTZ NOT NULL
            )
            """
        )

        registro = conexao.execute(
            """
            SELECT hash_arquivo
            FROM migracoes_banco
            WHERE id_migracao = ?
            """,
            [id_migracao],
        ).fetchone()

        if registro:
            if registro[0] != hash_arquivo:
                raise RuntimeError(
                    f"A migracao {id_migracao} foi "
                    "alterada depois de aplicada."
                )

            logger.info(
                "%s ja aplicada e integra.",
                id_migracao,
            )

            return {
                "id_migracao": id_migracao,
                "arquivo": arquivo.name,
                "situacao": "ja_aplicada",
                "hash_arquivo": hash_arquivo,
            }

        conexao.execute("BEGIN TRANSACTION")
        conexao.execute(sql)

        conexao.execute(
            """
            INSERT INTO migracoes_banco (
                id_migracao,
                descricao,
                hash_arquivo,
                aplicada_em_utc
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                id_migracao,
                arquivo.stem,
                hash_arquivo,
                datetime.now(timezone.utc),
            ],
        )

        conexao.execute("COMMIT")

        logger.info(
            "%s aplicada com sucesso.",
            id_migracao,
        )

        return {
            "id_migracao": id_migracao,
            "arquivo": arquivo.name,
            "situacao": "aplicada",
            "hash_arquivo": hash_arquivo,
        }

    except Exception:
        try:
            conexao.execute("ROLLBACK")
        except Exception:
            pass

        raise

    finally:
        conexao.close()


@task(name="registrar-auditoria-migracoes")
def registrar_auditoria(
    resultados: list[dict[str, Any]],
) -> str:
    id_evento = gerar_id_evento()
    instante = datetime.now(timezone.utc)

    evento = {
        "id_evento": id_evento,
        "data_hora_utc": instante.isoformat(),
        "tipo_ator": "script",
        "id_ator": "aplicar_migracoes",
        "acao": "aplicar_migracoes_banco",
        "id_projeto": None,
        "entidade": "banco_dados",
        "id_entidade": None,
        "situacao": "concluido",
        "detalhes": resultados,
    }

    CAMINHO_AUDITORIA.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with CAMINHO_AUDITORIA.open(
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

    conexao = duckdb.connect(
        str(CAMINHO_BANCO)
    )

    try:
        conexao.execute(
            """
            INSERT INTO eventos_auditoria (
                id_evento,
                data_hora_utc,
                tipo_ator,
                id_ator,
                acao,
                id_projeto,
                entidade,
                id_entidade,
                situacao,
                detalhes_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                id_evento,
                instante,
                "script",
                "aplicar_migracoes",
                "aplicar_migracoes_banco",
                None,
                "banco_dados",
                None,
                "concluido",
                json.dumps(
                    resultados,
                    ensure_ascii=False,
                ),
            ],
        )
    finally:
        conexao.close()

    return id_evento


@flow(name="aplicar-migracoes-do-banco")
def aplicar_migracoes() -> dict[str, Any]:
    caminhos = listar_migracoes()

    resultados = []

    for caminho in caminhos:
        resultados.append(
            aplicar_migracao_individual(
                caminho
            )
        )

    id_evento = registrar_auditoria(
        resultados
    )

    return {
        "situacao": "concluido",
        "quantidade_migracoes": len(resultados),
        "resultados": resultados,
        "id_evento_auditoria": id_evento,
    }


if __name__ == "__main__":
    resultado_final = aplicar_migracoes()

    print()
    print("RESULTADO DAS MIGRACOES")
    print(
        json.dumps(
            resultado_final,
            ensure_ascii=False,
            indent=2,
        )
    )
