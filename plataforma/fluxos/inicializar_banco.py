from __future__ import annotations

import hashlib
import json
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

CAMINHO_MIGRACAO = (
    RAIZ_PLATAFORMA
    / "plataforma"
    / "banco_dados"
    / "migracoes"
    / "0001_esquema_inicial.sql"
)

CAMINHO_AUDITORIA = (
    RAIZ_PLATAFORMA
    / "auditoria"
    / "eventos_auditoria.jsonl"
)

ID_MIGRACAO = "MIG-0001"
DESCRICAO_MIGRACAO = "Cria esquema inicial da plataforma"


def calcular_hash(arquivo: Path) -> str:
    return hashlib.sha256(
        arquivo.read_bytes()
    ).hexdigest()


def gerar_id_evento() -> str:
    return (
        "EVT-"
        + datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
    )


@task(name="carregar-migracao")
def carregar_migracao() -> tuple[str, str]:
    logger = get_run_logger()

    if not CAMINHO_MIGRACAO.exists():
        raise FileNotFoundError(
            f"Migracao nao encontrada: {CAMINHO_MIGRACAO}"
        )

    sql = CAMINHO_MIGRACAO.read_text(
        encoding="utf-8-sig"
    )

    hash_arquivo = calcular_hash(
        CAMINHO_MIGRACAO
    )

    logger.info(
        "Migracao carregada: %s",
        CAMINHO_MIGRACAO.name,
    )

    return sql, hash_arquivo


@task(name="aplicar-migracao")
def aplicar_migracao(
    sql: str,
    hash_arquivo: str,
) -> dict[str, Any]:
    logger = get_run_logger()

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
            [ID_MIGRACAO],
        ).fetchone()

        if registro:
            hash_registrado = registro[0]

            if hash_registrado != hash_arquivo:
                raise RuntimeError(
                    "A migracao ja foi aplicada, mas o "
                    "arquivo foi alterado posteriormente."
                )

            logger.info(
                "Migracao ja aplicada e integra: %s",
                ID_MIGRACAO,
            )

            return {
                "situacao": "ja_aplicada",
                "id_migracao": ID_MIGRACAO,
                "hash_arquivo": hash_arquivo,
            }

        conexao.execute(
            "BEGIN TRANSACTION"
        )

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
                ID_MIGRACAO,
                DESCRICAO_MIGRACAO,
                hash_arquivo,
                datetime.now(timezone.utc),
            ],
        )

        conexao.execute(
            "COMMIT"
        )

        logger.info(
            "Migracao aplicada: %s",
            ID_MIGRACAO,
        )

        return {
            "situacao": "aplicada",
            "id_migracao": ID_MIGRACAO,
            "hash_arquivo": hash_arquivo,
        }

    except Exception:
        try:
            conexao.execute(
                "ROLLBACK"
            )
        except Exception:
            pass

        raise

    finally:
        conexao.close()


@task(name="registrar-auditoria-inicializacao")
def registrar_auditoria(
    resultado_migracao: dict[str, Any],
) -> str:
    logger = get_run_logger()

    instante = datetime.now(timezone.utc)
    id_evento = gerar_id_evento()

    evento = {
        "id_evento": id_evento,
        "data_hora_utc": instante.isoformat(),
        "tipo_ator": "script",
        "id_ator": "inicializar_banco",
        "acao": "inicializar_banco_plataforma",
        "id_projeto": None,
        "entidade": "banco_dados",
        "id_entidade": ID_MIGRACAO,
        "situacao": resultado_migracao["situacao"],
        "detalhes": resultado_migracao,
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
                "inicializar_banco",
                "inicializar_banco_plataforma",
                None,
                "banco_dados",
                ID_MIGRACAO,
                resultado_migracao["situacao"],
                json.dumps(
                    resultado_migracao,
                    ensure_ascii=False,
                ),
            ],
        )
    finally:
        conexao.close()

    logger.info(
        "Evento de auditoria registrado: %s",
        id_evento,
    )

    return id_evento


@flow(name="inicializar-banco-da-plataforma")
def inicializar_banco() -> dict[str, Any]:
    logger = get_run_logger()

    logger.info(
        "Iniciando banco DuckDB da plataforma."
    )

    sql, hash_arquivo = carregar_migracao()

    resultado_migracao = aplicar_migracao(
        sql,
        hash_arquivo,
    )

    id_evento = registrar_auditoria(
        resultado_migracao
    )

    resultado = {
        "situacao": "concluido",
        "banco_relativo": CAMINHO_BANCO.relative_to(
            RAIZ_PLATAFORMA
        ).as_posix(),
        "migracao": resultado_migracao,
        "id_evento_auditoria": id_evento,
    }

    logger.info(
        "Inicializacao do banco concluida."
    )

    return resultado


if __name__ == "__main__":
    resultado_final = inicializar_banco()

    print()
    print("RESULTADO DA INICIALIZACAO")
    print(
        json.dumps(
            resultado_final,
            ensure_ascii=False,
            indent=2,
        )
    )
