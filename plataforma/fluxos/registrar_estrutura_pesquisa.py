from __future__ import annotations

import getpass
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import yaml
from prefect import flow, task
from prefect.logging import get_run_logger


RAIZ_PLATAFORMA = Path(__file__).resolve().parents[2]

CAMINHO_BANCO = (
    RAIZ_PLATAFORMA
    / "plataforma"
    / "banco_dados"
    / "rato_de_biblioteca.duckdb"
)

CAMINHO_AUDITORIA = (
    RAIZ_PLATAFORMA
    / "auditoria"
    / "eventos_auditoria.jsonl"
)

PADROES_ID = {
    "pergunta": re.compile(
        r"^PER-\d{4}-\d{3}$"
    ),
    "proposicao": re.compile(
        r"^PRO-\d{4}-\d{3}$"
    ),
    "eixo": re.compile(
        r"^EIX-\d{4}-\d{3}$"
    ),
    "versao": re.compile(
        r"^EST-\d{4}-\d{4}$"
    ),
}


def calcular_hash(arquivo: Path) -> str:
    return hashlib.sha256(
        arquivo.read_bytes()
    ).hexdigest()


def carregar_yaml(
    caminho: str,
) -> tuple[dict[str, Any], Path, str]:
    arquivo = Path(caminho).resolve()

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Arquivo nao encontrado: {arquivo}"
        )

    dados = yaml.safe_load(
        arquivo.read_text(
            encoding="utf-8-sig"
        )
    )

    if not isinstance(dados, dict):
        raise ValueError(
            f"YAML invalido: {arquivo}"
        )

    return (
        dados,
        arquivo,
        calcular_hash(arquivo),
    )


def gerar_id_evento() -> str:
    return (
        "EVT-"
        + datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
    )


@task(name="carregar-estrutura-pesquisa")
def carregar_estrutura(
    caminho_perguntas: str,
    caminho_proposicoes: str,
    caminho_eixos: str,
) -> dict[str, Any]:
    perguntas, arquivo_perguntas, hash_perguntas = (
        carregar_yaml(caminho_perguntas)
    )

    proposicoes, arquivo_proposicoes, hash_proposicoes = (
        carregar_yaml(caminho_proposicoes)
    )

    eixos, arquivo_eixos, hash_eixos = (
        carregar_yaml(caminho_eixos)
    )

    return {
        "perguntas": perguntas,
        "proposicoes": proposicoes,
        "eixos": eixos,
        "arquivos": {
            "perguntas": str(arquivo_perguntas),
            "proposicoes": str(arquivo_proposicoes),
            "eixos": str(arquivo_eixos),
        },
        "hashes": {
            "perguntas": hash_perguntas,
            "proposicoes": hash_proposicoes,
            "eixos": hash_eixos,
        },
    }


@task(name="validar-estrutura-pesquisa")
def validar_estrutura(
    estrutura: dict[str, Any],
) -> None:
    conjuntos = [
        estrutura["perguntas"],
        estrutura["proposicoes"],
        estrutura["eixos"],
    ]

    projetos = {
        conjunto.get("id_projeto")
        for conjunto in conjuntos
    }

    versoes = {
        conjunto.get("id_versao_estrutura")
        for conjunto in conjuntos
    }

    numeros_versao = {
        str(conjunto.get("numero_versao"))
        for conjunto in conjuntos
    }

    situacoes = {
        conjunto.get("situacao_versao")
        for conjunto in conjuntos
    }

    if len(projetos) != 1:
        raise ValueError(
            "Os arquivos indicam projetos diferentes."
        )

    if len(versoes) != 1:
        raise ValueError(
            "Os arquivos indicam versoes diferentes."
        )

    if len(numeros_versao) != 1:
        raise ValueError(
            "Os numeros de versao divergem."
        )

    if len(situacoes) != 1:
        raise ValueError(
            "As situacoes da versao divergem."
        )

    id_versao = next(iter(versoes))

    if not PADROES_ID["versao"].fullmatch(
        str(id_versao)
    ):
        raise ValueError(
            "id_versao_estrutura fora do padrao."
        )

    listas = [
        (
            estrutura["perguntas"].get(
                "perguntas"
            ),
            "id_pergunta",
            PADROES_ID["pergunta"],
        ),
        (
            estrutura["proposicoes"].get(
                "proposicoes"
            ),
            "id_proposicao",
            PADROES_ID["proposicao"],
        ),
        (
            estrutura["eixos"].get(
                "eixos"
            ),
            "id_eixo",
            PADROES_ID["eixo"],
        ),
    ]

    for itens, campo_id, padrao in listas:
        if not isinstance(itens, list) or not itens:
            raise ValueError(
                f"Lista ausente ou vazia: {campo_id}"
            )

        identificadores = []

        for item in itens:
            identificador = item.get(campo_id)

            if not padrao.fullmatch(
                str(identificador)
            ):
                raise ValueError(
                    f"Identificador invalido: "
                    f"{identificador}"
                )

            identificadores.append(
                identificador
            )

        if len(identificadores) != len(
            set(identificadores)
        ):
            raise ValueError(
                f"Identificadores duplicados: {campo_id}"
            )


@task(name="registrar-estrutura-no-banco")
def registrar_estrutura(
    estrutura: dict[str, Any],
) -> dict[str, Any]:
    perguntas = estrutura["perguntas"]
    proposicoes = estrutura["proposicoes"]
    eixos = estrutura["eixos"]
    hashes = estrutura["hashes"]

    id_projeto = perguntas["id_projeto"]
    id_versao = perguntas[
        "id_versao_estrutura"
    ]
    numero_versao = str(
        perguntas["numero_versao"]
    )
    situacao_versao = perguntas[
        "situacao_versao"
    ]

    agora = datetime.now(timezone.utc)
    usuario = getpass.getuser()

    conexao = duckdb.connect(
        str(CAMINHO_BANCO)
    )

    try:
        projeto = conexao.execute(
            """
            SELECT id_projeto
            FROM projetos
            WHERE id_projeto = ?
            """,
            [id_projeto],
        ).fetchone()

        if not projeto:
            raise ValueError(
                f"Projeto nao cadastrado: {id_projeto}"
            )

        versao_existente = conexao.execute(
            """
            SELECT
                hash_perguntas,
                hash_proposicoes,
                hash_eixos
            FROM versoes_estrutura_pesquisa
            WHERE id_versao_estrutura = ?
            """,
            [id_versao],
        ).fetchone()

        hashes_atuais = (
            hashes["perguntas"],
            hashes["proposicoes"],
            hashes["eixos"],
        )

        if versao_existente:
            if tuple(versao_existente) != hashes_atuais:
                raise RuntimeError(
                    "A versao ja foi registrada, mas "
                    "os arquivos foram alterados."
                )

            return {
                "id_projeto": id_projeto,
                "id_versao_estrutura": id_versao,
                "situacao": "ja_registrada",
            }

        conteudo = {
            "perguntas": perguntas,
            "proposicoes": proposicoes,
            "eixos": eixos,
        }

        conexao.execute("BEGIN TRANSACTION")

        conexao.execute(
            """
            UPDATE versoes_estrutura_pesquisa
            SET situacao = 'substituida'
            WHERE id_projeto = ?
              AND situacao <> 'substituida'
            """,
            [id_projeto],
        )

        conexao.execute(
            """
            DELETE FROM perguntas_pesquisa
            WHERE id_projeto = ?
            """,
            [id_projeto],
        )

        conexao.execute(
            """
            DELETE FROM proposicoes_iniciais
            WHERE id_projeto = ?
            """,
            [id_projeto],
        )

        conexao.execute(
            """
            DELETE FROM eixos_tematicos
            WHERE id_projeto = ?
            """,
            [id_projeto],
        )

        for item in perguntas["perguntas"]:
            conexao.execute(
                """
                INSERT INTO perguntas_pesquisa (
                    id_pergunta,
                    id_projeto,
                    texto_pergunta,
                    tipo_pergunta,
                    situacao,
                    versao,
                    criada_em_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    item["id_pergunta"],
                    id_projeto,
                    item["texto"],
                    item["tipo"],
                    item["situacao"],
                    numero_versao,
                    agora,
                ],
            )

        for item in proposicoes["proposicoes"]:
            conexao.execute(
                """
                INSERT INTO proposicoes_iniciais (
                    id_proposicao,
                    id_projeto,
                    texto_proposicao,
                    situacao,
                    criada_em_utc
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    item["id_proposicao"],
                    id_projeto,
                    item["texto"],
                    item["situacao"],
                    agora,
                ],
            )

        for item in eixos["eixos"]:
            conexao.execute(
                """
                INSERT INTO eixos_tematicos (
                    id_eixo,
                    id_projeto,
                    titulo,
                    descricao,
                    situacao
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    item["id_eixo"],
                    id_projeto,
                    item["titulo"],
                    item.get("descricao"),
                    item["situacao"],
                ],
            )

        conexao.execute(
            """
            INSERT INTO versoes_estrutura_pesquisa (
                id_versao_estrutura,
                id_projeto,
                numero_versao,
                situacao,
                hash_perguntas,
                hash_proposicoes,
                hash_eixos,
                conteudo_json,
                registrada_em_utc,
                registrada_por
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                id_versao,
                id_projeto,
                numero_versao,
                situacao_versao,
                hashes["perguntas"],
                hashes["proposicoes"],
                hashes["eixos"],
                json.dumps(
                    conteudo,
                    ensure_ascii=False,
                ),
                agora,
                usuario,
            ],
        )

        conexao.execute("COMMIT")

        return {
            "id_projeto": id_projeto,
            "id_versao_estrutura": id_versao,
            "situacao": "registrada",
            "quantidade_perguntas": len(
                perguntas["perguntas"]
            ),
            "quantidade_proposicoes": len(
                proposicoes["proposicoes"]
            ),
            "quantidade_eixos": len(
                eixos["eixos"]
            ),
        }

    except Exception:
        try:
            conexao.execute("ROLLBACK")
        except Exception:
            pass

        raise

    finally:
        conexao.close()


@task(name="registrar-auditoria-estrutura")
def registrar_auditoria(
    resultado: dict[str, Any],
) -> str:
    instante = datetime.now(timezone.utc)
    id_evento = gerar_id_evento()

    evento = {
        "id_evento": id_evento,
        "data_hora_utc": instante.isoformat(),
        "tipo_ator": "script",
        "id_ator": "registrar_estrutura_pesquisa",
        "acao": "registrar_estrutura_pesquisa",
        "id_projeto": resultado["id_projeto"],
        "entidade": "estrutura_pesquisa",
        "id_entidade": resultado[
            "id_versao_estrutura"
        ],
        "situacao": resultado["situacao"],
        "detalhes": resultado,
    }

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
                "registrar_estrutura_pesquisa",
                "registrar_estrutura_pesquisa",
                resultado["id_projeto"],
                "estrutura_pesquisa",
                resultado["id_versao_estrutura"],
                resultado["situacao"],
                json.dumps(
                    resultado,
                    ensure_ascii=False,
                ),
            ],
        )
    finally:
        conexao.close()

    return id_evento


@flow(name="registrar-estrutura-da-pesquisa")
def executar_registro(
    caminho_perguntas: str,
    caminho_proposicoes: str,
    caminho_eixos: str,
) -> dict[str, Any]:
    estrutura = carregar_estrutura(
        caminho_perguntas,
        caminho_proposicoes,
        caminho_eixos,
    )

    validar_estrutura(estrutura)

    resultado = registrar_estrutura(
        estrutura
    )

    resultado["id_evento_auditoria"] = (
        registrar_auditoria(resultado)
    )

    return resultado


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit(
            "Uso: python registrar_estrutura_pesquisa.py "
            "<perguntas.yaml> "
            "<proposicoes.yaml> "
            "<eixos.yaml>"
        )

    resultado_final = executar_registro(
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
    )

    print()
    print("RESULTADO DO REGISTRO")
    print(
        json.dumps(
            resultado_final,
            ensure_ascii=False,
            indent=2,
        )
    )
