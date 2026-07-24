from __future__ import annotations

import argparse
import getpass
import json
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
from prefect import flow, task
from prefect.logging import get_run_logger


RAIZ_PROJETO = Path(__file__).resolve().parents[2]

CAMINHO_BANCO = (
    RAIZ_PROJETO
    / "plataforma"
    / "banco_dados"
    / "rato_de_biblioteca.duckdb"
)

CAMINHO_AUDITORIA = (
    RAIZ_PROJETO
    / "auditoria"
    / "eventos_auditoria.jsonl"
)


def agora_utc() -> datetime:
    return datetime.now(timezone.utc)


def gerar_id(prefixo: str) -> str:
    return f"{prefixo}-{uuid.uuid4().hex.upper()}"


def tornar_json_seguro(
    valor: Any,
) -> Any:
    if isinstance(
        valor,
        (datetime, date),
    ):
        return valor.isoformat()

    if isinstance(valor, Decimal):
        return float(valor)

    if isinstance(valor, dict):
        return {
            chave: tornar_json_seguro(
                conteudo
            )
            for chave, conteudo
            in valor.items()
        }

    if isinstance(valor, list):
        return [
            tornar_json_seguro(item)
            for item in valor
        ]

    if isinstance(valor, tuple):
        return [
            tornar_json_seguro(item)
            for item in valor
        ]

    return valor


def interpretar_json(
    valor: str | None,
) -> Any:
    if not valor:
        return None

    try:
        return json.loads(valor)
    except json.JSONDecodeError:
        return valor


@task(name="selecionar-itens-amostra-precisao")
def selecionar_itens(
    id_versao_consultas: str,
    itens_por_consulta: int,
) -> dict[str, Any]:
    logger = get_run_logger()

    if itens_por_consulta < 1:
        raise ValueError(
            "itens_por_consulta deve ser maior que zero."
        )

    conexao = duckdb.connect(
        str(CAMINHO_BANCO),
        read_only=True,
    )

    try:
        versao = conexao.execute(
            """
            SELECT
                id_projeto,
                id_versao_consultas
            FROM versoes_consultas_descoberta
            WHERE id_versao_consultas = ?
            """,
            [id_versao_consultas],
        ).fetchone()

        if not versao:
            raise ValueError(
                "Versao de consultas nao cadastrada: "
                f"{id_versao_consultas}"
            )

        id_projeto = versao[0]

        cursor = conexao.execute(
            """
            WITH execucoes_ordenadas AS (
                SELECT
                    e.id_execucao,
                    e.id_consulta,
                    e.iniciada_em_utc,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.id_consulta
                        ORDER BY
                            e.iniciada_em_utc DESC,
                            e.id_execucao DESC
                    ) AS ordem_execucao
                FROM execucoes_consulta e
                JOIN consultas c
                    ON c.id_consulta =
                       e.id_consulta
                WHERE c.id_versao_consultas = ?
                  AND e.situacao = 'concluido'
            ),
            candidatos AS (
                SELECT
                    c.id_projeto,
                    c.id_versao_consultas,
                    c.id_consulta,
                    c.id_familia,
                    c.id_eixo,
                    et.titulo AS eixo,
                    c.idioma AS idioma_consulta,
                    c.texto_consulta,
                    e.id_execucao,
                    e.iniciada_em_utc,
                    r.posicao_resultado,
                    r.chave_deduplicacao,
                    r.id_openalex,
                    r.doi_normalizado,
                    r.titulo,
                    r.ano_publicacao,
                    r.idioma,
                    r.tipo_documental,
                    r.citado_por_contagem,
                    r.pontuacao_relevancia,
                    r.is_oa,
                    r.oa_status,
                    r.repositorio_tem_texto_integral,
                    r.url_landing_oa,
                    r.url_pdf_oa,
                    r.licenca,
                    r.autores_json,
                    r.topico_primario_json,
                    r.resumo_metadados_json,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.id_consulta
                        ORDER BY
                            r.posicao_resultado,
                            r.chave_deduplicacao
                    ) AS ordem_na_consulta
                FROM execucoes_ordenadas e
                JOIN consultas c
                    ON c.id_consulta =
                       e.id_consulta
                JOIN resultados_ensaio_consulta r
                    ON r.id_execucao =
                       e.id_execucao
                LEFT JOIN eixos_tematicos et
                    ON et.id_eixo =
                       c.id_eixo
                WHERE e.ordem_execucao = 1
            )
            SELECT *
            FROM candidatos
            WHERE ordem_na_consulta <= ?
            ORDER BY
                id_consulta,
                ordem_na_consulta
            """,
            [
                id_versao_consultas,
                itens_por_consulta,
            ],
        )

        colunas = [
            item[0]
            for item in cursor.description
        ]

        itens = [
            dict(zip(colunas, linha))
            for linha in cursor.fetchall()
        ]

    finally:
        conexao.close()

    if not itens:
        raise RuntimeError(
            "Nenhum resultado de ensaio foi encontrado. "
            "Execute primeiro o ensaio OpenAlex."
        )

    logger.info(
        "%s item(ns) selecionado(s).",
        len(itens),
    )

    return {
        "id_projeto": id_projeto,
        "id_versao_consultas":
            id_versao_consultas,
        "itens_por_consulta":
            itens_por_consulta,
        "itens": itens,
    }


@task(name="registrar-amostra-precisao")
def registrar_amostra(
    pacote: dict[str, Any],
) -> dict[str, Any]:
    id_amostra = gerar_id("AMP")
    instante = agora_utc()
    usuario = getpass.getuser()

    parametros = {
        "itens_por_consulta":
            pacote["itens_por_consulta"],
        "ordenacao":
            "posicao_resultado_ascendente",
        "execucao":
            "ultima_execucao_concluida",
    }

    registros = []

    for ordem_global, item in enumerate(
        pacote["itens"],
        start=1,
    ):
        snapshot = {
            **item,
            "autores":
                interpretar_json(
                    item.get("autores_json")
                ),
            "topico_primario":
                interpretar_json(
                    item.get(
                        "topico_primario_json"
                    )
                ),
            "resumo_metadados":
                interpretar_json(
                    item.get(
                        "resumo_metadados_json"
                    )
                ),
        }

        snapshot.pop(
            "autores_json",
            None,
        )

        snapshot.pop(
            "topico_primario_json",
            None,
        )

        snapshot.pop(
            "resumo_metadados_json",
            None,
        )

        registros.append(
            [
                gerar_id("IAM"),
                id_amostra,
                item["id_execucao"],
                item["id_consulta"],
                item["posicao_resultado"],
                item["chave_deduplicacao"],
                ordem_global,
                json.dumps(
                    tornar_json_seguro(
                        snapshot
                    ),
                    ensure_ascii=False,
                ),
                instante,
            ]
        )

    conexao = duckdb.connect(
        str(CAMINHO_BANCO)
    )

    id_evento = gerar_id("EVT")

    resumo = {
        "id_amostra": id_amostra,
        "id_projeto":
            pacote["id_projeto"],
        "id_versao_consultas":
            pacote[
                "id_versao_consultas"
            ],
        "estrategia": "primeiros_n",
        "itens_por_consulta":
            pacote["itens_por_consulta"],
        "quantidade_itens":
            len(registros),
        "quantidade_consultas":
            len(
                {
                    item["id_consulta"]
                    for item
                    in pacote["itens"]
                }
            ),
        "situacao": "aberta",
    }

    try:
        conexao.execute(
            "BEGIN TRANSACTION"
        )

        conexao.execute(
            """
            INSERT INTO
            amostras_avaliacao_precisao (
                id_amostra,
                id_projeto,
                id_versao_consultas,
                estrategia,
                parametros_json,
                situacao,
                criada_em_utc,
                criada_por,
                concluida_em_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                id_amostra,
                pacote["id_projeto"],
                pacote[
                    "id_versao_consultas"
                ],
                "primeiros_n",
                json.dumps(
                    parametros,
                    ensure_ascii=False,
                ),
                "aberta",
                instante,
                usuario,
                None,
            ],
        )

        conexao.executemany(
            """
            INSERT INTO itens_amostra_precisao (
                id_item_amostra,
                id_amostra,
                id_execucao,
                id_consulta,
                posicao_resultado,
                chave_deduplicacao,
                ordem_item,
                snapshot_json,
                criado_em_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            registros,
        )

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
                "gerar_amostra_precisao",
                "gerar_amostra_avaliacao_precisao",
                pacote["id_projeto"],
                "amostra_precisao",
                id_amostra,
                "aberta",
                json.dumps(
                    resumo,
                    ensure_ascii=False,
                ),
            ],
        )

        conexao.execute(
            "COMMIT"
        )

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

    CAMINHO_AUDITORIA.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    evento = {
        "id_evento": id_evento,
        "data_hora_utc":
            instante.isoformat(),
        "tipo_ator": "script",
        "id_ator":
            "gerar_amostra_precisao",
        "acao":
            "gerar_amostra_avaliacao_precisao",
        "id_projeto":
            pacote["id_projeto"],
        "entidade":
            "amostra_precisao",
        "id_entidade": id_amostra,
        "situacao": "aberta",
        "detalhes": resumo,
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

    resumo[
        "id_evento_auditoria"
    ] = id_evento

    return resumo


@flow(name="gerar-amostra-avaliacao-precisao")
def executar(
    id_versao_consultas: str,
    itens_por_consulta: int,
) -> dict[str, Any]:
    pacote = selecionar_itens(
        id_versao_consultas,
        itens_por_consulta,
    )

    return registrar_amostra(
        pacote
    )


def argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera amostra humana para "
            "avaliacao de precisao."
        )
    )

    parser.add_argument(
        "--id-versao-consultas",
        required=True,
    )

    parser.add_argument(
        "--itens-por-consulta",
        type=int,
        default=5,
    )

    return parser.parse_args()


if __name__ == "__main__":
    opcoes = argumentos()

    resultado = executar(
        id_versao_consultas=
            opcoes.id_versao_consultas,
        itens_por_consulta=
            opcoes.itens_por_consulta,
    )

    print()
    print("RESULTADO DA AMOSTRA")
    print(
        json.dumps(
            resultado,
            ensure_ascii=False,
            indent=2,
        )
    )
