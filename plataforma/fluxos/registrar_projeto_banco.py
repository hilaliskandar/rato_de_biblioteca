from __future__ import annotations

import hashlib
import json
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


@task(name="carregar-manifesto-projeto")
def carregar_manifesto(
    caminho_manifesto: str,
) -> tuple[dict[str, Any], str, str]:
    logger = get_run_logger()

    arquivo = Path(caminho_manifesto).resolve()

    if not arquivo.exists():
        raise FileNotFoundError(
            f"Manifesto nao encontrado: {arquivo}"
        )

    dados = yaml.safe_load(
        arquivo.read_text(
            encoding="utf-8-sig"
        )
    )

    if not isinstance(dados, dict):
        raise ValueError(
            "O manifesto deve conter um objeto YAML."
        )

    hash_manifesto = calcular_hash(
        arquivo
    )

    logger.info(
        "Manifesto carregado: %s",
        arquivo,
    )

    return dados, str(arquivo), hash_manifesto


@task(name="validar-manifesto-para-banco")
def validar_manifesto(
    dados: dict[str, Any],
) -> None:
    campos = [
        "id_projeto",
        "nome_curto",
        "titulo",
        "descricao",
        "situacao",
        "tipos_revisao",
        "fontes_descoberta",
        "politica_acesso",
        "padroes_relato",
        "idiomas_busca",
        "controle_humano",
    ]

    ausentes = [
        campo
        for campo in campos
        if campo not in dados
    ]

    if ausentes:
        raise ValueError(
            "Campos obrigatorios ausentes: "
            + ", ".join(ausentes)
        )

    politica = dados["politica_acesso"]

    regras = {
        "somente_acesso_aberto": True,
        "texto_integral_obrigatorio": True,
        "acesso_institucional_aceito": False,
        "copia_privada_aceita": False,
        "verificar_localizacao_aberta": True,
    }

    divergencias = [
        chave
        for chave, esperado in regras.items()
        if politica.get(chave) is not esperado
    ]

    if divergencias:
        raise ValueError(
            "Politica de acesso invalida: "
            + ", ".join(divergencias)
        )


@task(name="gravar-projeto-no-duckdb")
def gravar_projeto(
    dados: dict[str, Any],
    caminho_manifesto: str,
    hash_manifesto: str,
) -> dict[str, Any]:
    logger = get_run_logger()

    if not CAMINHO_BANCO.exists():
        raise FileNotFoundError(
            "Banco ainda nao inicializado."
        )

    id_projeto = dados["id_projeto"]
    agora = datetime.now(timezone.utc)

    conexao = duckdb.connect(
        str(CAMINHO_BANCO)
    )

    try:
        conexao.execute(
            "BEGIN TRANSACTION"
        )

        tabelas_dependentes = [
            "tipos_revisao_projeto",
            "fontes_descoberta_projeto",
            "politicas_acesso_projeto",
            "padroes_relato_projeto",
            "idiomas_busca_projeto",
            "controles_humanos_projeto",
        ]

        for tabela in tabelas_dependentes:
            conexao.execute(
                f"""
                DELETE FROM {tabela}
                WHERE id_projeto = ?
                """,
                [id_projeto],
            )

        registro_existente = conexao.execute(
            """
            SELECT criado_em_utc
            FROM projetos
            WHERE id_projeto = ?
            """,
            [id_projeto],
        ).fetchone()

        criado_em = (
            registro_existente[0]
            if registro_existente
            else agora
        )

        conexao.execute(
            """
            DELETE FROM projetos
            WHERE id_projeto = ?
            """,
            [id_projeto],
        )

        conexao.execute(
            """
            INSERT INTO projetos (
                id_projeto,
                nome_curto,
                titulo,
                descricao,
                situacao,
                projeto_piloto,
                hash_manifesto,
                criado_em_utc,
                atualizado_em_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                id_projeto,
                dados["nome_curto"],
                dados["titulo"],
                dados["descricao"],
                dados["situacao"],
                bool(
                    dados.get(
                        "projeto_piloto",
                        False,
                    )
                ),
                hash_manifesto,
                criado_em,
                agora,
            ],
        )

        for tipo_revisao in dados["tipos_revisao"]:
            conexao.execute(
                """
                INSERT INTO tipos_revisao_projeto
                VALUES (?, ?)
                """,
                [
                    id_projeto,
                    tipo_revisao,
                ],
            )

        for fonte in dados["fontes_descoberta"]:
            conexao.execute(
                """
                INSERT INTO fontes_descoberta_projeto
                VALUES (?, ?)
                """,
                [
                    id_projeto,
                    fonte,
                ],
            )

        politica = dados["politica_acesso"]

        conexao.execute(
            """
            INSERT INTO politicas_acesso_projeto
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                id_projeto,
                politica[
                    "somente_acesso_aberto"
                ],
                politica[
                    "texto_integral_obrigatorio"
                ],
                politica[
                    "acesso_institucional_aceito"
                ],
                politica[
                    "copia_privada_aceita"
                ],
                politica[
                    "verificar_localizacao_aberta"
                ],
            ],
        )

        for etapa, padrao in dados[
            "padroes_relato"
        ].items():
            conexao.execute(
                """
                INSERT INTO padroes_relato_projeto
                VALUES (?, ?, ?)
                """,
                [
                    id_projeto,
                    etapa,
                    padrao,
                ],
            )

        for idioma in dados["idiomas_busca"]:
            conexao.execute(
                """
                INSERT INTO idiomas_busca_projeto
                VALUES (?, ?)
                """,
                [
                    id_projeto,
                    idioma,
                ],
            )

        for chave, valor in dados[
            "controle_humano"
        ].items():
            if isinstance(valor, bool):
                valor_booleano = valor
                valor_texto = None
            else:
                valor_booleano = None
                valor_texto = str(valor)

            conexao.execute(
                """
                INSERT INTO controles_humanos_projeto (
                    id_projeto,
                    chave,
                    valor_booleano,
                    valor_texto
                )
                VALUES (?, ?, ?, ?)
                """,
                [
                    id_projeto,
                    chave,
                    valor_booleano,
                    valor_texto,
                ],
            )

        conexao.execute(
            "COMMIT"
        )

        logger.info(
            "Projeto gravado no banco: %s",
            id_projeto,
        )

        return {
            "id_projeto": id_projeto,
            "situacao": "registrado",
            "manifesto_relativo": (
                Path(caminho_manifesto)
                .relative_to(RAIZ_PLATAFORMA)
                .as_posix()
            ),
            "hash_manifesto": hash_manifesto,
        }

    except Exception:
        conexao.execute(
            "ROLLBACK"
        )
        raise

    finally:
        conexao.close()


@task(name="registrar-auditoria-projeto")
def registrar_auditoria(
    resultado: dict[str, Any],
) -> str:
    logger = get_run_logger()

    instante = datetime.now(timezone.utc)
    id_evento = gerar_id_evento()

    evento = {
        "id_evento": id_evento,
        "data_hora_utc": instante.isoformat(),
        "tipo_ator": "script",
        "id_ator": "registrar_projeto_banco",
        "acao": "registrar_projeto",
        "id_projeto": resultado["id_projeto"],
        "entidade": "projeto",
        "id_entidade": resultado["id_projeto"],
        "situacao": resultado["situacao"],
        "detalhes": resultado,
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
                "registrar_projeto_banco",
                "registrar_projeto",
                resultado["id_projeto"],
                "projeto",
                resultado["id_projeto"],
                resultado["situacao"],
                json.dumps(
                    resultado,
                    ensure_ascii=False,
                ),
            ],
        )
    finally:
        conexao.close()

    logger.info(
        "Auditoria registrada: %s",
        id_evento,
    )

    return id_evento


@flow(name="registrar-projeto-no-banco")
def registrar_projeto(
    caminho_manifesto: str,
) -> dict[str, Any]:
    dados, caminho, hash_manifesto = (
        carregar_manifesto(
            caminho_manifesto
        )
    )

    validar_manifesto(dados)

    resultado = gravar_projeto(
        dados,
        caminho,
        hash_manifesto,
    )

    id_evento = registrar_auditoria(
        resultado
    )

    resultado["id_evento_auditoria"] = (
        id_evento
    )

    return resultado


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(
            "Uso: python registrar_projeto_banco.py "
            "<caminho_projeto.yaml>"
        )

    resultado_final = registrar_projeto(
        sys.argv[1]
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
