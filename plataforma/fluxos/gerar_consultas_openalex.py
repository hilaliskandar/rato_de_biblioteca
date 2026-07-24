from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import yaml
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

PADRAO_VERSAO = re.compile(
    r"^VCQ-\d{4}-\d{4}$"
)

IDIOMAS_PERMITIDOS = {
    "pt",
    "en",
    "es",
}


def agora_utc() -> datetime:
    return datetime.now(timezone.utc)


def calcular_hash_bytes(
    conteudo: bytes,
) -> str:
    return hashlib.sha256(
        conteudo
    ).hexdigest()


def calcular_hash_arquivo(
    arquivo: Path,
) -> str:
    return calcular_hash_bytes(
        arquivo.read_bytes()
    )


def gerar_id_evento() -> str:
    return (
        "EVT-"
        + uuid.uuid4().hex.upper()
    )


def carregar_yaml(
    arquivo: Path,
) -> dict[str, Any]:
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

    return dados


def citar_termo(
    termo: str,
) -> str:
    valor = termo.strip().replace(
        '"',
        '\\"',
    )

    if " " in valor:
        return f'"{valor}"'

    return valor


def bloco_booleano(
    termos: list[str],
) -> str:
    termos_unicos = sorted(
        {
            termo.strip()
            for termo in termos
            if termo.strip()
        },
        key=str.casefold,
    )

    if not termos_unicos:
        raise ValueError(
            "Nao e possivel criar bloco vazio."
        )

    expressoes = [
        citar_termo(termo)
        for termo in termos_unicos
    ]

    if len(expressoes) == 1:
        return expressoes[0]

    return (
        "("
        + " OR ".join(expressoes)
        + ")"
    )


def numero_projeto(
    id_projeto: str,
) -> str:
    correspondencia = re.fullmatch(
        r"PRJ-(\d{4})",
        id_projeto,
    )

    if not correspondencia:
        raise ValueError(
            f"id_projeto invalido: {id_projeto}"
        )

    return correspondencia.group(1)


@task(name="carregar-configuracao-consultas")
def carregar_configuracao(
    caminho_configuracao: str,
) -> dict[str, Any]:
    logger = get_run_logger()

    arquivo = Path(
        caminho_configuracao
    ).resolve()

    configuracao = carregar_yaml(
        arquivo
    )

    logger.info(
        "Configuracao carregada: %s",
        arquivo,
    )

    return {
        "configuracao": configuracao,
        "arquivo": str(arquivo),
        "hash_configuracao":
            calcular_hash_arquivo(arquivo),
    }


@task(name="carregar-base-do-vocabulario")
def carregar_base(
    configuracao: dict[str, Any],
) -> dict[str, Any]:
    id_projeto = configuracao[
        "id_projeto"
    ]

    id_versao_vocabulario = configuracao[
        "id_versao_vocabulario"
    ]

    conexao = duckdb.connect(
        str(CAMINHO_BANCO),
        read_only=True,
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

        versao_vocabulario = conexao.execute(
            """
            SELECT id_versao_vocabulario
            FROM versoes_vocabulario_controlado
            WHERE id_versao_vocabulario = ?
              AND id_projeto = ?
            """,
            [
                id_versao_vocabulario,
                id_projeto,
            ],
        ).fetchone()

        if not versao_vocabulario:
            raise ValueError(
                "Versao do vocabulario nao cadastrada: "
                f"{id_versao_vocabulario}"
            )

        cursor_termos = conexao.execute(
            """
            SELECT
                t.id_termo,
                t.idioma,
                t.termo,
                t.tipo_termo,
                te.id_eixo
            FROM termos_vocabulario t
            JOIN termos_vocabulario_eixos te
                ON te.id_termo = t.id_termo
            WHERE t.id_projeto = ?
              AND t.id_versao_vocabulario = ?
              AND t.situacao = 'ativo'
            ORDER BY
                te.id_eixo,
                t.idioma,
                t.id_termo
            """,
            [
                id_projeto,
                id_versao_vocabulario,
            ],
        )

        colunas_termos = [
            coluna[0]
            for coluna
            in cursor_termos.description
        ]

        termos = [
            dict(zip(colunas_termos, linha))
            for linha
            in cursor_termos.fetchall()
        ]

        cursor_eixos = conexao.execute(
            """
            SELECT
                id_eixo,
                titulo
            FROM eixos_tematicos
            WHERE id_projeto = ?
              AND situacao = 'ativo'
            """,
            [id_projeto],
        )

        eixos = {
            linha[0]: linha[1]
            for linha
            in cursor_eixos.fetchall()
        }

        cursor_sentinelas = conexao.execute(
            """
            SELECT
                r.id_referencia,
                re.id_eixo
            FROM referencias_sentinela r
            JOIN referencias_sentinela_eixos re
                ON re.id_referencia =
                   r.id_referencia
            WHERE r.id_projeto = ?
              AND r.id_versao_vocabulario = ?
              AND r.situacao = 'ativa'
            """,
            [
                id_projeto,
                id_versao_vocabulario,
            ],
        )

        sentinelas_eixos: dict[
            str,
            set[str],
        ] = {}

        for id_referencia, id_eixo in (
            cursor_sentinelas.fetchall()
        ):
            sentinelas_eixos.setdefault(
                id_referencia,
                set(),
            ).add(id_eixo)

    finally:
        conexao.close()

    return {
        "termos": termos,
        "eixos": eixos,
        "sentinelas_eixos": {
            chave: sorted(valor)
            for chave, valor
            in sentinelas_eixos.items()
        },
    }


@task(name="validar-configuracao-consultas")
def validar_configuracao(
    configuracao: dict[str, Any],
    base: dict[str, Any],
) -> None:
    campos = [
        "id_projeto",
        "id_versao_vocabulario",
        "id_versao_consultas",
        "numero_versao",
        "situacao_versao",
        "politica",
        "idiomas",
        "eixos_iniciais",
        "termos_contexto_habitacional",
        "sentinelas_esperadas_por_idioma",
    ]

    ausentes = [
        campo
        for campo in campos
        if campo not in configuracao
    ]

    if ausentes:
        raise ValueError(
            "Campos ausentes: "
            + ", ".join(ausentes)
        )

    if not PADRAO_VERSAO.fullmatch(
        str(
            configuracao[
                "id_versao_consultas"
            ]
        )
    ):
        raise ValueError(
            "id_versao_consultas fora do padrao."
        )

    idiomas = configuracao["idiomas"]

    if not isinstance(idiomas, list):
        raise ValueError(
            "idiomas deve ser uma lista."
        )

    idiomas_invalidos = (
        set(idiomas)
        - IDIOMAS_PERMITIDOS
    )

    if idiomas_invalidos:
        raise ValueError(
            "Idiomas invalidos: "
            + ", ".join(
                sorted(idiomas_invalidos)
            )
        )

    eixos_disponiveis = set(
        base["eixos"]
    )

    eixos_informados = set(
        configuracao["eixos_iniciais"]
    )

    eixos_ausentes = (
        eixos_informados
        - eixos_disponiveis
    )

    if eixos_ausentes:
        raise ValueError(
            "Eixos nao cadastrados: "
            + ", ".join(
                sorted(eixos_ausentes)
            )
        )

    termos_por_id = {
        item["id_termo"]: item
        for item in base["termos"]
    }

    for idioma in idiomas:
        ids_contexto = configuracao[
            "termos_contexto_habitacional"
        ].get(idioma, [])

        if not ids_contexto:
            raise ValueError(
                "Nenhum termo de contexto para "
                f"o idioma {idioma}."
            )

        for id_termo in ids_contexto:
            if id_termo not in termos_por_id:
                raise ValueError(
                    "Termo de contexto nao cadastrado: "
                    f"{id_termo}"
                )

            idioma_termo = termos_por_id[
                id_termo
            ]["idioma"]

            if idioma_termo != idioma:
                raise ValueError(
                    f"{id_termo} pertence a "
                    f"{idioma_termo}, nao a {idioma}."
                )


@task(name="gerar-consultas-versionadas")
def gerar_consultas(
    configuracao: dict[str, Any],
    base: dict[str, Any],
) -> dict[str, Any]:
    id_projeto = configuracao[
        "id_projeto"
    ]

    numero = numero_projeto(
        id_projeto
    )

    termos_por_id = {
        item["id_termo"]: item
        for item in base["termos"]
    }

    termos_por_eixo_idioma: dict[
        tuple[str, str],
        list[dict[str, Any]],
    ] = {}

    for item in base["termos"]:
        chave = (
            item["id_eixo"],
            item["idioma"],
        )

        termos_por_eixo_idioma.setdefault(
            chave,
            [],
        ).append(item)

    filtros = configuracao[
        "politica"
    ]["filtros_openalex"]

    filtro_openalex = ",".join(
        filtros
    )

    consultas: list[
        dict[str, Any]
    ] = []

    familias: list[
        dict[str, Any]
    ] = []

    contador_consulta = 0

    for indice_eixo, id_eixo in enumerate(
        configuracao["eixos_iniciais"],
        start=1,
    ):
        id_familia = (
            f"FAM-{numero}-"
            f"{indice_eixo:03d}"
        )

        familias.append(
            {
                "id_familia": id_familia,
                "id_eixo": id_eixo,
                "titulo": base["eixos"][
                    id_eixo
                ],
                "objetivo": (
                    "Recuperar estudos relacionados "
                    "ao eixo tematico e aos resultados "
                    "habitacionais."
                ),
                "situacao": "rascunho",
            }
        )

        for idioma in configuracao[
            "idiomas"
        ]:
            ids_contexto = configuracao[
                "termos_contexto_habitacional"
            ][idioma]

            conjunto_contexto = set(
                ids_contexto
            )

            termos_eixo = [
                item
                for item
                in termos_por_eixo_idioma.get(
                    (id_eixo, idioma),
                    [],
                )
                if item["id_termo"]
                not in conjunto_contexto
            ]

            if not termos_eixo:
                continue

            termos_contexto = [
                termos_por_id[id_termo]
                for id_termo
                in ids_contexto
            ]

            bloco_eixo = bloco_booleano(
                [
                    item["termo"]
                    for item
                    in termos_eixo
                ]
            )

            bloco_contexto = bloco_booleano(
                [
                    item["termo"]
                    for item
                    in termos_contexto
                ]
            )

            texto_consulta = (
                f"{bloco_eixo} "
                f"AND {bloco_contexto}"
            )

            contador_consulta += 1

            id_consulta = (
                f"CON-{numero}-"
                f"{contador_consulta:03d}-"
                f"{idioma.upper()}"
            )

            sentinelas_idioma = configuracao[
                "sentinelas_esperadas_por_idioma"
            ].get(idioma, [])

            sentinelas_esperadas = []

            for id_referencia in (
                sentinelas_idioma
            ):
                eixos_referencia = set(
                    base[
                        "sentinelas_eixos"
                    ].get(
                        id_referencia,
                        [],
                    )
                )

                if id_eixo in eixos_referencia:
                    sentinelas_esperadas.append(
                        id_referencia
                    )

            dados_hash = {
                "id_consulta": id_consulta,
                "id_eixo": id_eixo,
                "idioma": idioma,
                "parametro_busca":
                    configuracao[
                        "politica"
                    ]["parametro_busca"],
                "texto_consulta":
                    texto_consulta,
                "filtro_openalex":
                    filtro_openalex,
                "limite_ensaio":
                    configuracao[
                        "politica"
                    ]["limite_ensaio"],
            }

            hash_consulta = (
                calcular_hash_bytes(
                    json.dumps(
                        dados_hash,
                        ensure_ascii=False,
                        sort_keys=True,
                    ).encode("utf-8")
                )
            )

            consultas.append(
                {
                    **dados_hash,
                    "id_familia": id_familia,
                    "modalidade":
                        "busca_textual_controlada",
                    "ordenacao":
                        configuracao[
                            "politica"
                        ]["ordenacao"],
                    "situacao": "rascunho",
                    "hash_consulta":
                        hash_consulta,
                    "termos_eixo": [
                        item["id_termo"]
                        for item
                        in termos_eixo
                    ],
                    "termos_contexto":
                        ids_contexto,
                    "sentinelas_esperadas":
                        sorted(
                            sentinelas_esperadas
                        ),
                }
            )

    if not consultas:
        raise RuntimeError(
            "Nenhuma consulta foi gerada."
        )

    return {
        "id_projeto": id_projeto,
        "id_versao_vocabulario":
            configuracao[
                "id_versao_vocabulario"
            ],
        "id_versao_consultas":
            configuracao[
                "id_versao_consultas"
            ],
        "numero_versao": str(
            configuracao[
                "numero_versao"
            ]
        ),
        "situacao_versao":
            configuracao[
                "situacao_versao"
            ],
        "politica": configuracao[
            "politica"
        ],
        "familias": familias,
        "consultas": consultas,
    }


@task(name="gravar-arquivo-consultas")
def gravar_arquivo(
    gerado: dict[str, Any],
    caminho_configuracao: str,
) -> dict[str, Any]:
    arquivo_configuracao = Path(
        caminho_configuracao
    ).resolve()

    arquivo_saida = (
        arquivo_configuracao.parent
        / "consultas_geradas.yaml"
    )

    texto = yaml.safe_dump(
        gerado,
        allow_unicode=True,
        sort_keys=False,
        width=1000,
    )

    arquivo_saida.write_text(
        texto,
        encoding="utf-8",
    )

    return {
        "arquivo_saida":
            str(arquivo_saida),
        "hash_consultas":
            calcular_hash_arquivo(
                arquivo_saida
            ),
    }


@task(name="registrar-consultas-no-banco")
def registrar_no_banco(
    gerado: dict[str, Any],
    caminho_configuracao: str,
    hash_configuracao: str,
    arquivo_saida: str,
    hash_consultas: str,
) -> dict[str, Any]:
    conexao = duckdb.connect(
        str(CAMINHO_BANCO)
    )

    id_versao = gerado[
        "id_versao_consultas"
    ]

    id_projeto = gerado[
        "id_projeto"
    ]

    agora = agora_utc()

    try:
        existente = conexao.execute(
            """
            SELECT
                hash_configuracao,
                hash_consultas
            FROM versoes_consultas_descoberta
            WHERE id_versao_consultas = ?
            """,
            [id_versao],
        ).fetchone()

        if existente:
            if tuple(existente) != (
                hash_configuracao,
                hash_consultas,
            ):
                raise RuntimeError(
                    "A versao ja foi registrada, "
                    "mas seus arquivos foram alterados."
                )

            return {
                "id_projeto": id_projeto,
                "id_versao_consultas":
                    id_versao,
                "situacao":
                    "ja_registrada",
                "quantidade_familias": len(
                    gerado["familias"]
                ),
                "quantidade_consultas": len(
                    gerado["consultas"]
                ),
            }

        conexao.execute(
            "BEGIN TRANSACTION"
        )

        conexao.execute(
            """
            UPDATE versoes_consultas_descoberta
            SET situacao = 'substituida'
            WHERE id_projeto = ?
              AND situacao <> 'substituida'
            """,
            [id_projeto],
        )

        conteudo_json = json.dumps(
            gerado,
            ensure_ascii=False,
        )

        conexao.execute(
            """
            INSERT INTO versoes_consultas_descoberta (
                id_versao_consultas,
                id_projeto,
                id_versao_vocabulario,
                numero_versao,
                situacao,
                hash_configuracao,
                hash_consultas,
                conteudo_json,
                criada_em_utc,
                criada_por
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                id_versao,
                id_projeto,
                gerado[
                    "id_versao_vocabulario"
                ],
                gerado["numero_versao"],
                gerado[
                    "situacao_versao"
                ],
                hash_configuracao,
                hash_consultas,
                conteudo_json,
                agora,
                getpass.getuser(),
            ],
        )

        for familia in gerado[
            "familias"
        ]:
            conexao.execute(
                """
                INSERT INTO
                familias_consultas_descoberta (
                    id_familia,
                    id_projeto,
                    id_versao_consultas,
                    id_eixo,
                    titulo,
                    objetivo,
                    situacao
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    familia["id_familia"],
                    id_projeto,
                    id_versao,
                    familia["id_eixo"],
                    familia["titulo"],
                    familia["objetivo"],
                    familia["situacao"],
                ],
            )

        for consulta in gerado[
            "consultas"
        ]:
            filtros_json = json.dumps(
                {
                    "filter":
                        consulta[
                            "filtro_openalex"
                        ]
                },
                ensure_ascii=False,
            )

            conexao.execute(
                """
                INSERT INTO consultas (
                    id_consulta,
                    id_projeto,
                    id_eixo,
                    idioma,
                    modalidade,
                    texto_consulta,
                    filtros_json,
                    situacao,
                    hash_consulta,
                    criada_em_utc,
                    congelada_em_utc,
                    id_versao_consultas,
                    id_familia,
                    parametro_busca,
                    ordenacao,
                    limite_ensaio,
                    aprovada_humanamente
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    consulta["id_consulta"],
                    id_projeto,
                    consulta["id_eixo"],
                    consulta["idioma"],
                    consulta["modalidade"],
                    consulta[
                        "texto_consulta"
                    ],
                    filtros_json,
                    consulta["situacao"],
                    consulta[
                        "hash_consulta"
                    ],
                    agora,
                    None,
                    id_versao,
                    consulta["id_familia"],
                    consulta[
                        "parametro_busca"
                    ],
                    consulta["ordenacao"],
                    consulta[
                        "limite_ensaio"
                    ],
                    False,
                ],
            )

            for id_termo in consulta[
                "termos_eixo"
            ]:
                conexao.execute(
                    """
                    INSERT INTO consultas_termos
                    VALUES (?, ?, ?)
                    """,
                    [
                        consulta[
                            "id_consulta"
                        ],
                        id_termo,
                        "eixo",
                    ],
                )

            for id_termo in consulta[
                "termos_contexto"
            ]:
                conexao.execute(
                    """
                    INSERT INTO consultas_termos
                    VALUES (?, ?, ?)
                    """,
                    [
                        consulta[
                            "id_consulta"
                        ],
                        id_termo,
                        "contexto_habitacional",
                    ],
                )

            for id_referencia in consulta[
                "sentinelas_esperadas"
            ]:
                conexao.execute(
                    """
                    INSERT INTO
                    consultas_sentinelas_esperadas (
                        id_consulta,
                        id_referencia,
                        motivo
                    )
                    VALUES (?, ?, ?)
                    """,
                    [
                        consulta[
                            "id_consulta"
                        ],
                        id_referencia,
                        (
                            "Referencia compartilha "
                            "eixo e idioma preferencial."
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

    return {
        "id_projeto": id_projeto,
        "id_versao_consultas":
            id_versao,
        "situacao": "registrada",
        "quantidade_familias": len(
            gerado["familias"]
        ),
        "quantidade_consultas": len(
            gerado["consultas"]
        ),
        "arquivo_configuracao_relativo":
            Path(
                caminho_configuracao
            ).resolve().relative_to(
                RAIZ_PROJETO
            ).as_posix(),
        "arquivo_consultas_relativo":
            Path(
                arquivo_saida
            ).resolve().relative_to(
                RAIZ_PROJETO
            ).as_posix(),
    }


@task(name="auditar-geracao-consultas")
def registrar_auditoria(
    resultado: dict[str, Any],
) -> str:
    instante = agora_utc()
    id_evento = gerar_id_evento()

    evento = {
        "id_evento": id_evento,
        "data_hora_utc":
            instante.isoformat(),
        "tipo_ator": "script",
        "id_ator":
            "gerar_consultas_openalex",
        "acao":
            "gerar_consultas_descoberta",
        "id_projeto":
            resultado["id_projeto"],
        "entidade":
            "versao_consultas",
        "id_entidade":
            resultado[
                "id_versao_consultas"
            ],
        "situacao":
            resultado["situacao"],
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
                "gerar_consultas_openalex",
                "gerar_consultas_descoberta",
                resultado["id_projeto"],
                "versao_consultas",
                resultado[
                    "id_versao_consultas"
                ],
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


@flow(name="gerar-consultas-openalex")
def executar(
    caminho_configuracao: str,
) -> dict[str, Any]:
    pacote = carregar_configuracao(
        caminho_configuracao
    )

    configuracao = pacote[
        "configuracao"
    ]

    base = carregar_base(
        configuracao
    )

    validar_configuracao(
        configuracao,
        base,
    )

    gerado = gerar_consultas(
        configuracao,
        base,
    )

    saida = gravar_arquivo(
        gerado,
        caminho_configuracao,
    )

    resultado = registrar_no_banco(
        gerado=gerado,
        caminho_configuracao=
            caminho_configuracao,
        hash_configuracao=
            pacote["hash_configuracao"],
        arquivo_saida=
            saida["arquivo_saida"],
        hash_consultas=
            saida["hash_consultas"],
    )

    resultado[
        "id_evento_auditoria"
    ] = registrar_auditoria(
        resultado
    )

    return resultado


def argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera consultas OpenAlex "
            "a partir do vocabulario controlado."
        )
    )

    parser.add_argument(
        "--configuracao",
        required=True,
    )

    return parser.parse_args()


if __name__ == "__main__":
    opcoes = argumentos()

    resultado_final = executar(
        opcoes.configuracao
    )

    print()
    print("RESULTADO DA GERACAO")
    print(
        json.dumps(
            resultado_final,
            ensure_ascii=False,
            indent=2,
        )
    )
