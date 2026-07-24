from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
import unicodedata
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import httpx
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger


RAIZ_PROJETO = Path(__file__).resolve().parents[2]

load_dotenv(
    dotenv_path=RAIZ_PROJETO / ".env",
    override=False,
)

CAMINHO_BANCO = (
    RAIZ_PROJETO
    / "plataforma"
    / "banco_dados"
    / "rato_de_biblioteca.duckdb"
)

PASTA_RESULTADOS = (
    RAIZ_PROJETO
    / "armazenamento"
    / "openalex"
    / "ensaios_consultas"
)

URL_OPENALEX = "https://api.openalex.org/works"

VERSAO_CODIGO = "ensaio-openalex-0.1.0"

CAMPOS_OPENALEX = ",".join(
    [
        "id",
        "doi",
        "display_name",
        "publication_year",
        "publication_date",
        "type",
        "language",
        "cited_by_count",
        "relevance_score",
        "open_access",
        "best_oa_location",
        "primary_location",
        "authorships",
        "primary_topic",
    ]
)

MAXIMO_TENTATIVAS = 4


def agora_utc() -> datetime:
    return datetime.now(timezone.utc)


def gerar_id(prefixo: str) -> str:
    return f"{prefixo}-{uuid.uuid4().hex.upper()}"


def normalizar_doi(
    doi: str | None,
) -> str | None:
    if not doi:
        return None

    valor = doi.strip().lower()

    for prefixo in (
        "https://doi.org/",
        "http://doi.org/",
        "http://dx.doi.org/",
        "doi:",
    ):
        if valor.startswith(prefixo):
            valor = valor[len(prefixo):]
            break

    return valor.strip() or None


def normalizar_texto(
    texto: str | None,
) -> str:
    if not texto:
        return ""

    valor = unicodedata.normalize(
        "NFKD",
        texto,
    )

    valor = "".join(
        caractere
        for caractere in valor
        if not unicodedata.combining(
            caractere
        )
    )

    valor = valor.casefold()

    valor = re.sub(
        r"[^a-z0-9\s]",
        " ",
        valor,
    )

    return re.sub(
        r"\s+",
        " ",
        valor,
    ).strip()


def id_openalex_curto(
    id_openalex: str | None,
) -> str | None:
    if not id_openalex:
        return None

    return id_openalex.rstrip("/").split("/")[-1]


def chave_deduplicacao(
    obra: dict[str, Any],
) -> str:
    id_openalex = id_openalex_curto(
        obra.get("id")
    )

    if id_openalex:
        return f"openalex:{id_openalex}"

    doi = normalizar_doi(
        obra.get("doi")
    )

    if doi:
        return f"doi:{doi}"

    titulo = normalizar_texto(
        obra.get("display_name")
    )

    ano = obra.get(
        "publication_year"
    )

    conteudo = f"{titulo}|{ano}"

    resumo = hashlib.sha256(
        conteudo.encode("utf-8")
    ).hexdigest()[:24]

    return f"titulo_ano:{resumo}"


def hash_arquivo(
    arquivo: Path,
) -> str:
    return hashlib.sha256(
        arquivo.read_bytes()
    ).hexdigest()


def converter_numero(
    valor: str | None,
    tipo: type,
) -> int | float | None:
    if valor is None:
        return None

    try:
        return tipo(valor)
    except (TypeError, ValueError):
        return None


def extrair_cabecalhos_limite(
    resposta: httpx.Response,
) -> dict[str, Any]:
    return {
        "limite_diario": converter_numero(
            resposta.headers.get(
                "X-RateLimit-Limit"
            ),
            float,
        ),
        "limite_restante": converter_numero(
            resposta.headers.get(
                "X-RateLimit-Remaining"
            ),
            float,
        ),
        "creditos_usados": converter_numero(
            resposta.headers.get(
                "X-RateLimit-Credits-Used"
            ),
            float,
        ),
        "reinicio_limite_segundos":
            converter_numero(
                resposta.headers.get(
                    "X-RateLimit-Reset"
                ),
                int,
            ),
    }


def escolher_localizacao(
    obra: dict[str, Any],
) -> dict[str, Any]:
    return (
        obra.get("best_oa_location")
        or obra.get("primary_location")
        or {}
    )


def extrair_autores(
    obra: dict[str, Any],
) -> list[str]:
    nomes: list[str] = []

    for autoria in obra.get(
        "authorships",
        [],
    ):
        autor = autoria.get(
            "author",
            {},
        )

        nome = autor.get(
            "display_name"
        )

        if nome:
            nomes.append(nome)

    return nomes


def interpretar_resultado(
    obra: dict[str, Any],
    posicao: int,
) -> dict[str, Any]:
    acesso = obra.get("open_access") or {}
    localizacao = escolher_localizacao(obra)

    return {
        "posicao_resultado": posicao,
        "chave_deduplicacao":
            chave_deduplicacao(obra),
        "id_openalex": obra.get("id"),
        "doi_normalizado":
            normalizar_doi(
                obra.get("doi")
            ),
        "titulo": (
            obra.get("display_name")
            or "Titulo nao informado"
        ),
        "ano_publicacao":
            obra.get("publication_year"),
        "idioma": obra.get("language"),
        "tipo_documental":
            obra.get("type"),
        "citado_por_contagem":
            obra.get("cited_by_count"),
        "pontuacao_relevancia":
            obra.get("relevance_score"),
        "is_oa": acesso.get("is_oa"),
        "oa_status":
            acesso.get("oa_status"),
        "repositorio_tem_texto_integral":
            acesso.get(
                "any_repository_has_fulltext"
            ),
        "url_landing_oa": (
            localizacao.get(
                "landing_page_url"
            )
            or acesso.get("oa_url")
        ),
        "url_pdf_oa":
            localizacao.get("pdf_url"),
        "licenca": (
            localizacao.get("license")
            or localizacao.get("license_id")
        ),
        "autores":
            extrair_autores(obra),
        "topico_primario":
            obra.get("primary_topic"),
        "resumo_metadados": {
            "publication_date":
                obra.get("publication_date"),
            "localizacao": localizacao,
        },
    }


@task(name="carregar-consultas-para-ensaio")
def carregar_consultas(
    id_versao_consultas: str,
    forcar: bool,
    limite_substituto: int | None,
) -> list[dict[str, Any]]:
    logger = get_run_logger()

    if not CAMINHO_BANCO.exists():
        raise FileNotFoundError(
            f"Banco nao encontrado: {CAMINHO_BANCO}"
        )

    conexao = duckdb.connect(
        str(CAMINHO_BANCO),
        read_only=True,
    )

    try:
        filtro_execucao = ""

        if not forcar:
            filtro_execucao = """
                AND NOT EXISTS (
                    SELECT 1
                    FROM execucoes_consulta e
                    WHERE e.id_consulta =
                          c.id_consulta
                      AND e.situacao =
                          'concluido'
                )
            """

        consulta_sql = f"""
            SELECT
                c.id_projeto,
                c.id_versao_consultas,
                c.id_consulta,
                c.id_familia,
                c.id_eixo,
                c.idioma,
                c.texto_consulta,
                c.filtros_json,
                c.limite_ensaio
            FROM consultas c
            WHERE c.id_versao_consultas = ?
              AND c.situacao IN (
                  'rascunho',
                  'aprovada',
                  'em_teste'
              )
              {filtro_execucao}
            ORDER BY
                c.id_eixo,
                c.idioma,
                c.id_consulta
        """

        cursor = conexao.execute(
            consulta_sql,
            [id_versao_consultas],
        )

        colunas = [
            item[0]
            for item in cursor.description
        ]

        consultas = [
            dict(zip(colunas, linha))
            for linha in cursor.fetchall()
        ]

        for consulta in consultas:
            filtros = json.loads(
                consulta["filtros_json"]
                or "{}"
            )

            consulta["filtro_openalex"] = (
                filtros.get("filter")
            )

            limite = (
                limite_substituto
                if limite_substituto
                is not None
                else consulta[
                    "limite_ensaio"
                ]
            )

            limite = int(limite or 25)

            if limite < 1 or limite > 100:
                raise ValueError(
                    "O limite de ensaio deve "
                    "estar entre 1 e 100."
                )

            consulta["limite_efetivo"] = (
                limite
            )

            sentinelas_cursor = (
                conexao.execute(
                    """
                    SELECT
                        r.id_referencia,
                        r.doi_normalizado,
                        r.id_openalex
                    FROM
                        consultas_sentinelas_esperadas ce
                    JOIN referencias_sentinela r
                        ON r.id_referencia =
                           ce.id_referencia
                    WHERE ce.id_consulta = ?
                    ORDER BY r.id_referencia
                    """,
                    [
                        consulta[
                            "id_consulta"
                        ]
                    ],
                )
            )

            colunas_sentinelas = [
                item[0]
                for item
                in sentinelas_cursor.description
            ]

            consulta["sentinelas"] = [
                dict(
                    zip(
                        colunas_sentinelas,
                        linha,
                    )
                )
                for linha
                in sentinelas_cursor.fetchall()
            ]

    finally:
        conexao.close()

    logger.info(
        "%s consulta(s) preparada(s).",
        len(consultas),
    )

    return consultas


def consultar_uma_vez(
    cliente: httpx.Client,
    chave_api: str,
    consulta: dict[str, Any],
) -> tuple[
    httpx.Response | None,
    dict[str, Any] | None,
    str | None,
]:
    parametros: dict[str, Any] = {
        "api_key": chave_api,
        "search":
            consulta["texto_consulta"],
        "per_page":
            consulta["limite_efetivo"],
        "select": CAMPOS_OPENALEX,
    }

    if consulta.get(
        "filtro_openalex"
    ):
        parametros["filter"] = consulta[
            "filtro_openalex"
        ]

    ultimo_erro: str | None = None

    for tentativa in range(
        1,
        MAXIMO_TENTATIVAS + 1,
    ):
        try:
            resposta = cliente.get(
                URL_OPENALEX,
                params=parametros,
            )

            if resposta.status_code == 200:
                return (
                    resposta,
                    resposta.json(),
                    None,
                )

            if (
                resposta.status_code == 429
                or resposta.status_code >= 500
            ):
                ultimo_erro = (
                    f"HTTP "
                    f"{resposta.status_code}: "
                    f"{resposta.text[:500]}"
                )

                time.sleep(
                    min(
                        2 ** tentativa,
                        20,
                    )
                )
                continue

            return (
                resposta,
                None,
                (
                    f"HTTP "
                    f"{resposta.status_code}: "
                    f"{resposta.text[:1000]}"
                ),
            )

        except httpx.HTTPError as erro:
            ultimo_erro = str(erro)

            if tentativa < MAXIMO_TENTATIVAS:
                time.sleep(
                    min(
                        2 ** tentativa,
                        20,
                    )
                )
                continue

    return None, None, ultimo_erro


@task(name="executar-ensaios-openalex")
def executar_consultas(
    consultas: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    logger = get_run_logger()

    chave_api = os.getenv(
        "OPENALEX_API_KEY",
        "",
    ).strip()

    if not chave_api:
        raise RuntimeError(
            "OPENALEX_API_KEY nao encontrada "
            "no ambiente ou no arquivo .env."
        )

    pacotes: list[
        dict[str, Any]
    ] = []

    with httpx.Client(
        timeout=httpx.Timeout(45.0),
        follow_redirects=True,
        headers={
            "Accept": "application/json",
            "User-Agent":
                "rato-de-biblioteca/0.1",
        },
    ) as cliente:
        for indice, consulta in enumerate(
            consultas,
            start=1,
        ):
            logger.info(
                "[%s/%s] Executando %s",
                indice,
                len(consultas),
                consulta["id_consulta"],
            )

            id_execucao = gerar_id(
                "EXE-OA"
            )

            inicio = agora_utc()

            resposta, dados, erro = (
                consultar_uma_vez(
                    cliente,
                    chave_api,
                    consulta,
                )
            )

            fim = agora_utc()

            codigo_http = (
                resposta.status_code
                if resposta is not None
                else None
            )

            cabecalhos = (
                extrair_cabecalhos_limite(
                    resposta
                )
                if resposta is not None
                else {
                    "limite_diario": None,
                    "limite_restante": None,
                    "creditos_usados": None,
                    "reinicio_limite_segundos":
                        None,
                }
            )

            if dados is None:
                dados = {
                    "meta": {},
                    "results": [],
                    "erro": erro,
                }

            resultados_brutos = (
                dados.get("results")
                or []
            )

            resultados = [
                interpretar_resultado(
                    obra,
                    posicao,
                )
                for posicao, obra
                in enumerate(
                    resultados_brutos,
                    start=1,
                )
            ]

            situacao = (
                "concluido"
                if codigo_http == 200
                else "erro"
            )

            pasta_execucao = (
                PASTA_RESULTADOS
                / consulta[
                    "id_versao_consultas"
                ]
                / id_execucao
            )

            pasta_execucao.mkdir(
                parents=True,
                exist_ok=True,
            )

            arquivo_resposta = (
                pasta_execucao
                / "resposta_openalex.json"
            )

            arquivo_resposta.write_text(
                json.dumps(
                    dados,
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            parametros_seguros = {
                "search":
                    consulta[
                        "texto_consulta"
                    ],
                "filter":
                    consulta.get(
                        "filtro_openalex"
                    ),
                "per_page":
                    consulta[
                        "limite_efetivo"
                    ],
                "select":
                    CAMPOS_OPENALEX,
            }

            pacotes.append(
                {
                    "id_execucao": id_execucao,
                    "consulta": consulta,
                    "inicio": inicio,
                    "fim": fim,
                    "situacao": situacao,
                    "codigo_http": codigo_http,
                    "parametros":
                        parametros_seguros,
                    "meta":
                        dados.get("meta")
                        or {},
                    "cabecalhos":
                        cabecalhos,
                    "resultados":
                        resultados,
                    "arquivo_resposta":
                        str(
                            arquivo_resposta
                        ),
                    "hash_saida":
                        hash_arquivo(
                            arquivo_resposta
                        ),
                    "erro": erro,
                }
            )

    return pacotes


def normalizar_openalex(
    id_openalex: str | None,
) -> str | None:
    return id_openalex_curto(
        id_openalex
    )


def avaliar_sentinelas(
    consulta: dict[str, Any],
    resultados: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    por_openalex: dict[str, int] = {}
    por_doi: dict[str, int] = {}

    for resultado in resultados:
        posicao = resultado[
            "posicao_resultado"
        ]

        openalex = normalizar_openalex(
            resultado.get(
                "id_openalex"
            )
        )

        doi = normalizar_doi(
            resultado.get(
                "doi_normalizado"
            )
        )

        if openalex:
            por_openalex.setdefault(
                openalex,
                posicao,
            )

        if doi:
            por_doi.setdefault(
                doi,
                posicao,
            )

    avaliacoes = []

    for sentinela in consulta[
        "sentinelas"
    ]:
        openalex = normalizar_openalex(
            sentinela.get(
                "id_openalex"
            )
        )

        doi = normalizar_doi(
            sentinela.get(
                "doi_normalizado"
            )
        )

        recuperada = False
        forma = None
        posicao = None

        if (
            openalex
            and openalex in por_openalex
        ):
            recuperada = True
            forma = "id_openalex"
            posicao = por_openalex[
                openalex
            ]

        elif doi and doi in por_doi:
            recuperada = True
            forma = "doi"
            posicao = por_doi[doi]

        avaliacoes.append(
            {
                "id_referencia":
                    sentinela[
                        "id_referencia"
                    ],
                "recuperada":
                    recuperada,
                "forma_recuperacao":
                    forma,
                "posicao_resultado":
                    posicao,
            }
        )

    return avaliacoes


@task(name="persistir-ensaios-openalex")
def persistir_ensaios(
    id_versao_consultas: str,
    pacotes: list[dict[str, Any]],
) -> dict[str, Any]:
    logger = get_run_logger()

    conexao = duckdb.connect(
        str(CAMINHO_BANCO)
    )

    todas_chaves: list[str] = []
    custo_total = 0.0
    sentinelas_esperadas: set[str] = set()
    sentinelas_recuperadas: set[str] = set()
    execucoes_concluidas = 0

    try:
        conexao.execute(
            "BEGIN TRANSACTION"
        )

        for pacote in pacotes:
            consulta = pacote["consulta"]
            meta = pacote["meta"]
            cabecalhos = pacote[
                "cabecalhos"
            ]
            resultados = pacote[
                "resultados"
            ]

            arquivo_relativo = (
                Path(
                    pacote[
                        "arquivo_resposta"
                    ]
                )
                .resolve()
                .relative_to(
                    RAIZ_PROJETO
                )
                .as_posix()
            )

            custo = float(
                meta.get("cost_usd")
                or 0.0
            )

            custo_total += custo

            if (
                pacote["situacao"]
                == "concluido"
            ):
                execucoes_concluidas += 1

            conexao.execute(
                """
                INSERT INTO execucoes_consulta (
                    id_execucao,
                    id_consulta,
                    iniciada_em_utc,
                    encerrada_em_utc,
                    situacao,
                    quantidade_informada,
                    quantidade_recuperada,
                    quantidade_paginas,
                    custo_api,
                    arquivo_manifesto_relativo,
                    hash_saida,
                    versao_codigo,
                    codigo_http,
                    parametros_json,
                    resposta_meta_json,
                    limite_diario,
                    limite_restante,
                    creditos_usados,
                    reinicio_limite_segundos,
                    erro
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    pacote[
                        "id_execucao"
                    ],
                    consulta[
                        "id_consulta"
                    ],
                    pacote["inicio"],
                    pacote["fim"],
                    pacote["situacao"],
                    meta.get("count"),
                    len(resultados),
                    1,
                    custo,
                    arquivo_relativo,
                    pacote["hash_saida"],
                    VERSAO_CODIGO,
                    pacote[
                        "codigo_http"
                    ],
                    json.dumps(
                        pacote[
                            "parametros"
                        ],
                        ensure_ascii=False,
                    ),
                    json.dumps(
                        meta,
                        ensure_ascii=False,
                    ),
                    cabecalhos[
                        "limite_diario"
                    ],
                    cabecalhos[
                        "limite_restante"
                    ],
                    cabecalhos[
                        "creditos_usados"
                    ],
                    cabecalhos[
                        "reinicio_limite_segundos"
                    ],
                    pacote["erro"],
                ],
            )

            for resultado in resultados:
                todas_chaves.append(
                    resultado[
                        "chave_deduplicacao"
                    ]
                )

                conexao.execute(
                    """
                    INSERT INTO
                    resultados_ensaio_consulta (
                        id_execucao,
                        id_consulta,
                        posicao_resultado,
                        chave_deduplicacao,
                        id_openalex,
                        doi_normalizado,
                        titulo,
                        ano_publicacao,
                        idioma,
                        tipo_documental,
                        citado_por_contagem,
                        pontuacao_relevancia,
                        is_oa,
                        oa_status,
                        repositorio_tem_texto_integral,
                        url_landing_oa,
                        url_pdf_oa,
                        licenca,
                        autores_json,
                        topico_primario_json,
                        resumo_metadados_json
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        pacote[
                            "id_execucao"
                        ],
                        consulta[
                            "id_consulta"
                        ],
                        resultado[
                            "posicao_resultado"
                        ],
                        resultado[
                            "chave_deduplicacao"
                        ],
                        resultado[
                            "id_openalex"
                        ],
                        resultado[
                            "doi_normalizado"
                        ],
                        resultado["titulo"],
                        resultado[
                            "ano_publicacao"
                        ],
                        resultado["idioma"],
                        resultado[
                            "tipo_documental"
                        ],
                        resultado[
                            "citado_por_contagem"
                        ],
                        resultado[
                            "pontuacao_relevancia"
                        ],
                        resultado["is_oa"],
                        resultado[
                            "oa_status"
                        ],
                        resultado[
                            "repositorio_tem_texto_integral"
                        ],
                        resultado[
                            "url_landing_oa"
                        ],
                        resultado[
                            "url_pdf_oa"
                        ],
                        resultado["licenca"],
                        json.dumps(
                            resultado[
                                "autores"
                            ],
                            ensure_ascii=False,
                        ),
                        json.dumps(
                            resultado[
                                "topico_primario"
                            ],
                            ensure_ascii=False,
                        ),
                        json.dumps(
                            resultado[
                                "resumo_metadados"
                            ],
                            ensure_ascii=False,
                        ),
                    ],
                )

            avaliacoes = avaliar_sentinelas(
                consulta,
                resultados,
            )

            quantidade_esperada = len(
                avaliacoes
            )

            quantidade_recuperada = sum(
                1
                for item in avaliacoes
                if item["recuperada"]
            )

            for avaliacao in avaliacoes:
                sentinelas_esperadas.add(
                    avaliacao[
                        "id_referencia"
                    ]
                )

                if avaliacao[
                    "recuperada"
                ]:
                    sentinelas_recuperadas.add(
                        avaliacao[
                            "id_referencia"
                        ]
                    )

                conexao.execute(
                    """
                    INSERT INTO
                    recuperacoes_sentinelas_ensaio (
                        id_execucao,
                        id_consulta,
                        id_referencia,
                        recuperada,
                        forma_recuperacao,
                        posicao_resultado,
                        avaliada_em_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        pacote[
                            "id_execucao"
                        ],
                        consulta[
                            "id_consulta"
                        ],
                        avaliacao[
                            "id_referencia"
                        ],
                        avaliacao[
                            "recuperada"
                        ],
                        avaliacao[
                            "forma_recuperacao"
                        ],
                        avaliacao[
                            "posicao_resultado"
                        ],
                        pacote["fim"],
                    ],
                )

            recuperacao = (
                quantidade_recuperada
                / quantidade_esperada
                if quantidade_esperada
                else None
            )

            metricas = [
                (
                    "quantidade_total_api",
                    meta.get("count"),
                    None,
                ),
                (
                    "quantidade_recuperada",
                    len(resultados),
                    None,
                ),
                (
                    "custo_usd",
                    custo,
                    None,
                ),
                (
                    "recuperacao_sentinelas",
                    recuperacao,
                    (
                        "Sem sentinelas esperadas."
                        if recuperacao is None
                        else None
                    ),
                ),
            ]

            for (
                tipo_avaliacao,
                valor_numerico,
                valor_texto,
            ) in metricas:
                conexao.execute(
                    """
                    INSERT INTO avaliacoes_consultas (
                        id_avaliacao,
                        id_projeto,
                        id_consulta,
                        tipo_avaliacao,
                        valor_numerico,
                        valor_texto,
                        tamanho_amostra,
                        avaliada_em_utc,
                        tipo_ator,
                        id_ator,
                        detalhes_json
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    [
                        gerar_id("AVA-CON"),
                        consulta[
                            "id_projeto"
                        ],
                        consulta[
                            "id_consulta"
                        ],
                        tipo_avaliacao,
                        valor_numerico,
                        valor_texto,
                        len(resultados),
                        pacote["fim"],
                        "script",
                        "executar_ensaio_consultas",
                        json.dumps(
                            {
                                "id_execucao":
                                    pacote[
                                        "id_execucao"
                                    ],
                                "id_versao_consultas":
                                    id_versao_consultas,
                            },
                            ensure_ascii=False,
                        ),
                    ],
                )

        total_resultados = len(
            todas_chaves
        )

        total_unicos = len(
            set(todas_chaves)
        )

        taxa_duplicacao = (
            1
            - (
                total_unicos
                / total_resultados
            )
            if total_resultados
            else 0.0
        )

        quantidade_sentinelas = len(
            sentinelas_esperadas
        )

        quantidade_sentinelas_recuperadas = (
            len(
                sentinelas_recuperadas
            )
        )

        recuperacao_geral = (
            quantidade_sentinelas_recuperadas
            / quantidade_sentinelas
            if quantidade_sentinelas
            else None
        )

        id_projeto = (
            pacotes[0]["consulta"][
                "id_projeto"
            ]
            if pacotes
            else None
        )

        avaliacao_versao = {
            "quantidade_consultas":
                len(pacotes),
            "quantidade_execucoes_concluidas":
                execucoes_concluidas,
            "quantidade_resultados":
                total_resultados,
            "quantidade_resultados_unicos":
                total_unicos,
            "taxa_duplicacao":
                taxa_duplicacao,
            "quantidade_sentinelas_esperadas":
                quantidade_sentinelas,
            "quantidade_sentinelas_recuperadas":
                quantidade_sentinelas_recuperadas,
            "recuperacao_sentinelas":
                recuperacao_geral,
            "custo_total_usd":
                custo_total,
        }

        conexao.execute(
            """
            INSERT INTO avaliacoes_versoes_consultas (
                id_avaliacao_versao,
                id_projeto,
                id_versao_consultas,
                quantidade_consultas,
                quantidade_execucoes_concluidas,
                quantidade_resultados,
                quantidade_resultados_unicos,
                taxa_duplicacao,
                quantidade_sentinelas_esperadas,
                quantidade_sentinelas_recuperadas,
                recuperacao_sentinelas,
                custo_total_usd,
                avaliada_em_utc,
                detalhes_json
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                gerar_id("AVA-VCQ"),
                id_projeto,
                id_versao_consultas,
                avaliacao_versao[
                    "quantidade_consultas"
                ],
                avaliacao_versao[
                    "quantidade_execucoes_concluidas"
                ],
                total_resultados,
                total_unicos,
                taxa_duplicacao,
                quantidade_sentinelas,
                quantidade_sentinelas_recuperadas,
                recuperacao_geral,
                custo_total,
                agora_utc(),
                json.dumps(
                    avaliacao_versao,
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

    pasta_versao = (
        PASTA_RESULTADOS
        / id_versao_consultas
    )

    pasta_versao.mkdir(
        parents=True,
        exist_ok=True,
    )

    instante = agora_utc()

    arquivo_manifesto = (
        pasta_versao
        / (
            "manifesto_ensaio_"
            + instante.strftime(
                "%Y%m%dT%H%M%S%fZ"
            )
            + ".json"
        )
    )

    resumo = {
        "id_versao_consultas":
            id_versao_consultas,
        "executado_em_utc":
            instante.isoformat(),
        **avaliacao_versao,
        "execucoes": [
            {
                "id_execucao":
                    pacote[
                        "id_execucao"
                    ],
                "id_consulta":
                    pacote[
                        "consulta"
                    ]["id_consulta"],
                "situacao":
                    pacote["situacao"],
                "codigo_http":
                    pacote[
                        "codigo_http"
                    ],
                "quantidade_recuperada":
                    len(
                        pacote[
                            "resultados"
                        ]
                    ),
                "custo_usd":
                    pacote[
                        "meta"
                    ].get(
                        "cost_usd"
                    ),
                "arquivo_resposta":
                    Path(
                        pacote[
                            "arquivo_resposta"
                        ]
                    )
                    .resolve()
                    .relative_to(
                        RAIZ_PROJETO
                    )
                    .as_posix(),
                "erro":
                    pacote["erro"],
            }
            for pacote in pacotes
        ],
    }

    arquivo_manifesto.write_text(
        json.dumps(
            resumo,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Manifesto do ensaio: %s",
        arquivo_manifesto,
    )

    resumo[
        "manifesto_relativo"
    ] = arquivo_manifesto.relative_to(
        RAIZ_PROJETO
    ).as_posix()

    return resumo


@flow(name="executar-ensaio-consultas-openalex")
def executar_fluxo(
    id_versao_consultas: str,
    forcar: bool = False,
    limite: int | None = None,
) -> dict[str, Any]:
    consultas = carregar_consultas(
        id_versao_consultas=
            id_versao_consultas,
        forcar=forcar,
        limite_substituto=limite,
    )

    if not consultas:
        return {
            "id_versao_consultas":
                id_versao_consultas,
            "situacao":
                "nenhuma_consulta_pendente",
            "quantidade_consultas": 0,
        }

    pacotes = executar_consultas(
        consultas
    )

    resultado = persistir_ensaios(
        id_versao_consultas,
        pacotes,
    )

    resultado["situacao"] = (
        "concluido"
    )

    return resultado


def construir_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Executa ensaios limitados das "
            "consultas OpenAlex."
        )
    )

    parser.add_argument(
        "--id-versao-consultas",
        required=True,
    )

    parser.add_argument(
        "--limite",
        type=int,
        default=None,
        help=(
            "Substitui temporariamente o "
            "limite configurado, entre 1 e 100."
        ),
    )

    parser.add_argument(
        "--forcar",
        action="store_true",
        help=(
            "Executa novamente consultas que "
            "ja possuem ensaio concluido."
        ),
    )

    return parser.parse_args()


if __name__ == "__main__":
    argumentos = construir_argumentos()

    resultado_final = executar_fluxo(
        id_versao_consultas=
            argumentos.id_versao_consultas,
        forcar=argumentos.forcar,
        limite=argumentos.limite,
    )

    print()
    print("RESULTADO DO ENSAIO")
    print(
        json.dumps(
            resultado_final,
            ensure_ascii=False,
            indent=2,
        )
    )
