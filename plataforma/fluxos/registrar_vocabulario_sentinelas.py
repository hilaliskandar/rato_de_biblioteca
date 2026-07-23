from __future__ import annotations

import getpass
import hashlib
import json
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import yaml
from prefect import flow, task


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

PADRAO_VERSAO = re.compile(
    r"^VOC-\d{4}-\d{4}$"
)

PADRAO_TERMO = re.compile(
    r"^TER-\d{4}-\d{4}$"
)

PADRAO_REFERENCIA = re.compile(
    r"^REF-\d{4}-\d{3}$"
)

PADRAO_DOI = re.compile(
    r"^10\.\d{4,9}/\S+$",
    flags=re.IGNORECASE,
)

IDIOMAS_PERMITIDOS = {
    "pt",
    "en",
    "es",
}

TIPOS_TERMO_PERMITIDOS = {
    "descritor",
    "sinonimo",
    "expressao",
}

ESTADOS_ACESSO_PERMITIDOS = {
    "a_verificar",
    "verificado_aberto",
    "sem_texto_aberto",
    "divergente",
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

    return dados, arquivo, calcular_hash(arquivo)


def normalizar_texto(texto: str) -> str:
    texto = unicodedata.normalize(
        "NFKD",
        texto,
    )

    texto = "".join(
        caractere
        for caractere in texto
        if not unicodedata.combining(
            caractere
        )
    )

    texto = texto.casefold()
    texto = re.sub(
        r"\s+",
        " ",
        texto,
    ).strip()

    return texto


def normalizar_doi(
    doi: str | None,
) -> str | None:
    if not doi:
        return None

    doi = doi.strip().lower()

    prefixos = [
        "https://doi.org/",
        "http://doi.org/",
        "doi:",
    ]

    for prefixo in prefixos:
        if doi.startswith(prefixo):
            doi = doi[len(prefixo):]

    return doi.strip()


def gerar_id_evento() -> str:
    return (
        "EVT-"
        + datetime.now(timezone.utc).strftime(
            "%Y%m%dT%H%M%S%fZ"
        )
    )


@task(name="carregar-vocabulario-e-sentinelas")
def carregar_conteudo(
    caminho_vocabulario: str,
    caminho_sentinelas: str,
) -> dict[str, Any]:
    vocabulario, arquivo_vocabulario, hash_vocabulario = (
        carregar_yaml(caminho_vocabulario)
    )

    sentinelas, arquivo_sentinelas, hash_sentinelas = (
        carregar_yaml(caminho_sentinelas)
    )

    return {
        "vocabulario": vocabulario,
        "sentinelas": sentinelas,
        "arquivos": {
            "vocabulario": str(
                arquivo_vocabulario
            ),
            "sentinelas": str(
                arquivo_sentinelas
            ),
        },
        "hashes": {
            "vocabulario": hash_vocabulario,
            "sentinelas": hash_sentinelas,
        },
    }


@task(name="validar-vocabulario-e-sentinelas")
def validar_conteudo(
    conteudo: dict[str, Any],
) -> None:
    vocabulario = conteudo["vocabulario"]
    sentinelas = conteudo["sentinelas"]

    campos_comuns = [
        "id_projeto",
        "id_versao_vocabulario",
        "numero_versao",
        "situacao_versao",
    ]

    for campo in campos_comuns:
        if vocabulario.get(campo) != sentinelas.get(campo):
            raise ValueError(
                f"Divergencia entre arquivos no campo: {campo}"
            )

    id_versao = vocabulario[
        "id_versao_vocabulario"
    ]

    if not PADRAO_VERSAO.fullmatch(
        str(id_versao)
    ):
        raise ValueError(
            "id_versao_vocabulario fora do padrao."
        )

    termos = vocabulario.get("termos")

    if not isinstance(termos, list) or not termos:
        raise ValueError(
            "A lista de termos esta ausente ou vazia."
        )

    ids_termos: set[str] = set()
    termos_normalizados: set[
        tuple[str, str]
    ] = set()

    for termo in termos:
        id_termo = str(
            termo.get("id_termo", "")
        )

        if not PADRAO_TERMO.fullmatch(id_termo):
            raise ValueError(
                f"id_termo invalido: {id_termo}"
            )

        if id_termo in ids_termos:
            raise ValueError(
                f"id_termo duplicado: {id_termo}"
            )

        ids_termos.add(id_termo)

        idioma = termo.get("idioma")

        if idioma not in IDIOMAS_PERMITIDOS:
            raise ValueError(
                f"Idioma invalido em {id_termo}: {idioma}"
            )

        tipo = termo.get("tipo_termo")

        if tipo not in TIPOS_TERMO_PERMITIDOS:
            raise ValueError(
                f"Tipo de termo invalido em {id_termo}: {tipo}"
            )

        texto = str(
            termo.get("termo", "")
        ).strip()

        if not texto:
            raise ValueError(
                f"Termo vazio: {id_termo}"
            )

        chave = (
            idioma,
            normalizar_texto(texto),
        )

        if chave in termos_normalizados:
            raise ValueError(
                f"Termo duplicado: {idioma} - {texto}"
            )

        termos_normalizados.add(chave)

        eixos = termo.get("eixos")

        if not isinstance(eixos, list) or not eixos:
            raise ValueError(
                f"Termo sem eixo: {id_termo}"
            )

    referencias = sentinelas.get(
        "referencias"
    )

    if (
        not isinstance(referencias, list)
        or not referencias
    ):
        raise ValueError(
            "A lista de referencias esta ausente ou vazia."
        )

    ids_referencias: set[str] = set()
    dois: set[str] = set()

    for referencia in referencias:
        id_referencia = str(
            referencia.get(
                "id_referencia",
                "",
            )
        )

        if not PADRAO_REFERENCIA.fullmatch(
            id_referencia
        ):
            raise ValueError(
                f"id_referencia invalido: {id_referencia}"
            )

        if id_referencia in ids_referencias:
            raise ValueError(
                f"id_referencia duplicado: {id_referencia}"
            )

        ids_referencias.add(
            id_referencia
        )

        if not str(
            referencia.get("titulo", "")
        ).strip():
            raise ValueError(
                f"Titulo vazio: {id_referencia}"
            )

        autores = referencia.get("autores")

        if not isinstance(autores, list) or not autores:
            raise ValueError(
                f"Autores ausentes: {id_referencia}"
            )

        doi = normalizar_doi(
            referencia.get("doi")
        )

        if doi:
            if not PADRAO_DOI.fullmatch(doi):
                raise ValueError(
                    f"DOI invalido: {doi}"
                )

            if doi in dois:
                raise ValueError(
                    f"DOI duplicado: {doi}"
                )

            dois.add(doi)

        estado_acesso = referencia.get(
            "estado_acesso_aberto"
        )

        if (
            estado_acesso
            not in ESTADOS_ACESSO_PERMITIDOS
        ):
            raise ValueError(
                "Estado de acesso invalido em "
                f"{id_referencia}: {estado_acesso}"
            )

        eixos = referencia.get("eixos")

        if not isinstance(eixos, list) or not eixos:
            raise ValueError(
                f"Referencia sem eixo: {id_referencia}"
            )


@task(name="registrar-vocabulario-no-banco")
def registrar_conteudo(
    conteudo: dict[str, Any],
) -> dict[str, Any]:
    vocabulario = conteudo["vocabulario"]
    sentinelas = conteudo["sentinelas"]
    hashes = conteudo["hashes"]

    id_projeto = vocabulario["id_projeto"]
    id_versao = vocabulario[
        "id_versao_vocabulario"
    ]

    conexao = duckdb.connect(
        str(CAMINHO_BANCO)
    )

    agora = datetime.now(timezone.utc)

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

        eixos_banco = {
            linha[0]
            for linha in conexao.execute(
                """
                SELECT id_eixo
                FROM eixos_tematicos
                WHERE id_projeto = ?
                """,
                [id_projeto],
            ).fetchall()
        }

        eixos_informados: set[str] = set()

        for termo in vocabulario["termos"]:
            eixos_informados.update(
                termo["eixos"]
            )

        for referencia in sentinelas["referencias"]:
            eixos_informados.update(
                referencia["eixos"]
            )

        eixos_ausentes = sorted(
            eixos_informados - eixos_banco
        )

        if eixos_ausentes:
            raise ValueError(
                "Eixos nao cadastrados: "
                + ", ".join(eixos_ausentes)
            )

        versao_existente = conexao.execute(
            """
            SELECT
                hash_vocabulario,
                hash_sentinelas
            FROM versoes_vocabulario_controlado
            WHERE id_versao_vocabulario = ?
            """,
            [id_versao],
        ).fetchone()

        hashes_atuais = (
            hashes["vocabulario"],
            hashes["sentinelas"],
        )

        if versao_existente:
            if tuple(versao_existente) != hashes_atuais:
                raise RuntimeError(
                    "A versao ja foi registrada, mas "
                    "os arquivos foram modificados."
                )

            return {
                "id_projeto": id_projeto,
                "id_versao_vocabulario": id_versao,
                "situacao": "ja_registrada",
            }

        conexao.execute(
            "BEGIN TRANSACTION"
        )

        conexao.execute(
            """
            UPDATE versoes_vocabulario_controlado
            SET situacao = 'substituida'
            WHERE id_projeto = ?
              AND situacao <> 'substituida'
            """,
            [id_projeto],
        )

        conteudo_json = json.dumps(
            {
                "vocabulario": vocabulario,
                "sentinelas": sentinelas,
            },
            ensure_ascii=False,
        )

        conexao.execute(
            """
            INSERT INTO versoes_vocabulario_controlado (
                id_versao_vocabulario,
                id_projeto,
                numero_versao,
                situacao,
                hash_vocabulario,
                hash_sentinelas,
                conteudo_json,
                registrada_em_utc,
                registrada_por
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                id_versao,
                id_projeto,
                str(
                    vocabulario[
                        "numero_versao"
                    ]
                ),
                vocabulario[
                    "situacao_versao"
                ],
                hashes["vocabulario"],
                hashes["sentinelas"],
                conteudo_json,
                agora,
                getpass.getuser(),
            ],
        )

        for termo in vocabulario["termos"]:
            conexao.execute(
                """
                INSERT INTO termos_vocabulario (
                    id_termo,
                    id_projeto,
                    id_versao_vocabulario,
                    idioma,
                    termo,
                    termo_normalizado,
                    tipo_termo,
                    situacao,
                    observacao
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    termo["id_termo"],
                    id_projeto,
                    id_versao,
                    termo["idioma"],
                    termo["termo"],
                    normalizar_texto(
                        termo["termo"]
                    ),
                    termo["tipo_termo"],
                    termo["situacao"],
                    termo.get("observacao"),
                ],
            )

            for id_eixo in termo["eixos"]:
                conexao.execute(
                    """
                    INSERT INTO termos_vocabulario_eixos
                    VALUES (?, ?)
                    """,
                    [
                        termo["id_termo"],
                        id_eixo,
                    ],
                )

        for referencia in sentinelas[
            "referencias"
        ]:
            conexao.execute(
                """
                INSERT INTO referencias_sentinela (
                    id_referencia,
                    id_projeto,
                    id_versao_vocabulario,
                    titulo,
                    autores_json,
                    ano,
                    doi_normalizado,
                    id_openalex,
                    prioridade,
                    estado_acesso_aberto,
                    endereco_texto_aberto,
                    justificativa,
                    situacao
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    referencia["id_referencia"],
                    id_projeto,
                    id_versao,
                    referencia["titulo"],
                    json.dumps(
                        referencia["autores"],
                        ensure_ascii=False,
                    ),
                    referencia.get("ano"),
                    normalizar_doi(
                        referencia.get("doi")
                    ),
                    referencia.get(
                        "id_openalex"
                    ),
                    referencia["prioridade"],
                    referencia[
                        "estado_acesso_aberto"
                    ],
                    referencia.get(
                        "endereco_texto_aberto"
                    ),
                    referencia.get(
                        "justificativa"
                    ),
                    referencia["situacao"],
                ],
            )

            for id_eixo in referencia["eixos"]:
                conexao.execute(
                    """
                    INSERT INTO referencias_sentinela_eixos
                    VALUES (?, ?)
                    """,
                    [
                        referencia[
                            "id_referencia"
                        ],
                        id_eixo,
                    ],
                )

        conexao.execute("COMMIT")

        return {
            "id_projeto": id_projeto,
            "id_versao_vocabulario": id_versao,
            "situacao": "registrada",
            "quantidade_termos": len(
                vocabulario["termos"]
            ),
            "quantidade_sentinelas": len(
                sentinelas["referencias"]
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


@task(name="auditar-registro-vocabulario")
def registrar_auditoria(
    resultado: dict[str, Any],
) -> str:
    instante = datetime.now(timezone.utc)
    id_evento = gerar_id_evento()

    evento = {
        "id_evento": id_evento,
        "data_hora_utc": instante.isoformat(),
        "tipo_ator": "script",
        "id_ator": (
            "registrar_vocabulario_sentinelas"
        ),
        "acao": (
            "registrar_vocabulario_e_sentinelas"
        ),
        "id_projeto": resultado["id_projeto"],
        "entidade": "vocabulario_controlado",
        "id_entidade": resultado[
            "id_versao_vocabulario"
        ],
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
                "registrar_vocabulario_sentinelas",
                "registrar_vocabulario_e_sentinelas",
                resultado["id_projeto"],
                "vocabulario_controlado",
                resultado[
                    "id_versao_vocabulario"
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


@flow(name="registrar-vocabulario-e-sentinelas")
def executar_registro(
    caminho_vocabulario: str,
    caminho_sentinelas: str,
) -> dict[str, Any]:
    conteudo = carregar_conteudo(
        caminho_vocabulario,
        caminho_sentinelas,
    )

    validar_conteudo(conteudo)

    resultado = registrar_conteudo(
        conteudo
    )

    resultado["id_evento_auditoria"] = (
        registrar_auditoria(resultado)
    )

    return resultado


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit(
            "Uso: python "
            "registrar_vocabulario_sentinelas.py "
            "<vocabulario.yaml> "
            "<referencias_sentinela.yaml>"
        )

    resultado_final = executar_registro(
        sys.argv[1],
        sys.argv[2],
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
