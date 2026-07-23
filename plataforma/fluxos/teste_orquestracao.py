from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, task
from prefect.logging import get_run_logger


RAIZ_PROJETO = Path(__file__).resolve().parents[2]
PASTA_SAIDA = (
    RAIZ_PROJETO
    / "armazenamento"
    / "testes_orquestracao"
)


@task(name="preparar-diretorio")
def preparar_diretorio() -> str:
    logger = get_run_logger()

    PASTA_SAIDA.mkdir(
        parents=True,
        exist_ok=True,
    )

    logger.info(
        "Diretorio preparado: %s",
        PASTA_SAIDA,
    )

    return str(PASTA_SAIDA)


@task(name="gerar-arquivo-teste")
def gerar_arquivo_teste(
    pasta_saida: str,
) -> str:
    logger = get_run_logger()

    instante = datetime.now(timezone.utc)
    identificador = instante.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    arquivo = (
        Path(pasta_saida)
        / f"teste_prefect_{identificador}.json"
    )

    conteudo = {
        "tipo_registro": "teste_orquestracao",
        "situacao": "gerado",
        "gerado_em_utc": instante.isoformat(),
        "raiz_projeto": str(RAIZ_PROJETO),
    }

    arquivo.write_text(
        json.dumps(
            conteudo,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Arquivo criado: %s",
        arquivo,
    )

    return str(arquivo)


@task(name="calcular-hash")
def calcular_hash(
    caminho_arquivo: str,
) -> str:
    logger = get_run_logger()

    arquivo = Path(caminho_arquivo)

    hash_sha256 = hashlib.sha256(
        arquivo.read_bytes()
    ).hexdigest()

    logger.info(
        "SHA-256: %s",
        hash_sha256,
    )

    return hash_sha256


@task(name="gerar-manifesto")
def gerar_manifesto(
    caminho_arquivo: str,
    hash_sha256: str,
) -> str:
    logger = get_run_logger()

    arquivo = Path(caminho_arquivo)

    manifesto = arquivo.with_name(
        arquivo.name.replace(
            "teste_prefect_",
            "manifesto_",
        )
    )

    conteudo = {
        "tipo_registro":
            "manifesto_teste_orquestracao",
        "situacao": "concluido",
        "arquivo_produzido": str(arquivo),
        "tamanho_bytes": arquivo.stat().st_size,
        "algoritmo_hash": "sha256",
        "hash_sha256": hash_sha256,
        "gerado_em_utc": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    manifesto.write_text(
        json.dumps(
            conteudo,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logger.info(
        "Manifesto criado: %s",
        manifesto,
    )

    return str(manifesto)


@flow(name="teste-inicial-de-orquestracao")
def executar_teste() -> dict[str, str]:
    logger = get_run_logger()

    logger.info(
        "Iniciando teste de orquestracao."
    )

    pasta = preparar_diretorio()
    arquivo = gerar_arquivo_teste(pasta)
    hash_sha256 = calcular_hash(arquivo)
    manifesto = gerar_manifesto(
        arquivo,
        hash_sha256,
    )

    resultado = {
        "situacao": "concluido",
        "arquivo": arquivo,
        "manifesto": manifesto,
        "hash_sha256": hash_sha256,
    }

    logger.info(
        "Teste concluido com sucesso."
    )

    return resultado


if __name__ == "__main__":
    resultado = executar_teste()

    print()
    print("RESULTADO DO TESTE")
    print(
        json.dumps(
            resultado,
            ensure_ascii=False,
            indent=2,
        )
    )
