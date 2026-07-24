from __future__ import annotations

import getpass
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st


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


st.set_page_config(
    page_title="Avaliação de precisão",
    page_icon="📚",
    layout="wide",
)


ROTULOS_DECISAO = {
    "relevante":
        "Relevante",
    "irrelevante":
        "Irrelevante",
    "incerto":
        "Incerto",
}


CRITERIOS = {
    "aderencia_tematica":
        "Aderência temática",
    "mecanismo_regulatorio":
        "Examina mecanismo regulatório",
    "resultado_habitacional":
        "Examina resultado habitacional",
    "metodo_relevante":
        "Método útil à revisão",
    "correlato_aplicavel":
        "Correlato aplicável",
    "fora_do_escopo":
        "Fora do escopo",
    "contexto_inadequado":
        "Contexto inadequado",
    "tipo_documental_inadequado":
        "Tipo documental inadequado",
    "evidencia_insuficiente":
        "Metadados insuficientes",
}


def agora_utc() -> datetime:
    return datetime.now(timezone.utc)


def gerar_id(prefixo: str) -> str:
    return f"{prefixo}-{uuid.uuid4().hex.upper()}"


def conectar(
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(
        str(CAMINHO_BANCO),
        read_only=read_only,
    )


def carregar_amostras() -> pd.DataFrame:
    conexao = conectar(
        read_only=True
    )

    try:
        return conexao.execute(
            """
            SELECT
                id_amostra,
                id_projeto,
                id_versao_consultas,
                estrategia,
                situacao,
                quantidade_itens,
                quantidade_avaliada,
                quantidade_relevante,
                quantidade_irrelevante,
                quantidade_incerta,
                taxa_conclusao,
                precisao_decidida,
                taxa_incerteza,
                criada_em_utc
            FROM vw_resumo_amostras_precisao
            ORDER BY criada_em_utc DESC
            """
        ).fetchdf()

    finally:
        conexao.close()


def carregar_itens(
    id_amostra: str,
    filtro: str,
) -> pd.DataFrame:
    condicao = ""

    if filtro == "Pendentes":
        condicao = (
            "AND id_decisao IS NULL"
        )

    elif filtro == "Avaliados":
        condicao = (
            "AND id_decisao IS NOT NULL"
        )

    conexao = conectar(
        read_only=True
    )

    try:
        return conexao.execute(
            f"""
            SELECT *
            FROM vw_itens_avaliacao_precisao
            WHERE id_amostra = ?
              {condicao}
            ORDER BY ordem_item
            """,
            [id_amostra],
        ).fetchdf()

    finally:
        conexao.close()


def carregar_resumo_consultas(
    id_amostra: str,
) -> pd.DataFrame:
    conexao = conectar(
        read_only=True
    )

    try:
        return conexao.execute(
            """
            SELECT
                id_consulta,
                id_eixo,
                eixo,
                idioma_consulta,
                quantidade_itens,
                quantidade_avaliada,
                quantidade_relevante,
                quantidade_irrelevante,
                quantidade_incerta,
                precisao_decidida
            FROM vw_precisao_por_consulta
            WHERE id_amostra = ?
            ORDER BY
                id_eixo,
                idioma_consulta,
                id_consulta
            """,
            [id_amostra],
        ).fetchdf()

    finally:
        conexao.close()


def carregar_historico(
    id_item_amostra: str,
) -> pd.DataFrame:
    conexao = conectar(
        read_only=True
    )

    try:
        return conexao.execute(
            """
            SELECT
                id_decisao,
                decisao,
                justificativa,
                criterios_json,
                avaliador,
                decidida_em_utc,
                substitui_id_decisao
            FROM decisoes_avaliacao_precisao
            WHERE id_item_amostra = ?
            ORDER BY
                decidida_em_utc DESC,
                id_decisao DESC
            """,
            [id_item_amostra],
        ).fetchdf()

    finally:
        conexao.close()


def salvar_decisao(
    item: dict[str, Any],
    decisao: str,
    justificativa: str,
    criterios: list[str],
    avaliador: str,
) -> str:
    instante = agora_utc()
    id_decisao = gerar_id("DCP")
    id_evento = gerar_id("EVT")

    conexao = conectar()

    try:
        conexao.execute(
            "BEGIN TRANSACTION"
        )

        decisao_anterior = conexao.execute(
            """
            SELECT id_decisao
            FROM decisoes_avaliacao_precisao
            WHERE id_item_amostra = ?
            ORDER BY
                decidida_em_utc DESC,
                id_decisao DESC
            LIMIT 1
            """,
            [
                item[
                    "id_item_amostra"
                ]
            ],
        ).fetchone()

        substitui = (
            decisao_anterior[0]
            if decisao_anterior
            else None
        )

        conexao.execute(
            """
            INSERT INTO
            decisoes_avaliacao_precisao (
                id_decisao,
                id_item_amostra,
                id_amostra,
                id_projeto,
                id_consulta,
                decisao,
                justificativa,
                criterios_json,
                avaliador,
                decidida_em_utc,
                substitui_id_decisao,
                origem
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                id_decisao,
                item[
                    "id_item_amostra"
                ],
                item["id_amostra"],
                item["id_projeto"],
                item["id_consulta"],
                decisao,
                justificativa.strip()
                    or None,
                json.dumps(
                    criterios,
                    ensure_ascii=False,
                ),
                avaliador.strip(),
                instante,
                substitui,
                "interface_streamlit",
            ],
        )

        total = conexao.execute(
            """
            SELECT COUNT(*)
            FROM itens_amostra_precisao
            WHERE id_amostra = ?
            """,
            [item["id_amostra"]],
        ).fetchone()[0]

        avaliados = conexao.execute(
            """
            SELECT COUNT(
                DISTINCT id_item_amostra
            )
            FROM decisoes_avaliacao_precisao
            WHERE id_amostra = ?
            """,
            [item["id_amostra"]],
        ).fetchone()[0]

        if total > 0 and avaliados >= total:
            conexao.execute(
                """
                UPDATE amostras_avaliacao_precisao
                SET
                    situacao = 'concluida',
                    concluida_em_utc = ?
                WHERE id_amostra = ?
                """,
                [
                    instante,
                    item["id_amostra"],
                ],
            )

        detalhes = {
            "id_decisao": id_decisao,
            "id_item_amostra":
                item[
                    "id_item_amostra"
                ],
            "id_amostra":
                item["id_amostra"],
            "id_consulta":
                item["id_consulta"],
            "decisao": decisao,
            "criterios": criterios,
            "avaliador": avaliador,
            "substitui_id_decisao":
                substitui,
        }

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
                "humano",
                avaliador.strip(),
                "avaliar_precisao_consulta",
                item["id_projeto"],
                "item_amostra_precisao",
                item[
                    "id_item_amostra"
                ],
                decisao,
                json.dumps(
                    detalhes,
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
        "tipo_ator": "humano",
        "id_ator": avaliador.strip(),
        "acao":
            "avaliar_precisao_consulta",
        "id_projeto":
            item["id_projeto"],
        "entidade":
            "item_amostra_precisao",
        "id_entidade":
            item[
                "id_item_amostra"
            ],
        "situacao": decisao,
        "detalhes": {
            "id_decisao": id_decisao,
            "id_amostra":
                item["id_amostra"],
            "id_consulta":
                item["id_consulta"],
            "criterios": criterios,
        },
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

    return id_decisao


def dados_exportacao(
    id_amostra: str,
) -> bytes:
    conexao = conectar(
        read_only=True
    )

    try:
        dados = conexao.execute(
            """
            SELECT
                id_amostra,
                id_item_amostra,
                id_consulta,
                id_eixo,
                eixo,
                idioma_consulta,
                posicao_resultado,
                chave_deduplicacao,
                decisao,
                justificativa,
                criterios_json,
                avaliador,
                decidida_em_utc,
                snapshot_json
            FROM vw_itens_avaliacao_precisao
            WHERE id_amostra = ?
            ORDER BY ordem_item
            """,
            [id_amostra],
        ).fetchdf()

    finally:
        conexao.close()

    return dados.to_csv(
        index=False
    ).encode("utf-8-sig")


st.title(
    "Avaliação humana da precisão"
)

st.caption(
    "Classificação rastreável dos resultados "
    "recuperados pelas consultas OpenAlex."
)

if not CAMINHO_BANCO.exists():
    st.error(
        "Banco DuckDB não encontrado."
    )
    st.stop()

amostras = carregar_amostras()

if amostras.empty:
    st.warning(
        "Nenhuma amostra foi criada."
    )
    st.stop()

opcoes_amostra = (
    amostras["id_amostra"]
    .astype(str)
    .tolist()
)

mapa_rotulos = {}

for _, linha in amostras.iterrows():
    mapa_rotulos[
        linha["id_amostra"]
    ] = (
        f"{linha['id_amostra']} | "
        f"{linha['id_versao_consultas']} | "
        f"{linha['quantidade_avaliada']}/"
        f"{linha['quantidade_itens']} avaliados"
    )

with st.sidebar:
    st.header("Sessão")

    id_amostra = st.selectbox(
        "Amostra",
        opcoes_amostra,
        format_func=lambda valor:
            mapa_rotulos[valor],
    )

    avaliador = st.text_input(
        "Avaliador",
        value=getpass.getuser(),
    )

    filtro = st.selectbox(
        "Itens exibidos",
        [
            "Pendentes",
            "Todos",
            "Avaliados",
        ],
    )

    st.download_button(
        "Exportar amostra em CSV",
        data=dados_exportacao(
            id_amostra
        ),
        file_name=(
            f"{id_amostra}_avaliacao.csv"
        ),
        mime="text/csv",
    )

chave_contexto = (
    id_amostra,
    filtro,
)

if (
    st.session_state.get(
        "contexto_lista"
    )
    != chave_contexto
):
    st.session_state[
        "contexto_lista"
    ] = chave_contexto

    st.session_state[
        "indice_item"
    ] = 0

linha_resumo = amostras[
    amostras["id_amostra"]
    == id_amostra
].iloc[0]

colunas_metricas = st.columns(5)

colunas_metricas[0].metric(
    "Itens",
    int(
        linha_resumo[
            "quantidade_itens"
        ]
    ),
)

colunas_metricas[1].metric(
    "Avaliados",
    int(
        linha_resumo[
            "quantidade_avaliada"
        ]
    ),
)

colunas_metricas[2].metric(
    "Conclusão",
    (
        f"{float(linha_resumo['taxa_conclusao']):.1%}"
    ),
)

precisao = linha_resumo[
    "precisao_decidida"
]

colunas_metricas[3].metric(
    "Precisão decidida",
    (
        "—"
        if pd.isna(precisao)
        else f"{float(precisao):.1%}"
    ),
)

incerteza = linha_resumo[
    "taxa_incerteza"
]

colunas_metricas[4].metric(
    "Incerteza",
    (
        "—"
        if pd.isna(incerteza)
        else f"{float(incerteza):.1%}"
    ),
)

itens = carregar_itens(
    id_amostra,
    filtro,
)

if itens.empty:
    st.success(
        "Não há itens neste filtro."
    )

    resumo_consultas = (
        carregar_resumo_consultas(
            id_amostra
        )
    )

    st.subheader(
        "Resultados por consulta"
    )

    st.dataframe(
        resumo_consultas,
        use_container_width=True,
        hide_index=True,
    )

    st.stop()

indice = int(
    st.session_state.get(
        "indice_item",
        0,
    )
)

indice = max(
    0,
    min(
        indice,
        len(itens) - 1,
    ),
)

st.session_state[
    "indice_item"
] = indice

item = itens.iloc[indice].to_dict()

snapshot = json.loads(
    item["snapshot_json"]
)

cabecalho = st.columns(
    [1, 1, 1, 1]
)

cabecalho[0].write(
    f"**Item:** {indice + 1}/{len(itens)}"
)

cabecalho[1].write(
    f"**Consulta:** {item['id_consulta']}"
)

cabecalho[2].write(
    f"**Eixo:** {item['id_eixo']}"
)

cabecalho[3].write(
    f"**Posição:** {item['posicao_resultado']}"
)

st.subheader(
    snapshot.get(
        "titulo",
        "Título não informado",
    )
)

autores = snapshot.get(
    "autores"
) or []

if autores:
    st.write(
        "**Autores:** "
        + "; ".join(autores)
    )

dados_basicos = st.columns(5)

dados_basicos[0].write(
    "**Ano:** "
    + str(
        snapshot.get(
            "ano_publicacao"
        )
        or "—"
    )
)

dados_basicos[1].write(
    "**Idioma:** "
    + str(
        snapshot.get("idioma")
        or "—"
    )
)

dados_basicos[2].write(
    "**Tipo:** "
    + str(
        snapshot.get(
            "tipo_documental"
        )
        or "—"
    )
)

dados_basicos[3].write(
    "**Citações:** "
    + str(
        snapshot.get(
            "citado_por_contagem"
        )
        or 0
    )
)

dados_basicos[4].write(
    "**Acesso aberto:** "
    + (
        "sim"
        if snapshot.get("is_oa")
        else "não/indeterminado"
    )
)

topico = snapshot.get(
    "topico_primario"
)

if isinstance(topico, dict):
    nome_topico = (
        topico.get("display_name")
        or topico.get("name")
    )

    if nome_topico:
        st.write(
            f"**Tópico OpenAlex:** {nome_topico}"
        )

links = []

url_landing = snapshot.get(
    "url_landing_oa"
)

url_pdf = snapshot.get(
    "url_pdf_oa"
)

if url_landing:
    links.append(
        f"[Página da obra]({url_landing})"
    )

if url_pdf:
    links.append(
        f"[PDF indicado pelo OpenAlex]({url_pdf})"
    )

if links:
    st.markdown(
        " | ".join(links)
    )

with st.expander(
    "Consulta que recuperou o item"
):
    st.code(
        item["texto_consulta"],
        language=None,
    )

with st.expander(
    "Metadados registrados"
):
    st.json(snapshot)

decisao_atual = item.get(
    "decisao"
)

criterios_atuais = []

if item.get("criterios_json"):
    try:
        criterios_atuais = json.loads(
            item["criterios_json"]
        )
    except json.JSONDecodeError:
        criterios_atuais = []

opcoes_decisao = [
    "Selecione",
    "relevante",
    "irrelevante",
    "incerto",
]

indice_decisao = 0

if decisao_atual in opcoes_decisao:
    indice_decisao = (
        opcoes_decisao.index(
            decisao_atual
        )
    )

with st.form(
    key=(
        "formulario_"
        + item["id_item_amostra"]
    )
):
    decisao = st.selectbox(
        "Decisão",
        opcoes_decisao,
        index=indice_decisao,
        format_func=lambda valor: (
            "Selecione"
            if valor == "Selecione"
            else ROTULOS_DECISAO[valor]
        ),
    )

    criterios = st.multiselect(
        "Critérios",
        options=list(
            CRITERIOS.keys()
        ),
        default=[
            criterio
            for criterio
            in criterios_atuais
            if criterio in CRITERIOS
        ],
        format_func=lambda valor:
            CRITERIOS[valor],
    )

    justificativa = st.text_area(
        "Justificativa",
        value=(
            item.get(
                "justificativa"
            )
            or ""
        ),
        height=130,
        help=(
            "Obrigatória para irrelevante "
            "e incerto."
        ),
    )

    enviar = st.form_submit_button(
        "Registrar decisão",
        use_container_width=True,
    )

if enviar:
    erros = []

    if decisao == "Selecione":
        erros.append(
            "Selecione uma decisão."
        )

    if not avaliador.strip():
        erros.append(
            "Informe o avaliador."
        )

    if (
        decisao in {
            "irrelevante",
            "incerto",
        }
        and len(
            justificativa.strip()
        ) < 10
    ):
        erros.append(
            "A justificativa deve ter "
            "ao menos 10 caracteres."
        )

    if erros:
        for erro in erros:
            st.error(erro)

    else:
        salvar_decisao(
            item=item,
            decisao=decisao,
            justificativa=
                justificativa,
            criterios=criterios,
            avaliador=avaliador,
        )

        st.success(
            "Decisão registrada."
        )

        st.rerun()

navegacao = st.columns(
    [1, 1, 4]
)

if navegacao[0].button(
    "← Anterior",
    disabled=indice <= 0,
    use_container_width=True,
):
    st.session_state[
        "indice_item"
    ] = indice - 1

    st.rerun()

if navegacao[1].button(
    "Próximo →",
    disabled=indice >= len(itens) - 1,
    use_container_width=True,
):
    st.session_state[
        "indice_item"
    ] = indice + 1

    st.rerun()

historico = carregar_historico(
    item["id_item_amostra"]
)

if not historico.empty:
    with st.expander(
        "Histórico de decisões"
    ):
        st.dataframe(
            historico,
            use_container_width=True,
            hide_index=True,
        )

st.divider()

st.subheader(
    "Resultados por consulta"
)

resumo_consultas = (
    carregar_resumo_consultas(
        id_amostra
    )
)

st.dataframe(
    resumo_consultas,
    use_container_width=True,
    hide_index=True,
)
