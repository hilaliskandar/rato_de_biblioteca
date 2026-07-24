ALTER TABLE execucoes_consulta
ADD COLUMN IF NOT EXISTS codigo_http INTEGER;

ALTER TABLE execucoes_consulta
ADD COLUMN IF NOT EXISTS parametros_json VARCHAR;

ALTER TABLE execucoes_consulta
ADD COLUMN IF NOT EXISTS resposta_meta_json VARCHAR;

ALTER TABLE execucoes_consulta
ADD COLUMN IF NOT EXISTS limite_diario DOUBLE;

ALTER TABLE execucoes_consulta
ADD COLUMN IF NOT EXISTS limite_restante DOUBLE;

ALTER TABLE execucoes_consulta
ADD COLUMN IF NOT EXISTS creditos_usados DOUBLE;

ALTER TABLE execucoes_consulta
ADD COLUMN IF NOT EXISTS reinicio_limite_segundos BIGINT;

ALTER TABLE execucoes_consulta
ADD COLUMN IF NOT EXISTS erro VARCHAR;

CREATE TABLE IF NOT EXISTS resultados_ensaio_consulta (
    id_execucao VARCHAR NOT NULL,
    id_consulta VARCHAR NOT NULL,
    posicao_resultado INTEGER NOT NULL,
    chave_deduplicacao VARCHAR NOT NULL,
    id_openalex VARCHAR,
    doi_normalizado VARCHAR,
    titulo VARCHAR NOT NULL,
    ano_publicacao INTEGER,
    idioma VARCHAR,
    tipo_documental VARCHAR,
    citado_por_contagem BIGINT,
    pontuacao_relevancia DOUBLE,
    is_oa BOOLEAN,
    oa_status VARCHAR,
    repositorio_tem_texto_integral BOOLEAN,
    url_landing_oa VARCHAR,
    url_pdf_oa VARCHAR,
    licenca VARCHAR,
    autores_json VARCHAR,
    topico_primario_json VARCHAR,
    resumo_metadados_json VARCHAR,
    PRIMARY KEY (
        id_execucao,
        posicao_resultado
    )
);

CREATE TABLE IF NOT EXISTS recuperacoes_sentinelas_ensaio (
    id_execucao VARCHAR NOT NULL,
    id_consulta VARCHAR NOT NULL,
    id_referencia VARCHAR NOT NULL,
    recuperada BOOLEAN NOT NULL,
    forma_recuperacao VARCHAR,
    posicao_resultado INTEGER,
    avaliada_em_utc TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (
        id_execucao,
        id_referencia
    )
);

CREATE TABLE IF NOT EXISTS avaliacoes_versoes_consultas (
    id_avaliacao_versao VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_versao_consultas VARCHAR NOT NULL,
    quantidade_consultas INTEGER NOT NULL,
    quantidade_execucoes_concluidas INTEGER NOT NULL,
    quantidade_resultados BIGINT NOT NULL,
    quantidade_resultados_unicos BIGINT NOT NULL,
    taxa_duplicacao DOUBLE,
    quantidade_sentinelas_esperadas INTEGER,
    quantidade_sentinelas_recuperadas INTEGER,
    recuperacao_sentinelas DOUBLE,
    custo_total_usd DOUBLE,
    avaliada_em_utc TIMESTAMPTZ NOT NULL,
    detalhes_json VARCHAR
);

CREATE OR REPLACE VIEW vw_ultima_execucao_consulta AS
WITH ordenadas AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (
            PARTITION BY e.id_consulta
            ORDER BY
                e.iniciada_em_utc DESC,
                e.id_execucao DESC
        ) AS ordem
    FROM execucoes_consulta e
)
SELECT * EXCLUDE (ordem)
FROM ordenadas
WHERE ordem = 1;

CREATE OR REPLACE VIEW vw_resultados_ensaio_atuais AS
SELECT
    c.id_projeto,
    c.id_versao_consultas,
    c.id_familia,
    c.id_eixo,
    c.idioma AS idioma_consulta,
    c.id_consulta,
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
    r.licenca
FROM consultas c
JOIN vw_ultima_execucao_consulta e
    ON e.id_consulta = c.id_consulta
   AND e.situacao = 'concluido'
JOIN resultados_ensaio_consulta r
    ON r.id_execucao = e.id_execucao;

CREATE OR REPLACE VIEW vw_resumo_ensaios_consultas AS
SELECT
    c.id_projeto,
    c.id_versao_consultas,
    COUNT(DISTINCT c.id_consulta) AS quantidade_consultas,
    COUNT(
        DISTINCT CASE
            WHEN e.situacao = 'concluido'
            THEN e.id_execucao
        END
    ) AS quantidade_execucoes_concluidas,
    SUM(
        CASE
            WHEN e.situacao = 'concluido'
            THEN e.quantidade_recuperada
            ELSE 0
        END
    ) AS quantidade_resultados,
    SUM(
        CASE
            WHEN e.situacao = 'concluido'
            THEN COALESCE(e.custo_api, 0)
            ELSE 0
        END
    ) AS custo_total_usd,
    MAX(e.encerrada_em_utc) AS ultima_execucao_em_utc
FROM consultas c
LEFT JOIN vw_ultima_execucao_consulta e
    ON e.id_consulta = c.id_consulta
WHERE c.id_versao_consultas IS NOT NULL
GROUP BY
    c.id_projeto,
    c.id_versao_consultas;
