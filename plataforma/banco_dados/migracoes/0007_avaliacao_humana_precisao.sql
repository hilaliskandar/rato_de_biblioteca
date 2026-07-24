CREATE TABLE IF NOT EXISTS amostras_avaliacao_precisao (
    id_amostra VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_versao_consultas VARCHAR NOT NULL,
    estrategia VARCHAR NOT NULL,
    parametros_json VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    criada_em_utc TIMESTAMPTZ NOT NULL,
    criada_por VARCHAR NOT NULL,
    concluida_em_utc TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS itens_amostra_precisao (
    id_item_amostra VARCHAR PRIMARY KEY,
    id_amostra VARCHAR NOT NULL,
    id_execucao VARCHAR NOT NULL,
    id_consulta VARCHAR NOT NULL,
    posicao_resultado INTEGER NOT NULL,
    chave_deduplicacao VARCHAR NOT NULL,
    ordem_item INTEGER NOT NULL,
    snapshot_json VARCHAR NOT NULL,
    criado_em_utc TIMESTAMPTZ NOT NULL,
    UNIQUE (
        id_amostra,
        id_consulta,
        chave_deduplicacao
    )
);

CREATE TABLE IF NOT EXISTS decisoes_avaliacao_precisao (
    id_decisao VARCHAR PRIMARY KEY,
    id_item_amostra VARCHAR NOT NULL,
    id_amostra VARCHAR NOT NULL,
    id_projeto VARCHAR NOT NULL,
    id_consulta VARCHAR NOT NULL,
    decisao VARCHAR NOT NULL,
    justificativa VARCHAR,
    criterios_json VARCHAR NOT NULL,
    avaliador VARCHAR NOT NULL,
    decidida_em_utc TIMESTAMPTZ NOT NULL,
    substitui_id_decisao VARCHAR,
    origem VARCHAR NOT NULL
);

CREATE OR REPLACE VIEW
vw_decisoes_precisao_vigentes AS
WITH ordenadas AS (
    SELECT
        d.*,
        ROW_NUMBER() OVER (
            PARTITION BY d.id_item_amostra
            ORDER BY
                d.decidida_em_utc DESC,
                d.id_decisao DESC
        ) AS ordem
    FROM decisoes_avaliacao_precisao d
)
SELECT * EXCLUDE (ordem)
FROM ordenadas
WHERE ordem = 1;

CREATE OR REPLACE VIEW
vw_itens_avaliacao_precisao AS
SELECT
    a.id_amostra,
    a.id_projeto,
    a.id_versao_consultas,
    a.estrategia,
    a.situacao AS situacao_amostra,
    i.id_item_amostra,
    i.id_execucao,
    i.id_consulta,
    i.posicao_resultado,
    i.chave_deduplicacao,
    i.ordem_item,
    i.snapshot_json,
    c.id_familia,
    c.id_eixo,
    e.titulo AS eixo,
    c.idioma AS idioma_consulta,
    c.texto_consulta,
    d.id_decisao,
    d.decisao,
    d.justificativa,
    d.criterios_json,
    d.avaliador,
    d.decidida_em_utc,
    d.substitui_id_decisao
FROM amostras_avaliacao_precisao a
JOIN itens_amostra_precisao i
    ON i.id_amostra = a.id_amostra
JOIN consultas c
    ON c.id_consulta = i.id_consulta
LEFT JOIN eixos_tematicos e
    ON e.id_eixo = c.id_eixo
LEFT JOIN vw_decisoes_precisao_vigentes d
    ON d.id_item_amostra = i.id_item_amostra;

CREATE OR REPLACE VIEW
vw_resumo_amostras_precisao AS
SELECT
    a.id_amostra,
    a.id_projeto,
    a.id_versao_consultas,
    a.estrategia,
    a.situacao,
    COUNT(DISTINCT i.id_item_amostra)
        AS quantidade_itens,
    COUNT(DISTINCT d.id_item_amostra)
        AS quantidade_avaliada,
    SUM(
        CASE
            WHEN d.decisao = 'relevante'
            THEN 1
            ELSE 0
        END
    ) AS quantidade_relevante,
    SUM(
        CASE
            WHEN d.decisao = 'irrelevante'
            THEN 1
            ELSE 0
        END
    ) AS quantidade_irrelevante,
    SUM(
        CASE
            WHEN d.decisao = 'incerto'
            THEN 1
            ELSE 0
        END
    ) AS quantidade_incerta,
    CASE
        WHEN COUNT(DISTINCT i.id_item_amostra) > 0
        THEN
            COUNT(DISTINCT d.id_item_amostra)::DOUBLE
            / COUNT(DISTINCT i.id_item_amostra)
        ELSE 0
    END AS taxa_conclusao,
    CASE
        WHEN SUM(
            CASE
                WHEN d.decisao IN (
                    'relevante',
                    'irrelevante'
                )
                THEN 1
                ELSE 0
            END
        ) > 0
        THEN
            SUM(
                CASE
                    WHEN d.decisao = 'relevante'
                    THEN 1
                    ELSE 0
                END
            )::DOUBLE
            / SUM(
                CASE
                    WHEN d.decisao IN (
                        'relevante',
                        'irrelevante'
                    )
                    THEN 1
                    ELSE 0
                END
            )
        ELSE NULL
    END AS precisao_decidida,
    CASE
        WHEN COUNT(DISTINCT d.id_item_amostra) > 0
        THEN
            SUM(
                CASE
                    WHEN d.decisao = 'incerto'
                    THEN 1
                    ELSE 0
                END
            )::DOUBLE
            / COUNT(DISTINCT d.id_item_amostra)
        ELSE NULL
    END AS taxa_incerteza,
    a.criada_em_utc,
    a.concluida_em_utc
FROM amostras_avaliacao_precisao a
LEFT JOIN itens_amostra_precisao i
    ON i.id_amostra = a.id_amostra
LEFT JOIN vw_decisoes_precisao_vigentes d
    ON d.id_item_amostra = i.id_item_amostra
GROUP BY
    a.id_amostra,
    a.id_projeto,
    a.id_versao_consultas,
    a.estrategia,
    a.situacao,
    a.criada_em_utc,
    a.concluida_em_utc;

CREATE OR REPLACE VIEW
vw_precisao_por_consulta AS
SELECT
    i.id_amostra,
    i.id_consulta,
    i.id_eixo,
    i.eixo,
    i.idioma_consulta,
    COUNT(*) AS quantidade_itens,
    COUNT(i.id_decisao) AS quantidade_avaliada,
    SUM(
        CASE
            WHEN i.decisao = 'relevante'
            THEN 1
            ELSE 0
        END
    ) AS quantidade_relevante,
    SUM(
        CASE
            WHEN i.decisao = 'irrelevante'
            THEN 1
            ELSE 0
        END
    ) AS quantidade_irrelevante,
    SUM(
        CASE
            WHEN i.decisao = 'incerto'
            THEN 1
            ELSE 0
        END
    ) AS quantidade_incerta,
    CASE
        WHEN SUM(
            CASE
                WHEN i.decisao IN (
                    'relevante',
                    'irrelevante'
                )
                THEN 1
                ELSE 0
            END
        ) > 0
        THEN
            SUM(
                CASE
                    WHEN i.decisao = 'relevante'
                    THEN 1
                    ELSE 0
                END
            )::DOUBLE
            / SUM(
                CASE
                    WHEN i.decisao IN (
                        'relevante',
                        'irrelevante'
                    )
                    THEN 1
                    ELSE 0
                END
            )
        ELSE NULL
    END AS precisao_decidida
FROM vw_itens_avaliacao_precisao i
GROUP BY
    i.id_amostra,
    i.id_consulta,
    i.id_eixo,
    i.eixo,
    i.idioma_consulta;
