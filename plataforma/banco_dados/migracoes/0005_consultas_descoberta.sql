CREATE TABLE IF NOT EXISTS versoes_consultas_descoberta (
    id_versao_consultas VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_versao_vocabulario VARCHAR NOT NULL,
    numero_versao VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    hash_configuracao VARCHAR NOT NULL,
    hash_consultas VARCHAR NOT NULL,
    conteudo_json VARCHAR NOT NULL,
    criada_em_utc TIMESTAMPTZ NOT NULL,
    criada_por VARCHAR NOT NULL,
    aprovada_em_utc TIMESTAMPTZ,
    aprovada_por VARCHAR,
    justificativa_aprovacao VARCHAR,
    UNIQUE (
        id_projeto,
        numero_versao
    )
);

CREATE TABLE IF NOT EXISTS familias_consultas_descoberta (
    id_familia VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_versao_consultas VARCHAR NOT NULL,
    id_eixo VARCHAR NOT NULL,
    titulo VARCHAR NOT NULL,
    objetivo VARCHAR,
    situacao VARCHAR NOT NULL
);

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS id_versao_consultas VARCHAR;

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS id_familia VARCHAR;

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS parametro_busca VARCHAR;

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS ordenacao VARCHAR;

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS limite_ensaio INTEGER;

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS aprovada_humanamente BOOLEAN
    DEFAULT FALSE;

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS aprovada_em_utc TIMESTAMPTZ;

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS aprovada_por VARCHAR;

ALTER TABLE consultas
ADD COLUMN IF NOT EXISTS justificativa_aprovacao VARCHAR;

CREATE TABLE IF NOT EXISTS consultas_termos (
    id_consulta VARCHAR NOT NULL,
    id_termo VARCHAR NOT NULL,
    funcao_termo VARCHAR NOT NULL,
    PRIMARY KEY (
        id_consulta,
        id_termo,
        funcao_termo
    )
);

CREATE TABLE IF NOT EXISTS consultas_sentinelas_esperadas (
    id_consulta VARCHAR NOT NULL,
    id_referencia VARCHAR NOT NULL,
    motivo VARCHAR,
    PRIMARY KEY (
        id_consulta,
        id_referencia
    )
);

CREATE TABLE IF NOT EXISTS avaliacoes_consultas (
    id_avaliacao VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_consulta VARCHAR NOT NULL,
    tipo_avaliacao VARCHAR NOT NULL,
    valor_numerico DOUBLE,
    valor_texto VARCHAR,
    tamanho_amostra INTEGER,
    avaliada_em_utc TIMESTAMPTZ NOT NULL,
    tipo_ator VARCHAR NOT NULL,
    id_ator VARCHAR NOT NULL,
    detalhes_json VARCHAR
);

CREATE OR REPLACE VIEW vw_consultas_descoberta AS
SELECT
    c.id_projeto,
    c.id_versao_consultas,
    c.id_familia,
    c.id_consulta,
    c.id_eixo,
    e.titulo AS eixo,
    c.idioma,
    c.modalidade,
    c.parametro_busca,
    c.texto_consulta,
    c.filtros_json,
    c.ordenacao,
    c.limite_ensaio,
    c.situacao,
    c.aprovada_humanamente,
    c.hash_consulta,
    COUNT(
        DISTINCT ct.id_termo
    ) AS quantidade_termos,
    COUNT(
        DISTINCT cs.id_referencia
    ) AS quantidade_sentinelas_esperadas
FROM consultas c
LEFT JOIN eixos_tematicos e
    ON e.id_eixo = c.id_eixo
LEFT JOIN consultas_termos ct
    ON ct.id_consulta = c.id_consulta
LEFT JOIN consultas_sentinelas_esperadas cs
    ON cs.id_consulta = c.id_consulta
WHERE c.id_versao_consultas IS NOT NULL
GROUP BY
    c.id_projeto,
    c.id_versao_consultas,
    c.id_familia,
    c.id_consulta,
    c.id_eixo,
    e.titulo,
    c.idioma,
    c.modalidade,
    c.parametro_busca,
    c.texto_consulta,
    c.filtros_json,
    c.ordenacao,
    c.limite_ensaio,
    c.situacao,
    c.aprovada_humanamente,
    c.hash_consulta;

CREATE OR REPLACE VIEW vw_resumo_versoes_consultas AS
SELECT
    v.id_projeto,
    v.id_versao_consultas,
    v.numero_versao,
    v.situacao,
    COUNT(
        DISTINCT f.id_familia
    ) AS quantidade_familias,
    COUNT(
        DISTINCT c.id_consulta
    ) AS quantidade_consultas,
    COUNT(
        DISTINCT c.idioma
    ) AS quantidade_idiomas,
    SUM(
        CASE
            WHEN c.aprovada_humanamente
            THEN 1
            ELSE 0
        END
    ) AS quantidade_aprovadas,
    v.criada_em_utc,
    v.aprovada_em_utc
FROM versoes_consultas_descoberta v
LEFT JOIN familias_consultas_descoberta f
    ON f.id_versao_consultas =
       v.id_versao_consultas
LEFT JOIN consultas c
    ON c.id_versao_consultas =
       v.id_versao_consultas
GROUP BY
    v.id_projeto,
    v.id_versao_consultas,
    v.numero_versao,
    v.situacao,
    v.criada_em_utc,
    v.aprovada_em_utc;
