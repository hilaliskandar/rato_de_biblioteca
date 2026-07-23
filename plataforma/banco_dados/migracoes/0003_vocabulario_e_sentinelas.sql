CREATE TABLE IF NOT EXISTS versoes_vocabulario_controlado (
    id_versao_vocabulario VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    numero_versao VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    hash_vocabulario VARCHAR NOT NULL,
    hash_sentinelas VARCHAR NOT NULL,
    conteudo_json VARCHAR NOT NULL,
    registrada_em_utc TIMESTAMPTZ NOT NULL,
    registrada_por VARCHAR NOT NULL,
    UNIQUE (id_projeto, numero_versao)
);

CREATE TABLE IF NOT EXISTS termos_vocabulario (
    id_termo VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_versao_vocabulario VARCHAR NOT NULL,
    idioma VARCHAR NOT NULL,
    termo VARCHAR NOT NULL,
    termo_normalizado VARCHAR NOT NULL,
    tipo_termo VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    observacao VARCHAR,
    UNIQUE (
        id_projeto,
        id_versao_vocabulario,
        idioma,
        termo_normalizado
    )
);

CREATE TABLE IF NOT EXISTS termos_vocabulario_eixos (
    id_termo VARCHAR NOT NULL,
    id_eixo VARCHAR NOT NULL,
    PRIMARY KEY (id_termo, id_eixo)
);

CREATE TABLE IF NOT EXISTS referencias_sentinela (
    id_referencia VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_versao_vocabulario VARCHAR NOT NULL,
    titulo VARCHAR NOT NULL,
    autores_json VARCHAR NOT NULL,
    ano INTEGER,
    doi_normalizado VARCHAR,
    id_openalex VARCHAR,
    prioridade VARCHAR NOT NULL,
    estado_acesso_aberto VARCHAR NOT NULL,
    endereco_texto_aberto VARCHAR,
    justificativa VARCHAR,
    situacao VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS referencias_sentinela_eixos (
    id_referencia VARCHAR NOT NULL,
    id_eixo VARCHAR NOT NULL,
    PRIMARY KEY (id_referencia, id_eixo)
);

CREATE TABLE IF NOT EXISTS avaliacoes_consulta_sentinela (
    id_avaliacao VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_consulta VARCHAR,
    id_referencia VARCHAR NOT NULL,
    recuperada BOOLEAN NOT NULL,
    posicao_resultado BIGINT,
    avaliada_em_utc TIMESTAMPTZ NOT NULL,
    detalhes_json VARCHAR
);

CREATE OR REPLACE VIEW vw_vocabulario_ativo AS
WITH versao_atual AS (
    SELECT
        id_projeto,
        id_versao_vocabulario,
        ROW_NUMBER() OVER (
            PARTITION BY id_projeto
            ORDER BY registrada_em_utc DESC
        ) AS ordem
    FROM versoes_vocabulario_controlado
    WHERE situacao <> 'substituida'
)
SELECT
    t.id_projeto,
    t.id_versao_vocabulario,
    t.id_termo,
    t.idioma,
    t.termo,
    t.termo_normalizado,
    t.tipo_termo,
    t.situacao,
    te.id_eixo
FROM termos_vocabulario t
JOIN versao_atual v
    ON v.id_projeto = t.id_projeto
   AND v.id_versao_vocabulario = t.id_versao_vocabulario
   AND v.ordem = 1
LEFT JOIN termos_vocabulario_eixos te
    ON te.id_termo = t.id_termo;

CREATE OR REPLACE VIEW vw_resumo_vocabulario AS
SELECT
    v.id_projeto,
    v.id_versao_vocabulario,
    v.numero_versao,
    v.situacao,
    COUNT(DISTINCT t.id_termo) AS quantidade_termos,
    COUNT(DISTINCT t.idioma) AS quantidade_idiomas,
    COUNT(DISTINCT r.id_referencia) AS quantidade_sentinelas,
    v.registrada_em_utc
FROM versoes_vocabulario_controlado v
LEFT JOIN termos_vocabulario t
    ON t.id_versao_vocabulario = v.id_versao_vocabulario
LEFT JOIN referencias_sentinela r
    ON r.id_versao_vocabulario = v.id_versao_vocabulario
GROUP BY
    v.id_projeto,
    v.id_versao_vocabulario,
    v.numero_versao,
    v.situacao,
    v.registrada_em_utc;
