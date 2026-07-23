CREATE TABLE IF NOT EXISTS versoes_estrutura_pesquisa (
    id_versao_estrutura VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    numero_versao VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    hash_perguntas VARCHAR NOT NULL,
    hash_proposicoes VARCHAR NOT NULL,
    hash_eixos VARCHAR NOT NULL,
    conteudo_json VARCHAR NOT NULL,
    registrada_em_utc TIMESTAMPTZ NOT NULL,
    registrada_por VARCHAR NOT NULL
);

CREATE OR REPLACE VIEW vw_resumo_estrutura_pesquisa AS
SELECT
    p.id_projeto,
    p.nome_curto,
    p.titulo,
    (
        SELECT COUNT(*)
        FROM perguntas_pesquisa q
        WHERE q.id_projeto = p.id_projeto
    ) AS quantidade_perguntas,
    (
        SELECT COUNT(*)
        FROM proposicoes_iniciais pr
        WHERE pr.id_projeto = p.id_projeto
    ) AS quantidade_proposicoes,
    (
        SELECT COUNT(*)
        FROM eixos_tematicos e
        WHERE e.id_projeto = p.id_projeto
    ) AS quantidade_eixos,
    (
        SELECT COUNT(*)
        FROM versoes_estrutura_pesquisa v
        WHERE v.id_projeto = p.id_projeto
    ) AS quantidade_versoes
FROM projetos p;
