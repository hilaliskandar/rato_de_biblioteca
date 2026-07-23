CREATE TABLE IF NOT EXISTS migracoes_banco (
    id_migracao VARCHAR PRIMARY KEY,
    descricao VARCHAR NOT NULL,
    hash_arquivo VARCHAR NOT NULL,
    aplicada_em_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS projetos (
    id_projeto VARCHAR PRIMARY KEY,
    nome_curto VARCHAR NOT NULL UNIQUE,
    titulo VARCHAR NOT NULL,
    descricao VARCHAR,
    situacao VARCHAR NOT NULL,
    projeto_piloto BOOLEAN NOT NULL DEFAULT FALSE,
    hash_manifesto VARCHAR NOT NULL,
    criado_em_utc TIMESTAMPTZ NOT NULL,
    atualizado_em_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS tipos_revisao_projeto (
    id_projeto VARCHAR NOT NULL,
    tipo_revisao VARCHAR NOT NULL,
    PRIMARY KEY (id_projeto, tipo_revisao)
);

CREATE TABLE IF NOT EXISTS fontes_descoberta_projeto (
    id_projeto VARCHAR NOT NULL,
    fonte_descoberta VARCHAR NOT NULL,
    PRIMARY KEY (id_projeto, fonte_descoberta)
);

CREATE TABLE IF NOT EXISTS politicas_acesso_projeto (
    id_projeto VARCHAR PRIMARY KEY,
    somente_acesso_aberto BOOLEAN NOT NULL,
    texto_integral_obrigatorio BOOLEAN NOT NULL,
    acesso_institucional_aceito BOOLEAN NOT NULL,
    copia_privada_aceita BOOLEAN NOT NULL,
    verificar_localizacao_aberta BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS padroes_relato_projeto (
    id_projeto VARCHAR NOT NULL,
    etapa VARCHAR NOT NULL,
    padrao VARCHAR NOT NULL,
    PRIMARY KEY (id_projeto, etapa)
);

CREATE TABLE IF NOT EXISTS idiomas_busca_projeto (
    id_projeto VARCHAR NOT NULL,
    idioma VARCHAR NOT NULL,
    PRIMARY KEY (id_projeto, idioma)
);

CREATE TABLE IF NOT EXISTS controles_humanos_projeto (
    id_projeto VARCHAR NOT NULL,
    chave VARCHAR NOT NULL,
    valor_booleano BOOLEAN,
    valor_texto VARCHAR,
    PRIMARY KEY (id_projeto, chave)
);

CREATE TABLE IF NOT EXISTS versoes_protocolo (
    id_versao VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    numero_versao VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    arquivo_relativo VARCHAR,
    hash_arquivo VARCHAR,
    congelado_em_utc TIMESTAMPTZ,
    aprovado_por VARCHAR,
    justificativa VARCHAR
);

CREATE TABLE IF NOT EXISTS perguntas_pesquisa (
    id_pergunta VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    texto_pergunta VARCHAR NOT NULL,
    tipo_pergunta VARCHAR,
    situacao VARCHAR NOT NULL,
    versao VARCHAR,
    criada_em_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS proposicoes_iniciais (
    id_proposicao VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    texto_proposicao VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    criada_em_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS eixos_tematicos (
    id_eixo VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    titulo VARCHAR NOT NULL,
    descricao VARCHAR,
    situacao VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS consultas (
    id_consulta VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_eixo VARCHAR,
    idioma VARCHAR,
    modalidade VARCHAR NOT NULL,
    texto_consulta VARCHAR NOT NULL,
    filtros_json VARCHAR,
    situacao VARCHAR NOT NULL,
    hash_consulta VARCHAR NOT NULL,
    criada_em_utc TIMESTAMPTZ NOT NULL,
    congelada_em_utc TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS execucoes_consulta (
    id_execucao VARCHAR PRIMARY KEY,
    id_consulta VARCHAR NOT NULL,
    iniciada_em_utc TIMESTAMPTZ NOT NULL,
    encerrada_em_utc TIMESTAMPTZ,
    situacao VARCHAR NOT NULL,
    quantidade_informada BIGINT,
    quantidade_recuperada BIGINT,
    quantidade_paginas BIGINT,
    custo_api DOUBLE,
    arquivo_manifesto_relativo VARCHAR,
    hash_saida VARCHAR,
    versao_codigo VARCHAR
);

CREATE TABLE IF NOT EXISTS obras (
    id_obra VARCHAR PRIMARY KEY,
    id_openalex VARCHAR,
    doi_normalizado VARCHAR,
    titulo VARCHAR NOT NULL,
    ano_publicacao INTEGER,
    idioma VARCHAR,
    tipo_documental VARCHAR,
    fonte VARCHAR,
    autores_json VARCHAR,
    metadados_json VARCHAR,
    criada_em_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS obras_projeto (
    id_projeto VARCHAR NOT NULL,
    id_obra VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    rotas_descoberta_json VARCHAR,
    consultas_origem_json VARCHAR,
    incluida_em_utc TIMESTAMPTZ,
    PRIMARY KEY (id_projeto, id_obra)
);

CREATE TABLE IF NOT EXISTS verificacoes_acesso_aberto (
    id_verificacao VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_obra VARCHAR NOT NULL,
    situacao VARCHAR NOT NULL,
    endereco_texto VARCHAR,
    tipo_versao VARCHAR,
    licenca VARCHAR,
    redistribuicao_permitida BOOLEAN,
    verificada_em_utc TIMESTAMPTZ NOT NULL,
    verificada_por VARCHAR NOT NULL,
    detalhes_json VARCHAR
);

CREATE TABLE IF NOT EXISTS decisoes_triagem (
    id_decisao VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    id_obra VARCHAR NOT NULL,
    etapa VARCHAR NOT NULL,
    decisao VARCHAR NOT NULL,
    codigo_criterio VARCHAR,
    justificativa VARCHAR,
    confianca VARCHAR,
    tipo_ator VARCHAR NOT NULL,
    id_ator VARCHAR NOT NULL,
    decidida_em_utc TIMESTAMPTZ NOT NULL,
    decisao_final BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS itens_prisma (
    id_registro VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    instrumento VARCHAR NOT NULL,
    id_item VARCHAR NOT NULL,
    estado_automatico VARCHAR NOT NULL,
    estado_humano VARCHAR,
    evidencias_json VARCHAR,
    justificativa VARCHAR,
    atualizado_em_utc TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS execucoes_bibliometricas (
    id_execucao_bibliometrica VARCHAR PRIMARY KEY,
    id_projeto VARCHAR NOT NULL,
    corpus VARCHAR NOT NULL,
    configuracao_json VARCHAR,
    iniciada_em_utc TIMESTAMPTZ NOT NULL,
    encerrada_em_utc TIMESTAMPTZ,
    situacao VARCHAR NOT NULL,
    pasta_resultados_relativa VARCHAR,
    hash_resultados VARCHAR
);

CREATE TABLE IF NOT EXISTS eventos_auditoria (
    id_evento VARCHAR PRIMARY KEY,
    data_hora_utc TIMESTAMPTZ NOT NULL,
    tipo_ator VARCHAR NOT NULL,
    id_ator VARCHAR NOT NULL,
    acao VARCHAR NOT NULL,
    id_projeto VARCHAR,
    entidade VARCHAR,
    id_entidade VARCHAR,
    situacao VARCHAR NOT NULL,
    detalhes_json VARCHAR
);

CREATE OR REPLACE VIEW vw_resumo_projetos AS
SELECT
    p.id_projeto,
    p.nome_curto,
    p.titulo,
    p.situacao,
    p.projeto_piloto,
    pa.somente_acesso_aberto,
    pa.texto_integral_obrigatorio,
    COUNT(DISTINCT tr.tipo_revisao) AS quantidade_tipos_revisao,
    COUNT(DISTINCT fd.fonte_descoberta) AS quantidade_fontes,
    COUNT(DISTINCT ib.idioma) AS quantidade_idiomas,
    p.atualizado_em_utc
FROM projetos p
LEFT JOIN politicas_acesso_projeto pa
    ON pa.id_projeto = p.id_projeto
LEFT JOIN tipos_revisao_projeto tr
    ON tr.id_projeto = p.id_projeto
LEFT JOIN fontes_descoberta_projeto fd
    ON fd.id_projeto = p.id_projeto
LEFT JOIN idiomas_busca_projeto ib
    ON ib.id_projeto = p.id_projeto
GROUP BY
    p.id_projeto,
    p.nome_curto,
    p.titulo,
    p.situacao,
    p.projeto_piloto,
    pa.somente_acesso_aberto,
    pa.texto_integral_obrigatorio,
    p.atualizado_em_utc;
