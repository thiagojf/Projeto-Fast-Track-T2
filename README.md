# Projeto Analítico Legislativo — Câmara dos Deputados

## Visão Geral

Este projeto tem como objetivo construir uma plataforma analítica legislativa utilizando arquitetura Medalhão (Bronze, Silver e Gold) desenvolvido utilizando Databricks, PySpark a partir de dados públicos da Câmara dos Deputados.

O projeto contempla ingestão, tratamento, modelagem dimensional e disponibilização de tabelas analíticas para consumo em ferramentas de Business Intelligence, Analytics e Data Science.
---

# Arquitetura do Projeto
<img width="1536" height="1024" alt="b67c081c-4869-4d12-a1f8-c4032efa7880" src="https://github.com/user-attachments/assets/a74492fb-ed2e-4089-9afc-7dac82a42092" />

---

# Tecnologias Utilizadas

| Tecnologia | Finalidade |
|---|---|
| Databricks | Plataforma principal de processamento |
| PySpark | Transformações e ingestão |
| SQL | Modelagem e analytics |
| Delta Lake | Armazenamento transacional |
| Unity Catalog | Governança de dados |
| API Dados Abertos Câmara | Fonte de dados |
| Power BI | Consumo analítico |

---

# Arquitetura Medalhão

## Camada Bronze

Camada responsável pela ingestão RAW dos dados provenientes das APIs públicas da Câmara.

Principais características:

- Dados semi-estruturados
- Retenção do payload original
- Auditoria de ingestão
- Controle de paginação
- Retry automático
- Particionamento

---

## Camada Silver

Camada responsável por tratamento, limpeza e modelagem dos dados.

Principais atividades:

- Tipagem
- Padronização de colunas
- Deduplicação
- Criação de dimensões
- Criação de bridges
- Enriquecimento dos dados

---

## Camada Gold

Camada analítica para consumo de dashboards e análises.

Principais entregas:

- Indicadores parlamentares
- Ranking de deputados
- Taxa de presença
- Densidade de eventos
- Top gastos por partido
- Comparativos eleitorais

---

# Módulos Entregues

## 1. Atlas das Frentes Parlamentares

### Objetivos

- Mapear frentes parlamentares
- Identificar diversidade partidária
- Identificar deputados com maior participação em frentes

### Principais tabelas

- gold_frentes_membros
- gold_ranking_deputados
- bridge_frente_deputado

### Destaques Analíticos

- Índice de Herfindahl-Hirschman (IHH)
- Ranking de deputados por participação
- Interseção entre frentes

---

## 2. Calendário Analítico Legislativo

### Objetivos

- Consolidar eventos legislativos
- Analisar presença parlamentar
- Identificar comportamento legislativo em períodos eleitorais

### Principais tabelas

- gold_eventos
- gold_taxa_presenca_eventos
- gold_densidade_eventos_semana
- gold_comparativo_periodo_eleitoral

### Destaques Analíticos

- Taxa de participação em eventos
- Densidade semanal de eventos
- Comparativo antes/durante período eleitoral

---

## 4. Raio-X de Gastos CEAP

### Objetivos

- Ingestão de despesas parlamentares
- Modelagem dimensional financeira
- Ranking de gastos por partido

### Principais tabelas

- despesas
- dim_fornecedor
- dim_categoria_despesa
- fat_despesas
- top_gastos_partido

### Destaques Analíticos

- Top 10 gastos por partido
- Ranking mensal de despesas
- Análise de fornecedores

---

# Estrutura de Diretórios

```text
Projeto/
│
├── notebooks/
│   ├── Modulos/
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       └── views/
│
├── docs/
│   ├── Arquitetura_Medalhao_Projeto.png
│   └── dicionario_dados.md
│
├── README.md
│
└── apresentacao/
```

---

# Ordem de Execução dos Notebooks

## Bronze

1. bronze_deputados
2. bronze_frentes
3. bronze_frentes_membros
4. bronze_eventos
5. bronze_eventos_presenca
6. bronze_tipo_evento
7. bronze_despesas

---

## Silver

1. dim_deputado
2. bridge_frente_deputado
3. dim_tipo_evento
4. dim_evento
5. dim_orgao
6. bridge_evento_orgao
7. dim_data
8. despesas
9. dim_fornecedor
10. dim_categoria_despesa

---

## Gold

1. gold_frentes_membros
2. gold_ranking_deputados
3. gold_eventos
4. gold_taxa_presenca_eventos
5. gold_densidade_eventos_semana
6. gold_comparativo_periodo_eleitoral
7. fat_despesas
8. top_gastos_partido

---

# Desafios Técnicos

Os principais desafios enfrentados durante o desenvolvimento foram:

- Paginação das APIs públicas
- Timeout em endpoints específicos
- Modelagem dimensional
- Dados semi-estruturados
- Padronização de entidades
- Relacionamentos muitos-para-muitos
- Governança de dados

---

# Próximos Passos

Como evolução futura do projeto:

- Módulo completo de votações
- Correlação entre votos e frentes parlamentares
- Pipeline de CPIs
- Detecção de anomalias em despesas
- Dashboards Power BI
- Incremental refresh
- Data Quality automatizado

---

# Dicionário de Dados

 # gold.comparativo_periodo_eleitoral

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | comparativo_periodo_eleitoral | 0 | ano | INT |
| gold | comparativo_periodo_eleitoral | 1 | periodo_eleitoral | BOOLEAN |
| gold | comparativo_periodo_eleitoral | 2 | descricao_periodo_eleitoral | STRING |
| gold | comparativo_periodo_eleitoral | 3 | descricao_tipo_evento | STRING |
| gold | comparativo_periodo_eleitoral | 4 | qtd_eventos | LONG |
| gold | comparativo_periodo_eleitoral | 5 | media_eventos_semana | DOUBLE |
| gold | comparativo_periodo_eleitoral | 6 | qtd_orgaos | LONG |
| gold | comparativo_periodo_eleitoral | 7 | updated_at | TIMESTAMP |
| gold | comparativo_periodo_eleitoral | 8 | pipeline_version | STRING |

# gold.densidade_eventos_semana

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | densidade_eventos_semana | 0 | ano | INT |
| gold | densidade_eventos_semana | 1 | semana_ano | INT |
| gold | densidade_eventos_semana | 2 | ano_semana | STRING |
| gold | densidade_eventos_semana | 3 | qtd_eventos | LONG |
| gold | densidade_eventos_semana | 4 | possui_evento | BOOLEAN |
| gold | densidade_eventos_semana | 5 | updated_at | TIMESTAMP |
| gold | densidade_eventos_semana | 6 | pipeline_version | STRING |

# gold.eventos

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | eventos | 0 | sk_evento | INT |
| gold | eventos | 1 | nk_evento | LONG |
| gold | eventos | 2 | data_evento | DATE |
| gold | eventos | 3 | sk_data | INT |
| gold | eventos | 4 | sk_tipo_evento | INT |
| gold | eventos | 5 | descricao_tipo_evento | STRING |
| gold | eventos | 6 | sk_orgao | INT |
| gold | eventos | 7 | sigla_orgao | STRING |
| gold | eventos | 8 | nome_orgao | STRING |
| gold | eventos | 9 | situacao_evento | STRING |
| gold | eventos | 10 | descricao_evento | STRING |
| gold | eventos | 11 | ano | INT |
| gold | eventos | 12 | mes | INT |
| gold | eventos | 13 | semana_ano | INT |
| gold | eventos | 14 | updated_at | TIMESTAMP |
| gold | eventos | 15 | pipeline_version | STRING |

# gold.fat_despesas

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | fat_despesas | 0 | sk_deputado | INT |
| gold | fat_despesas | 1 | sigla_partido | STRING |
| gold | fat_despesas | 2 | sk_fornecedor | INT |
| gold | fat_despesas | 3 | sk_categoria_despesa | INT |
| gold | fat_despesas | 4 | nk_deputado | LONG |
| gold | fat_despesas | 5 | ano | INT |
| gold | fat_despesas | 6 | mes | INT |
| gold | fat_despesas | 7 | ano_mes | STRING |
| gold | fat_despesas | 8 | data_documento | DATE |
| gold | fat_despesas | 9 | tipo_documento | STRING |
| gold | fat_despesas | 10 | cod_documento | LONG |
| gold | fat_despesas | 11 | cod_lote | LONG |
| gold | fat_despesas | 12 | parcela | INT |
| gold | fat_despesas | 13 | valor_documento | DOUBLE |
| gold | fat_despesas | 14 | valor_liquido | DOUBLE |
| gold | fat_despesas | 15 | valor_glosa | DOUBLE |
| gold | fat_despesas | 16 | url_documento | STRING |
| gold | fat_despesas | 17 | updated_at | TIMESTAMP |
| gold | fat_despesas | 18 | pipeline_version | STRING |

# gold.gold_frentes_membros

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | gold_frentes_membros | 0 | sk_frente | INT |
| gold | gold_frentes_membros | 1 | sk_deputado | INT |
| gold | gold_frentes_membros | 2 | nk_frente | LONG |
| gold | gold_frentes_membros | 3 | nk_deputado | LONG |
| gold | gold_frentes_membros | 4 | titulo_frente | STRING |
| gold | gold_frentes_membros | 5 | id_legislatura | INT |
| gold | gold_frentes_membros | 6 | nome_deputado | STRING |
| gold | gold_frentes_membros | 7 | sigla_partido | STRING |
| gold | gold_frentes_membros | 8 | sigla_uf | STRING |
| gold | gold_frentes_membros | 9 | updated_at | TIMESTAMP |
| gold | gold_frentes_membros | 10 | pipeline_version | STRING |

# gold.gold_ihh_frentes

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | gold_ihh_frentes | 0 | nk_frente | LONG |
| gold | gold_ihh_frentes | 1 | titulo_frente | STRING |
| gold | gold_ihh_frentes | 2 | qtd_deputados_frente | LONG |
| gold | gold_ihh_frentes | 3 | qtd_partidos | LONG |
| gold | gold_ihh_frentes | 4 | ihh | DOUBLE |
| gold | gold_ihh_frentes | 5 | classificacao_diversidade | STRING |
| gold | gold_ihh_frentes | 6 | updated_at | TIMESTAMP |
| gold | gold_ihh_frentes | 7 | pipeline_version | STRING |

# gold.gold_ranking_deputados

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | gold_ranking_deputados | 0 | nk_deputado | LONG |
| gold | gold_ranking_deputados | 1 | nome_deputado | STRING |
| gold | gold_ranking_deputados | 2 | sigla_partido | STRING |
| gold | gold_ranking_deputados | 3 | sigla_uf | STRING |
| gold | gold_ranking_deputados | 4 | qtd_frentes | LONG |
| gold | gold_ranking_deputados | 5 | frentes_participa | ARRAY |
| gold | gold_ranking_deputados | 6 | updated_at | TIMESTAMP |
| gold | gold_ranking_deputados | 7 | pipeline_version | STRING |

# gold.taxa_presenca_evento

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | taxa_presenca_evento | 0 | nk_deputado | LONG |
| gold | taxa_presenca_evento | 1 | nome_deputado | STRING |
| gold | taxa_presenca_evento | 2 | sigla_partido | STRING |
| gold | taxa_presenca_evento | 3 | sigla_uf | STRING |
| gold | taxa_presenca_evento | 4 | ano_evento | INT |
| gold | taxa_presenca_evento | 5 | sk_tipo_evento | INT |
| gold | taxa_presenca_evento | 6 | nome_tipo_evento | STRING |
| gold | taxa_presenca_evento | 7 | qtd_eventos_participados | LONG |
| gold | taxa_presenca_evento | 8 | qtd_total_eventos_tipo | LONG |
| gold | taxa_presenca_evento | 9 | taxa_participacao | DOUBLE |
| gold | taxa_presenca_evento | 10 | percentual_taxa_participacao | DOUBLE |
| gold | taxa_presenca_evento | 11 | updated_at | TIMESTAMP |
| gold | taxa_presenca_evento | 12 | pipeline_version | STRING |

# gold.top_gastos_partido

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | top_gastos_partido | 0 | ano | INT |
| gold | top_gastos_partido | 1 | mes | INT |
| gold | top_gastos_partido | 2 | ano_mes | STRING |
| gold | top_gastos_partido | 3 | sigla_partido | STRING |
| gold | top_gastos_partido | 4 | qtd_deputados | LONG |
| gold | top_gastos_partido | 5 | qtd_despesas | LONG |
| gold | top_gastos_partido | 6 | valor_total_liquido | DOUBLE |
| gold | top_gastos_partido | 7 | ranking_gasto_partido | INT |
| gold | top_gastos_partido | 8 | updated_at | TIMESTAMP |
| gold | top_gastos_partido | 9 | pipeline_version | STRING |

# gold.vw_eventos_futuros

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| gold | vw_eventos_futuros | 0 | sk_evento | INT |
| gold | vw_eventos_futuros | 1 | nk_evento | LONG |
| gold | vw_eventos_futuros | 2 | data_evento | DATE |
| gold | vw_eventos_futuros | 3 | sk_data | INT |
| gold | vw_eventos_futuros | 4 | sk_tipo_evento | INT |
| gold | vw_eventos_futuros | 5 | descricao_tipo_evento | STRING |
| gold | vw_eventos_futuros | 6 | sk_orgao | INT |
| gold | vw_eventos_futuros | 7 | sigla_orgao | STRING |
| gold | vw_eventos_futuros | 8 | nome_orgao | STRING |
| gold | vw_eventos_futuros | 9 | situacao_evento | STRING |
| gold | vw_eventos_futuros | 10 | descricao_evento | STRING |
| gold | vw_eventos_futuros | 11 | ano | INT |
| gold | vw_eventos_futuros | 12 | mes | INT |
| gold | vw_eventos_futuros | 13 | semana_ano | INT |
| gold | vw_eventos_futuros | 14 | updated_at | TIMESTAMP |
| gold | vw_eventos_futuros | 15 | pipeline_version | STRING |

# silver.bridge_evento_orgao

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | bridge_evento_orgao | 0 | sk_evento | INT |
| silver | bridge_evento_orgao | 1 | sk_orgao | INT |
| silver | bridge_evento_orgao | 2 | nk_evento | LONG |
| silver | bridge_evento_orgao | 3 | nk_orgao | STRING |
| silver | bridge_evento_orgao | 4 | updated_at | TIMESTAMP |
| silver | bridge_evento_orgao | 5 | pipeline_version | STRING |

# silver.bridge_frente_deputado

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | bridge_frente_deputado | 0 | sk_frente | INT |
| silver | bridge_frente_deputado | 1 | sk_deputado | INT |
| silver | bridge_frente_deputado | 2 | nk_frente | LONG |
| silver | bridge_frente_deputado | 3 | nk_deputado | LONG |
| silver | bridge_frente_deputado | 4 | nome_deputado | STRING |
| silver | bridge_frente_deputado | 5 | sigla_partido | STRING |
| silver | bridge_frente_deputado | 6 | sigla_uf | STRING |
| silver | bridge_frente_deputado | 7 | id_legislatura | INT |
| silver | bridge_frente_deputado | 8 | bronze_ingested_at | TIMESTAMP |
| silver | bridge_frente_deputado | 9 | updated_at | TIMESTAMP |
| silver | bridge_frente_deputado | 10 | pipeline_version | STRING |

# silver.despesas

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | despesas | 0 | nk_deputado | INT |
| silver | despesas | 1 | ano | INT |
| silver | despesas | 2 | mes | INT |
| silver | despesas | 3 | ano_mes | STRING |
| silver | despesas | 4 | tipo_despesa | STRING |
| silver | despesas | 5 | cod_documento | LONG |
| silver | despesas | 6 | tipo_documento | STRING |
| silver | despesas | 7 | cod_tipo_documento | INT |
| silver | despesas | 8 | data_documento | DATE |
| silver | despesas | 9 | num_documento | STRING |
| silver | despesas | 10 | valor_documento | DOUBLE |
| silver | despesas | 11 | valor_liquido | DOUBLE |
| silver | despesas | 12 | valor_glosa | DOUBLE |
| silver | despesas | 13 | nome_fornecedor | STRING |
| silver | despesas | 14 | cnpj_cpf_fornecedor | STRING |
| silver | despesas | 15 | cnpj_cpf_fornecedor_limpo | STRING |
| silver | despesas | 16 | url_documento | STRING |
| silver | despesas | 17 | cod_lote | LONG |
| silver | despesas | 18 | parcela | INT |
| silver | despesas | 19 | data_despesa | DATE |
| silver | despesas | 20 | ano_despesa | INT |
| silver | despesas | 21 | mes_despesa | INT |
| silver | despesas | 22 | semana_despesa | INT |
| silver | despesas | 23 | updated_at | TIMESTAMP |
| silver | despesas | 24 | pipeline_version | STRING |

# silver.dim_categoria_despesa

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | dim_categoria_despesa | 0 | sk_categoria_despesa | INT |
| silver | dim_categoria_despesa | 1 | nk_categoria_despesa | STRING |
| silver | dim_categoria_despesa | 2 | updated_at | TIMESTAMP |
| silver | dim_categoria_despesa | 3 | pipeline_version | STRING |

# silver.dim_data

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | dim_data | 0 | sk_data | INT |
| silver | dim_data | 1 | data | DATE |
| silver | dim_data | 2 | ano | INT |
| silver | dim_data | 3 | mes | INT |
| silver | dim_data | 4 | nome_mes | STRING |
| silver | dim_data | 5 | trimestre | INT |
| silver | dim_data | 6 | ano_mes | STRING |
| silver | dim_data | 7 | semana_ano | INT |
| silver | dim_data | 8 | dia_mes | INT |
| silver | dim_data | 9 | dia_semana | INT |
| silver | dim_data | 10 | nome_dia_semana | STRING |
| silver | dim_data | 11 | is_fim_semana | BOOLEAN |
| silver | dim_data | 12 | periodo_eleitoral | BOOLEAN |
| silver | dim_data | 13 | descricao_periodo_eleitoral | STRING |
| silver | dim_data | 14 | updated_at | TIMESTAMP |
| silver | dim_data | 15 | pipeline_version | STRING |

# silver.dim_deputado

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | dim_deputado | 0 | sk_deputado | INT |
| silver | dim_deputado | 1 | nk_deputado | LONG |
| silver | dim_deputado | 2 | uri_deputado | STRING |
| silver | dim_deputado | 3 | nome_deputado | STRING |
| silver | dim_deputado | 4 | sigla_partido | STRING |
| silver | dim_deputado | 5 | id_partido | LONG |
| silver | dim_deputado | 6 | sigla_uf | STRING |
| silver | dim_deputado | 7 | id_legislatura | INT |
| silver | dim_deputado | 8 | url_foto | STRING |
| silver | dim_deputado | 9 | email | STRING |
| silver | dim_deputado | 10 | valid_from | DATE |
| silver | dim_deputado | 11 | valid_to | DATE |
| silver | dim_deputado | 12 | is_current | BOOLEAN |
| silver | dim_deputado | 13 | bronze_ingested_at | TIMESTAMP |
| silver | dim_deputado | 14 | updated_at | TIMESTAMP |
| silver | dim_deputado | 15 | pipeline_version | STRING |

# silver.dim_evento

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | dim_evento | 0 | sk_evento | INT |
| silver | dim_evento | 1 | nk_evento | LONG |
| silver | dim_evento | 2 | uri_evento | STRING |
| silver | dim_evento | 3 | sk_tipo_evento | INT |
| silver | dim_evento | 4 | cod_tipo_evento | LONG |
| silver | dim_evento | 5 | data_hora_inicio | TIMESTAMP |
| silver | dim_evento | 6 | data_hora_fim | TIMESTAMP |
| silver | dim_evento | 7 | data_evento | DATE |
| silver | dim_evento | 8 | ano_evento | INT |
| silver | dim_evento | 9 | mes_evento | INT |
| silver | dim_evento | 10 | semana_evento | INT |
| silver | dim_evento | 11 | situacao_evento | STRING |
| silver | dim_evento | 12 | descricao_tipo_evento | STRING |
| silver | dim_evento | 13 | descricao_evento | STRING |
| silver | dim_evento | 14 | local_externo | STRING |
| silver | dim_evento | 15 | local_camara_nome | STRING |
| silver | dim_evento | 16 | local_camara_predio | STRING |
| silver | dim_evento | 17 | local_camara_sala | STRING |
| silver | dim_evento | 18 | local_camara_andar | STRING |
| silver | dim_evento | 19 | url_registro | STRING |
| silver | dim_evento | 20 | orgaos | ARRAY |
| silver | dim_evento | 21 | raw_payload | STRING |
| silver | dim_evento | 22 | bronze_ingested_at | TIMESTAMP |
| silver | dim_evento | 23 | updated_at | TIMESTAMP |
| silver | dim_evento | 24 | pipeline_version | STRING |

# silver.dim_fornecedor

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | dim_fornecedor | 0 | sk_fornecedor | INT |
| silver | dim_fornecedor | 1 | nk_fornecedor | STRING |
| silver | dim_fornecedor | 2 | cnpj_cpf_fornecedor | STRING |
| silver | dim_fornecedor | 3 | cnpj_cpf_fornecedor_limpo | STRING |
| silver | dim_fornecedor | 4 | nome_fornecedor | STRING |
| silver | dim_fornecedor | 5 | tipo_pessoa | STRING |
| silver | dim_fornecedor | 6 | updated_at | TIMESTAMP |
| silver | dim_fornecedor | 7 | pipeline_version | STRING |

# silver.dim_frente

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | dim_frente | 0 | sk_frente | INT |
| silver | dim_frente | 1 | nk_frente | LONG |
| silver | dim_frente | 2 | id_legislatura | INT |
| silver | dim_frente | 3 | titulo_frente | STRING |
| silver | dim_frente | 4 | uri_frente | STRING |
| silver | dim_frente | 5 | bronze_ingested_at | TIMESTAMP |
| silver | dim_frente | 6 | updated_at | TIMESTAMP |
| silver | dim_frente | 7 | pipeline_version | STRING |

# silver.dim_orgao

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | dim_orgao | 0 | sk_orgao | INT |
| silver | dim_orgao | 1 | nk_orgao | STRING |
| silver | dim_orgao | 2 | sigla_orgao | STRING |
| silver | dim_orgao | 3 | nome_orgao | STRING |
| silver | dim_orgao | 4 | apelido_orgao | STRING |
| silver | dim_orgao | 5 | cod_tipo_orgao | STRING |
| silver | dim_orgao | 6 | tipo_orgao | STRING |
| silver | dim_orgao | 7 | nome_publicacao | STRING |
| silver | dim_orgao | 8 | nome_resumido | STRING |
| silver | dim_orgao | 9 | updated_at | TIMESTAMP |
| silver | dim_orgao | 10 | pipeline_version | STRING |

# silver.dim_tipo_evento

| Schema | Tabela | Posição | Nome Coluna | Tipo Dado |
|---|---|---:|---|---|
| silver | dim_tipo_evento | 0 | sk_tipo_evento | INT |
| silver | dim_tipo_evento | 1 | cod_tipo_evento | LONG |
| silver | dim_tipo_evento | 2 | sigla_tipo_evento | STRING |
| silver | dim_tipo_evento | 3 | nome_tipo_evento | STRING |
| silver | dim_tipo_evento | 4 | descricao_tipo_evento | STRING |
| silver | dim_tipo_evento | 5 | raw_payload | STRING |
| silver | dim_tipo_evento | 6 | bronze_ingested_at | TIMESTAMP |
| silver | dim_tipo_evento | 7 | updated_at | TIMESTAMP |
| silver | dim_tipo_evento | 8 | pipeline_version | STRING |
```

```
# Autor
## Thiago Faria

```


