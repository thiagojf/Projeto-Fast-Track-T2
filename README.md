# Projeto Analítico Legislativo — Câmara dos Deputados

## Visão Geral

Este projeto tem como objetivo construir uma plataforma analítica legislativa utilizando arquitetura Medalhão (Bronze, Silver e Gold) no Databricks a partir de dados públicos da Câmara dos Deputados.

O projeto contempla ingestão, tratamento, modelagem dimensional e disponibilização de tabelas analíticas para consumo em ferramentas de Business Intelligence, Analytics e Data Science.

---

# Arquitetura do Projeto

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

## Bronze Layer

Camada responsável pela ingestão RAW dos dados provenientes das APIs públicas da Câmara.

Principais características:

- Dados semi-estruturados
- Retenção do payload original
- Auditoria de ingestão
- Controle de paginação
- Retry automático
- Particionamento

---

## Silver Layer

Camada responsável por tratamento, limpeza e modelagem dos dados.

Principais atividades:

- Tipagem
- Padronização de colunas
- Deduplicação
- Criação de dimensões
- Criação de bridges
- Enriquecimento dos dados

---

## Gold Layer

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

## 3. Raio-X de Gastos CEAP

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
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── views/
│
├── docs/
│   ├── arquitetura_medalhao.png
│   ├── modelo_dimensional.png
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

## gold_eventos

| Campo | Tipo | Descrição |
|---|---|---|
| nk_evento | bigint | Identificador natural do evento |
| sk_tipo_evento | int | Chave da dimensão tipo evento |
| data_evento | date | Data do evento |

---

## fat_despesas

| Campo | Tipo | Descrição |
|---|---|---|
| sk_deputado | int | Chave surrogate deputado |
| sk_fornecedor | int | Chave surrogate fornecedor |
| valor_liquido | double | Valor líquido da despesa |
```

# Autor

Thiago Faria

Projeto desenvolvido utilizando Databricks, PySpark e arquitetura Medalhão para análise legislativa da Câmara dos Deputados.

