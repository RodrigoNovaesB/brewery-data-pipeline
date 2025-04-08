# Brewery Data Pipeline

Pipeline para processamento de dados de cervejarias com Airflow.

## Arquitetura
```mermaid
flowchart LR
    A[API] --> B[Bronze\nJSON]
    B --> C[Silver\nParquet Particionado]
    C --> D[Gold\nDados Agregados]