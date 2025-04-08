# Metadados das Camadas

## Bronze (Raw)
| Campo         | Tipo   | Descrição                          | Valores Válidos               |
|---------------|--------|------------------------------------|-------------------------------|
| brewery_type  | string | Tipo da cervejaria                 | micro, nano, regional, etc.  |
| state         | string | Estado (sigla de 2 letras)         | CO, CA, NY, etc.             |

## Gold (Aggregated)
| Campo        | Tipo   | Restrições                         |
|--------------|--------|------------------------------------|
| count        | int    | Número inteiro ≥ 1                 |