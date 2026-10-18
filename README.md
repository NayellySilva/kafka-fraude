# Detecção de Fraude com Apache Kafka (TDE 03)

Projeto que simula transações financeiras e detecta fraudes em tempo real.

## Arquitetura
- **Producer** (Python): gera uma transação aleatória a cada 3 segundos e publica no tópico `transacoes` do Kafka.
- **Kafka**: transporta as mensagens entre o producer e o consumer.
- **Consumer** (Python): lê as transações, salva todas no banco SQLite, aplica as regras de fraude e exibe alertas no console.

## Padrão da transação
| Campo | Tipo | Exemplo |
|---|---|---|
| transaction_id | texto (UUID) | "9f1c2a..." |
| client_id | inteiro | 3 |
| amount | decimal | 1520.75 |
| city | texto | "Fortaleza" |
| timestamp | data/hora ISO | "2026-10-18T13:40:00" |

## Regras de fraude
| Código | Regra |
|---|---|
| ALTO_VALOR | Transação com amount >= 10000 |
| TEMPO_60s | Mesmo cliente com 4 transações em menos de 60 segundos |
| GEO_10m | Mesmo cliente com 2 transações em cidades diferentes em 10 minutos |