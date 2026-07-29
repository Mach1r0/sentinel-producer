# Security Event Producer (Go + Kafka)

Serviço em Go que gera/simula eventos de segurança (autenticação, execução de processo, alertas de rede) e os publica em tópicos Kafka, seguindo um schema definido — primeira metade de um pipeline completo de ingestão de eventos.

Este projeto é o **produtor** do pipeline. O segundo projeto (consumer + OpenSearch) é quem consome esses eventos e os indexa.

## Requisitos funcionais

- [ ] Definir um **schema de evento** em JSON (campos mínimos: `timestamp`, `event_type`, `source_ip`, `severity`, `raw` — pode se inspirar livremente em OCSF sem copiar nada específico de trabalho)
- [ ] Gerar eventos simulados de pelo menos 2 tipos (ex: `auth_failed`, `process_created`) com dados plausíveis (IPs aleatórios, usuários fictícios, timestamps realistas)
- [ ] Publicar cada evento em um tópico Kafka usando `segmentio/kafka-go`
- [ ] Definir estratégia de particionamento (ex: por `source_ip`, pra manter ordem de eventos do mesmo host na mesma partição)
- [ ] Implementar producer com **batching** (enviar em lotes, não 1 mensagem por vez) para throughput realista
- [ ] Tratamento de erro com retry e backoff exponencial em caso de falha de publicação
- [ ] Shutdown gracioso (capturar SIGINT/SIGTERM, drenar buffer antes de encerrar)
- [ ] (Opcional) Ler eventos reais do seu projeto Sniffer em vez de só simular — conecta os 3 projetos que você já tem

## Requisitos não funcionais

- [ ] Testes unitários da geração/serialização de eventos (sem precisar de Kafka rodando)
- [ ] Teste de integração com Kafka local via `docker-compose` (pode usar build tag `integration` para separar dos testes unitários)
- [ ] Configuração via flags ou variáveis de ambiente (broker, tópico, taxa de eventos/segundo)
- [ ] Métricas básicas expostas (contagem de eventos publicados, falhas) — pode ser só log estruturado, não precisa de Prometheus

## Flags sugeridas

```
-broker       string  Endereço do broker Kafka (default: "localhost:9092")
-topic        string  Tópico de destino (default: "security.events")
-rate         int     Eventos por segundo a gerar (default: 10)
-batch-size   int     Tamanho do lote antes de flush (default: 50)
-source       string  "simulated" ou "sniffer" (se ligar ao projeto do sniffer)
```

## Estrutura de pastas sugerida

```
event-producer/
├── cmd/
│   └── producer/
│       └── main.go
├── internal/
│   ├── event/
│   │   ├── schema.go
│   │   ├── generator.go
│   │   └── generator_test.go
│   └── kafka/
│       ├── producer.go
│       └── producer_test.go
├── docker-compose.yml
├── go.mod
├── README.md
└── LICENSE
```

## Critérios de "pronto"

1. Roda continuamente publicando eventos simulados a uma taxa configurável
2. Testes unitários passam sem depender de Kafka
3. `docker compose up` sobe Kafka local e o producer consegue publicar nele
4. README documenta o schema de evento e a estratégia de particionamento (e o porquê da escolha)

---

## README.md (rascunho para o repositório)

```markdown
# Security Event Producer

Gerador e publicador de eventos de segurança simulados, escrito em Go,
usando Kafka como camada de transporte. Primeira metade de um pipeline
de ingestão de eventos (Producer → Kafka → Consumer → OpenSearch).

## Schema de evento

\`\`\`json
{
  "timestamp": "2026-08-01T14:32:00Z",
  "event_type": "auth_failed",
  "source_ip": "10.0.0.15",
  "severity": "medium",
  "raw": { "user": "admin", "attempts": 3 }
}
\`\`\`

## Por que particionar por source_ip?

Manter todos os eventos de um mesmo host na mesma partição preserva a
ordem de chegada por origem, o que é importante para detecções baseadas
em sequência (ex: brute force = múltiplas falhas seguidas do mesmo IP).

## Uso

\`\`\`bash
docker compose up -d kafka
go run ./cmd/producer -rate 20 -topic security.events
\`\`\`

## Arquitetura

[Gerador de Eventos] -> [Batching] -> [Kafka Producer] -> [Tópico: security.events]

## Licença

MIT
```