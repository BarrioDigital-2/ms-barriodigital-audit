# ms-barriodigital-audit

Microservicio de **auditoría** de BarrioDigital. Es el **consumidor de Kafka**: lee los eventos de trámites de `requests.events`,
publica un registro de auditoría por cada uno en `audit.timeline`, envía los mensajes que fallan a `audit.DLT`
y mantiene un dashboard con **Kafka Streams**.

| Dato | Valor |
|------|-------|
| Puerto | `8087` |
| Stack | Spring Boot 4.1 · Java 17 · Maven · Spring Kafka · Kafka Streams |
| Base de datos | No usa (el historial vive en `audit.timeline`) |

```
requests ──► [ requests.events ] ──► auditoria-processor ──► [ audit.timeline ]
                                 │          └─ falla x3 ───► [ audit.DLT ]
                                 └─► dashboard-processor (Kafka Streams: conteo por comuna y por tipo)
```

## Topics

| Topic | Rol de audit | Particiones | Política | Retención | Quién lo crea |
|-------|--------------|-------------|----------|-----------|---------------|
| `requests.events` | consume | 3 | `delete` | 7 días | ms-requests |
| `audit.timeline` | **publica** | 3 | **`compact,delete`** | 30 días | audit (`config/KafkaTopicConfig`) |
| `audit.DLT` | **publica** (errores) | 3 | `delete` | 14 días | audit (`config/KafkaTopicConfig`) |

Réplicas = `KAFKA_REPLICAS` (1 en local, 3 en AWS). La creación automática de topics del broker está desactivada.

## Consumidor: `auditoria-processor` (`consumer/AuditoriaConsumer`)

1. Recibe `SolicitudEvent` (JSON → DTO con `ErrorHandlingDeserializer` + `JacksonJsonDeserializer`; se ignora el tipo del productor).
2. **Valida** que traiga `id` (sin id no se sabe a qué trámite corresponde).
3. Publica un `AuditoriaRegistro` en **`audit.timeline` con key = id del trámite**:
   ```json
   { "solicitudId": 12, "evento": "SOLICITUD_ACTUALIZADA", "tipoTramite": "PODA_ARBOLES", "estado": "ADMITIDO",
     "direccion": "Av. Siempre Viva 742", "fechaEvento": "...", "fechaAuditoria": "..." }
   ```
   Con `compact`, Kafka conserva el **último registro de cada key**: el topic guarda el último estado de cada trámite.

## Errores: reintentos y DLT (`config/KafkaErrorConfig`)

| Caso | Qué pasa |
|------|----------|
| El listener lanza excepción (ej. evento sin `id`) | `DefaultErrorHandler`: **3 reintentos** con 1 s entre cada uno (`FixedBackOff(1000, 3)`) → `audit.DLT` |
| JSON inválido | Va **directo** a `audit.DLT` (reintentar fallaría igual) |

`DeadLetterPublishingRecoverer` publica el mensaje original **en la misma partición** de `audit.DLT`, con headers
del error: `kafka_dlt-exception-message`, `kafka_dlt-original-topic`, `-partition`, `-offset`, `-consumer-group`.
Usa dos templates: JSON para eventos válidos que fallaron y **bytes** para JSON inválido (llega intacto).

## Kafka Streams: `dashboard-processor` (`config/KafkaStreamsConfig`)

Topología sobre `requests.events`:
- **Solicitudes por comuna** en ventanas de 60 minutos (store `solicitudes-por-comuna-store`).
- **Total por tipo de evento** (store `solicitudes-por-tipo-store`).

Kafka Streams crea sus topics internos (`...-repartition` y `...-changelog`).

> Si arranca antes de que exista `requests.events`, Streams se apaga (`MissingSourceTopicException`): reiniciar audit cuando el topic exista.

## Cómo levantar (local)

```powershell
# 1. Infraestructura, desde barriodigital-infra/local
docker compose up -d

# 2. Levantar ms-requests una vez (crea requests.events) y luego este microservicio
.\mvnw clean spring-boot:run "-Dmaven.test.skip=true"
```

| Variable | Por defecto |
|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `AUDIT_TOPIC` | `requests.events` |
| `KAFKA_REPLICAS` | `1` |

## Cómo probar

```powershell
# Evento normal → aparece en audit.timeline
'{"id":101,"tipo":"SOLICITUD_CREADA","titulo":"PODA_ARBOLES","estado":"INGRESADO","timestamp":"2026-11-21T10:00:00"}' | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:9093 --topic requests.events
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9093 --topic audit.timeline --from-beginning --property print.key=true --property key.separator=" | "

# Evento sin id → 3 reintentos y audit.DLT ;  JSON inválido → audit.DLT directo
'{"tipo":"SOLICITUD_CREADA","estado":"INGRESADO"}' | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:9093 --topic requests.events
'esto no es json' | docker exec -i kafka kafka-console-producer --bootstrap-server kafka:9093 --topic requests.events
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9093 --topic audit.DLT --from-beginning --property print.headers=true
```

Lag del consumidor: `GET http://localhost:8086/api/v1/kafka-admin/consumer-groups/auditoria-processor/lag` (ms-barriodigital-kafka-admin).

## Despliegue

En AWS corre en la EC2 **apps** (`barriodigital-infra/aws/apps`) contra el cluster de 3 brokers de la EC2 **kafka**.
