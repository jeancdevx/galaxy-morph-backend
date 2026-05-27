# Arquitectura Backend — galaxy-morph-backend

## Tu patrón anterior (tree-rings) que replicaremos

Después de inspeccionar tus dos repos, este es el patrón que ya implementaste:

```
┌─────────────────────────────────────────────────────────┐
│  tree-rings-kafka-api (NestJS)                          │
│                                                         │
│  POST /analysis/request-upload                          │
│    → StorageService.generatePresignedUrl()              │
│    → devuelve {key, putUrl} para cada imagen            │
│                                                         │
│  POST /analysis/start-process                           │
│    → KafkaService.emit(ingestion_topic, payload)        │
│    → publica N mensajes (uno por imagen) a Kafka        │
│    → devuelve {jobId, status: "QUEUED"}                 │
│                                                         │
│  WS (Socket.io) — NotificationsGateway                  │
│    → cliente se conecta con ?clientId=xxx               │
│    → ConsumerService escucha results_topic              │
│    → al recibir resultado → notifyClient(clientId, data)│
└────────────┬────────────────────────────┬───────────────┘
             │ produce                    │ consume
             ▼                            ▼
    ┌─ ingestion_topic ─┐      ┌─ results_topic ──┐
    │   Kafka (3 brokers)│      │   Kafka           │
    │   + 3 Zookeepers   │      │                   │
    └────────┬───────────┘      └───────────────────┘
             │ consume                    ▲ produce
             ▼                            │
┌─────────────────────────────────────────┴───────────────┐
│  apache-spark-perception-tree-rings (PySpark)           │
│                                                         │
│  main_streaming.py                                      │
│    → readStream from ingestion_topic                    │
│    → foreachBatch + mapPartitions                       │
│    → VisionPipeline.process_job() per row               │
│    → write results to results_topic via Kafka           │
│                                                         │
│  Docker: 1 master + N workers (--scale spark-worker=3)  │
│  Each worker: 2 cores, 2GB RAM                          │
│  Model loaded ONCE per partition (not per row)          │
└─────────────────────────────────────────────────────────┘
```

---

## Galaxy-morph: misma arquitectura, adaptada

### Diferencias clave vs tree-rings

| Aspecto | Tree Rings | Galaxy Morph |
|---|---|---|
| **API** | NestJS (TypeScript) | **NestJS** (mantienes el stack) |
| **Procesamiento** | Vision algorithms (slicing, Sobel, CNN) | **ResNet50 inference** (.pt model) |
| **Input a Spark** | URL imagen + coordenadas X,Y | **URL imagen de galaxia** |
| **Output de Spark** | Ring count + visualizaciones | **5 probabilidades + clase predicha** |
| **R2 uploads** | Imagen original + outputs visuales | **Imagen original** (no output visual) |

### Infraestructura Docker

```
docker-compose.yml (galaxy-morph-backend)

Servicios:
├── zookeeper-1, zookeeper-2, zookeeper-3   (Kafka coordination)
├── kafka-1, kafka-2, kafka-3               (Message brokers)
├── kafka-ui                                (Monitoring :8090)
├── spark-master                            (Cluster manager :8080)
├── spark-worker (×N, scalable)             (Processing nodes)
└── api                                     (NestJS REST + WS :3000)
```

### Tópicos Kafka

| Tópico | Productor | Consumidor | Payload |
|---|---|---|---|
| `galaxy.ingestion` | API (NestJS) | Spark Streaming | `{jobId, file, clientId, timestamp}` |
| `galaxy.results` | Spark Workers | API (NestJS) | `{jobId, clientId, status, classification}` |

---

## API REST — Endpoints (RESTful correcto)

### Recurso: `/uploads`

| Método | Ruta | Responsabilidad | Request | Response |
|---|---|---|---|---|
| `POST` | `/uploads/presigned` | Generar URLs prefirmadas para subir N imágenes a R2 | `{images: [{filename, contentType}]}` | `{uploads: [{key, putUrl, expiresAt}]}` |

### Recurso: `/classifications`

| Método | Ruta | Responsabilidad | Request | Response |
|---|---|---|---|---|
| `POST` | `/classifications` | Iniciar clasificación de N imágenes (publica a Kafka) | `{clientId, images: [{key}]}` | `{jobId, status: "QUEUED", count: N}` |
| `GET` | `/classifications/:jobId` | Consultar estado/resultado de un job | — | `{jobId, status, results: [...]}` |
| `GET` | `/classifications` | Historial de clasificaciones | `?page&limit` | Lista paginada |

### WebSocket (Socket.io)

| Evento | Dirección | Payload |
|---|---|---|
| `connection` | Cliente → Servidor | Query: `?clientId=xxx` |
| `classification:result` | Servidor → Cliente | `{jobId, imageKey, class, confidence, probabilities}` |
| `classification:error` | Servidor → Cliente | `{jobId, imageKey, error}` |
| `classification:complete` | Servidor → Cliente | `{jobId, status: "COMPLETE", totalProcessed: N}` |

---

## Flujo completo

```
1. UPLOAD
   Cliente → POST /uploads/presigned
   API → genera presigned URLs con R2 SDK
   API → responde [{key, putUrl, expiresAt}, ...]
   Cliente → PUT putUrl (directo a R2, bypassa la API)

2. CLASIFICAR
   Cliente → WS connect(?clientId=abc123)
   Cliente → POST /classifications {clientId: "abc123", images: [{key: "..."}]}
   API → genera jobId
   API → por cada imagen: KafkaService.emit("galaxy.ingestion", {jobId, file, clientId})
   API → responde {jobId, status: "QUEUED", count: N}

3. PROCESAMIENTO DISTRIBUIDO
   Spark readStream ← galaxy.ingestion
   foreachBatch:
     mapPartitions:
       - Carga modelo ResNet50 UNA VEZ por partición
       - Por cada imagen:
         1. Descarga de R2
         2. Preprocesa (resize, normalize)
         3. Inferencia ResNet50
         4. resultado = {class, confidence, probabilities}
     → Escribe JSON a galaxy.results

4. RESPUESTA EN TIEMPO REAL
   ConsumerService (NestJS) ← galaxy.results
   Por cada mensaje:
     → NotificationsGateway.notifyClient(clientId, resultado)
     → Cliente recibe por WebSocket en tiempo real
```

---

## Estructura del proyecto galaxy-morph-backend

```
galaxy-morph-backend/
├── docker-compose.yml          # Kafka + Zookeeper + Spark + API
├── Dockerfile.api              # NestJS API image
├── Dockerfile.spark            # Spark + PyTorch model image
│
├── src/                        # NestJS API
│   ├── main.ts
│   ├── app.module.ts
│   │
│   ├── uploads/                # Presigned URL generation
│   │   ├── uploads.controller.ts
│   │   ├── uploads.service.ts
│   │   ├── uploads.module.ts
│   │   └── dtos/
│   │       └── request-presigned.dto.ts
│   │
│   ├── classifications/        # Job management + Kafka producer
│   │   ├── classifications.controller.ts
│   │   ├── classifications.service.ts
│   │   ├── classifications.module.ts
│   │   └── dtos/
│   │       └── start-classification.dto.ts
│   │
│   ├── kafka/                  # Kafka producer + consumer
│   │   ├── kafka.module.ts
│   │   ├── kafka.service.ts    # Producer
│   │   ├── consumer.service.ts # Listens galaxy.results
│   │   ├── interfaces/
│   │   │   ├── ingestion-payload.interface.ts
│   │   │   └── result-payload.interface.ts
│   │   └── enums/
│   │       └── job-status.enum.ts
│   │
│   ├── notifications/          # WebSocket gateway (Socket.io)
│   │   ├── notifications.module.ts
│   │   └── notifications.gateway.ts
│   │
│   ├── storage/                # R2 client (presigned URLs)
│   │   ├── storage.module.ts
│   │   └── storage.service.ts
│   │
│   └── config/                 # Environment configs
│       ├── config.module.ts
│       └── env/
│           ├── kafka.config.ts
│           ├── storage.config.ts
│           └── api.config.ts
│
├── spark/                      # Spark processing (Python)
│   ├── main_streaming.py       # foreachBatch + mapPartitions
│   ├── spark_session.py
│   ├── kafka_config.py
│   └── inference/              # ResNet50 inference engine
│       ├── __init__.py
│       ├── classifier.py       # Loads .pt model, runs inference
│       └── transforms.py       # Image preprocessing
│
└── models/                     # Trained model weights
    └── best_model.pt           # From galaxy-morph-ml training
```

---

## Preguntas abiertas

1. **¿Vas con NestJS** (como tu repo anterior) o reconsideras FastAPI? Con NestJS el modelo se queda 100% en Spark (Python). Con FastAPI podrías tener un endpoint de inferencia directa sin Spark para single-image classification.

2. **¿Base de datos?** ¿Quieres persistir el historial de clasificaciones? Si sí, ¿PostgreSQL? ¿O solo in-memory por ahora?

3. **¿El `best_model.pt`** lo guardas en R2 y Spark lo descarga al iniciar, o lo montas como volumen Docker?
