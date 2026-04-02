# 🌌 Galaxy Morph Backend

Un sistema distribuido en tiempo real, orientado a eventos (Event-Driven), diseñado para la clasificación de morfología de galaxias utilizando Deep Learning (PyTorch ResNet50).

Acoplado a una arquitectura de nivel empresarial, este backend separa completamente la ingesta web del procesamiento pesado de Machine Learning utilizando **Apache Kafka** y **Apache Spark**. ¡Cero bloqueos, máxima escalabilidad!

---

## 🏗️ Arquitectura y Flujo de Datos

Este proyecto **no procesa** las imágenes pesadas en el hilo principal de la aplicación. Todo el ciclo de vida de una imagen fluye de manera asíncrona a través de los siguientes pasos automatizados:

1. **Solicitud de Subida (Presigned URLs):** 
   El cliente hace una petición `POST /api/uploads/presigned` indicando el nombre y tipo del archivo. La API de NestJS no acepta el archivo físico; en su lugar, se comunica mediante AWS SDK con Cloudflare R2 y le devuelve al cliente una URL firmada criptográficamente (`putUrl`) y una llave única (`key`).
2. **Transferencia Directa (Bypass del Servidor):** 
   El cliente hace un `PUT` binario directamente a la URL de Cloudflare R2 enviando los bytes de la imagen. Esto evita que los servidores web colapsen por la transferencia de archivos pesados.
3. **Inicio del Trabajo (Ingesta):** 
   Una vez que el cliente termina la subida a R2, le avisa al backend disparando un `POST /api/classifications` pasándole el `key` del archivo. 
4. **Encolamiento en Kafka:**
   La API genera un identificador de trabajo (`jobId`) con estado `PENDING` en memoria y publica inmediatamente un mensaje en **Apache Kafka** bajo el topic `galaxy.ingestion`. La API responde al usuario con éxito en milisegundos confirmando que el trabajo ha entrado a la cola.
5. **Cómputo Distribuido (Spark + PyTorch):** 
   Desacoplado de la API, existe un clúster de **Apache Spark** (Master y 3 Workers). Spark monitorea Kafka en paralelo bajo un patrón *Structured Streaming*. El primer worker disponible detecta el mensaje, inicia la descarga interna del archivo desde Cloudflare R2 usando el `key`, carga el modelo `best_model.pt` a memoria, y ejecuta la clasificación matricial con PyTorch asíncronamente.
6. **Publicación del Veredicto:** 
   El worker de Spark, una vez terminada la inferencia, envía un nuevo mensaje al topic `galaxy.results` detallando la probabilidad y la clase de galaxia.
7. **Notificación en Tiempo Real:** 
   La API de NestJS, que actúa simultáneamente como *Kafka Consumer*, intercepta el mensaje de `galaxy.results`. Si el cliente original sigue conectado mediante **Socket.IO** (WebSockets), el servidor le envía un evento emitido push (`classification:result`) instantáneo con el resultado. Además, almacena el estado como `SUCCESS` para consultas históricas pasivas vía `GET /api/classifications/:jobId`.

---

## 📋 Pre-Requisitos

Para levantar y ejecutar este proyecto localmente, asegúrate de tener el entorno preparado:

1. **[Docker Desktop](https://www.docker.com/products/docker-desktop/)** (O Docker Engine + Docker Compose instalado nativamente en tu Linux).
2. **[Git](https://git-scm.com/)** para clonar los repositorios.
3. **[Insomnia](https://insomnia.rest/)** o Postman si quieres hacer debugging local usando la colección de REST y WebSockets incluida en los archivos.
4. (Opcional pero recomendado) **Node.js 22+ y pnpm** si planeas contribuir modificando el código del framework NestJS fuera de Docker.

---

## 🔌 Servicios y Puertos Expuestos

Al orquestar el entorno en tu máquina local, la red interna expone las siguientes herramientas:

| Sistema | Puerto Local | ¿Para qué sirve? |
| :--- | :--- | :--- |
| **NestJS API** | `3001` | Endpoints REST web y el gateway WebSocket (Socket.IO). |
| **Spark Master UI** | `8080` | Dashboard visual de Spark. Muestra memoria, nucleos y los trabajadores alistados. |
| **Spark Worker UI** | `8081` | (El puerto es internamente dinámico pero expuesto). Perfil de cada peón. |
| **Kafka UI** | `8090` | Interfaz gráfica inmersiva (Proveedus) para inspeccionar *brokers*, particiones, y leer los JSONs viajando en vivo dentro de los *topics*. |

*(Los propios Spark Workers, el Contenedor de Driver de Streaming y la red inter-broker de Kafka corren confinados bajo la red bridge aislando toda colisión)*.

---

## 🚀 Instalación y Guía para Contribuidores (Open Source)

Reproducir este entorno consta de un ecosistema en dos partes, debido a que este backend requiere el modelo de IA físico para funcionar.

### 1. Clonar ambos repositorios estructuradamente
Los contenedores montan puentes de volumen (*Docker Volume Bind Mounts*) a la carpeta hermana del entorno. Es imperativo que clones **ambos repositorios en el mismo directorio principal** (lado a lado):

```bash
# Entrar a/crear tu directorio de proyectos personal
mkdir galaxy-morph-workspace && cd galaxy-morph-workspace

# 1. Clonar PRIMERO el proyecto de Machine Learning base (Vital)
git clone https://github.com/jeancdevx/galaxy-morph-ml

# 2. Clonar ESTE Backend
git clone https://github.com/jeancdevx/galaxy-morph-backend
```

### 2. Configurar el Modelo en el Repositorio Auxiliar
Asegúrate de leer el README de `galaxy-morph-ml`. Tienes dos opciones allá: entrar y ejecutar el entrenamiento desde 0, o simplemente descargar el `best_model.pt` final.
Asegúrate religiosamente que el archivo originado culmine en esta ruta cruzada:
`../galaxy-morph-ml/models/checkpoints/best_model.pt`

### 3. Variables de Entorno y Credenciales S3
Entra a la carpeta de este backend y crea tu configuración `.env` privada basada en la plantilla:

```bash
cd galaxy-morph-backend
cp .env.example .env
```

Abre tu nuevo `.env` y rellénalo con credenciales de **Cloudflare R2** (Es compatible con S3 de AWS):

| Variable | Dónde encontrarla | Descripción |
| :--- | :--- | :--- |
| `R2_ACCOUNT_ID` | [Cloudflare Dashboard](https://dash.cloudflare.com) > R2 | Tu ID de cuenta. Usualmente visible en el panel general derecho de la cabina de R2. |
| `R2_ACCESS_KEY_ID` | Cloudflare Dashboard > R2 > Manage R2 API Tokens | La llave pública alfanumérica que generas con permisos de *Read/Write*. |
| `R2_SECRET_ACCESS_KEY` | Cloudflare Dashboard > R2 > Manage R2 API Tokens | El secreto alfanumérico largo. (Cloudflare solo te lo muestra 1 vez al crear el token). |
| `R2_BUCKET_NAME` | Cloudflare Dashboard > R2 | El nombre exacto de la cubeta (bucket) que creaste para esto, ej: `galaxy-morph-data`. |
| `R2_ENDPOINT` | R2 Bucket Settings | La URL base de la cubeta. Tiene la forma: `https://<Tu_Account_ID>.r2.cloudflarestorage.com` |

### 4. Orquestar el Clúster
La infraestructura es 100% como Código (IaC).
No tienes que pre-configurar temas manualmente. El contenedor de automatización `kafka-init` escaneará la salud de los administradores y levantará los tópicos (`galaxy.ingestion`, `galaxy.results`) con factor 3 de particionamiento de forma silente antes del despegue de la red neuronal.

Lanza el sistema indicándole tu requerimiento de poder paralelizado (por ejemplo, escalar a 3 trabajadores):

```bash
# ¡Magia! Descarga de imágenes y encendido atómico.
docker compose up -d --scale spark-worker=3
```

Para corroborar que el Chofer (Driver) asincrónico logró empalmar las bibliotecas a los *workers*:
```bash
docker compose logs -f spark-driver
```

### 5. Consumir la API
El proyecto incluye todo lo necesario para simular flujos al instante.
Busca y arrastra el archivo **`galaxy-morph-api-insomnia.yaml`** hacia tu cliente REST local [(Descargar software Insomnia)](https://insomnia.rest/). Automáticamente te generará la colección con los Endpoints y Eventos (REST / WebSockets).

¡Usa tu entorno recién creado para clasificar la profunda morfología del universo! 🌌
