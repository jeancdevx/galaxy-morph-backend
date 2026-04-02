# 🌌 Galaxy Morph Backend

Un sistema distribuido en tiempo real, orientado a eventos (Event-Driven), diseñado para la clasificación de morfología de galaxias utilizando Deep Learning (ResNet50).

Acoplado a una arquitectura de nivel empresarial, este backend separa completamente la ingesta web del procesamiento pesado de Machine Learning utilizando **Apache Kafka** y **Apache Spark**. ¡Cero bloqueos, máxima escalabilidad!

---

## 🏗️ Arquitectura y Flujo de Datos

Este proyecto **no procesa** las imágenes en el hilo principal de la API. Todo fluye de manera asíncrona:

1. **Upload Desacoplado (S3/R2):** La API de NestJS firma criptográficamente un "Presigned URL" para que el cliente web (frontend) suba la foto directamente a Cloudflare R2, ahorrando ancho de banda del servidor.
2. **Ingesta Orientada a Eventos:** Una vez subida la imagen, la API dispara un evento (Job) hacia el topic `galaxy.ingestion` de **Apache Kafka**. La API responde al usuario en 10 milisegundos.
3. **Cómputo Distribuido (Spark + PyTorch):** Un clúster estandarizado de **Apache Spark** (Master + Workers) monitorea Kafka ininterrumpidamente. Un worker libre descarga la imagen de R2 y ejecuta la inferencia con la red neuronal (ResNet50).
4. **Respuesta Asíncrona:** Spark publica el resultado de las probabilidades de clasificación en el topic `galaxy.results`.
5. **Tiempo Real (WebSockets):** La API de NestJS, actuando como Consumer de Kafka, recibe el veredicto y mediante **Socket.IO** lanza una notificación push en vivo al cliente web conectado.

---

## 🔌 Servicios y Puertos Expuestos

Al levantar el entorno con Docker Compose, tendrás acceso a los siguientes puertos vitales en tu máquina local:

| Servicio | Puerto | Descripción |
| :--- | :--- | :--- |
| **NestJS API** | `3001` | Endpoints REST de la aplicación y Servidor Socket.IO |
| **Spark Master UI** | `8080` | Panel de control de Apache Spark para ver estado y recursos de los workers |
| **Kafka UI** | `8090` | Interfaz gráfica inmersiva para vigilar los *brokers*, *topics* y *messages* |

*(Los Spark Workers, el Contenedor Driver y los Brokers de Kafka corren en puertos internos de la red de Docker aislando las colisiones).*

---

## 🚀 Guía Rápida para Contribuidores (Open Source)

Para replicar este proyecto, es imperativo entender que depende directamente del modelo de Machine Learning (`.pt`) alojado en un repositorio especial hermano. Sigue estos pasos exactos para simular el entorno distribuido localmente.

### 1. Clonar ambos repositorios unidos
Para que la magia de los *"Docker Volume Bind Mounts"* ocurra y Spark pueda importar el modelo de IA hacia sí mismo (ahorrándote re-compilaciones), **ambos proyectos deben vivir en la misma carpeta padre**:

```bash
# Crear tu directorio maestro de galaxias
mkdir galaxy-morph-workspace && cd galaxy-morph-workspace

# Clonar primero el proyecto de Machine Learning (¡El modelo pre-entrenado!)
git clone https://github.com/jeancdevx/galaxy-morph-ml

# Clonar este Backend
git clone https://github.com/jeancdevx/galaxy-morph-backend
```

### 2. Preparar el Modelo Parametrizado (ML)
Asegúrate de seguir las instrucciones del repositorio [galaxy-morph-ml](https://github.com/jeancdevx/galaxy-morph-ml) para descargar o entrenar el modelo de PyTorch. 
El archivo físico `.pt` originado en ese proyecto, **debe existir materialmente** en esta ruta relativa exacta:
`../galaxy-morph-ml/models/checkpoints/best_model.pt`

### 3. Variables de Entorno (Credenciales)
Entra a la carpeta de este backend y crea tu archivo `.env` base:

```bash
cd galaxy-morph-backend
cp .env.example .env
```
*(Asegúrate de rellenar tus credenciales de Cloudflare R2 u Object Storage S3 Compatible directamente en el nuevo archivo `.env`).*

### 4. Orquestar y Levantar el Clúster
La infraestructura de este repositorio está programada 100% como Código (IaC).
No tendrás que crear repositorios asíncronos a mano; un contenedor automatizado (`kafka-init`) inyectará y particionará *(partitions=3)* los topics esenciales antes de que Spark siquiera despierte para evitar colisiones.

Levanta todo el sistema escalando de manera inteligente poder computacional a **3 trabajadores paralelos**:

```bash
docker compose up -d --scale spark-worker=3
```

Podrás ver el estado del conductor principal asíncrono con:
```bash
docker compose logs -f spark-driver
```

### 5. Probar Tu IA
¡Tu redacción espacial está lista! Puedes usar el archivo `Insomnia_2026-03-31.yaml` incluido en la raíz de este repositorio para importar los endpoints directo a Insomnia / Postman y ver el clúster descargar desde Cloudflare y pensar en paralelo en menos de 5 segundos.
