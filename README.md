# 🧩 Kafka Java Demo – Cluster Kafka con Docker e Java 17 (Temurin)

Questo progetto dimostra come utilizzare **Apache Kafka** con **Java 17 (Eclipse Temurin)** e un **cluster Kafka a 3 broker** eseguito tramite **Docker Compose**. 
Include:
- Un `producer` 
- Un `producerEOS`
- Un `consumer`
- Un generatore di messaggi sulla temperatura registrata da un termometro, che utilizza il producer EOS
- Un generatore di messaggi sul colore di un semaforo, che utilizza il producer EOS
- Un partitioner personalizzato, leggermente differente dal default
- Uno `schema registry` dove vengono salvati gli schemi Avro utilizzati
- Due schemi Avro, uno per una key e uno per un value
- Un `AircraftEventProducer` che consente l'invio di messaggi seguendo alcuni schemi Avro salvati
- Un generatore di messaggi sulla partenza o arrivo di aerei in degli aeroporti
- Un `AircraftEventConsumer`
- Due printer per i messaggi scaricati dai topic
---

## 📦 Componenti utilizzati

| Componente     | Versione / Info           |
|----------------|---------------------------|
| Java           | 17 (Eclipse Temurin)      |
| Kafka Client   | 3.7.1                     |
| Docker Compose | v3.8                      |
| Kafka (Broker) | Confluent Kafka 7.6.0     |
| Zookeeper      | Confluent Zookeeper 7.6.0 |
| SLF4J          | 2.0.9                     |
| Schema Registry| 7.6.0                     |
| Build System   | Maven                     |

---

## 🖥️ Prerequisiti

Assicurati di avere installati i seguenti strumenti sul tuo computer:


| Strumento                                                 | Versione consigliata | Descrizione                                     |
|-----------------------------------------------------------|----------------------|-------------------------------------------------|
| [Java JDK](https://adoptium.net/)                         | 17 (Temurin)         | Linguaggio e runtime Java                       |
| [Apache Maven](https://maven.apache.org/)                 | 3.8+                 | Build system per compilare il progetto          |
| [Docker](https://www.docker.com/products/docker-desktop/) | Ultima stabile       | Container engine per eseguire Kafka e Zookeeper |
| [Docker Compose](https://docs.docker.com/compose/)        | v2 o superiore       | Orchestrazione dei container                    |

#### 📁 Repository locale

Clona o scarica il progetto in una cartella sul tuo computer:

```bash
git clone https://github.com/...
```

---

#### ✅ Verifica che tutto sia installato

Esegui questi comandi per verificare l’ambiente:

```bash
java -version
mvn -v
docker --version
docker compose version
```

Tutti dovrebbero restituire informazioni valide senza errori.


## 📁 Struttura del progetto

```text
kafka-demo/
├── docker-compose.yaml         # Cluster Kafka (3 broker + Zookeeper)
├── pom.xml                     # Dipendenze Maven
├── README.md                   # Questo file
└── src/
    └── main/java/kafka/
        ├── ...
```



---

## 🚀 Gestione dei container che realizzano il cluster Kafka

> **Tutti i comandi vanno eseguiti dal terminale del tuo sistema operativo**, nella stessa cartella dove si trova il file `docker-compose.yaml`.
> Assicurati che i container Kafka siano in esecuzione (`docker compose up -d`).

### ▶️ Avviare i container con Docker Compose

Prima di tutto, assicurati che l'Engine Docker sia avviato.
In Docker Desktop, assicurarsi anche di stare utilizzando "Use the WSL 2 based engine".

Esegui il seguente comando dalla cartella del progetto per avviare i container e scaricare le immagini se necessario:

```bash
docker compose up -d
```
Questo avvierà:

- 🐘 **1 container Zookeeper**
- 📦 **3 container Kafka broker**, sulle seguenti porte:
    - `kafka1`: `localhost:9092`
    - `kafka2`: `localhost:9093`
    - `kafka3`: `localhost:9094`
 -   **1 Schema registry**

Per verificare che i container siano attivi:
```bash
docker ps
```

```bash
CONTAINER ID   IMAGE                                    COMMAND                  CREATED              STATUS              PORTS                                        NAMES
some_id_3     confluentinc/cp-kafka:7.6.0               "/etc/confluent/dock…"   About a minute ago   Up About a minute   9092/tcp, 0.0.0.0:9094->9094/tcp             kafka3
some_id_2     confluentinc/cp-kafka:7.6.0               "/etc/confluent/dock…"   About a minute ago   Up About a minute   9092/tcp, 0.0.0.0:9093->9093/tcp             kafka2
some_id_1     confluentinc/cp-kafka:7.6.0               "/etc/confluent/dock…"   About a minute ago   Up About a minute   0.0.0.0:9092->9092/tcp                       kafka1
some_id_0     confluentinc/cp-zookeeper:7.6.0           "/etc/confluent/dock…"   About a minute ago   Up About a minute   2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp   zookeeper
some_id_4     confluentinc/cp-schema-registry:7.6.0     "/etc/confluent/dock…"   About a minute ago   Up About a minute   0.0.0.0:8081->8081/tcp                       schema-registry
```

## 🧹 Pulizia dei container Docker

Quando non ti servono più i container Kafka e Zookeeper, puoi fermarli o eliminarli completamente.

### 🛑 Fermare i container (senza eliminarli)

Questo comando **spegne i container**, ma non li rimuove:

```bash
docker compose stop
```

### ▶️ Riavviare i container fermati

Per **riavviare i container fermati** (senza ricrearli):

```bash
docker compose start
```

Questo è utile se hai già avviato il cluster almeno una volta con `up` e vuoi solo riaccenderlo.


---

### ❌ Eliminare i container

Per **rimuovere i container**, mantenendo però le immagini e i volumi:

```bash
docker compose down
```

---

### 💣 Eliminare anche i volumi

Se vuoi **rimuovere i container e anche i dati** (inclusi log, topic, offset, ecc.):

```bash
docker compose down -v
```

⚠️ Attenzione: questo comando cancella **tutti i dati** salvati da Kafka (topic, messaggi, ecc.).

---


## 🧪 Comandi utili da terminale

Puoi usare questi comandi tramite Docker per gestire i topic, testare i messaggi e monitorare il cluster.

---

Entrare nel container:
```bash
docker exec -it kafka1 bash
```
📝 **Nota:** puoi usare `kafka2` o `kafka3` al posto di `kafka1`, dato che tutti i broker appartengono allo stesso cluster.

### 📋 Elencare tutti i topic

```bash
kafka-topics --list --bootstrap-server localhost:9092
```
➡️ Mostra l'elenco dei topic nel cluster.

---

### 🔍 Descrivere un topic

```bash
kafka-topics --describe --topic demo-topic --bootstrap-server localhost:9092
```
➡️ Mostra informazioni sul topic `demo-topic`, incluse partizioni e repliche.

---

### 📤 Inviare un messaggio manuale (console producer)

```bash
kafka-console-producer --topic demo-topic --bootstrap-server localhost:9092
```
➡️ Apre un prompt interattivo dove puoi scrivere messaggi che verranno inviati al topic.

---

### 📥 Leggere i messaggi da un topic (console consumer)

```bash
kafka-console-consumer --topic demo-topic --from-beginning --bootstrap-server localhost:9092
```

➡️ Legge tutti i messaggi presenti nel topic `demo-topic`, anche quelli già inviati.

---

### 🔧 Creare un topic

Da dentro la shell, crea un topic chiamato "demo-topic":
```bash
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic demo-topic
```

---

### 🚮 Eliminare un topic

```bash
kafka-topics --delete --topic demo-topic --bootstrap-server localhost:9092
```

➡️ Elimina il topic `demo-topic` dal cluster.

---

## 📘 Schema Registry

Questo progetto utilizza un Kafka Producer configurato con **Avro** e **Schema Registry**, quindi ogni schema (sia per la **key** che per il **value**) viene automaticamente registrato nel **Schema Registry** all’indirizzo specificato tramite la proprietà:
```bash
schema.registry.url = http://localhost:8081
```
#### 🔍 Come visualizzare gli schemi registrati

Puoi visualizzare gli schemi registrati nel registry in due modi:

- **Via browser**  
  Vai all'indirizzo:
  ```bash
  http://localhost:8081/subjects
  ```
- **Via terminale usando `curl`**
  Per vedere l'elenco dei subject registrati:
  ```bash
  curl http://localhost:8081/subjects
  ```
---

## 🧪 Utilizzare Producer e Consumer Avro da CLI (con Schema Registry)

Se vuoi **testare manualmente** l'invio e la lettura di messaggi Avro dal terminale, puoi usare i tool `kafka-avro-console-producer` e `kafka-avro-console-consumer`, già inclusi nell'immagine `confluentinc/cp-kafka`.

> 💡 I comandi seguenti vanno eseguiti **all'interno del container Docker** (es. `kafka1`), oppure adattati per PowerShell/WSL2.  
> ⚠️ Assicurati che lo Schema Registry sia attivo su `http://localhost:8081`.

---

### ▶️ Entrare nel container `kafka1`

```bash
docker exec -it kafka1 bash
```

### 📤 Avro Producer da console

Per inviare un messaggio Avro al topic `aircraft-topic`:
```bash
kafka-avro-console-producer \
  --topic aircraft-topic \
  --bootstrap-server kafka1:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property key.schema='{
     "type": "record",
     "name": "AircraftKey",
     "namespace": "com.example.avro",
     "fields": [
       {"name": "tailNumber", "type": "string"},
       {"name": "model", "type": "string"},
       {"name": "airline", "type": {
         "type": "enum",
         "name": "Airline",
         "symbols": ["RYANAIR", "DELTA", "LUFTHANSA", "EMIRATES", "AIRFRANCE", "EASYJET", "UNITED", "QATAR", "ALITALIA"]
       }}
     ]
   }' \
  --property value.schema='{
     "type": "record",
     "name": "AircraftEvent",
     "namespace": "com.example.avro",
     "fields": [
       {"name": "eventId", "type": "string"},
       {"name": "eventType", "type": {"type": "enum", "name": "AircraftEventType", "symbols": ["DEPARTURE", "ARRIVAL"]}},
       {"name": "airportCode", "type": "string"},
       {"name": "scheduledTime", "type": "long"},
       {"name": "actualTime", "type": "long"}
     ]
   }' \
  --property parse.key=true \
  --property key.separator=:

```
Una volta aperto il prompt, puoi inviare messaggi Avro come JSON su una sola riga (key e value separati da `:`):
```bash
{"tailNumber":"EI-ABC","model":"A320","airline":"RYANAIR"}:{"eventId":"EV002","eventType":"ARRIVAL","airportCode":"FCO","scheduledTime":1716400200000,"actualTime":1716400250000}


```

### 📤 Avro Consumer da console

Per leggere messaggi Avro dal topic `aircraft-topic`:
```bash
kafka-avro-console-consumer \
  --topic aircraft-topic \
  --bootstrap-server kafka1:9092 \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081 \
  --property print.key=true \
  --property key.separator=:

```
Questo consumer mostrerà i record Avro deserializzati.
