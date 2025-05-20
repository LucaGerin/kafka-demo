# 🧩 Kafka Java Demo – Cluster Kafka con Docker e Java 17 (Temurin)

Questo progetto dimostra come utilizzare **Apache Kafka** con **Java 17 (Eclipse Temurin)** e un **cluster Kafka a 3 broker** eseguito tramite **Docker Compose**. 
Include:
- Un `producer` 
- Un `producerEOS`
- un `consumer`

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

Per verificare che i container siano attivi:
```bash
docker ps
```

```bash
CONTAINER ID   IMAGE                             COMMAND                  CREATED              STATUS              PORTS                                        NAMES
some_id_3     confluentinc/cp-kafka:7.6.0       "/etc/confluent/dock…"   About a minute ago   Up About a minute   9092/tcp, 0.0.0.0:9094->9094/tcp             kafka3
some_id_2     confluentinc/cp-kafka:7.6.0       "/etc/confluent/dock…"   About a minute ago   Up About a minute   9092/tcp, 0.0.0.0:9093->9093/tcp             kafka2
some_id_1     confluentinc/cp-kafka:7.6.0       "/etc/confluent/dock…"   About a minute ago   Up About a minute   0.0.0.0:9092->9092/tcp                       kafka1
some_id_0     confluentinc/cp-zookeeper:7.6.0   "/etc/confluent/dock…"   About a minute ago   Up About a minute   2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp   zookeeper
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

> **Tutti i comandi vanno eseguiti dal terminale del tuo sistema operativo**, nella stessa cartella dove si trova il file `docker-compose.yaml`.
> Assicurati che i container Kafka siano in esecuzione (`docker compose up -d`).

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