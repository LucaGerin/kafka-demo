package org.example.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.util.TimestampFormatter;

import java.util.Properties;

public class KafkaProducerDemo implements MessageProducer<String, String>{

    private final String producerId;
    private final String topic;
    private final Producer<String, String> producer;

    // Codici ANSI per il colore
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_CYAN = "\u001B[36m";

    public KafkaProducerDemo(String topic, String bootstrapServers, String producerId) {
        this.topic = topic;
        this.producerId = producerId;

        /*
         * === Kafka Producer Configuration ===
         *
         * bootstrap.servers
         * - Descrizione: Elenco di host:porta dei broker Kafka usati per stabilire la connessione iniziale al cluster.
         *
         * key.serializer
         * Classe utilizzata per serializzare la chiave del messaggio.
         * Deve implementare l'interfaccia org.apache.kafka.common.serialization.Serializer.
         *
         * value.serializer
         * Classe utilizzata per serializzare il valore del messaggio.
         * Deve anch'essa implementare l'interfaccia Serializer.
         *
         * compression.type
         * Specifica come i dati devono essere compressi prima dell'invio.
         * Valori supportati: none, snappy, gzip, lz4, zstd.
         * Il valore di default è none.
         * La compressione viene applicata a batch di record.
         *
         * acks
         * Definisce quanti acknowledgment il producer richiede dal leader prima di considerare una richiesta completata.
         * Impatta sulla durabilità dei record.
         * Valori possibili:
         *     - acks=0 -> Il producer NON aspetta alcun acknowledgment dal broker.
         *              Massime prestazioni, ma nessuna garanzia di consegna.
         *     - acks=1 -> Il producer aspetta che SOLO il leader abbia scritto il messaggio
         *              nel log locale. Buon compromesso tra velocità e sicurezza. E' l'opzione di DEFAULT.
         *     - acks=all -> Il producer aspetta che TUTTE le repliche sincronizzate (in-sync replicas)
         *              abbiano confermato di aver ricevuto il messaggio.
         *              Massima garanzia di durabilità, ma più lento.
         */


        // ------- Configura le proprietà del Kafka Producer che sarà creato con new KafkaProducer<>(props) -------
        // NB: Si usa al posto del nome delle properties la classe ProducerConfig. Per esempio, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG equivale a "bootstrap.servers" ma è controllato a compile time.

        // Crea un oggetto Properties, che è una mappa chiave/valore usata da Kafka per configurare il producer.
        Properties props = new Properties();

        // Si imposta bootstrap.servers, che specifica l'indirizzo del broker (o, sarebbe meglio, lista di broker) Kafka a cui il producer deve connettersi, "bootstrap" perché serve solo per iniziare in quanto Kafka poi scopre gli altri broker automaticamente
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Dice al producer come serializzare la key (key.serializer) e il value (value.serializer) del messaggio
        // In questo caso si è scelto StringSerializer, che converte le chiavi da String a byte (byte[]) secondo UTF8.
        // Altri serializers disponibili: ByteArraySerializer, IntegerSerializer, LongSerializer
        // Kafka ha i suoi serializer pronti in org.apache.kafka.common.serialization
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Configura "compression.type", il tipo di compressione dei messaggi, in questo caso è superfluo perchè il default è già "none"
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");

        // Configura "acks", qui superfluo perchè il default è già 1
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        // Configurazioni di ottimizzazione:
        // "linger.ms": Tempo di attesa prima di inviare un batch, per raggruppare più record (default: 0). Si applica per partizione.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        // "batch.size": Dimensione massima di un batch in byte (default: 16384 = 16 KB). Si applica per partizione. NB: Successo o failure di un invio è stabilito per batch, non per singoli messaggi che contiene.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);
        // "buffer.memory": Memoria totale disponibile per buffering dei record (default: 33554432 = 32 MB). NB: E' il  totale dato dalle partizioni di un topic.
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);

        // Configurazione della policy di Retries (Importa solo se acks non è 0)
        // Configura "retries", il numero di tentativi di ritrasmissione in caso di errore transitorio (es. timeout, errori di rete)
        props.put(ProducerConfig.RETRIES_CONFIG, 5); // Default è MAX_INT
        // Configura "retry.backoff.ms", il tempo di attesa (in ms) tra un retry e l’altro retry.backoff.ms
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 200); // default: 100

        // Configura "max.in.flight.requests.per.connection", cioè quanti batch di messaggi il producer può inviare contemporaneamente (in parallelo) a una singola connessione verso un singolo broker, senza attendere risposta
        // Il valore di default è 3
        // Impostare a 1 questa proprietà significa non inviare un nuovo batch di messaggi finché il precedente non ha ricevuto risposta dal broker, quindi mantenere l'ordine assoluto dei messaggi anche se avviene un retry
        // ALTERNATIVA (migliore) PER MANTENERE L'ORDINE: usare idempotent producers
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 3);

        // Configura "request.timeout.ms", il tempo massimo che il producer aspetta una risposta dal broker prima di ritentare (default: 30000 ms) (Errore se sono finiti i tentativi stabiliti con "retries")
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000"); // 20 secondi

        // Configura "max.block.ms", il tempo massimo che il metodo send() può restare bloccato (default: 60000 ms), dopodiché TimeoutException
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000"); // 30 secondi

        // Configura "delivery.timeout.ms", il tempo massimo totale per la consegna del messaggio (batching + retries + in-flight)
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000"); // 60 secondi

        /*
         * === PARTITIONING ===
         * Quando un Kafka producer invia un messaggio a un topic, Kafka deve decidere *a quale partizione* di quel topic
         * inviarlo. Questo processo è chiamato "partitioning".
         *
         * Il comportamento di default del partitioning:
         * 1. Con CHIAVE specificata:
         *    - Kafka usa la hash della chiave per determinare la partizione:
         *          partition = hash(key) % numero_partizioni
         *    - Questo garantisce che tutti i messaggi con la stessa chiave finiscano sempre nella stessa partizione,
         *      mantenendo così l'ORDINE di quei messaggi.
         * 2. Senza CHIAVE (key = null):
         *    - Kafka assegna le partizioni in modo round-robin tra le partizioni disponibili per quel topic.
         *    - L'assegnazione round-robin è mantenuta per produttore (producer instance).
         *    - In questo caso, Kafka non garantisce alcun ordine dei messaggi.
         *
         * È possibile personalizzare la logica di partizionamento implementando un proprio `Partitioner` e specificandolo nella configurazione del producer.
         *
         * Configurazione opzionale per personalizzare il partizionamento:
         *   props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.mio.PartiionerPersonalizzato");
         */
        // "partitioner.class" specifica un partitioner personalizzato da usare
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.example.partitioner.CustomPartitioner");


        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public String getProducerId() {
        return producerId;
    }

    @Override
    public void sendMessage(String key, String value) {

        // Crea il record da spedire passando topic, key, value
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        /*  Costruttore con più parametri:
         *      ProducerRecord<String, String> record = new ProducerRecord<>(topic, partition, timestamp, key, value);
         *  NB: la partition specificata qui ha precedenza maggiore rispetto a quella calcolata dal partitioner in utilizzo (qui posso anche metterla null)
         */

        /*
         * Invio di records da parte di un Kafka producer, può essere::
         *
         * 1. Invio ASINCRONO (default):
         *    - Il metodo Producer<K, V>.send() restituisce immediatamente un oggetto Future<RecordMetadata>.
         *          Future<RecordMetadata> send(ProducerRecord<K, V> var1);
         *          Future<RecordMetadata> send(ProducerRecord<K, V> var1, Callback var2);
         *    - Il messaggio viene inviato "in background", e l'applicazione continua a eseguire senza aspettare la conferma.
         *      In particolare, il messaggio viene messo in un buffer locale e un altro thread si occupa dell'invio di questi messaggi (anche in batch). L'invio si può forzare con il metodo:
         *          Producer<K, V>.flush();
         *    - Preferisci l'invio asincrono per alte prestazioni e throughput.
         *    - È possibile registrare una callback per gestire successi o errori:
         *
         *    producer.send(record, (metadata, exception) -> {
         *        if (exception == null) {
         *            System.out.println("Messaggio inviato con successo: " + metadata);
         *        } else {
         *            exception.printStackTrace();
         *        }
         *    });
         *
         * 2. Invio SINCRONO:
         *    - Si forza l'attesa del completamento dell'invio tramite `.get()` sul Future.
         *    - L'esecuzione si blocca finché il broker non risponde (o genera un errore).
         *    - È utile quando è necessario garantire l'invio prima di procedere.
         *    - Usa l'invio sincrono quando hai bisogno di garanzie forti sull'invio (es. logging critico o transazioni).
         *
         *    try {
         *        RecordMetadata metadata = producer.send(record).get(); // invio sincrono
         *        System.out.println("Messaggio inviato: " + metadata);
         *    } catch (Exception e) {
         *        e.printStackTrace();
         *    }
         *
         */

        // Invia in modo asincrono il record e gestisce i metadata che sono ritornati o le exception
        /*
         * send(record, Callback)
         * Callback è un'interfaccia con un metodo che si chiama onCompletion che è invocato quando il send() è stato completato
         * Callback ha come parametri metadata e exception: uno solo dei due non sarà "null" dopo l'invocazione di Callback
         *      onCompletion(RecordMetadata metadata, java.lang.Exception exception)
         */
        producer.send(record, (metadata, exception) -> {

            if (exception == null) {
                String log = String.format(
                        ANSI_CYAN + "[Producer %s]" + ANSI_RESET + ": ✅ Messaggio con key=\"%s\", value=\"%s\", timestamp=%s \n\t\t\t\tinviato al topic \"%s\", partizione %d, offset=%d",
                        producerId,
                        record.key(),
                        record.value(),
                        TimestampFormatter.format(metadata.timestamp()), //NB: record.timestamp() è null, viene riempito dopo l'invio
                        metadata.topic(),
                        metadata.partition(),
                        metadata.offset()
                );
                System.out.println(log);

            } else {
                exception.printStackTrace();
            }
        });
    }

    @Override
    public void close() {
        System.out.println(ANSI_CYAN +"[Producer " + producerId + "]" + ANSI_RESET + ": Chiudo il producer.");
        /* chiudere il Producer con uno tra:
         *    - producer.close() -> aspetta il completamento delle richieste
         *    - producer.close(timeout, timeUnit) -> aspetta il completamento delle richieste o il timeout finisca
         */
        producer.close();
    }

}
