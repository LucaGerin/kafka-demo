package org.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.example.util.TimestampFormatter;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class KafkaConsumerDemo implements Runnable {

    private final String consumerId;
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private volatile boolean keepConsuming = true;

    // Codici ANSI per il colore
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";

    public KafkaConsumerDemo(String topic, String bootstrapServers, String groupId, String consumerId) {
        this.topic = topic;
        this.consumerId = consumerId;

        /*
         * Configurazioni principali di un Kafka Consumer in Java:
         *
         * bootstrap.servers
         * Elenco di coppie host/porta dei broker utilizzate per stabilire la connessione iniziale al cluster Kafka.
         *
         * key.deserializer
         * Classe usata per deserializzare la chiave dei messaggi. Deve implementare l'interfaccia Deserializer.
         *
         * value.deserializer
         * Classe usata per deserializzare il valore dei messaggi. Deve implementare l'interfaccia Deserializer.
         *
         * group.id
         * Stringa univoca che identifica il Consumer Group a cui appartiene il consumer.
         *
         * enable.auto.commit
         * Se impostata su true (valore predefinito), il consumer effettuerà automaticamente il commit degli offset.
         * Questo comportamento dipende anche dal valore di:
         *
         * auto.commit.interval.ms
         * intervallo di commit automatico, valore predefinito è 5000 ms
         */

        // ------- Configura le proprietà del Kafka Consumer -------
        //Crea un oggetto Properties, che è una mappa chiave/valore usata da Kafka per configurare il consumer.
        Properties props = new Properties();

        // Specifica l'indirizzo del broker (o, meglio, lista di broker) Kafka a cui il producer deve connettersi, "bootstrap" perché serve solo per iniziare in quanto Kafka poi scopre gli altri broker automaticamente
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Dice al consumer come convertire la key ("key.deserializer") e il value ("value.deserializer") dei messaggi da byte[] a oggetti Java
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Imposta "group.id": l'ID del gruppo consumer
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Imposta "enable.auto.commit" su true per il commit automatico dell'offset (il default è comunque true)
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        // Specifica cosa fare se il gruppo consumer non ha un valido offset salvato in precedenza:
        // - "earliest": consuma dall'inizio del topic
        // - "latest": consuma solo i nuovi messaggi (default)
        // - "none": solleva un errore se non c'è nessun offset salvato
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Imposta "max.partition.fetch.bytes", il numero massimo di byte che il consumer può recuperare da una singola partizione in una singola fetch.
        // Valore di default: 1 MB (1048576 byte)
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "1048576");

        // Imposta "max.poll.records", il numero massimo di record totali (contando tutte le partizioni) che il consumer può restituire in una singola chiamata a poll().
        // Valore di default: 500
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        // Imposta "fetch.max.wait.ms", il tempo massimo (in millisecondi) che il consumer aspetta per ricevere dati dal broker
        // anche se la quantità minima di byte richiesta (fetch.min.bytes) non è ancora disponibile.
        // Default: 500 ms
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");

        // Imposta "fetch.min.bytes", il numero minimo di byte che il broker deve avere pronti prima di rispondere a una richiesta fetch() chiamata a basso livello.
        // NB: non c'è corrispondenza 1:1 tra poll(9 e fetch() chiamato sul broker, per motivi di ottimizzazione poll() prende dati che potrebbero essere già stati scaricati dal broker con uno o più fetch().
        // Se non ci sono abbastanza dati, il broker può attendere fino a fetch.max.wait.ms prima di rispondere.
        // Default: 1 byte
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");

        /* Performance optimization:
         * Alto throughput (Più dati in un batch): alto fetch.min.bytes e ragionevole fetch.max.wait.ms
         * Bassa latency (Scarica dati il più veloce possibile): fetch.min.bytes=1 (default)
         */

        // Configura "isolation.level", che controlla la visibilità dei messaggi in presenza di producer transazionali (Exactly-Once Semantics - EOS).
        // Serve per decidere se il consumer deve leggere anche i messaggi prodotti da transazioni NON ancora committate o solo quelli sicuri.
        // Valori possibili:
        //      - "read_committed" (consigliato per EOS):
        //          Il consumer legge SOLO i messaggi che provengono da transazioni che sono state correttamente "committate", mentre i messaggi da transazioni in corso o abortite vengono IGNORATI.
        //      - "read_uncommitted" (default):
        //          Il consumer legge TUTTI i messaggi, inclusi quelli prodotti da transazioni NON ancora concluse (committed o aborted).
        //          Potrebbero essere letti messaggi che poi saranno annullati → rischio di inconsistenza.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {

        // Listener per gestire il rebalance
        /*
         * === ConsumerRebalanceListener ===
         * Kafka può riassegnare dinamicamente le partizioni ai consumer quando:
         * - un nuovo consumer entra o esce dal gruppo,
         * - il topic cambia numero di partizioni,
         * - il consumer viene riavviato.
         *
         * Questo listener permette di eseguire azioni personalizzate durante due fasi del rebalance:
         * 1. onPartitionsRevoked(...)
         *    - Chiamato PRIMA che le partizioni vengano revocate dal consumer. È il punto ideale per salvare lo stato, committare gli offset manualmente, ecc.
         * 2. onPartitionsAssigned(...)
         *    - Chiamato DOPO che le nuove partizioni sono state assegnate.
         *    - Qui si possono, ad esempio, eseguire operazioni di seek (es: seekToBeginning), logging, ecc.
         */
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Nessuna azione necessaria qui
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) { //NB: le partizioni sono assegnate quando un poll() scarica i metadata! Quindi questo metodo parte dopo il primo poll() dopo una modifica alle partizioni.

                /*
                 * ======================
                 * Kafka Consumer Offsets
                 * ======================
                 * --- GET OFFSETS ---
                 * 1. position(TopicPartition)
                 *    - Ritorna l'offset del prossimo record che verrà letto dalla partizione specificata.
                 * 2. offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch)
                 *    - Ritorna, per ogni partizione specificata, l'offset corrispondente a un certo timestamp.
                 *    - Utile per iniziare a leggere i messaggi da un certo momento (es: da ieri alle 10:00).
                 * --- CHANGE OFFSETS ---
                 * 3. seekToBeginning(Collection<TopicPartition>)
                 *    - Fa il seek all'offset iniziale di ciascuna partizione specificata.
                 * 4. seekToEnd(Collection<TopicPartition>)
                 *    - Fa il seek all'ultimo offset disponibile (dopo l’ultimo messaggio).
                 *    - Utile per leggere solo i nuovi messaggi ignorando quelli vecchi.
                 * 5. seek(TopicPartition, offset)
                 *    - Fa il seek a un offset specifico in una determinata partizione.
                 *    - Utile per riprendere la lettura da un punto preciso.
                 *
                 * --- ESEMPIO  ---
                 * TopicPartition tp = new TopicPartition("my-topic", 0);
                 * consumer.assign(Arrays.asList(tp));
                 * consumer.seekToBeginning(Arrays.asList(tp)); // Vai all'inizio
                 * consumer.seek(tp, 123);                      // Vai a un offset specifico
                 * long currentOffset = consumer.position(tp);  // Controlla l'offset attuale
                 */
                // Stampa l'offset iniziale da cui il consumer inizierà a leggere per ogni partizione
                for (TopicPartition partition : partitions) {
                    try {
                        long offset = consumer.position(partition); // Offset da cui riprenderà la lettura
                        System.out.println(ANSI_GREEN + "[Consumer " + consumerId + "]" + ANSI_RESET +
                                " Partition " + partition.partition() + " - Starting offset: " + offset);
                    } catch (Exception e) {
                        System.err.println("[Consumer " + consumerId + "]: Errore nel recupero offset: " + e.getMessage());
                    }
                }
                /*
                 * NB: consumer.position(...) richiede che ci sia già stato un poll() che ha attivato il rebalance.
                 * Quindi, se non chiamato dentro onPartitionsAssigned(...), deve essere preceduto da codice come:
                 * while (consumer.assignment().isEmpty()) {
                 *     consumer.poll(Duration.ofMillis(100));
                 * }
                 */

                /*
                // Resetta l'offset di ogni partizione all'inizio, a 0, quando ti è assegnata
                System.out.printf(ANSI_GREEN + "[Consumer " + consumerId + "]" + ANSI_RESET + ": vado al primo offset\n");
                consumer.seekToBeginning(partitions);
                 */

            }
        };

        try {
            // Si iscrive a una lista di topic (in questo caso uno solo)
            // NB: subscribe(Collection<String> topics) sostituisce e sovrascrive la lista di topic precedente
            consumer.subscribe(Collections.singletonList(topic), listener);
            System.out.println(ANSI_GREEN + "[Consumer " + consumerId + "]" + ANSI_RESET + ": Consumer avviato. In attesa di messaggi...");







            // Ciclo principale: continua a leggere finché il thread non viene interrotto
            while (keepConsuming && !Thread.currentThread().isInterrupted()) {
                // Effettua il polling: attende fino a 1 secondo per ricevere nuovi messaggi
                // Il consumer "interroga" Kafka per nuovi messaggi:
                // - Se ce ne sono, li riceve
                // - Se non ce ne sono, riceve una risposta vuota
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));



                // Elabora ogni messaggio ricevuto
                for (ConsumerRecord<String, String> record : records) {

                    // Stampa il messaggio
                    System.out.printf(ANSI_GREEN +"[Consumer " + consumerId + "]" + ANSI_RESET + ": ⬇️ Ricevuto: topic=%s, partition=%d, offset=%d, key=%s, timestamp=%s,\n\t\t\t\tvalue=\"%s\" \n",
                            record.topic(), record.partition(), record.offset(), record.key(), TimestampFormatter.format(record.timestamp()), record.value());
                }
            }
        } catch (WakeupException e) {
            System.out.println(ANSI_GREEN +"[Consumer " + consumerId + "]" + ANSI_RESET + ": ⚠️ Consumer svegliato per chiusura.");
        } catch (InterruptException e) {
            // Il thread è stato interrotto (es. da consumerThread.interrupt())
            System.out.println("----------------");
            System.out.println(ANSI_GREEN +"[Consumer " + consumerId + "]" + ANSI_RESET + ": ℹ️ Consumer interrotto.");
        } catch (Exception e) {
            System.err.println("[Consumer " + consumerId + "]: ❌ Errore nel consumer: " + e.getMessage());
        } finally {
            consumer.close();
            System.out.println(ANSI_GREEN +"[Consumer " + consumerId + "]" + ANSI_RESET + ": 🔚 Consumer chiuso.");
        }
    }

    public void shutdown() {
        keepConsuming = false;
        consumer.wakeup();
    }


    /**
     * Esegue il seek del consumer a un offset calcolato in base a un timestamp fornito:
     * 1. Costruisce una mappa con tutte le partizioni assegnate e il timestamp desiderato.
     * 2. Chiede a Kafka di restituire, per ogni partizione, l'offset corrispondente a quel timestamp.
     * 3. Esegue un seek esplicito su ciascuna partizione a quell'offset.
     *
     * Utile per iniziare a leggere da un certo punto nel tempo (es. "da ieri alle 12:00").
     *
     * @param timestamp il valore del timestamp (in millisecondi) da cui iniziare a leggere.
     */
    private void seekToTimestamp(long timestamp) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();

        // ① Aggiunge ogni partizione assegnata con il timestamp desiderato
        for (TopicPartition partition : consumer.assignment()) {
            timestampsToSearch.put(partition, timestamp);
        }

        // ② Ottiene gli offset corrispondenti ai timestamp richiesti
        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(timestampsToSearch);

        // ③ Esegue seek a ciascun offset restituito
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : result.entrySet()) {
            OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
            if (offsetAndTimestamp != null) {
                consumer.seek(entry.getKey(), offsetAndTimestamp.offset());
                System.out.println(ANSI_GREEN + "[Consumer " + consumerId + "]" + ANSI_RESET +
                        " Seek su partition " + entry.getKey().partition() +
                        " al timestamp " + timestamp +
                        " → offset=" + offsetAndTimestamp.offset());
            } else {
                System.out.println("[Consumer " + consumerId + "]: ⚠️ Nessun offset trovato per il timestamp in partition " + entry.getKey().partition());
            }
        }
    }



}
