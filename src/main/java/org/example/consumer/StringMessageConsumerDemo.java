package org.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.example.util.TimestampFormatter;

import java.time.Duration;
import java.util.*;

public class StringMessageConsumerDemo implements MessageConsumer<String, String> {

    private final String consumerId;
    private final KafkaConsumer<String, String> consumer;
    private final String topic;

    // Codici ANSI per il colore
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";

    /**
     * Costruttore
     * @param topic
     * @param bootstrapServers
     * @param groupId
     * @param consumerId
     */
    public StringMessageConsumerDemo(String topic, String bootstrapServers, String groupId, String consumerId) {
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
        // NB: L'offset committato è l'offset del PROSSIMO record da leggere, non dell'ultimo letto!
        // L'offset committato è quello da cui un consumer che subentra a uno che sostituisce inizia a laggere
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        /*
         * Quando enable.auto.commit è impostato su false, il Kafka consumer NON committerà automaticamente gli offset.
         * Questo significa che sei responsabile di effettuare manualmente il commit dell'offset, decidendo *quando* farlo.
         * Ci sono due modalità principali di commit manuale:
         * 1. At-most-once:
         *    - Si committa PRIMA di elaborare il messaggio.
         *    - Vantaggio: nessun messaggio verrà mai elaborato due volte.
         *    - Svantaggio: se il consumer crasha dopo il commit ma prima dell'elaborazione, il messaggio viene perso.
         *    Esempio:
         *      record = consumer.poll();
         *      consumer.commitSync();
         *      process(record); // potrebbe non essere eseguito se il processo crasha dopo il commit
         *
         * 2. At-least-once:
         *    - Si committa DOPO l'elaborazione del messaggio.
         *    - Vantaggio: ogni messaggio sarà elaborato almeno una volta.
         *    - Svantaggio: in caso di crash subito dopo l'elaborazione ma prima del commit, il messaggio sarà rielaborato.
         *    Esempio:
         *      record = consumer.poll();
         *      process(record);
         *      consumer.commitSync(); // commit bloccante e più sicuro (gestisce anche eventuali errori di rete)
         *
         * In alternativa a commitSync(), si può usare commitAsync(), che è non bloccante e generalmente più veloce, ma non garantisce il retry automatico e può fallire silenziosamente.
         * È utile per scenari ad alta performance dove una minima perdita di offset è accettabile.
         *    Esempio:
         *      record = consumer.poll();
         *      process(record);
         *      consumer.commitAsync(); // attenzione: errori non gestiti automaticamente
         *
         *    NB: puoi anche combinare commitAsync() con una callback per gestire eventuali errori:
         *      consumer.commitAsync((offsets, exception) -> {
         *          if (exception != null) {
         *              // log o retry manuale
         *              System.err.println("Commit fallito: " + exception.getMessage());
         *          }
         *      });
         *
         *    NB: questi metodi hanno anche signature in cui si può specificare l'offset fino al quale fare commit
         */


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

        /*
         * ================================
         * Kafka Group Coordinator
         * ================================
         * Il Group Coordinator è un componente del broker Kafka che gestisce i consumer appartenenti a uno stesso consumer group.
         * Le sue responsabilità includono:
         * - assegnare partizioni ai consumer (rebalance), quando un cosumer lascia un gruppo, joina u gruppo, cambia subscription ai topic, o se cambia il numero di partizioni.
         * - monitorare lo stato dei consumer tramite heartbeat,
         * - considerare un consumer "morto" se non comunica entro una certa finestra temporale,
         * - scatenare il ribilanciamento (rebalance) quando cambia la composizione del gruppo, chiedendo al group leader di eseguirlo.
         *
         * I parametri session.timeout.ms, heartbeat.interval.ms, session.timeout.ms controllano il comportamento di questa comunicazione.
         */
        // "session.timeout.ms" configura il timeout della sessione consumer, cioè il tempo massimo che un consumer può restare "silente" (Default: 10.000 ms (10 secondi))
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");  // Kafka considererà il consumer morto se non riceve heartbeat entro 15 secondi

        // "heartbeat.interval.ms"configura l'intervallo tra heartbeat, cioè quanto spesso il consumer invia segnali heartbeat al coordinator (Default: 3.000 ms)
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");

        // "session.timeout.ms" è il timeout massimo tra due chiamate consecutive a poll() (Default: 300.000 ms (5 minuti))
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");  // Se il consumer impiega più di questo tempo a chiamare poll(), viene considerato "lento" e rimosso dal gruppo


        this.consumer = new KafkaConsumer<>(props);

        // Si iscrive a una lista di topic (in questo caso uno solo), e attacca anche un listener
        // NB: subscribe(Collection<String> topics) sostituisce e sovrascrive la lista di topic precedente
        ConsumerRebalanceListener listener = createRebalanceListener();
        this.consumer.subscribe(Collections.singletonList(topic), listener);

        System.out.println(ANSI_GREEN + "[Consumer " + consumerId + "]" + ANSI_RESET + ": Consumer avviato. In attesa di messaggi...");
    }

    /**
     * Metodo per creare un rebalance listener che stampa su System out le partizioni e i loro offset di partenza
     */
    private ConsumerRebalanceListener createRebalanceListener() {
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
        return new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // Azioni da eseguire PRIMA che le partizioni vengano rimosse dal consumer
                System.out.printf(ANSI_GREEN + "[Consumer %s]" + ANSI_RESET + ":⚠️  Partizioni revocate: %s\n", consumerId, partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) { //NB: le partizioni sono assegnate quando un poll() scarica i metadata! Quindi questo metodo parte dopo il primo poll() dopo una modifica alle partizioni.

                // Azioni da eseguire DOPO che le partizioni sono state assegnate
                System.out.printf(ANSI_GREEN + "[Consumer %s]" + ANSI_RESET + ": Partizioni assegnate: %s\n", consumerId, partitions);


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
                        System.out.println(ANSI_GREEN + "[Consumer " + consumerId + "]" + ANSI_RESET + ": Partition " + partition.partition() + " - Starting offset: " + offset);
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
    }

    @Override
    public String getConsumerId() {
        return this.consumerId;
    }

    @Override
    public List<ConsumerRecord<String, String>> pollMessages() {

        List<ConsumerRecord<String, String>> results = new ArrayList<>();

        try {
            // Effettua il polling: attende fino a 1 secondo per ricevere nuovi messaggi
            // Il consumer "interroga" Kafka per nuovi messaggi:
            // - Se ce ne sono, li riceve
            // - Se non ce ne sono, riceve una risposta vuota
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                results.add(record);
            }

        } catch (WakeupException | InterruptException e) {
                // Expected if shutdown is triggered via interrupt()
                Thread.currentThread().interrupt(); // ripristina il flag di interrupt
        }

        return results;
    }

    @Override
    public void close() {
        // Chiude il consumer
        this.consumer.close();
        System.out.println(ANSI_GREEN + "[Consumer " + this.consumerId + "]" + ANSI_RESET + ": Consumer chiuso.");
    }


    /**
     * Esegue il seek del consumer a un offset calcolato in base a un timestamp fornito:
     * 1. Costruisce una mappa con tutte le partizioni assegnate e il timestamp desiderato.
     * 2. Chiede a Kafka di restituire, per ogni partizione, l'offset corrispondente a quel timestamp.
     * 3. Esegue un seek esplicito su ciascuna partizione a quell'offset.
     * Utile per iniziare a leggere da un certo punto nel tempo (es. "da ieri alle 12:00").
     * NB: in kafka l'ordine è dato dall'arrivo, i messaggi non sono ordinati per timestamp!
     * Quindi potrei tornare a un certo timestamp ma leggere successivamente messaggi con timestamp precedente
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
